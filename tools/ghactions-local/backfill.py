#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Backfill a local GitHub Actions usage database for selected ASF projects."""

import argparse
import asyncio
import datetime as dt
import email.utils
import json
import os
import re
import sqlite3
import sys
import time
import urllib.parse

import aiohttp

CREATE_RUNS_DB = """CREATE TABLE "runs" (
    "id"	INTEGER NOT NULL UNIQUE,
    "project"	TEXT NOT NULL,
    "repo"	TEXT NOT NULL,
    "workflow_id" INTEGER NOT NULL,
    "workflow_name"	TEXT,
    "workflow_path"	TEXT,
    "seconds_used"	INTEGER NOT NULL,
    "run_start"	INTEGER NOT NULL,
    "run_finish"	INTEGER NOT NULL,
    "jobs"	TEXT,
    PRIMARY KEY("id" AUTOINCREMENT)
);"""

DEFAULT_PROJECTS = ("cassandra", "polaris", "iceberg", "airflow", "spark", "hadoop", "hive", "parquet")
DEFAULT_DB = "var/ghactions.db"
DEFAULT_CONCURRENCY = 1
DEFAULT_MAX_RETRIES = 4
GITHUB_API = "https://api.github.com"
MAX_GITHUB_LIST_RESULTS = 1000
RATE_LIMIT_RESET_MARGIN_SECONDS = 1
SECONDARY_RATE_LIMIT_RETRY_SECONDS = 60
TRANSIENT_RETRY_SECONDS = 1


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7, help="Number of days to backfill, default: 7")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"SQLite DB path, default: {DEFAULT_DB}")
    parser.add_argument(
        "--project",
        action="append",
        dest="projects",
        help="ASF project/repo to backfill. Can be repeated. Defaults to cassandra, mina, polaris, iceberg.",
    )
    parser.add_argument("--org", default="apache", help="GitHub organization, default: apache")
    parser.add_argument("--append", action="store_true", help="Append to an existing DB instead of recreating it")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Concurrent GitHub requests, default: {DEFAULT_CONCURRENCY}",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Retries for transient GitHub API failures, default: {DEFAULT_MAX_RETRIES}",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN"),
        help="GitHub token. Defaults to GH_TOKEN or GITHUB_TOKEN.",
    )
    return parser.parse_args()


class GitHubClient:
    def __init__(
        self,
        session,
        token,
        concurrency,
        max_retries=DEFAULT_MAX_RETRIES,
        sleep=asyncio.sleep,
        monotonic=time.monotonic,
        wall_clock=time.time,
    ):
        self.session = session
        self.token = token
        self.limiter = asyncio.Semaphore(concurrency)
        self.max_retries = max_retries
        self.sleep = sleep
        self.monotonic = monotonic
        self.wall_clock = wall_clock
        self.cooldown_until = 0
        self.cooldown_lock = asyncio.Lock()

    async def get_json(self, url):
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        for attempt in range(self.max_retries + 1):
            await self.wait_for_cooldown()
            try:
                async with self.limiter:
                    async with self.session.get(url, headers=headers) as resp:
                        self.print_rate_limit_headers(resp.status, resp.headers)
                        if resp.status < 400:
                            await self.defer_if_rate_limited(resp.headers)
                            return await resp.json(), parse_next_link(resp.headers.get("Link"))
                        body = await resp.text()
                        retry = self.retry_delay(resp.status, resp.headers, body, attempt)
            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                retry = (TRANSIENT_RETRY_SECONDS * (2**attempt), f"transient error: {error}")
                body = str(error)
                status = None
            else:
                status = resp.status

            if retry is None or attempt == self.max_retries:
                status_text = f"HTTP/{status}" if status is not None else "network error"
                raise RuntimeError(f"GitHub API failed: {status_text} for {url}: {body}")

            delay, reason = retry
            await self.defer(delay, f"{reason}; retry {attempt + 1}/{self.max_retries}")

    @staticmethod
    def rate_limit_headers(headers):
        return [
            (name, value)
            for name, value in headers.items()
            if name.lower().startswith("x-ratelimit-")
        ]

    def print_rate_limit_headers(self, status, headers):
        values = self.rate_limit_headers(headers)
        if values:
            formatted = ", ".join(f"{name}={value}" for name, value in values)
            print(f"GitHub API HTTP/{status} rate-limit headers: {formatted}", file=sys.stderr, flush=True)

    async def wait_for_cooldown(self):
        while True:
            async with self.cooldown_lock:
                delay = self.cooldown_until - self.monotonic()
            if delay <= 0:
                return
            await self.sleep(delay)

    async def defer(self, delay, reason):
        if delay <= 0:
            return
        deadline = self.monotonic() + delay
        async with self.cooldown_lock:
            if deadline <= self.cooldown_until:
                return
            self.cooldown_until = deadline
        print(f"GitHub API paused for {delay:.1f}s: {reason}", file=sys.stderr, flush=True)

    async def defer_if_rate_limited(self, headers):
        if headers.get("X-RateLimit-Remaining") != "0":
            return
        delay = self.rate_limit_reset_delay(headers)
        if delay is not None:
            await self.defer(delay, "X-RateLimit-Remaining is 0")

    def retry_delay(self, status, headers, body, attempt):
        retry_after = self.retry_after_delay(headers)
        if retry_after is not None and status in (403, 429):
            return retry_after, "GitHub requested Retry-After"

        if status in (403, 429) and headers.get("X-RateLimit-Remaining") == "0":
            reset_delay = self.rate_limit_reset_delay(headers)
            if reset_delay is not None:
                return reset_delay, "GitHub primary rate limit exhausted"

        rate_limited = status == 429 or "rate limit" in body.lower() or "abuse detection" in body.lower()
        if status in (403, 429) and rate_limited:
            return SECONDARY_RATE_LIMIT_RETRY_SECONDS * (2**attempt), "GitHub secondary rate limit"
        if status in (502, 503, 504):
            return TRANSIENT_RETRY_SECONDS * (2**attempt), f"GitHub HTTP/{status}"
        return None

    def retry_after_delay(self, headers):
        value = headers.get("Retry-After")
        if value is None:
            return None
        try:
            return max(0, float(value))
        except ValueError:
            try:
                retry_at = email.utils.parsedate_to_datetime(value)
            except (TypeError, ValueError, IndexError):
                return None
            return max(0, retry_at.timestamp() - self.wall_clock())

    def rate_limit_reset_delay(self, headers):
        value = headers.get("X-RateLimit-Reset")
        if value is None:
            return None
        try:
            reset_at = float(value)
        except ValueError:
            return None
        return max(0, reset_at - self.wall_clock()) + RATE_LIMIT_RESET_MARGIN_SECONDS

    async def get_all_pages(self, url, label):
        page_no = 1
        while url:
            data, url = await self.get_json(url)
            print(f"{label}: fetched page {page_no}", flush=True)
            page_no += 1
            yield data


def parse_next_link(link_header):
    if not link_header:
        return None
    for part in link_header.split(","):
        url_part, _, rel_part = part.strip().partition(";")
        if 'rel="next"' in rel_part:
            return url_part.strip()[1:-1]
    return None


def isoparse_ts(value):
    if not value:
        return None
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def infer_project(repo):
    match = re.match(r"^(?:incubator-)?([^-.]+)", repo)
    return match.group(1) if match else "unknown"


def job_used_runner(job):
    return bool(job.get("runner_name"))


def parse_jobs(jobs_data, repo):
    seconds_used = 0
    earliest_runner = None
    last_finish = None
    jobs = []
    for job in jobs_data.get("jobs", []):
        if not job_used_runner(job):
            continue
        start_ts = isoparse_ts(job.get("started_at"))
        end_ts = isoparse_ts(job.get("completed_at"))
        if start_ts is None or end_ts is None:
            continue
        workflow_name = job.get("workflow_name") or "Unknown"
        job_name = job.get("name") or workflow_name
        job_time = end_ts - start_ts
        seconds_used += job_time
        if earliest_runner is None or start_ts < earliest_runner:
            earliest_runner = start_ts
        if last_finish is None or last_finish < end_ts:
            last_finish = end_ts
        steps = []
        for step in job.get("steps", []):
            step_start_ts = isoparse_ts(step.get("started_at"))
            step_end_ts = isoparse_ts(step.get("completed_at"))
            if step_start_ts is None or step_end_ts is None:
                continue
            steps.append((step.get("name", ""), step_start_ts, step_end_ts - step_start_ts))
        jobs.append(
            {
                "name": workflow_name,
                "name_unique": f"{repo}/{workflow_name}",
                "job_name": job_name,
                "job_duration": job_time,
                "steps": steps,
                "labels": job.get("labels", []),
                "runner_name": job.get("runner_name", ""),
                "runner_group": job.get("runner_group_name", "GitHub Actions") or "GitHub Actions",
            }
        )
    return seconds_used, earliest_runner, last_finish, jobs


def setup_db(db_path, append):
    if os.path.exists(db_path) and not append:
        os.unlink(db_path)
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(CREATE_RUNS_DB.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1))
    return conn


async def list_recent_runs(client, org, repo, days):
    end = dt.datetime.now(dt.UTC)
    start = end - dt.timedelta(days=days)
    runs = await list_runs_window(client, org, repo, start, end)
    unique_runs = {run["id"]: run for run in runs}
    if len(unique_runs) != len(runs):
        duplicates = len(runs) - len(unique_runs)
        print(f"{repo}: removed {duplicates} duplicate workflow run(s) from split query windows", flush=True)
    return list(unique_runs.values())


def github_timestamp(value):
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


async def list_runs_window(client, org, repo, start, end, depth=0):
    created_range = f"{github_timestamp(start)}..{github_timestamp(end)}"
    params = urllib.parse.urlencode({"per_page": 100, "created": created_range})
    url = f"{GITHUB_API}/repos/{org}/{repo}/actions/runs?{params}"
    runs = []
    reported_total = None
    async for page in client.get_all_pages(url, f"{repo}: workflow runs"):
        page_runs = page.get("workflow_runs", [])
        if not page_runs:
            break
        runs.extend(page_runs)
        total = page.get("total_count")
        if total is not None and reported_total is None:
            reported_total = total
            if total > MAX_GITHUB_LIST_RESULTS and (end - start) > dt.timedelta(hours=1):
                mid = start + ((end - start) / 2)
                print(
                    f"{repo}: {total} workflow runs match {created_range}; splitting query window",
                    flush=True,
                )
                first_half = await list_runs_window(client, org, repo, start, mid, depth + 1)
                second_half = await list_runs_window(client, org, repo, mid, end, depth + 1)
                return first_half + second_half
        if reported_total is None:
            print(f"{repo}: discovered {len(runs)} workflow runs so far", flush=True)
        else:
            print(f"{repo}: discovered {len(runs)}/{reported_total} workflow runs", flush=True)
        if len(runs) >= MAX_GITHUB_LIST_RESULTS and reported_total and reported_total > MAX_GITHUB_LIST_RESULTS:
            print(
                f"{repo}: reached GitHub's {MAX_GITHUB_LIST_RESULTS}-result pagination cap for {created_range}",
                flush=True,
            )
            break
    if reported_total and reported_total > MAX_GITHUB_LIST_RESULTS and len(runs) >= MAX_GITHUB_LIST_RESULTS:
        print(
            f"{repo}: warning: {reported_total} workflow runs match {created_range}, but only {len(runs)} were fetched",
            flush=True,
        )
    return runs


async def workflow_metadata(client, workflow_url, cache):
    task = cache.get(workflow_url)
    if task is None:
        task = asyncio.create_task(client.get_json(workflow_url))
        cache[workflow_url] = task
    try:
        metadata, _ = await task
        return metadata
    except BaseException:
        if cache.get(workflow_url) is task:
            del cache[workflow_url]
        raise


def insert_run(conn, run_dict):
    fields = tuple(run_dict.keys())
    placeholders = ", ".join("?" for _ in fields)
    columns = ", ".join(fields)
    conn.execute(
        f"INSERT OR REPLACE INTO runs ({columns}) VALUES ({placeholders})",
        tuple(run_dict[field] for field in fields),
    )


async def parse_run(client, repo, run, metadata_cache):
    jobs_data, _ = await client.get_json(run["jobs_url"])
    seconds_used, run_start, run_finish, jobs = parse_jobs(jobs_data, repo)
    if not jobs or seconds_used <= 0 or run_start is None or run_finish is None:
        return None
    workflow = await workflow_metadata(client, run["workflow_url"], metadata_cache)
    return {
        "id": run["id"],
        "project": infer_project(repo),
        "repo": repo,
        "workflow_id": workflow.get("id", run.get("workflow_id", 0)),
        "workflow_name": workflow.get("name", run.get("name", "Unknown")),
        "workflow_path": workflow.get("path", ""),
        "seconds_used": int(seconds_used),
        "run_start": int(run_start),
        "run_finish": int(run_finish),
        "jobs": json.dumps(jobs),
    }


async def backfill_project(conn, client, org, repo, days, metadata_cache):
    print(f"Gathering information about {repo} for the last {days} day(s)", flush=True)
    runs = await list_recent_runs(client, org, repo, days)
    total = len(runs)
    if total == 0:
        print(f"{repo}: no workflow runs found", flush=True)
        return

    inserted = 0
    skipped = 0
    completed = 0
    tasks = [asyncio.create_task(parse_run(client, repo, run, metadata_cache)) for run in runs]
    progress_step = max(1, min(25, total // 10 or 1))
    for task in asyncio.as_completed(tasks):
        run_dict = await task
        completed += 1
        if run_dict is None:
            skipped += 1
        else:
            insert_run(conn, run_dict)
            inserted += 1
        if completed == total or completed % progress_step == 0:
            print(f"{repo}: processed {completed}/{total} runs, inserted {inserted}, skipped {skipped}", flush=True)
    conn.commit()
    print(f"{repo}: inserted {inserted} runs, skipped {skipped} runs without runner time", flush=True)


async def async_main(args):
    projects = args.projects or list(DEFAULT_PROJECTS)
    if args.concurrency < 1:
        raise ValueError("--concurrency must be at least 1")
    if args.max_retries < 0:
        raise ValueError("--max-retries must not be negative")
    if not args.token:
        print("Warning: no GitHub token set; anonymous API rate limits may be too low for backfills", file=sys.stderr)
    conn = setup_db(args.db, args.append)
    metadata_cache = {}
    started = time.time()
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = GitHubClient(session, args.token, args.concurrency, args.max_retries)
        for repo in projects:
            await backfill_project(conn, client, args.org, repo, args.days, metadata_cache)
    conn.close()
    print(f"Wrote {args.db} in {time.time() - started:.1f}s")


def main():
    asyncio.run(async_main(parse_args()))


if __name__ == "__main__":
    main()
