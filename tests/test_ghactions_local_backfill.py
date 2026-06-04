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
"""Tests for the local GitHub Actions backfill tool."""

import contextlib
import asyncio
import datetime as dt
import importlib.util
import io
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path


def load_backfill_module():
    if "aiohttp" not in sys.modules:
        aiohttp = types.ModuleType("aiohttp")
        aiohttp.ClientTimeout = lambda *args, **kwargs: None
        aiohttp.ClientSession = None
        sys.modules["aiohttp"] = aiohttp
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "tools" / "ghactions-local" / "backfill.py"
    spec = importlib.util.spec_from_file_location("ghactions_local_backfill", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


backfill = load_backfill_module()


class BackfillParsingTest(unittest.TestCase):
    def test_parse_jobs_skips_jobs_without_runner_and_preserves_job_name(self):
        seconds_used, run_start, run_finish, jobs = backfill.parse_jobs(
            {
                "jobs": [
                    {
                        "workflow_name": "CI",
                        "name": "unit-tests",
                        "runner_name": "GitHub Actions 1",
                        "runner_group_name": "GitHub Actions",
                        "started_at": "2026-06-04T10:00:00Z",
                        "completed_at": "2026-06-04T10:05:00Z",
                        "labels": ["ubuntu-latest"],
                        "steps": [
                            {
                                "name": "Run tests",
                                "started_at": "2026-06-04T10:01:00Z",
                                "completed_at": "2026-06-04T10:04:00Z",
                            }
                        ],
                    },
                    {
                        "workflow_name": "CI",
                        "name": "queued-only",
                        "runner_name": "",
                        "started_at": "2026-06-04T10:00:00Z",
                        "completed_at": "2026-06-04T10:05:00Z",
                        "labels": ["ubuntu-latest"],
                        "steps": [],
                    },
                ]
            },
            "sample",
        )

        self.assertEqual(seconds_used, 300)
        self.assertEqual(run_start, backfill.isoparse_ts("2026-06-04T10:00:00Z"))
        self.assertEqual(run_finish, backfill.isoparse_ts("2026-06-04T10:05:00Z"))
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["name"], "CI")
        self.assertEqual(jobs[0]["job_name"], "unit-tests")
        self.assertEqual(jobs[0]["steps"][0][0], "Run tests")

    def test_insert_run_replaces_existing_run_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ghactions.db"
            conn = backfill.setup_db(str(db_path), append=False)
            run_dict = {
                "id": 123,
                "project": "sample",
                "repo": "sample",
                "workflow_id": 1,
                "workflow_name": "CI",
                "workflow_path": ".github/workflows/ci.yml",
                "seconds_used": 60,
                "run_start": 1,
                "run_finish": 61,
                "jobs": "[]",
            }
            backfill.insert_run(conn, run_dict)
            run_dict["seconds_used"] = 120
            backfill.insert_run(conn, run_dict)
            conn.commit()

            rows = conn.execute("SELECT id, seconds_used FROM runs").fetchall()
            conn.close()

        self.assertEqual(rows, [(123, 120)])


class BackfillPaginationTest(unittest.IsolatedAsyncioTestCase):
    async def test_large_windows_are_split_before_github_result_cap(self):
        client = FakeRunsClient()
        start = dt.datetime(2026, 6, 1, tzinfo=dt.UTC)
        end = dt.datetime(2026, 6, 8, tzinfo=dt.UTC)

        with contextlib.redirect_stdout(io.StringIO()):
            runs = await backfill.list_runs_window(client, "apache", "iceberg", start, end)

        self.assertEqual({run["id"] for run in runs}, {1, 2})
        self.assertEqual(len(client.window_queries), 3)


class GitHubClientRateLimitTest(unittest.IsolatedAsyncioTestCase):
    async def test_prints_all_rate_limit_headers_and_waits_after_successful_exhaustion(self):
        clock = FakeClock(wall_time=100)
        session = FakeSession(
            [
                FakeResponse(
                    200,
                    {"X-RateLimit-Limit": "5000", "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "110"},
                    {"first": True},
                ),
                FakeResponse(200, {"X-RateLimit-Resource": "core"}, {"second": True}),
            ]
        )
        client = backfill.GitHubClient(
            session,
            None,
            concurrency=1,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
            wall_clock=clock.wall_clock,
        )

        with contextlib.redirect_stderr(io.StringIO()) as stderr:
            first, _ = await client.get_json("https://example.test/first")
            second, _ = await client.get_json("https://example.test/second")

        self.assertEqual(first, {"first": True})
        self.assertEqual(second, {"second": True})
        self.assertEqual(clock.sleeps, [11])
        output = stderr.getvalue()
        self.assertIn("X-RateLimit-Limit=5000", output)
        self.assertIn("X-RateLimit-Remaining=0", output)
        self.assertIn("X-RateLimit-Reset=110", output)
        self.assertIn("X-RateLimit-Resource=core", output)

    async def test_retries_rate_limit_response_after_retry_after(self):
        clock = FakeClock()
        session = FakeSession(
            [
                FakeResponse(429, {"Retry-After": "5"}, text="secondary rate limit"),
                FakeResponse(200, {}, {"ok": True}),
            ]
        )
        client = backfill.GitHubClient(
            session,
            None,
            concurrency=1,
            max_retries=1,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
            wall_clock=clock.wall_clock,
        )

        with contextlib.redirect_stderr(io.StringIO()):
            result, _ = await client.get_json("https://example.test/rate-limited")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(len(session.requests), 2)
        self.assertEqual(clock.sleeps, [5])

    async def test_retries_primary_rate_limit_response_after_reset(self):
        clock = FakeClock(wall_time=100)
        session = FakeSession(
            [
                FakeResponse(
                    403,
                    {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "110"},
                    text="API rate limit exceeded",
                ),
                FakeResponse(200, {}, {"ok": True}),
            ]
        )
        client = backfill.GitHubClient(
            session,
            None,
            concurrency=1,
            max_retries=1,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
            wall_clock=clock.wall_clock,
        )

        with contextlib.redirect_stderr(io.StringIO()):
            result, _ = await client.get_json("https://example.test/primary-rate-limited")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(len(session.requests), 2)
        self.assertEqual(clock.sleeps, [11])

    async def test_does_not_retry_an_ordinary_forbidden_response(self):
        session = FakeSession([FakeResponse(403, {}, text="resource not accessible")])
        client = backfill.GitHubClient(session, None, concurrency=1, max_retries=4)

        with self.assertRaisesRegex(RuntimeError, "HTTP/403"):
            await client.get_json("https://example.test/forbidden")

        self.assertEqual(len(session.requests), 1)

    async def test_workflow_metadata_coalesces_concurrent_cache_misses(self):
        client = BlockingMetadataClient()
        cache = {}

        first = asyncio.create_task(backfill.workflow_metadata(client, "https://example.test/workflow", cache))
        second = asyncio.create_task(backfill.workflow_metadata(client, "https://example.test/workflow", cache))
        await client.started.wait()
        self.assertEqual(client.calls, 1)
        client.release.set()

        self.assertEqual(await first, {"id": 1})
        self.assertEqual(await second, {"id": 1})


class FakeClock:
    def __init__(self, wall_time=0):
        self.current = 0
        self.wall_time = wall_time
        self.sleeps = []

    def monotonic(self):
        return self.current

    def wall_clock(self):
        return self.wall_time

    async def sleep(self, delay):
        self.sleeps.append(delay)
        self.current += delay


class FakeResponse:
    def __init__(self, status, headers, payload=None, text=""):
        self.status = status
        self.headers = headers
        self.payload = payload
        self.response_text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def json(self):
        return self.payload

    async def text(self):
        return self.response_text


class FakeSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []

    def get(self, url, headers):
        self.requests.append((url, headers))
        return next(self.responses)


class BlockingMetadataClient:
    def __init__(self):
        self.calls = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def get_json(self, _url):
        self.calls += 1
        self.started.set()
        await self.release.wait()
        return {"id": 1}, None


class FakeRunsClient:
    def __init__(self):
        self.window_queries = []

    async def get_all_pages(self, url, _label):
        self.window_queries.append(url)
        if len(self.window_queries) == 1:
            yield {"total_count": backfill.MAX_GITHUB_LIST_RESULTS + 1, "workflow_runs": [{"id": 999}]}
            return
        yield {"total_count": 1, "workflow_runs": [{"id": len(self.window_queries) - 1}]}


if __name__ == "__main__":
    unittest.main()
