# Local GitHub Actions Usage Tools

These tools create a local `ghactions.db` and run a GitHub-Actions-only dashboard server for development and review.

Generated files stay under the ignored `var/` directory.

## Prerequisites

- Python 3 with `venv`
- A GitHub token in `GH_TOKEN` or `GITHUB_TOKEN`

The token is strongly recommended. Anonymous GitHub API limits are usually too low for backfills.

## Backfill Data

From the repository root:

```bash
tools/ghactions-local/run-backfill.sh
```

Defaults:

- Projects: `cassandra`, `polaris`, `iceberg`, `airflow`, `spark`, `hadoop`, `hive`, `parquet`
- Days: `7`
- Database: `var/ghactions.db`

Useful options:

```bash
tools/ghactions-local/run-backfill.sh --project mina --project iceberg
DAYS=3 tools/ghactions-local/run-backfill.sh
tools/ghactions-local/run-backfill.sh --days 14 --concurrency 4
tools/ghactions-local/run-backfill.sh --append --project polaris
```

The backfill stores GitHub workflow run IDs as SQLite row IDs and uses `INSERT OR REPLACE`, so rerunning it refreshes matching runs. Without `--append`, the DB is recreated first.

For repositories with many workflow runs, the backfill splits the requested time window so it can fetch more than GitHub's 1,000-result listing cap.

The backfill defaults to one concurrent GitHub request to avoid secondary rate limits. It prints every `X-RateLimit-*` response header that GitHub supplies. On rate-limit responses it pauses all queued requests until `Retry-After` or `X-RateLimit-Reset`, and retries up to four times by default. Use `--max-retries` to change that limit.

## Start Local Server

After creating `var/ghactions.db`:

```bash
tools/ghactions-local/start-local.sh
```

Then open:

```text
http://127.0.0.1:8080/#ghactions
```

The launcher imports only the GitHub Actions endpoint and scanner. That avoids unrelated production startup tasks such as LDAP, Jira, and other scanners.

## Local Authentication

`server_local.py` installs a fake local ASF session for development only. It grants access to the local GitHub Actions UI without using OAuth.

This fake session is isolated to the local launcher and is not imported by production startup paths.

## Runtime Files

The scripts may create:

- `var/.venv`
- `var/ghactions.db`
- `reporting-dashboard.yaml`
- `htdocs/`

`var/` is ignored by git. `reporting-dashboard.yaml` and `htdocs/` are local runtime artifacts; do not commit machine-specific configuration.

## Testing

Run the local-tool unit tests:

```bash
python -m unittest tests.test_ghactions_local_backfill
```
