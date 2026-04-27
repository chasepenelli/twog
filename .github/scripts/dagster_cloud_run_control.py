#!/usr/bin/env python3
"""Small Dagster Cloud run-control helper for GitHub Actions."""

from __future__ import annotations

import argparse
import json
import platform
import sys
import urllib.error
import urllib.request
from collections.abc import Iterable
from typing import Any


TERMINAL_STATUSES = {"SUCCESS", "FAILURE", "CANCELED"}

RUN_STATUS_QUERY = """
query CliGetRunStatus($runId: ID!) {
  runOrError(runId: $runId) {
    __typename
    ... on Run {
      runId
      status
      canTerminate
    }
    ... on RunNotFoundError {
      runId
      message
    }
    ... on PythonError {
      message
      stack
    }
  }
}
"""

TERMINATE_RUNS_MUTATION = """
mutation GithubActionsTerminateRuns(
  $runIds: [String!]!
  $terminatePolicy: TerminateRunPolicy
) {
  terminateRuns(runIds: $runIds, terminatePolicy: $terminatePolicy) {
    __typename
    ... on TerminateRunsResult {
      terminateRunResults {
        __typename
        ... on TerminateRunSuccess {
          run {
            runId
            status
          }
        }
        ... on TerminateRunFailure {
          message
          run {
            runId
            status
          }
        }
        ... on RunNotFoundError {
          runId
          message
        }
        ... on UnauthorizedError {
          message
        }
        ... on PythonError {
          message
          stack
        }
      }
    }
    ... on PythonError {
      message
      stack
    }
  }
}
"""


def _graphql_endpoint(url: str, deployment: str) -> str:
    base_url = url.rstrip("/")
    if base_url.endswith(f"/{deployment}"):
        return f"{base_url}/graphql"
    return f"{base_url}/{deployment}/graphql"


def _headers(api_token: str, deployment: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Dagster-Cloud-Api-Token": api_token,
        "Dagster-Cloud-Deployment": deployment,
        "Dagster-Cloud-Scope": "deployment",
        "Dagster-Cloud-Version": "github-actions",
        "Python-Version": platform.python_version(),
    }


def _execute_graphql(
    *,
    url: str,
    api_token: str,
    deployment: str,
    query: str,
    variables: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    request = urllib.request.Request(
        _graphql_endpoint(url, deployment),
        data=json.dumps({"query": query, "variables": variables}).encode("utf-8"),
        headers=_headers(api_token, deployment),
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Dagster Cloud HTTP {error.code}: {body}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"Dagster Cloud request failed: {error}") from error

    if payload.get("errors"):
        raise RuntimeError(f"Dagster Cloud GraphQL errors: {json.dumps(payload['errors'])}")
    if not isinstance(payload.get("data"), dict):
        raise RuntimeError(f"Unexpected Dagster Cloud response: {json.dumps(payload)}")
    return payload["data"]


def _split_run_ids(raw_run_ids: Iterable[str]) -> list[str]:
    run_ids: list[str] = []
    for raw_value in raw_run_ids:
        for candidate in raw_value.replace(",", " ").split():
            value = candidate.strip()
            if value:
                run_ids.append(value)
    return run_ids


def _run_status(args: argparse.Namespace, run_id: str) -> dict[str, Any]:
    data = _execute_graphql(
        url=args.url,
        api_token=args.api_token,
        deployment=args.deployment,
        query=RUN_STATUS_QUERY,
        variables={"runId": run_id},
        timeout_seconds=args.timeout_seconds,
    )
    result = data["runOrError"]
    typename = result.get("__typename")
    if typename != "Run":
        raise RuntimeError(f"Unable to fetch status for {run_id}: {json.dumps(result)}")
    return result


def _terminate_runs(args: argparse.Namespace, run_ids: list[str]) -> list[dict[str, Any]]:
    data = _execute_graphql(
        url=args.url,
        api_token=args.api_token,
        deployment=args.deployment,
        query=TERMINATE_RUNS_MUTATION,
        variables={"runIds": run_ids, "terminatePolicy": args.policy},
        timeout_seconds=args.timeout_seconds,
    )
    result = data["terminateRuns"]
    if result.get("__typename") != "TerminateRunsResult":
        raise RuntimeError(f"Unable to terminate runs: {json.dumps(result)}")
    return result["terminateRunResults"]


def status_command(args: argparse.Namespace) -> int:
    run_ids = _split_run_ids(args.run_id)
    if not run_ids:
        raise RuntimeError("At least one --run-id value is required.")

    failed = False
    for run_id in run_ids:
        try:
            status = _run_status(args, run_id)
        except Exception as error:
            failed = True
            print(f"{run_id}: ERROR {error}", file=sys.stderr)
            continue
        print(f"{run_id}: {status['status']} canTerminate={status['canTerminate']}")
    return 1 if failed else 0


def terminate_command(args: argparse.Namespace) -> int:
    run_ids = _split_run_ids(args.run_id)
    if not run_ids:
        raise RuntimeError("At least one --run-id value is required.")

    runs_to_terminate: list[str] = []
    for run_id in run_ids:
        status = _run_status(args, run_id)
        current_status = status["status"]
        print(f"{run_id}: current_status={current_status} canTerminate={status['canTerminate']}")
        if args.skip_terminal and current_status in TERMINAL_STATUSES:
            continue
        runs_to_terminate.append(run_id)

    if not runs_to_terminate:
        print("No non-terminal runs to terminate.")
        return 0

    results = _terminate_runs(args, runs_to_terminate)
    failures = []
    for result in results:
        typename = result.get("__typename")
        if typename == "TerminateRunSuccess":
            run = result["run"]
            print(f"{run['runId']}: terminated status={run['status']}")
        else:
            failures.append(result)
            print(f"termination_failed: {json.dumps(result)}", file=sys.stderr)

    return 1 if failures else 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="Dagster Cloud base URL.")
    parser.add_argument("--api-token", required=True, help="Dagster Cloud API token.")
    parser.add_argument("--deployment", required=True, help="Dagster Cloud deployment name.")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=60.0,
        help="HTTP timeout for each Dagster Cloud GraphQL request.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser("status", help="Print run status.")
    status_parser.add_argument("--run-id", action="append", required=True)
    status_parser.set_defaults(func=status_command)

    terminate_parser = subparsers.add_parser("terminate", help="Terminate one or more runs.")
    terminate_parser.add_argument("--run-id", action="append", required=True)
    terminate_parser.add_argument(
        "--policy",
        choices=("SAFE_TERMINATE", "MARK_AS_CANCELED_IMMEDIATELY"),
        default="SAFE_TERMINATE",
        help="Dagster termination policy.",
    )
    terminate_parser.add_argument(
        "--skip-terminal",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip runs already in SUCCESS, FAILURE, or CANCELED.",
    )
    terminate_parser.set_defaults(func=terminate_command)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
