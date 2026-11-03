"""The Python steps of the fuzzer's workflows. Each command reads what its step passes in the environment.

valkey-pr-fuzzer.yml:
resolve       the Valkey repository, ref, and PR to test, as GITHUB_OUTPUT lines
matrix        the checked run_count and the random-fuzzer matrix, as GITHUB_OUTPUT lines
metadata      one run's metadata.json, from its batch folder
summarize     the job summary, the PR comment (its first line the marker the comment step finds it by),
              and whether the workflow failed

fuzzer-pr-batch.yml:
check-inputs  check the dispatch inputs, and write the scenarios and spec bundles they carry
drop-ledgers  remove the write ledgers of a batch folder's runs that passed

fuzzer-pr-batch.yml and fuzzer-run.yml:
summary       a batch folder's runs and failure signatures, appended to the job summary
"""

import glob
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

# GitHub runs at most 256 jobs from one matrix.
MAX_RUN_COUNT = 256
COMMENT_MARKER = "<!-- valkey-cluster-fuzzer-results -->"


def parse_pr_url(url):
    """``(owner/repo, number)`` of a GitHub PR URL, also with a trailing ``/files``, query, or fragment; else None."""

    parsed = urlparse(url)
    if parsed.hostname != "github.com":
        return None
    parts = parsed.path.strip("/").split("/")
    if len(parts) < 4 or parts[2] != "pull" or not parts[3].isdigit():
        return None
    return f"{parts[0]}/{parts[1]}", parts[3]


def resolve(pr_link="", valkey_repository="", valkey_ref="", pr_number="", pr_url=""):
    """The target to build and test, from the inputs (pr_link, when given, decides it); SystemExit on bad inputs."""

    pr_link, valkey_repository, valkey_ref = pr_link.strip(), valkey_repository.strip(), valkey_ref.strip()
    pr_number, pr_url = pr_number.strip(), pr_url.strip()
    if pr_number and not pr_number.isdigit():
        parsed = parse_pr_url(pr_number)
        if not parsed:
            raise SystemExit(
                f"inputs.pr_number must be a numeric PR number, got {pr_number!r}. Use the pr_link input for full URLs."
            )
        # A PR URL pasted into pr_number counts as pr_link.
        pr_link = pr_link or pr_number
        pr_number = parsed[1]

    if pr_link:
        parsed = parse_pr_url(pr_link)
        if not parsed:
            raise SystemExit(
                f"Invalid PR link format: {pr_link!r}. Expected: https://github.com/OWNER/REPO/pull/NUMBER"
            )
        parsed_repo, parsed_number = parsed
        parsed_ref = f"refs/pull/{parsed_number}/merge"
        # Overrides that disagree with the link would mix metadata from different PRs.
        conflicts = []
        if valkey_repository and valkey_repository != parsed_repo:
            conflicts.append(f"valkey_repository={valkey_repository!r} vs pr_link repo {parsed_repo!r}")
        if valkey_ref and valkey_ref != parsed_ref:
            conflicts.append(f"valkey_ref={valkey_ref!r} vs pr_link ref {parsed_ref!r}")
        if pr_number and pr_number != parsed_number:
            conflicts.append(f"pr_number={pr_number!r} vs pr_link number {parsed_number!r}")
        if conflicts:
            raise SystemExit(
                f"Conflicting inputs: {'; '.join(conflicts)}. Remove the conflicting overrides or adjust pr_link."
            )
        valkey_repository, valkey_ref, pr_number = parsed_repo, parsed_ref, parsed_number
    else:
        valkey_repository = valkey_repository or "valkey-io/valkey"
        if not valkey_ref:
            raise SystemExit("Either pr_link or valkey_ref must be provided")

    if pr_number:
        # Derived from the target, so summaries and comments cannot drift from it.
        pr_url = f"https://github.com/{valkey_repository}/pull/{pr_number}"
    # Runs for one PR share the key whatever ref or URL variant they name, so they never overwrite each other's comment.
    target = f"pr-{pr_number}" if pr_number else valkey_ref
    return {
        "valkey_repository": valkey_repository,
        "valkey_ref": valkey_ref,
        "pr_number": pr_number,
        "pr_url": pr_url,
        "concurrency_key": f"valkey-pr-fuzzer-{valkey_repository}-{target}",
    }


def run_count(raw):
    """``inputs.run_count`` as a whole number from 1 to :data:`MAX_RUN_COUNT`; SystemExit otherwise."""

    try:
        value = float(raw)
    except ValueError:
        value = None
    if value is None or not value.is_integer() or not 1 <= value <= MAX_RUN_COUNT:
        raise SystemExit(f"inputs.run_count must be a whole number from 1 to {MAX_RUN_COUNT}, got {raw!r}")
    return int(value)


def batch_runs(batch_dir):
    """A batch folder's summary.json (None when it wrote none), and each of its runs with its run report's
    outcome (empty for a run that left no report)."""

    path = os.path.join(batch_dir, "summary.json")
    if not os.path.exists(path):
        return None, []
    with open(path, encoding="utf-8") as handle:
        summary = json.load(handle)
    runs = []
    for run in summary["runs"]:
        outcome = {}
        if run["report"]:
            with open(os.path.join(batch_dir, run["report"]), encoding="utf-8") as handle:
                outcome = json.load(handle).get("outcome") or {}
        runs.append((run, outcome))
    return summary, runs


def failed_components(outcome):
    return [
        f"{item.get('component_id')}: {item.get('summary')}"
        for item in outcome.get("component_results") or []
        if item.get("status") in ("FAIL", "ERROR")
    ]


def metadata(run_id, batch_dir, setup_outcome, fuzzer_outcome):
    """One run's metadata: how its setup and fuzzer steps ended and what the batch's one run reported."""

    result = {
        "run_id": run_id,
        "status": "success" if setup_outcome == fuzzer_outcome == "success" else "failure",
        "stage": "fuzzer" if setup_outcome == "success" else "setup",
        "scenario_id": None,
        "seed": None,
        "outcome": None,
        "duration": None,
        "failed_components": [],
        "summary": None,
        "valkey_sha": None,
    }
    for run, outcome in batch_runs(batch_dir)[1]:
        # The batch's verdict: a run that left servers or directories behind is an ERROR.
        result.update(summary=run["summary"], outcome=run["status"], seed=run.get("seed"))
        if outcome:
            plan = outcome.get("plan") or {}
            result.update(
                scenario_id=plan.get("scenario_id"), seed=plan.get("seed"), valkey_sha=outcome.get("valkey_sha")
            )
            if outcome.get("started_at") is not None and outcome.get("ended_at") is not None:
                result["duration"] = round(outcome["ended_at"] - outcome["started_at"], 2)
            result["failed_components"] = failed_components(outcome)
    return result


def _cell(value, limit=None):
    text = str(value).replace("\n", " ").replace("|", "/")
    return text if limit is None or len(text) <= limit else text[: limit - 3] + "..."


def _or_dash(value):
    return "-" if value is None else value


def summarize(env, results):
    """``(job summary, PR comment or None, whether the workflow failed)`` for the runs' metadata by run ID.

    ``env`` holds the step's environment: the earlier jobs' results and outputs, and the inputs to show.
    """

    count = int(env["RUN_COUNT"]) if env["RUN_COUNT"] else 0
    missing = [run_id for run_id in range(1, count + 1) if run_id not in results]
    passed = sum(1 for result in results.values() if result["status"] == "success")
    failed = len(results) - passed
    matrix_ready = env["PREPARE_MATRIX_RESULT"] == "success"
    overall_failed = not matrix_ready or env["BUILD_RESULT"] != "success" or failed > 0 or bool(missing)
    overall = "FAIL" if overall_failed else "PASS"
    sha = env["RESOLVED_SHA"] or "unavailable"
    if matrix_ready:
        counts = [
            f"- Passed runs: {passed}/{count}",
            f"- Failed runs: {failed}",
            f"- Missing run artifacts: {len(missing)}",
        ]
    else:
        counts = ["- No runs were queued: see the prepare-matrix job for why."]
    missing_line = (
        ["", f"Missing artifacts for runs: {', '.join(str(run_id) for run_id in missing)}"] if missing else []
    )

    lines = [
        "## Valkey Cluster Fuzzer",
        "",
        f"- Valkey repository: `{env['VALKEY_REPOSITORY']}`",
        f"- Requested ref: `{env['VALKEY_REF']}`",
        f"- Resolved SHA: `{sha}`",
        f"- Fuzzer repository: `{env['FUZZER_REPOSITORY'] or 'unavailable'}`",
        f"- Fuzzer ref: `{env['FUZZER_REF'] or 'unavailable'}`",
        f"- Trigger label: `{env['LABEL_NAME']}`",
        f"- Overall result: `{overall}`",
        f"- Matrix preparation: `{env['PREPARE_MATRIX_RESULT']}`",
        f"- Build stage: `{env['BUILD_RESULT']}`",
    ]
    if env["PR_NUMBER"]:
        pr = f"[#{env['PR_NUMBER']}]({env['PR_URL']})" if env["PR_URL"] else f"#{env['PR_NUMBER']}"
        lines.append(f"- PR: {pr}")
    lines += counts
    if matrix_ready:
        lines += [
            "",
            "| Run | Status | Seed | Stage | Duration (s) | Outcome | Failed components |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for run_id in range(1, count + 1):
            result = results.get(run_id)
            if result is None:
                lines.append(f"| {run_id} | missing | - | artifact | - | - | artifact not uploaded |")
                continue
            failures = _cell("; ".join(result.get("failed_components") or []) or "-")
            lines.append(
                f"| {run_id} | {result['status']} | {_or_dash(result.get('seed'))} | {result.get('stage', '-')} "
                f"| {_or_dash(result.get('duration'))} | {result.get('outcome') or '-'} | {failures} |"
            )
    lines += missing_line

    if not env["PR_NUMBER"]:
        return "\n".join(lines) + "\n", None, overall_failed
    comment = [
        COMMENT_MARKER,
        "## Valkey Cluster Fuzzer Results",
        "",
        f"- Overall result: `{overall}`",
        f"- Workflow run: [View run]({env['RUN_URL']})",
        f"- Resolved SHA: `{sha}`",
        *counts,
    ]
    if matrix_ready and overall_failed:
        comment += [
            "",
            "<details>",
            "<summary>Failure details</summary>",
            "",
            "| Run | Status | Seed | Stage | Outcome | Failed components |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for run_id in range(1, count + 1):
            result = results.get(run_id)
            if result is None:
                comment.append(f"| {run_id} | missing | - | artifact | - | artifact not uploaded |")
            elif result["status"] != "success":
                failures = _cell("; ".join(result.get("failed_components") or []) or "-", limit=200)
                comment.append(
                    f"| {run_id} | {result['status']} | {_or_dash(result.get('seed'))} | {result.get('stage', '-')} "
                    f"| {result.get('outcome') or '-'} | {failures} |"
                )
        comment += [
            "",
            "Reproduce a run with `valkey-fuzzer run --seed <seed> --experiment valkey.cluster-chaos "
            "--valkey-binary <valkey-server>`, or with `valkey-fuzzer run batch/runs/<run>/scenario.yaml` "
            "from its artifact.",
            "",
            "</details>",
        ]
    comment += missing_line
    return "\n".join(lines) + "\n", "\n".join(comment) + "\n", overall_failed


def batch_summary(title, batch_dir, valkey=""):
    """The job summary of one batch folder: its status, each run, and the failure signatures."""

    lines = [f"## {title}", ""] + ([f"- Valkey: {valkey}"] if valkey else [])
    summary, runs = batch_runs(batch_dir)
    if summary is None:
        return "\n".join([*lines, "- The batch wrote no summary; see the step log."]) + "\n"
    lines += [
        f"- Batch status: `{summary['status']}`; runs by status: `{summary['counts']}`",
        "",
        "| Run | Status | Scenario | Seed | Valkey SHA | Summary | Failed components |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for run, outcome in runs:
        plan = outcome.get("plan") or {}
        failures = _cell("; ".join(failed_components(outcome)) or "-", limit=200)
        lines.append(
            f"| {run['id']} | {run['status']} | {plan.get('scenario_id') or '-'} | {_or_dash(run.get('seed'))} "
            f"| {outcome.get('valkey_sha') or '-'} | {_cell(run['summary'], limit=200)} | {failures} |"
        )
    if not runs:
        lines.append("| - | - | - | - | - | no run finished | - |")
    lines.append("")
    lines += [f"- Signature ({group['count']} run(s)): {group['label']}" for group in summary["signatures"]]
    lines.append("- Rerun a run with `valkey-fuzzer run runs/<run>/scenario.yaml` in the uploaded batch folder.")
    return "\n".join(lines) + "\n"


def drop_passing_ledgers(batch_dir):
    """Remove the workloads' write ledgers (``ledger.jsonl``, ``preload.jsonl``) of the runs that passed.

    A ledger holds every write a workload made, often tens of megabytes, and
    only the data oracle reads it: once a run passed, nothing needs it, and a
    batch folder that keeps them all grows too large to triage.
    """

    _, runs = batch_runs(batch_dir)
    for run, _ in runs:
        if run["status"] != "PASS" or not run["report"]:
            continue
        run_dir = os.path.join(batch_dir, os.path.dirname(run["report"]))
        for name in ("ledger.jsonl", "preload.jsonl"):
            for path in glob.glob(os.path.join(run_dir, "components", "*", name)):
                os.remove(path)


# The paths a spec bundle in spec_bundles may hold.
_FILE_NAME = r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}"
_BUNDLE_PATHS = (
    rf"(?:claims|mapping)\.yaml|model\.tla|{_FILE_NAME}\.cfg"
    rf"|negative-controls/{_FILE_NAME}\.(?:cfg|tla)|experiments/{_FILE_NAME}\.ya?ml"
)


def _yaml_mapping(text, what):
    # Imported here: the PR fuzzer's resolve and matrix steps run before anything is installed.
    import yaml

    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise SystemExit(f"{what} is not YAML: {exc}") from None
    if not isinstance(document, dict):
        raise SystemExit(f"{what} must be a mapping")
    return document


def check_inputs(env):
    """Check fuzzer-pr-batch.yml's inputs, and write its scenarios to CANDIDATES_DIR and its bundles to SPEC_DIR."""

    raw = env["INPUT_SCENARIOS"]
    if len(raw) > 60000:
        raise SystemExit(f"scenarios is {len(raw)} characters; the limit is 60,000")
    raw_bundles = env["INPUT_SPEC_BUNDLES"]
    if len(raw_bundles) > 60000:
        raise SystemExit(f"spec_bundles is {len(raw_bundles)} characters; the limit is 60,000")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", env["INPUT_VALKEY_REPOSITORY"]):
        raise SystemExit("valkey_repository must be OWNER/NAME")
    if not re.fullmatch(r"[0-9a-f]{40}", env["INPUT_VALKEY_SHA"]):
        raise SystemExit("valkey_sha must be a full 40-character lowercase commit SHA")
    if not re.fullmatch(r"[0-9]|1[0-9]|20", env["INPUT_RANDOM_SEEDS"]):
        raise SystemExit("random_seeds must be 0 to 20")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{0,100}", env["INPUT_REQUEST_ID"]):
        raise SystemExit("request_id must be at most 100 letters, digits, '.', '_', or '-'")
    try:
        scenarios = json.loads(raw)
    except ValueError as exc:
        raise SystemExit(f"scenarios is not JSON: {exc}") from None
    if not isinstance(scenarios, dict):
        raise SystemExit("scenarios must be a JSON object mapping a name to scenario YAML")
    if not scenarios and env["INPUT_RANDOM_SEEDS"] == "0":
        raise SystemExit("scenarios is empty and random_seeds is 0: there is nothing to run")
    out = Path(env["CANDIDATES_DIR"])
    out.mkdir(parents=True)
    for name, text in scenarios.items():
        if not re.fullmatch(_FILE_NAME, name) or ".." in name:
            raise SystemExit(f"scenario name {name!r} must be letters, digits, '.', '_', or '-'")
        if not isinstance(text, str):
            raise SystemExit(f"scenario {name}: the value must be YAML text")
        document = _yaml_mapping(text, f"scenario {name}")
        # Each of these names a binary the batch would run instead of valkey_sha's build.
        for field in ("valkey_binary", "instance_binaries"):
            if field in (document.get("deployment") or {}):
                raise SystemExit(f"scenario {name} sets deployment.{field}; the batch runs valkey_sha's build")
        if "artifacts" in document:
            raise SystemExit(f"scenario {name} declares artifacts; the batch runs valkey_sha's build")
        (out / f"{name}.yaml").write_text(text, encoding="utf-8")
    print(f"{len(scenarios)} scenario(s) written to {out}")

    # spec_bundles: {bundle name: {path in the bundle: text}}, written under SPEC_DIR.
    try:
        bundles = json.loads(raw_bundles) if raw_bundles.strip() else {}
    except ValueError as exc:
        raise SystemExit(f"spec_bundles is not JSON: {exc}") from None
    if not isinstance(bundles, dict):
        raise SystemExit("spec_bundles must be a JSON object mapping a bundle name to its files")
    spec_dir = Path(env["SPEC_DIR"])
    spec_dir.mkdir(parents=True)
    for bundle, files in bundles.items():
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", bundle):
            raise SystemExit(f"spec bundle name {bundle!r} must be lowercase letters, digits, or '-'")
        if not isinstance(files, dict) or not files:
            raise SystemExit(f"spec bundle {bundle}: the value must map paths in the bundle to file text")
        for required in ("claims.yaml", "model.tla", "mapping.yaml"):
            if required not in files:
                raise SystemExit(f"spec bundle {bundle} has no {required}")
        for path, text in files.items():
            if not isinstance(path, str) or not re.fullmatch(_BUNDLE_PATHS, path) or ".." in path:
                raise SystemExit(
                    f"spec bundle {bundle}: {path!r} must be claims.yaml, model.tla, mapping.yaml, <name>.cfg, "
                    "negative-controls/<name>.cfg or .tla, or experiments/<name>.yaml"
                )
            if not isinstance(text, str):
                raise SystemExit(f"spec bundle {bundle}: {path} must be file text")
            if path.endswith((".yaml", ".yml")):
                _yaml_mapping(text, f"spec bundle {bundle}: {path}")
            target = spec_dir / bundle / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        # The same checks the batch makes when a scenario names the bundle.
        from valkey_fuzzer.spec.bundle import SpecBundleError, load_bundle

        try:
            load_bundle(str(spec_dir / bundle))
        except SpecBundleError as exc:
            raise SystemExit(f"spec bundle {bundle} is invalid: {exc}") from None
    print(f"{len(bundles)} spec bundle(s) written to {spec_dir}")


def _print_outputs(values):
    for name, value in values.items():
        print(f"{name}={value}")


def main(command):
    env = os.environ
    if command == "resolve":
        names = ("PR_LINK", "VALKEY_REPOSITORY", "VALKEY_REF", "PR_NUMBER", "PR_URL")
        _print_outputs(resolve(**{name.lower(): env[f"INPUT_{name}"] for name in names}))
    elif command == "matrix":
        count = run_count(env["RUN_COUNT"])
        _print_outputs({"run_count": count, "runs": json.dumps([{"run_id": run_id} for run_id in range(1, count + 1)])})
    elif command == "metadata":
        os.makedirs(env["RUN_DIR"], exist_ok=True)
        result = metadata(
            int(env["RUN_ID"]), os.path.join(env["RUN_DIR"], "batch"), env["SETUP_OUTCOME"], env["FUZZER_OUTCOME"]
        )
        with open(os.path.join(env["RUN_DIR"], "metadata.json"), "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2)
    elif command == "summarize":
        results = {}
        for path in sorted(glob.glob(os.path.join(env["RESULTS_DIR"], "**", "metadata.json"), recursive=True)):
            with open(path, encoding="utf-8") as handle:
                result = json.load(handle)
            results[int(result["run_id"])] = result
        summary, comment, overall_failed = summarize(env, results)
        with open(env["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as handle:
            handle.write(summary)
        if comment is not None:
            with open("pr-comment.md", "w", encoding="utf-8") as handle:
                handle.write(comment)
        with open(env["GITHUB_OUTPUT"], "a", encoding="utf-8") as handle:
            handle.write(f"overall_failed={'true' if overall_failed else 'false'}\n")
    elif command == "check-inputs":
        check_inputs(env)
    elif command == "drop-ledgers":
        drop_passing_ledgers(env["BATCH_DIR"])
    elif command == "summary":
        valkey = ""
        if env.get("VALKEY_SHA"):
            valkey = f"`{env['VALKEY_REPOSITORY']}` `{env['VALKEY_REF'] or '-'}` `{env['VALKEY_SHA']}`"
        with open(env["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as handle:
            handle.write(batch_summary(env["SUMMARY_TITLE"], env["BATCH_DIR"], valkey))
    else:
        raise SystemExit(
            f"unknown command {command!r}; expected resolve, matrix, metadata, summarize, check-inputs, drop-ledgers, "
            "or summary"
        )


if __name__ == "__main__":
    main(sys.argv[1])
