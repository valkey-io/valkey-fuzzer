"""The Python steps of the fuzzer's workflows. Each command reads what its step passes in the environment.

valkey-pr-fuzzer.yml:
resolve       the Valkey repository, ref, and PR to test, as GITHUB_OUTPUT lines
matrix        the checked run_count and the random-fuzzer matrix, as GITHUB_OUTPUT lines
metadata      one run's metadata.json, from its batch folder
summarize     the job summary, the PR comment (its first line the marker the comment step finds it by),
              and whether the workflow failed

fuzzer-pr-batch.yml:
check-inputs  check the dispatch inputs, and write the scenarios, spec bundles, and experiments they carry
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


def batch_summary(title, batch_dir, valkey="", parent="", build="", module="", module_parent=""):
    """The job summary of one batch folder: its status, each run, and the failure signatures."""

    lines = [f"## {title}", ""] + ([f"- Valkey: {valkey}"] if valkey else [])
    lines += [f"- Parent commit (artifacts.parent): `{parent}`"] if parent else []
    lines += [f"- Build: `{build}`"] if build else []
    lines += [f"- Module (artifacts.module): {module}"] if module else []
    lines += [f"- Module parent commit (artifacts.module_parent): `{module_parent}`"] if module_parent else []
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
# An experiment file in experiments.
_EXPERIMENT_FILE = r"[a-z0-9][a-z0-9_.-]{0,99}\.yaml"
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


# The builds the PR batch offers; the make lines are in the workflow.
BUILDS = ("default", "asan", "ubsan")

# The module repositories the PR batch builds, by repository name (a fork builds as its upstream), and the file
# each build leaves in its checkout; the build commands are the workflow's table. valkey means its tests/modules,
# many modules, so module_file names the one to declare.
MODULE_FILES = {"valkey-json": "build/src/libjson.so", "valkey": None}
_MODULE_FILE = r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*\.so"
# What each artifact the batch may declare needs, for the refusal of a reference to one it does not.
_DECLARED_BY = {
    "artifacts.parent": "valkey_parent_sha",
    "artifacts.module": "module_repository",
    "artifacts.module_parent": "module_parent_sha",
}


def artifacts_block(declared):
    """The YAML appended to every scenario: ``declared`` maps an artifact name to its ``(provider, path)``, such as
    ``parent`` to the parent commit's build."""

    lines = ["artifacts:"]
    for name, (provider, path) in declared.items():
        # A JSON string is a YAML double-quoted scalar, whatever the path holds.
        lines += [f"  {name}:", f"    use: {provider}", "    with:", f"      path: {json.dumps(path)}"]
    return "\n".join(lines) + "\n"


def with_artifacts(text, document, declared, what):
    """``text`` with :func:`artifacts_block` appended, once the result parses to ``document`` plus it.

    The block is appended to the text, not merged into the parsed document, so the scenario runs byte for byte
    as it was sent; a flow-style top-level mapping or a missing final newline makes the append change or break
    the document, and the scenario is refused.
    """

    import yaml

    block = artifacts_block(declared)
    try:
        after = yaml.safe_load(text + block)
    except yaml.YAMLError:
        after = None
    if after != {**document, "artifacts": yaml.safe_load(block)["artifacts"]}:
        raise SystemExit(
            f"{what}: appending the artifacts block does not add exactly that key (a flow-style top-level "
            "mapping, or no final newline?)"
        )
    return text + block


def module_inputs(env):
    """The checked module inputs: ``(repository, sha, parent sha, module file)``, all empty without a module."""

    repository, sha = env["INPUT_MODULE_REPOSITORY"], env["INPUT_MODULE_SHA"]
    parent_sha, module_file = env["INPUT_MODULE_PARENT_SHA"], env["INPUT_MODULE_FILE"]
    if not repository:
        if sha or parent_sha or module_file:
            raise SystemExit("module_sha, module_parent_sha, and module_file need module_repository")
        return "", "", "", ""
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise SystemExit("module_repository must be empty or OWNER/NAME")
    name = repository.split("/")[1]
    if name not in MODULE_FILES:
        raise SystemExit(
            f"module_repository {repository} is not a module the batch builds; it builds {', '.join(MODULE_FILES)}"
        )
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise SystemExit("module_sha must be a full 40-character lowercase commit SHA")
    if parent_sha and not re.fullmatch(r"[0-9a-f]{40}", parent_sha):
        raise SystemExit("module_parent_sha must be empty or a full 40-character lowercase commit SHA")
    if parent_sha == sha:
        raise SystemExit("module_parent_sha is module_sha: a comparison with the same build shows nothing")
    module_file = module_file or MODULE_FILES[name]
    if module_file is None:
        raise SystemExit(f"{name} builds many modules: module_file must name one, such as tests/modules/datatype.so")
    if not re.fullmatch(_MODULE_FILE, module_file) or ".." in module_file.split("/"):
        raise SystemExit("module_file must be a relative path to a .so in the module's checkout")
    return repository, sha, parent_sha, module_file


def artifact_references(value):
    """Every ``artifacts.<id>`` an authored ``{$ref: artifacts.<id>.<output>}`` in ``value`` names, in order seen."""

    found = []
    if isinstance(value, dict):
        reference = value.get("$ref")
        if isinstance(reference, str) and reference.startswith("artifacts."):
            found.append(".".join(reference.split(".")[:2]))
        for item in value.values():
            found += artifact_references(item)
    elif isinstance(value, list):
        for item in value:
            found += artifact_references(item)
    return found


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
    parent_sha = env["INPUT_VALKEY_PARENT_SHA"]
    if parent_sha and not re.fullmatch(r"[0-9a-f]{40}", parent_sha):
        raise SystemExit("valkey_parent_sha must be empty or a full 40-character lowercase commit SHA")
    if parent_sha == env["INPUT_VALKEY_SHA"]:
        raise SystemExit("valkey_parent_sha is valkey_sha: a comparison with the same build shows nothing")
    if env["INPUT_BUILD"] not in BUILDS:
        raise SystemExit(f"build must be one of {', '.join(BUILDS)}")
    module_repository, _, module_parent_sha, module_file = module_inputs(env)
    # What the batch declares to every scenario: the parent build, and the module builds, loaded into the Valkey
    # build under test.
    declared = {}
    if parent_sha:
        declared["parent"] = ("valkey.artifact.local-binary", env["PARENT_BINARY"])
    if module_repository:
        declared["module"] = ("valkey.artifact.local-module", os.path.join(env["MODULE_DIR"], module_file))
    if module_parent_sha:
        declared["module_parent"] = (
            "valkey.artifact.local-module",
            os.path.join(env["MODULE_PARENT_DIR"], module_file),
        )
    # 4 seeds of each one-build experiment is what two workers finish inside the job's timeout.
    if not re.fullmatch(r"[0-4]", env["INPUT_RANDOM_SEEDS"]):
        raise SystemExit("random_seeds must be 0 to 4")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{0,100}", env["INPUT_REQUEST_ID"]):
        raise SystemExit("request_id must be at most 100 letters, digits, '.', '_', or '-'")
    excluded = env["INPUT_EXCLUDE_EXPERIMENTS"].split()
    if len(excluded) > 50 or not all(re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,99}", item) for item in excluded):
        raise SystemExit("exclude_experiments must be at most 50 experiment IDs separated by spaces")
    if excluded and env["INPUT_RANDOM_SEEDS"] == "0":
        raise SystemExit(
            "exclude_experiments leaves experiments out of the random baseline, which random_seeds 0 skips"
        )
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
            raise SystemExit(
                f"scenario {name} declares artifacts; the batch declares them (artifacts.parent, artifacts.module, "
                "artifacts.module_parent)"
            )
        # The batch declares only what it built; a reference to anything else would fail to compile once the
        # batch runs.
        for reference in artifact_references(document):
            if reference.split(".")[1] not in declared:
                needs = _DECLARED_BY.get(reference)
                raise SystemExit(
                    f"scenario {name} references {reference}, which the batch does not declare"
                    + (f" (no {needs})" if needs else "")
                )
        if declared:
            text = with_artifacts(text, document, declared, f"scenario {name}")
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

    # experiments: {file name: experiment YAML}, a proposal's own data-only experiments, written under
    # EXPERIMENTS_DIR (only when there are any), which the batch loads with --experiment-dir.
    raw_experiments = env["INPUT_EXPERIMENTS"]
    if len(raw_experiments) > 20000:
        raise SystemExit(f"experiments is {len(raw_experiments)} characters; the limit is 20,000")
    try:
        experiments = json.loads(raw_experiments) if raw_experiments.strip() else {}
    except ValueError as exc:
        raise SystemExit(f"experiments is not JSON: {exc}") from None
    if not isinstance(experiments, dict):
        raise SystemExit("experiments must be a JSON object mapping a file name to experiment YAML")
    experiments_dir = Path(env["EXPERIMENTS_DIR"])
    for name, text in experiments.items():
        if not re.fullmatch(_EXPERIMENT_FILE, name) or ".." in name:
            raise SystemExit(
                f"experiment file name {name!r} must be <name>.yaml, the name lowercase letters, digits, '.', '_', "
                "or '-'"
            )
        if not isinstance(text, str):
            raise SystemExit(f"experiment {name}: the value must be YAML text")
        document = _yaml_mapping(text, f"experiment {name}")
        if not isinstance(document.get("id"), str) or not document["id"].startswith("proposed."):
            raise SystemExit(f"experiment {name}: id must start with 'proposed.'")
        experiments_dir.mkdir(parents=True, exist_ok=True)
        (experiments_dir / name).write_text(text, encoding="utf-8")
    # The catalog the batch builds from every directory it is given: what does not load fails here, not there.
    directories = [str(path) for path in sorted(spec_dir.glob("*/experiments")) if path.is_dir()]
    if experiments:
        directories.append(str(experiments_dir))
    if directories:
        from valkey_fuzzer.service import RunService

        try:
            catalog = RunService(experiment_directories=tuple(directories)).catalog
        except ValueError as exc:  # an experiment document or registration error
            raise SystemExit(f"the experiments do not load: {exc}") from None
        print(
            f"the catalog loads with {len(directories)} experiment directory(ies): {len(catalog.experiments)} experiments"
        )
    print(f"{len(experiments)} experiment(s) written to {experiments_dir}")


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
        module = ""
        if env.get("MODULE_REPOSITORY"):
            module = f"`{env['MODULE_REPOSITORY']}` `{env['MODULE_SHA']}`"
        with open(env["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as handle:
            handle.write(
                batch_summary(
                    env["SUMMARY_TITLE"],
                    env["BATCH_DIR"],
                    valkey,
                    parent=env.get("VALKEY_PARENT_SHA", ""),
                    build=env.get("BUILD", ""),
                    module=module,
                    module_parent=env.get("MODULE_PARENT_SHA", ""),
                )
            )
    else:
        raise SystemExit(
            f"unknown command {command!r}; expected resolve, matrix, metadata, summarize, check-inputs, drop-ledgers, "
            "or summary"
        )


if __name__ == "__main__":
    main(sys.argv[1])
