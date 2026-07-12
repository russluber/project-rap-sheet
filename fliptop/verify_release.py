"""Offline verification for committed processed-data releases."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from pathlib import Path

from . import PROCESSED_DATA_DIR, PROJECT_ROOT
from .contracts import contract_versions
from .integrity import file_fingerprint

EXPECTED_OUTPUT_FILENAMES = {
    "ft_battles.json",
    "battle_participants.csv",
    "emcees.csv",
}


def _resolve_recorded_path(label: str, project_root: Path) -> Path:
    path = Path(label)
    return path if path.is_absolute() else project_root / path


def _row_count(path: Path) -> int:
    if path.suffix == ".json":
        with path.open(encoding="utf-8") as source:
            return sum(1 for line in source if line.strip())
    with path.open(newline="", encoding="utf-8") as source:
        return max(sum(1 for _ in csv.reader(source)) - 1, 0)


def _verify_recorded_files(
    records,
    *,
    project_root: Path,
    include_rows: bool,
) -> list[str]:
    problems: list[str] = []
    if not isinstance(records, dict) or not records:
        return ["manifest file records are missing or empty"]

    for label, record in records.items():
        if not isinstance(record, dict):
            problems.append(f"{label}: manifest record is not an object")
            continue
        path = _resolve_recorded_path(label, project_root)
        if not path.exists():
            problems.append(f"{label}: file is missing")
            continue
        actual = file_fingerprint(path)
        if record.get("sha256") != actual["sha256"]:
            problems.append(f"{label}: sha256 mismatch")
        if record.get("canonical_bytes") != actual["canonical_bytes"]:
            problems.append(f"{label}: canonical byte-size mismatch")
        if include_rows:
            try:
                rows = _row_count(path)
            except (OSError, csv.Error, UnicodeError) as exc:
                problems.append(f"{label}: cannot count rows ({exc})")
            else:
                if record.get("rows") != rows:
                    problems.append(f"{label}: row-count mismatch")
    return problems


def verify_release_manifest(
    manifest_path: Path = PROCESSED_DATA_DIR / "release_manifest.json",
    *,
    project_root: Path = PROJECT_ROOT,
) -> list[str]:
    """Return integrity/provenance problems for an official release manifest."""
    manifest_path = Path(manifest_path)
    project_root = Path(project_root)
    if not manifest_path.exists():
        return [f"release manifest is missing: {manifest_path}"]
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"release manifest is unreadable: {exc}"]

    problems: list[str] = []
    if manifest.get("schema_version") != 2:
        problems.append("unsupported release manifest schema_version")
    if manifest.get("contract_versions") != contract_versions():
        problems.append("contract versions do not match the current code")
    if manifest.get("release_problems"):
        problems.append("manifest records release blockers")

    pipeline_commit = manifest.get("pipeline_commit")
    if not isinstance(pipeline_commit, str) or len(pipeline_commit) != 40:
        problems.append("pipeline_commit is missing or invalid")
    elif (project_root / ".git").exists():
        commit_exists = subprocess.run(
            ["git", "cat-file", "-e", f"{pipeline_commit}^{{commit}}"],
            cwd=project_root,
            capture_output=True,
            check=False,
        ).returncode == 0
        if not commit_exists:
            problems.append("pipeline_commit does not exist in this Git repository")

    inputs = manifest.get("inputs")
    outputs = manifest.get("outputs")
    problems.extend(
        _verify_recorded_files(
            inputs,
            project_root=project_root,
            include_rows=False,
        )
    )
    problems.extend(
        _verify_recorded_files(
            outputs,
            project_root=project_root,
            include_rows=True,
        )
    )
    if isinstance(outputs, dict):
        output_names = {Path(label).name for label in outputs}
        if output_names != EXPECTED_OUTPUT_FILENAMES:
            problems.append("manifest does not describe the complete processed output bundle")
    return problems


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Verify the committed FlipTop processed-data release.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=PROCESSED_DATA_DIR / "release_manifest.json",
    )
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    args = parser.parse_args(argv)

    problems = verify_release_manifest(args.manifest, project_root=args.project_root)
    if problems:
        print("[verify] release FAILED:")
        for problem in problems:
            print(f"  - {problem}")
        raise SystemExit(1)
    print(f"[verify] release OK -> {args.manifest}")


if __name__ == "__main__":
    main()
