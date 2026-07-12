"""Tests for durable release manifests and offline verification."""

import json

from fliptop import PROJECT_ROOT, RAW_DATA_DIR
from fliptop.integrity import canonical_file_bytes, file_fingerprint
from fliptop.pipeline import build_pipeline_run
from fliptop.release import build_candidate_artifacts, publish_candidate_bundle
from fliptop.verify_release import main, verify_release_manifest


def _published_release(tmp_path):
    candidate = build_candidate_artifacts(build_pipeline_run(RAW_DATA_DIR))
    processed_dir = tmp_path / "processed"
    publish_candidate_bundle(candidate, processed_dir)
    return processed_dir, processed_dir / "release_manifest.json"


def test_publication_includes_a_verifiable_manifest(tmp_path):
    processed_dir, manifest_path = _published_release(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["schema_version"] == 2
    assert len(manifest["pipeline_commit"]) == 40
    assert manifest["inputs"]
    assert {item.rsplit("/", 1)[-1] for item in manifest["outputs"]} == {
        "ft_battles.json",
        "battle_participants.csv",
        "emcees.csv",
    }
    assert manifest["counts"]["candidate_battles"] == 1275
    assert manifest["release_problems"] == []
    assert verify_release_manifest(manifest_path, project_root=PROJECT_ROOT) == []
    assert (processed_dir / "ft_battles.json").exists()


def test_text_fingerprints_ignore_platform_line_endings(tmp_path):
    windows_file = tmp_path / "windows.csv"
    unix_file = tmp_path / "unix.csv"
    windows_file.write_bytes(b"name,value\r\nalpha,1\r\nbeta,2\r\n")
    unix_file.write_bytes(b"name,value\nalpha,1\nbeta,2\n")

    assert file_fingerprint(windows_file) == file_fingerprint(unix_file)


def test_verifier_accepts_equivalent_windows_line_endings(tmp_path):
    processed_dir, manifest_path = _published_release(tmp_path)
    participants_path = processed_dir / "battle_participants.csv"
    canonical = canonical_file_bytes(participants_path)
    participants_path.write_bytes(canonical.replace(b"\n", b"\r\n"))

    assert verify_release_manifest(manifest_path, project_root=PROJECT_ROOT) == []


def test_verifier_detects_output_tampering(tmp_path):
    processed_dir, manifest_path = _published_release(tmp_path)
    with (processed_dir / "ft_battles.json").open("a", encoding="utf-8") as output:
        output.write("{}\n")

    problems = verify_release_manifest(manifest_path, project_root=PROJECT_ROOT)

    assert any("ft_battles.json: sha256 mismatch" in problem for problem in problems)
    assert any("ft_battles.json: row-count mismatch" in problem for problem in problems)


def test_verifier_detects_contract_version_drift(tmp_path):
    _, manifest_path = _published_release(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["contract_versions"]["raw.youtube_uploads"] = 999
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    problems = verify_release_manifest(manifest_path, project_root=PROJECT_ROOT)

    assert "contract versions do not match the current code" in problems


def test_verifier_cli_returns_failure_for_missing_manifest(tmp_path, capsys):
    try:
        main(["--manifest", str(tmp_path / "missing.json")])
    except SystemExit as exc:
        assert exc.code == 1
    else:  # pragma: no cover
        raise AssertionError("expected verification failure")
    assert "release FAILED" in capsys.readouterr().out
