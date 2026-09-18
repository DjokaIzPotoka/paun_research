import hashlib

import pytest

from research.report.config import MissingConfigError, load_config, write_report


def test_missing_required_parameter_refuses_to_start(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("run_id: smoke\n", encoding="utf-8")

    with pytest.raises(MissingConfigError) as excinfo:
        load_config(config_path, required_keys=["run_id", "seed"])

    assert "seed" in str(excinfo.value)


def test_no_default_is_substituted_for_missing_parameter(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("run_id: smoke\n", encoding="utf-8")

    with pytest.raises(MissingConfigError) as excinfo:
        load_config(config_path, required_keys=["run_id", "seed"])

    assert excinfo.value.missing_keys == ["seed"]


def test_hash_matches_exact_file_bytes(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("run_id: smoke\nseed: 42\n", encoding="utf-8")

    run_config = load_config(config_path, required_keys=["run_id", "seed"])

    expected_hash = hashlib.sha256(config_path.read_bytes()).hexdigest()
    assert run_config.content_hash == expected_hash


def test_report_embeds_the_config_hash(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("run_id: smoke\nseed: 42\n", encoding="utf-8")
    run_config = load_config(config_path, required_keys=["run_id", "seed"])

    report_path = write_report(run_config, runs_dir=tmp_path / "runs")

    assert run_config.content_hash in report_path.read_text(encoding="utf-8")
