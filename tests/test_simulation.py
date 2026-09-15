import json
from collections import Counter

import pytest

from aipsycheval import constants as C
from aipsycheval import llm
from aipsycheval.storage import LOG_HEADERS

from .conftest import read_csv, run_simulation


def snapshot(output_dir):
    """All result rows per log, order-independent."""
    return {name: Counter(tuple(sorted(row.items())) for row in read_csv(output_dir / name)) for name in LOG_HEADERS}


def assert_no_duplicates(output_dir):
    for name in LOG_HEADERS:
        keys = ["pairing_id", "session_id"] + (["turn"] if name in C.TURN_LEVEL_LOGS else []) + (["speaker"] if name == C.CONVERSATION_LOG else [])
        counts = Counter(tuple(row[k] for k in keys) for row in read_csv(output_dir / name))
        assert max(counts.values(), default=1) == 1, f"duplicate rows in {name}"


def test_mock_run_produces_complete_logs(mock_config, tmp_path):
    assert run_simulation(mock_config()) == 0
    out = tmp_path / "run"
    states = [json.loads(p.read_text()) for p in (out / "state").glob("pairing_*.json")]
    assert len(states) == 6 and all(s["status"] in ("completed", "terminated") for s in states)
    assert_no_duplicates(out)

    pairings = {int(r["pairing_id"]): r["therapist_id"] for r in read_csv(out / "pairings.csv")}
    sessions_run = {(int(r["pairing_id"]), int(r["session_id"])) for r in read_csv(out / C.SURE_SURVEY_LOG)}
    for log in (C.NEQ_SURVEY_LOG, C.AFTER_SESSION_REPORT_LOG):
        assert {(int(r["pairing_id"]), int(r["session_id"])) for r in read_csv(out / log)} == sessions_run
    conversational = {key for key in sessions_run if pairings[key[0]] != "therapist_psych_material"}
    for log in (C.MI_BEHAVIOR_EVAL_LOG, C.MI_GLOBAL_EVAL_LOG, C.SRS_SURVEY_LOG, C.WAI_SURVEY_LOG):
        assert {(int(r["pairing_id"]), int(r["session_id"])) for r in read_csv(out / log)} == conversational

    wai = read_csv(out / C.WAI_SURVEY_LOG)[0]
    assert int(wai["composite_wai"]) == sum(int(wai[c]) for c in ("total_wai_task", "total_wai_bond", "total_wai_goal"))
    assert {r["therapist_id"] for r in read_csv(out / "therapists.csv")} == {"mock_mi", "mock_chat", "therapist_psych_material"}
    assert json.loads((out / "usage.json").read_text())["mock"]["calls"] > 0


def test_rerunning_a_finished_run_changes_nothing(mock_config, tmp_path):
    config = mock_config()
    run_simulation(config)
    before = snapshot(tmp_path / "run")
    assert run_simulation(config) == 0
    assert snapshot(tmp_path / "run") == before


def test_resume_after_hard_crash_matches_uninterrupted_run(mock_config, tmp_path):
    run_simulation(mock_config("reference"))
    reference = snapshot(tmp_path / "reference")

    run_simulation(mock_config("crashed"))
    out = tmp_path / "crashed"
    # Simulate a process killed right after turn 2 of session 2 was written but before later
    # checkpoints: roll the checkpoint back; every row after it is now uncommitted.
    pairing = next(int(r["pairing_id"]) for r in read_csv(out / "pairings.csv") if r["therapist_id"] == "mock_mi" and r["patient_id"] == "1")
    state_path = out / "state" / f"pairing_{pairing}.json"
    state = json.loads(state_path.read_text())
    if state["session"] < 2:
        pytest.skip("mock pairing ended after session 1")
    patient_row = next(r for r in read_csv(out / C.CONVERSATION_LOG) if int(r["pairing_id"]) == pairing and r["session_id"] == "2" and r["turn"] == "2" and r["speaker"] == "Patient")
    state.update(status="in_progress", session=2, stage="sure_done", turn=2, termination_reason=None,
                 psych_state={k: int(float(patient_row[k])) for k in C.PSYCHOLOGICAL_CONSTRUCTS_KEYS})
    state_path.write_text(json.dumps(state))

    assert run_simulation(mock_config("crashed")) == 0
    assert_no_duplicates(out)
    assert snapshot(out) == reference


def test_failed_pairings_resume_to_identical_results(mock_config, tmp_path, monkeypatch):
    run_simulation(mock_config("reference"))
    reference = snapshot(tmp_path / "reference")

    real_generate = llm.MockProvider.generate
    calls = {"n": 0}

    class Unauthorized(Exception):
        status_code = 401

    def flaky(self, messages, system, schema, json_mode):
        calls["n"] += 1
        if calls["n"] % 7 == 0:
            raise llm.InvalidResponse("transient: malformed JSON")  # retried
        if calls["n"] > 60:
            raise Unauthorized("invalid API key")  # not retried: pairing fails
        return real_generate(self, messages, system, schema, json_mode)

    monkeypatch.setattr(llm.MockProvider, "generate", flaky)
    assert run_simulation(mock_config("flaky")) > 0
    failed = [json.loads(p.read_text()) for p in (tmp_path / "flaky" / "state").glob("*.json")]
    assert any(s["status"] == "failed" and "invalid API key" in s["error"] for s in failed)

    monkeypatch.setattr(llm.MockProvider, "generate", real_generate)
    assert run_simulation(mock_config("flaky")) == 0
    assert_no_duplicates(tmp_path / "flaky")
    assert snapshot(tmp_path / "flaky") == reference


def test_adding_a_therapist_keeps_existing_pairing_ids(mock_config, tmp_path):
    config = mock_config()
    run_simulation(config)
    before = read_csv(tmp_path / "run" / "pairings.csv")
    text = config.read_text().replace("therapists:\n", "therapists:\n  - {id: added, type: python, function: tests.helpers:echo}\n")
    config.write_text(text)
    assert run_simulation(config) == 0
    after = read_csv(tmp_path / "run" / "pairings.csv")
    assert after[: len(before)] == before
    assert {r["therapist_id"] for r in after[len(before):]} == {"added"}
