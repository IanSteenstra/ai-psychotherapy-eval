"""CSV logs and per-pairing checkpoints for a run's output directory.

Layout of an output directory:
  pairings.csv, patient_personas.csv, therapists.csv   what was simulated
  conversation_log.csv, survey_*.csv, *_eval_logs.csv  results (one CSV per instrument)
  state/pairing_<id>.json                              checkpoint used to resume an interrupted run
  prompt_logs/                                         rendered prompts (only with simulation.save_prompts)
  usage.json                                           token usage per model

A row only counts once the pairing's checkpoint says its stage finished. When a run
is resumed, rows written after the last checkpoint (e.g. during a crash) are removed
so every turn, survey and report appears exactly once.
"""

from __future__ import annotations

import csv
import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

from . import constants as C
from . import scoring


@dataclass
class PairingState:
    pairing_id: int
    therapist_id: str
    patient_id: str
    session: int = 1
    stage: str = "start"
    turn: int = 0
    status: str = "pending"  # pending | in_progress | completed | terminated | failed
    termination_reason: str | None = None
    psych_state: dict | None = None
    therapist_state: dict = field(default_factory=dict)
    error: str | None = None

    @property
    def finished(self) -> bool:
        return self.status in ("completed", "terminated")

    def stage_index(self) -> int:
        return C.SESSION_STAGES.index(self.stage)


def _schema_headers(schema_file: str, base: list[str]) -> list[str]:
    with open(C.SCHEMA_DIR / schema_file, encoding="utf-8") as f:
        schema = json.load(f)
    headers = list(base)
    for key, value in schema["properties"].items():
        if value.get("type") == "object" and "properties" in value:
            headers.extend(f"{key}_{sub}" for sub in value["properties"])
        else:
            headers.append(key)
    return headers


def _build_log_headers() -> dict[str, list[str]]:
    session = ["pairing_id", "session_id"]
    turn = ["pairing_id", "session_id", "turn"]
    constructs = C.PSYCHOLOGICAL_CONSTRUCTS_KEYS
    events = [f"{e}_{part}" for e in C.ADVERSE_EVENT_KEYS for part in ("occurred", "attribution", "internal_justification")]
    neq = [f"question{i}_{part}" for i in range(1, C.NEQ_QUESTION_COUNT + 1) for part in ("experienced", "severity", "cause")]
    return {
        C.CONVERSATION_LOG: turn + ["speaker", "message", "session_conclusion", "appraisal_internal_reflection", "internal_justification", "goal", "strategy", "tactic"] + constructs,
        C.CRISIS_EVAL_LOG: _schema_headers("crisis_schema.json", turn),
        C.ACTION_PLAN_EVAL_LOG: _schema_headers("action_plan_schema.json", turn),
        C.MI_BEHAVIOR_EVAL_LOG: session + ["reasoning"] + C.MI_BEHAVIOR_CODES + scoring.MI_BEHAVIOR_METRIC_COLUMNS,
        C.MI_GLOBAL_EVAL_LOG: _schema_headers("global_scores_schema.json", session) + scoring.MI_GLOBAL_SCORE_COLUMNS,
        C.SURE_SURVEY_LOG: _schema_headers("survey_sure_schema.json", session) + scoring.SURE_SCORE_COLUMNS,
        C.SRS_SURVEY_LOG: _schema_headers("survey_srs_schema.json", session) + scoring.SRS_SCORE_COLUMNS,
        C.WAI_SURVEY_LOG: _schema_headers("survey_wai_schema.json", session) + scoring.WAI_SCORE_COLUMNS,
        C.NEQ_SURVEY_LOG: session + neq + ["other_incidents_or_effects"] + scoring.NEQ_SCORE_COLUMNS,
        C.AFTER_SESSION_REPORT_LOG: session + ["journal_summary", "state_change_justification"] + events + constructs,
    }


LOG_HEADERS = _build_log_headers()


def row_is_committed(log_name: str, state: PairingState | None, session: int, turn: int | None) -> bool:
    if state is None or state.status == "pending":
        return False
    if session < state.session:
        return True
    if session > state.session:
        return False
    required = C.SESSION_STAGES.index(C.LOG_STAGE[log_name])
    if state.stage_index() >= required:
        return True
    if log_name in C.TURN_LEVEL_LOGS and state.stage_index() >= C.SESSION_STAGES.index("sure_done"):
        return turn is not None and turn <= state.turn
    return False


class RunStore:
    def __init__(self, output_dir: Path, save_prompts: bool = False):
        self.output_dir = Path(output_dir)
        self.state_dir = self.output_dir / "state"
        self.prompt_dir = self.output_dir / "prompt_logs"
        self.save_prompts = save_prompts
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(exist_ok=True)
        if save_prompts:
            self.prompt_dir.mkdir(exist_ok=True)
        for name, headers in LOG_HEADERS.items():
            path = self.output_dir / name
            if path.exists() and path.stat().st_size > 0:
                with open(path, newline="", encoding="utf-8") as f:
                    existing = next(csv.reader(f), [])
                if existing != headers:
                    raise RuntimeError(
                        f"{path} has different columns than this version writes. "
                        "Use a new output_dir (or --output) for this run."
                    )
            else:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(headers)

    # --- logs ---
    def append(self, log_name: str, rows: list[dict]) -> None:
        if not rows:
            return
        with open(self.output_dir / log_name, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LOG_HEADERS[log_name], extrasaction="ignore")
            writer.writerows(rows)
            f.flush()
            os.fsync(f.fileno())

    def read_rows(self, log_name: str) -> list[dict]:
        with open(self.output_dir / log_name, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    def remove_uncommitted_rows(self, states: dict[int, PairingState]) -> int:
        """Drop rows not covered by a checkpoint. Must run before any pairing starts."""
        removed = 0
        for log_name, headers in LOG_HEADERS.items():
            rows = self.read_rows(log_name)
            kept = []
            for row in rows:
                try:
                    pairing_id, session = int(row["pairing_id"]), int(row["session_id"])
                    turn = int(row["turn"]) if row.get("turn") not in (None, "") else None
                except (KeyError, ValueError):
                    continue
                if row_is_committed(log_name, states.get(pairing_id), session, turn):
                    kept.append(row)
            if len(kept) != len(rows):
                removed += len(rows) - len(kept)
                tmp = self.output_dir / f".{log_name}.tmp"
                with open(tmp, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(kept)
                os.replace(tmp, self.output_dir / log_name)
        return removed

    # --- checkpoints ---
    def load_states(self) -> dict[int, PairingState]:
        states = {}
        for path in self.state_dir.glob("pairing_*.json"):
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            state = PairingState(**data)
            states[state.pairing_id] = state
        return states

    def save_state(self, state: PairingState) -> None:
        path = self.state_dir / f"pairing_{state.pairing_id}.json"
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(asdict(state), f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    # --- prompts ---
    def write_prompt(self, pairing_id: int, session_id: int, target: str, prompt: str) -> None:
        if not self.save_prompts:
            return
        safe_target = "".join(c for c in target if c.isalnum() or c in ("_", "-"))
        path = self.prompt_dir / f"p{pairing_id}_s{session_id}_{safe_target}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"--- PROMPT FOR: {target} ---\n")
            f.write(f"--- Pairing ID: {pairing_id}, Session ID: {session_id} ---\n")
            f.write("--------------------------------------------------\n\n")
            f.write(prompt)
