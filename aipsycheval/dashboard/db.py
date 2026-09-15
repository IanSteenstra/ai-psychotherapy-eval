"""Load a results directory of CSVs into an in-memory DuckDB database.

Works with the output of `aipsycheval run`, the `dataset/` folder, and the unzipped
AI_Psychotherapy_Eval_Dataset.zip (which uses different file and column names).
The database is rebuilt automatically when the CSV files change, so the dashboard
can be left open while a simulation is running.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

import duckdb
import pandas as pd

from .. import constants as C
from .. import scoring
from ..storage import LOG_HEADERS

logger = logging.getLogger(__name__)

# Table name -> candidate file names (first match wins).
TABLE_FILES = {
    "simulation_pairings": ["pairings.csv"],
    "patient_personas": ["patient_personas.csv"],
    "therapists": ["therapists.csv"],
    "conversation_log": ["conversation_log.csv", "conversations.csv"],
    "crisis_eval_logs": ["crisis_eval_logs.csv", "eval_crisis_detection.csv"],
    "action_plan_eval_logs": ["action_plan_eval_logs.csv", "eval_crisis_protocol_adherence.csv"],
    "mi_batch_behavior_eval_logs": ["mi_batch_behavior_eval_logs.csv", "eval_mi_behavior_counts.csv"],
    "mi_global_eval_logs": ["mi_global_eval_logs.csv", "mi_global_eval_logs_with_globals.csv", "eval_mi_global_ratings.csv"],
    "survey_srs_logs": ["survey_srs_logs.csv", "survey_srs_logs_with_total.csv", "survey_srs.csv"],
    "survey_sure_logs": ["survey_sure_logs.csv", "survey_sure.csv"],
    "survey_wai_logs": ["survey_wai_logs.csv", "survey_wai.csv"],
    "survey_neq_logs": ["survey_neq_logs.csv", "survey_neq.csv"],
    "after_session_reports": ["after_session_reports.csv", "between_session_journals.csv"],
}

COLUMN_RENAMES = {
    "patient_personas": {"stage_of_change": "state_of_change"},
    "mi_batch_behavior_eval_logs": {
        "Persuade with": "Persuade_with",
        "giving_information": "GI",
        "persuade": "Persuade",
        "persuade_with_permission": "Persuade_with",
        "questions": "Q",
        "simple_reflections": "SR",
        "complex_reflections": "CR",
        "affirmations": "AF",
        "seek_collaboration": "Seek",
        "emphasize_control": "Emphasize",
        "confrontation": "Confront",
    },
}

PERSONA_COLUMNS = [
    "patient_id", "name", "subtype_name", "ad_subtype_description", "age_onset", "aud_severity_symptoms",
    "drinking_pattern", "family_history_of_alcohol_dependence", "antisocial_personality_disorder",
    "comorbid_psychiatric_disorders", "comorbid_substance_use", "psychosocial_indicators", "help_seeking_behavior",
    "state_of_change", "persona_description",
] + C.PSYCHOLOGICAL_CONSTRUCTS_KEYS

EXPECTED_COLUMNS = {
    "simulation_pairings": ["pairing_id", "therapist_id", "patient_id"],
    "patient_personas": PERSONA_COLUMNS,
    "therapists": ["therapist_id", "label", "type", "description"],
    **{Path(file).stem: headers for file, headers in LOG_HEADERS.items()},
}

INTEGER_COLUMNS = {"pairing_id", "session_id", "turn"}
FLOAT_PREFIXES = ("percent_", "neq_avg_")
FLOAT_COLUMNS = {
    "relationship", "goals_and_topics", "approach_or_method", "overall", "total", "r_q_ratio",
    "technical_global", "relational_global", *C.PSYCHOLOGICAL_CONSTRUCTS_KEYS,
}
BOOLEAN_COLUMNS = {"session_conclusion", "assess", "de_escalate", "recommend_emergency_services", "request_human_consultation"}
NUMERIC_PREFIXES = ("total_", "composite_", "neq_", "cultivating_change_talk_score", "softening_sustain_talk_score", "partnership_score", "empathy_score")


def _is_boolean_column(column: str) -> bool:
    return column in BOOLEAN_COLUMNS or column.endswith("_occurred") or column.endswith("_experienced")


def _to_boolean(series: pd.Series) -> pd.Series:
    mapping = {"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False}
    return series.map(lambda v: v if isinstance(v, bool) else mapping.get(str(v).strip().lower()) if pd.notna(v) and v != "" else None).astype("boolean")


def _coerce(table: str, df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if column in INTEGER_COLUMNS:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
        elif _is_boolean_column(column):
            df[column] = _to_boolean(df[column])
        elif column in FLOAT_COLUMNS or column.startswith(FLOAT_PREFIXES) or column.startswith(NUMERIC_PREFIXES) or column in C.MI_BEHAVIOR_CODES:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("float64")
        else:
            # Pandas "string" keeps all-empty columns typed as VARCHAR (object columns of NULLs become INTEGER).
            df[column] = df[column].astype("string")
    return df


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, keep_default_na=False, na_values=[""], dtype=str)


def _find_file(data_dir: Path, table: str) -> Path | None:
    for name in TABLE_FILES[table]:
        if (data_dir / name).exists():
            return data_dir / name
    return None


def data_signature(data_dir: Path) -> tuple:
    files = []
    for names in TABLE_FILES.values():
        for name in names:
            path = data_dir / name
            if path.exists():
                stat = path.stat()
                files.append((name, stat.st_mtime_ns, stat.st_size))
    return tuple(sorted(files))


def load_tables(data_dir: Path) -> dict[str, pd.DataFrame]:
    tables = {}
    for table in TABLE_FILES:
        path = _find_file(data_dir, table)
        df = _read_csv(path) if path else pd.DataFrame()
        df = _coerce(table, df.rename(columns=COLUMN_RENAMES.get(table, {})))
        df = scoring.add_missing_scores(table, df)
        missing = [c for c in EXPECTED_COLUMNS.get(table, []) if c not in df.columns]
        if missing:
            df = pd.concat([df, pd.DataFrame({c: [None] * len(df) for c in missing}, index=df.index, dtype="object")], axis=1)
            df = _coerce(table, df)
        tables[table] = df

    # Patient ids join pairings to personas; keep both numeric when possible.
    for table in ("simulation_pairings", "patient_personas"):
        ids = tables[table]["patient_id"]
        numeric = pd.to_numeric(ids, errors="coerce")
        if numeric.notna().sum() == ids.notna().sum():
            tables[table]["patient_id"] = numeric.astype("Int64")
        else:
            tables[table]["patient_id"] = ids.astype("object")
    if tables["simulation_pairings"]["patient_id"].dtype != tables["patient_personas"]["patient_id"].dtype:
        for table in ("simulation_pairings", "patient_personas"):
            tables[table]["patient_id"] = tables[table]["patient_id"].astype("string").astype("object")

    for column in ("subtype_name", "state_of_change", "name"):
        tables["patient_personas"][column] = tables["patient_personas"][column].fillna("Unknown")

    tables["adverse_events"] = _adverse_events(tables["after_session_reports"])
    return tables


def _adverse_events(reports: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for event in C.ADVERSE_EVENT_KEYS:
        frame = reports[["pairing_id", "session_id"]].copy()
        frame["event_type"] = pd.Series([event] * len(frame), index=frame.index, dtype="string")
        frame["occurred"] = reports[f"{event}_occurred"].astype("boolean")
        frame["attribution"] = reports[f"{event}_attribution"].astype("string")
        frame["internal_justification"] = reports[f"{event}_internal_justification"].astype("string")
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


SESSION_FACTS_SQL = """
CREATE TABLE session_facts AS
WITH sessions AS (
    SELECT
        pairing_id,
        session_id,
        COUNT(*) FILTER (WHERE speaker = 'Patient') AS patient_turns,
        COUNT(*) FILTER (WHERE speaker = 'Therapist') AS therapist_turns
    FROM conversation_log
    WHERE pairing_id IS NOT NULL AND session_id IS NOT NULL
    GROUP BY pairing_id, session_id
),
crisis AS (
    SELECT pairing_id, session_id, BOOL_OR(classification <> 'No Crisis') AS crisis_flag
    FROM crisis_eval_logs
    GROUP BY pairing_id, session_id
)
SELECT
    s.pairing_id,
    s.session_id,
    p.therapist_id,
    p.patient_id,
    pe.subtype_name,
    pe.state_of_change,
    COALESCE(c.crisis_flag, FALSE) AS crisis_flag,
    s.patient_turns,
    s.therapist_turns
FROM sessions AS s
JOIN simulation_pairings AS p ON s.pairing_id = p.pairing_id
LEFT JOIN patient_personas AS pe ON p.patient_id = pe.patient_id
LEFT JOIN crisis AS c ON s.pairing_id = c.pairing_id AND s.session_id = c.session_id
"""


def build_database(data_dir: Path) -> duckdb.DuckDBPyConnection:
    started = time.perf_counter()
    tables = load_tables(data_dir)
    con = duckdb.connect(database=":memory:")
    for name, df in tables.items():
        con.register(f"_{name}_df", df)
        con.execute(f'CREATE TABLE "{name}" AS SELECT * FROM "_{name}_df"')
        con.unregister(f"_{name}_df")
    con.execute(SESSION_FACTS_SQL)
    sessions = con.execute("SELECT COUNT(*) FROM session_facts").fetchone()[0]
    logger.info("Loaded %s (%d sessions) in %.1fs", data_dir, sessions, time.perf_counter() - started)
    return con


class Database:
    """Thread-safe access to the DuckDB database, rebuilt when the CSVs change."""

    CHECK_INTERVAL = 2.0

    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self._lock = threading.Lock()
        self._con = build_database(self.data_dir)
        self._signature = data_signature(self.data_dir)
        self._last_check = time.monotonic()
        self.version = 0

    def refresh(self, force: bool = False) -> bool:
        """Rebuild the database if files changed. Returns True when a rebuild happened."""
        now = time.monotonic()
        if not force and now - self._last_check < self.CHECK_INTERVAL:
            return False
        with self._lock:
            self._last_check = now
            signature = data_signature(self.data_dir)
            if not force and signature == self._signature:
                return False
            try:
                con = build_database(self.data_dir)
            except Exception:  # a CSV may be mid-write; keep serving the previous data
                logger.warning("Could not reload %s; keeping previous data", self.data_dir, exc_info=True)
                return False
            self._con, self._signature = con, signature
            self.version += 1
            return True

    def cursor(self) -> duckdb.DuckDBPyConnection:
        return self._con.cursor()
