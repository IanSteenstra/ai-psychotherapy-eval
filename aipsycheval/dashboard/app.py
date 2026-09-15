"""Interactive dashboard (Flask) over a results directory, backed by an in-memory DuckDB database.

    aipsycheval dashboard dataset              # the dissertation data
    aipsycheval dashboard runs/my_eval         # your own run
"""

from __future__ import annotations

import base64
import logging
import math
import re
import threading
from functools import wraps
from io import BytesIO
from pathlib import Path
from urllib.parse import urlencode

import numpy as np
import pandas as pd
from flask import Flask, Response, jsonify, render_template, request, send_file
from flask.json.provider import DefaultJSONProvider

from .. import constants as C
from .db import Database

logger = logging.getLogger(__name__)

EXCLUDED_THERAPIST = "therapist_psych_material"  # reading material has no fidelity or alliance scores

WARNING_CONSTRUCTS = C.PSYCHOLOGICAL_CONSTRUCTS
INTENSITY_COLUMNS = list(WARNING_CONSTRUCTS)
ADVERSE_OUTCOME_DEFINITIONS = list(C.ADVERSE_EVENTS.items())
ADVERSE_OUTCOME_COLUMNS = [f"{event}_{part}" for event, _ in ADVERSE_OUTCOME_DEFINITIONS for part in ("occurred", "attribution", "internal_justification")]
REPORT_DETAIL_COLUMNS = INTENSITY_COLUMNS + ADVERSE_OUTCOME_COLUMNS

CHAIN_OF_THOUGHT_FIELDS = [
    ("session_conclusion", "Session Conclusion"),
    ("appraisal_internal_reflection", "Appraisal / Internal Reflection"),
    ("internal_justification", "Internal Justification"),
    ("goal", "Goal"),
    ("strategy", "Strategy"),
    ("tactic", "Tactic"),
] + [(key, label) for key, label in WARNING_CONSTRUCTS.items()]
CHAIN_OF_THOUGHT_COLUMNS = [f for f, _ in CHAIN_OF_THOUGHT_FIELDS]

THERAPIST_LABELS = {
    **C.DISSERTATION_THERAPIST_LABELS,
    "therapist_cai": "Character.AI",
    "therapist_chatgpt": "ChatGPT",
    "therapist_gpt_4_mi": "ChatGPT MI",
    "therapist_gemini": "Gemini",
    "therapist_gemini_mi": "Gemini MI",
    "therapist_safe": "Safety Therapist",
    "therapist_psychological": "Psychologist",
    "therapist_niaaa": "NIAAA Booklet",
}

SURE_DOMAIN_COLUMNS = ["total_sure_drug_use", "total_sure_self_care", "total_sure_relationships", "total_sure_material_resources", "total_sure_outlook"]

SESSION_KEY = "CONCAT(CAST({a}.pairing_id AS VARCHAR), '#', CAST({a}.session_id AS VARCHAR))"
SRS_TOTAL = "(COALESCE({a}.relationship, 0) + COALESCE({a}.goals_and_topics, 0) + COALESCE({a}.approach_or_method, 0) + COALESCE({a}.overall, 0))"

_FAVICON_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAA4AAAAOCAYAAAAfSC3RAAAAAXNSR0IArs4c6QAA"
    "AARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsIAAA7CARUoSoAAAAA4SURBVDhPY2Ag"
    "DzA2AvH///9/BoZGBgYGhjEwMDCMIBowMKAYJgkGKwKjGYg2GwwmTKYBAGiHAg0x"
    "Jm4kAAAAAElFTkSuQmCC"
)


def _to_native(value):
    if isinstance(value, dict):
        return {k: _to_native(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_native(v) for v in value]
    if isinstance(value, np.ndarray):
        return [_to_native(v) for v in value.tolist()]
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return None if math.isnan(value) or math.isinf(value) else float(value)
    if value is pd.NA or value is pd.NaT:
        return None
    return value


class JSONProvider(DefaultJSONProvider):
    """JSON that never emits NaN (invalid in browsers) and understands numpy/pandas scalars."""

    def dumps(self, obj, **kwargs):
        return super().dumps(_to_native(obj), **kwargs)


def records(df: pd.DataFrame) -> list[dict]:
    return _to_native(df.to_dict(orient="records"))


def coerce_boolean(value) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if value is None or value is pd.NA:
        return False
    if isinstance(value, (int, float)):
        return not (isinstance(value, float) and math.isnan(value)) and value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def normalize_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        return [v for v in value if v is not None and not (isinstance(v, float) and math.isnan(v)) and v != ""]
    return []


def create_app(data_dir: str | Path) -> Flask:
    app = Flask(__name__)
    app.json = JSONProvider(app)
    db = Database(Path(data_dir))
    cache: dict[str, object] = {}
    cache_lock = threading.Lock()
    state = {"allowed": None, "labels": None}

    # ------------------------------------------------------------------ helpers
    def query(sql: str, params: dict | None = None) -> pd.DataFrame:
        names = set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", sql))
        bound = {k: v for k, v in (params or {}).items() if k in names}
        return db.cursor().execute(sql, bound).df()

    @app.before_request
    def refresh_data():
        if request.path.startswith("/static"):
            return
        if db.refresh():
            with cache_lock:
                cache.clear()
            state["allowed"] = None
            state["labels"] = None

    def therapist_labels() -> dict:
        if state["labels"] is None:
            labels = dict(THERAPIST_LABELS)
            df = query("SELECT therapist_id, label FROM therapists WHERE therapist_id IS NOT NULL")
            labels.update({r["therapist_id"]: r["label"] or r["therapist_id"] for r in records(df)})
            state["labels"] = labels
        return state["labels"]

    def get_therapist_label(therapist_id) -> str:
        if not therapist_id:
            return ""
        return therapist_labels().get(therapist_id, therapist_id)

    @app.context_processor
    def inject_labels():
        return {"therapist_labels": therapist_labels()}

    def allowed_values() -> dict:
        if state["allowed"] is None:
            facts = query("SELECT DISTINCT therapist_id, subtype_name, state_of_change, session_id FROM session_facts")
            if facts.empty:
                facts = query(
                    "SELECT p.therapist_id, pe.subtype_name, pe.state_of_change, NULL::BIGINT AS session_id "
                    "FROM simulation_pairings p LEFT JOIN patient_personas pe ON p.patient_id = pe.patient_id"
                )
            events = query("SELECT DISTINCT event_type FROM adverse_events WHERE event_type IS NOT NULL")
            state["allowed"] = {
                "therapists": {v for v in facts["therapist_id"] if isinstance(v, str)},
                "subtypes": {v for v in facts["subtype_name"] if isinstance(v, str)},
                "states": {v for v in facts["state_of_change"] if isinstance(v, str)},
                "sessions": {int(v) for v in facts["session_id"].dropna()},
                "events": set(events["event_type"]),
            }
        return state["allowed"]

    def parse_pairing_ids() -> list[int]:
        ids = set()
        for raw in request.args.getlist("pairing"):
            for token in str(raw).split(","):
                token = token.strip()
                if token.isdigit():
                    ids.add(int(token))
        return sorted(ids)

    def filter_conditions(therapist_field, subtype_field, state_field, session_field, pairing_field, include_sessions=True):
        allowed = allowed_values()
        pairing_ids = parse_pairing_ids()
        conditions, params = [], {}
        if pairing_ids:
            conditions.append(f"{pairing_field} IN (SELECT UNNEST($pairing_ids))")
            params["pairing_ids"] = pairing_ids
            return conditions, params
        therapists = sorted({t for t in request.args.getlist("therapist") if t in allowed["therapists"]})
        subtypes = sorted({s for s in request.args.getlist("subtype") if s in allowed["subtypes"]})
        states = sorted({s for s in request.args.getlist("state") if s in allowed["states"]})
        sessions = sorted({int(s) for s in request.args.getlist("session") if s.isdigit() and int(s) in allowed["sessions"]})
        if therapists:
            conditions.append(f"{therapist_field} IN (SELECT UNNEST($therapists))")
            params["therapists"] = therapists
        if subtypes:
            conditions.append(f"{subtype_field} IN (SELECT UNNEST($subtypes))")
            params["subtypes"] = subtypes
        if states:
            conditions.append(f"{state_field} IN (SELECT UNNEST($states))")
            params["states"] = states
        if sessions and include_sessions:
            conditions.append(f"{session_field} IN (SELECT UNNEST($sessions))")
            params["sessions"] = sessions
        return conditions, params

    def where(conditions: list[str]) -> str:
        return f"WHERE {' AND '.join(conditions)}" if conditions else ""

    def facts_filters(extra: list[str] | None = None):
        """filtered_sessions CTE over session_facts using the request's filters."""
        conditions, params = filter_conditions("sf.therapist_id", "sf.subtype_name", "sf.state_of_change", "sf.session_id", "sf.pairing_id")
        conditions += extra or []
        cte = (
            "WITH filtered_sessions AS (\n"
            "    SELECT pairing_id, session_id, therapist_id, patient_id, subtype_name, state_of_change, crisis_flag, patient_turns, therapist_turns\n"
            f"    FROM session_facts AS sf {where(conditions)}\n"
            ")"
        )
        return cte, params

    def log_filters(alias: str, extra: list[str] | None = None):
        """Joins to pairings/personas and a WHERE clause for queries directly over a log table."""
        joins = (
            f"JOIN simulation_pairings AS pairings ON {alias}.pairing_id = pairings.pairing_id\n"
            "JOIN patient_personas AS personas ON pairings.patient_id = personas.patient_id"
        )
        conditions, params = filter_conditions("pairings.therapist_id", "personas.subtype_name", "personas.state_of_change", f"{alias}.session_id", f"{alias}.pairing_id")
        conditions += extra or []
        return joins, where(conditions), params

    def cached(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            pairs = [(k, v) for k in sorted(request.args) for v in sorted(request.args.getlist(k))]
            key = f"{f.__name__}?{urlencode(pairs)}"
            with cache_lock:
                hit = cache.get(key)
            if hit is not None:
                return Response(hit, mimetype="application/json")
            result = f(*args, **kwargs)
            if isinstance(result, Response) and result.status_code == 200:
                with cache_lock:
                    cache[key] = result.get_data()
            return result

        return wrapper

    def chain_of_thought(row: dict) -> list[dict]:
        details = []
        for field, label in CHAIN_OF_THOUGHT_FIELDS:
            value = row.get(field)
            if value is None or value == "" or (isinstance(value, float) and math.isnan(value)) or value is pd.NA:
                continue
            if field in INTENSITY_COLUMNS:
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    continue
            details.append({"id": field, "label": label, "value": _to_native(value)})
        return details

    # ------------------------------------------------------------------ pages
    @app.route("/")
    def dashboard():
        return render_template("index.html")

    @app.route("/therapist-prompts")
    def therapist_prompts():
        return render_template("therapist_prompts.html")

    @app.route("/patient-personas")
    def patient_personas():
        df = query(
            """
            SELECT patient_id, name, subtype_name, ad_subtype_description, age_onset, aud_severity_symptoms,
                   drinking_pattern, family_history_of_alcohol_dependence, antisocial_personality_disorder,
                   comorbid_psychiatric_disorders, comorbid_substance_use, psychosocial_indicators,
                   help_seeking_behavior, state_of_change, persona_description
            FROM patient_personas ORDER BY patient_id
            """
        )
        return render_template("patient_personas.html", personas=records(df))

    @app.route("/pairings")
    def pairings():
        return render_template("pairings.html")

    @app.route("/interventions")
    def interventions():
        return render_template("interventions.html")

    @app.route("/favicon.ico")
    def favicon():
        return send_file(BytesIO(_FAVICON_BYTES), mimetype="image/png")

    @app.route("/api/reload", methods=["POST"])
    def reload_data():
        db.refresh(force=True)
        with cache_lock:
            cache.clear()
        state["allowed"] = None
        state["labels"] = None
        return jsonify({"status": "success", "data_dir": str(db.data_dir)})

    # ------------------------------------------------------------------ filters & pairings
    @app.route("/api/filters")
    @cached
    def get_filters():
        allowed = allowed_values()
        return jsonify({key: sorted(allowed[key]) for key in ("therapists", "subtypes", "states", "sessions")})

    @app.route("/api/pairings/filters")
    @cached
    def get_pairings_filters():
        allowed = allowed_values()
        return jsonify({key: sorted(allowed[key]) for key in ("therapists", "subtypes", "states")})

    @app.route("/api/pairings/context")
    @cached
    def get_pairing_context():
        empty = {"pairings": [], "therapists": [], "subtypes": [], "states": [], "sessions": []}
        pairing_ids = parse_pairing_ids()
        if not pairing_ids:
            return jsonify(empty)
        df = query(
            """
            SELECT p.pairing_id, p.therapist_id, pe.subtype_name, pe.state_of_change,
                   ARRAY_AGG(DISTINCT sf.session_id ORDER BY sf.session_id) FILTER (WHERE sf.session_id IS NOT NULL) AS sessions
            FROM simulation_pairings AS p
            JOIN patient_personas AS pe ON p.patient_id = pe.patient_id
            LEFT JOIN session_facts AS sf ON p.pairing_id = sf.pairing_id
            WHERE p.pairing_id IN (SELECT UNNEST($pairing_ids))
            GROUP BY p.pairing_id, p.therapist_id, pe.subtype_name, pe.state_of_change
            ORDER BY p.pairing_id
            """,
            {"pairing_ids": pairing_ids},
        )
        if df.empty:
            return jsonify(empty)
        result = {"pairings": [], "therapists": set(), "subtypes": set(), "states": set(), "sessions": set()}
        for row in records(df):
            sessions = sorted(int(s) for s in normalize_list(row["sessions"]))
            result["therapists"].add(row["therapist_id"] or "")
            result["subtypes"].add(row["subtype_name"] or "")
            result["states"].add(row["state_of_change"] or "")
            result["sessions"].update(sessions)
            result["pairings"].append({
                "pairing_id": row["pairing_id"],
                "therapist_id": row["therapist_id"] or "",
                "therapist_label": get_therapist_label(row["therapist_id"]),
                "subtype_name": row["subtype_name"] or "",
                "state_of_change": row["state_of_change"] or "",
                "sessions": sessions,
            })
        return jsonify({key: sorted(v - {""}) if isinstance(v, set) else v for key, v in result.items()})

    @app.route("/api/pairings")
    @cached
    def pairings_overview():
        try:
            limit = max(1, min(int(request.args.get("limit", 500)), 2000))
        except (TypeError, ValueError):
            limit = 500
        allowed = allowed_values()
        conditions, params = [], {"limit": limit}
        pairing_ids = parse_pairing_ids()
        for arg, column, key in (("therapist", "pairings.therapist_id", "therapists"), ("subtype", "personas.subtype_name", "subtypes"), ("state", "personas.state_of_change", "states")):
            values = sorted({v for v in request.args.getlist(arg) if v in allowed[key]})
            if values:
                conditions.append(f"{column} IN (SELECT UNNEST(${key}))")
                params[key] = values
        if pairing_ids:
            conditions.append("pairings.pairing_id IN (SELECT UNNEST($pairing_ids))")
            params["pairing_ids"] = pairing_ids
        df = query(
            f"""
            SELECT pairings.pairing_id, pairings.therapist_id, personas.name AS patient_name, personas.subtype_name, personas.state_of_change
            FROM simulation_pairings AS pairings
            JOIN patient_personas AS personas ON pairings.patient_id = personas.patient_id
            {where(conditions)}
            ORDER BY pairings.pairing_id
            LIMIT $limit
            """,
            params,
        )
        if df.empty:
            return jsonify([])
        df["therapist_label"] = df["therapist_id"].apply(get_therapist_label)
        return jsonify(records(df[["pairing_id", "therapist_id", "therapist_label", "patient_name", "subtype_name", "state_of_change"]]))

    # ------------------------------------------------------------------ interventions (sessions browser)
    @app.route("/api/interventions")
    @cached
    def interventions_overview():
        crisis_filter = request.args.get("crisis", "any").lower()
        try:
            limit = max(1, min(int(request.args.get("limit", 200)), 500))
        except (TypeError, ValueError):
            limit = 200
        extra = []
        if crisis_filter == "with_crisis":
            extra.append("COALESCE(sf.crisis_flag, FALSE)")
        elif crisis_filter == "without_crisis":
            extra.append("NOT COALESCE(sf.crisis_flag, FALSE)")
        cte, params = facts_filters(extra)
        params["limit"] = limit
        df = query(
            f"""
            {cte},
            reports AS (
                SELECT pairing_id, session_id, ANY_VALUE(journal_summary) AS journal_summary, ANY_VALUE(state_change_justification) AS state_change_justification
                FROM after_session_reports GROUP BY pairing_id, session_id
            ),
            crisis AS (
                SELECT pairing_id, session_id, ARRAY_AGG(DISTINCT classification) FILTER (WHERE classification IS NOT NULL) AS classifications
                FROM crisis_eval_logs WHERE classification <> 'No Crisis' GROUP BY pairing_id, session_id
            )
            SELECT fs.pairing_id, fs.session_id, fs.therapist_id, personas.name AS patient_name, fs.subtype_name, fs.state_of_change,
                   reports.journal_summary, reports.state_change_justification,
                   COALESCE(fs.crisis_flag, FALSE) AS crisis_occurred, crisis.classifications AS crisis_types
            FROM filtered_sessions AS fs
            LEFT JOIN patient_personas AS personas ON fs.patient_id = personas.patient_id
            LEFT JOIN reports ON fs.pairing_id = reports.pairing_id AND fs.session_id = reports.session_id
            LEFT JOIN crisis ON fs.pairing_id = crisis.pairing_id AND fs.session_id = crisis.session_id
            ORDER BY fs.pairing_id, fs.session_id
            LIMIT $limit
            """,
            params,
        )
        if df.empty:
            return jsonify([])
        rows = records(df)
        for row in rows:
            row["crisis_types"] = normalize_list(row["crisis_types"])
            row["therapist_label"] = get_therapist_label(row["therapist_id"])
            row["journal_summary"] = row["journal_summary"] or ""
            row["state_change_justification"] = row["state_change_justification"] or ""
        return jsonify(rows)

    @app.route("/api/interventions/detail")
    @cached
    def interventions_detail():
        try:
            params = {"pairing_id": int(request.args.get("pairing_id")), "session_id": int(request.args.get("session_id"))}
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid parameters"}), 400
        report_fields = ["journal_summary", "state_change_justification"] + REPORT_DETAIL_COLUMNS
        summary = query(
            f"""
            WITH crisis_events AS (
                SELECT pairing_id, session_id,
                       ARRAY_AGG({{'turn': turn, 'classification': classification}} ORDER BY turn) AS events,
                       ARRAY_AGG(DISTINCT classification) FILTER (WHERE classification IS NOT NULL) AS classifications
                FROM crisis_eval_logs WHERE classification <> 'No Crisis' GROUP BY pairing_id, session_id
            ),
            reports AS (
                SELECT pairing_id, session_id, {', '.join(f'ANY_VALUE({c}) AS {c}' for c in report_fields)}
                FROM after_session_reports GROUP BY pairing_id, session_id
            )
            SELECT sf.therapist_id, personas.name AS patient_name, sf.subtype_name, sf.state_of_change,
                   {', '.join(f'reports.{c}' for c in report_fields)},
                   crisis_events.events AS crisis_events, crisis_events.classifications AS crisis_types,
                   COALESCE(sf.crisis_flag, FALSE) AS crisis_flag
            FROM session_facts AS sf
            LEFT JOIN patient_personas AS personas ON sf.patient_id = personas.patient_id
            LEFT JOIN reports ON sf.pairing_id = reports.pairing_id AND sf.session_id = reports.session_id
            LEFT JOIN crisis_events ON sf.pairing_id = crisis_events.pairing_id AND sf.session_id = crisis_events.session_id
            WHERE sf.pairing_id = $pairing_id AND sf.session_id = $session_id
            LIMIT 1
            """,
            params,
        )
        if summary.empty:
            return jsonify({"error": "Session not found"}), 404
        transcript = query(
            f"""
            SELECT turn, speaker, ANY_VALUE(message) AS message, {', '.join(f'ANY_VALUE({c}) AS {c}' for c in CHAIN_OF_THOUGHT_COLUMNS)}
            FROM conversation_log
            WHERE pairing_id = $pairing_id AND session_id = $session_id
            GROUP BY turn, speaker
            ORDER BY turn, CASE WHEN speaker = 'Patient' THEN 0 ELSE 1 END
            """,
            params,
        )
        record = records(summary)[0]
        risk_intensities = [
            {"id": key, "label": label, "value": int(record[key])} for key, label in WARNING_CONSTRUCTS.items() if record.get(key) is not None
        ]
        adverse_outcomes = [
            {
                "id": event,
                "label": label,
                "occurred": coerce_boolean(record.get(f"{event}_occurred")),
                "attribution": record.get(f"{event}_attribution") or "",
                "justification": record.get(f"{event}_internal_justification") or "",
            }
            for event, label in ADVERSE_OUTCOME_DEFINITIONS
        ]
        entries = []
        for row in records(transcript):
            is_patient = (row.get("speaker") or "").strip().lower() == "patient"
            entries.append({
                "turn": row["turn"],
                "speaker": row.get("speaker") or "",
                "message": row.get("message") or "",
                "chain_of_thought": chain_of_thought(row) if is_patient else [],
            })
        return jsonify({
            **params,
            "therapist_id": record.get("therapist_id") or "",
            "therapist_label": get_therapist_label(record.get("therapist_id")),
            "patient_name": record.get("patient_name") or "",
            "patient_subtype": record.get("subtype_name") or "",
            "state_of_change": record.get("state_of_change") or "",
            "journal_summary": record.get("journal_summary") or "",
            "state_change_justification": record.get("state_change_justification") or "",
            "crisis_events": [e for e in (record.get("crisis_events") or []) if e],
            "crisis_types": normalize_list(record.get("crisis_types")),
            "risk_intensities": risk_intensities,
            "adverse_outcomes": adverse_outcomes,
            "transcript": entries,
        })

    @app.route("/api/transcript-snippet")
    def transcript_snippet():
        try:
            pairing_id, session_id, turn = (int(request.args.get(k)) for k in ("pairing_id", "session_id", "turn"))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid parameters"}), 400
        df = query(
            """
            SELECT turn, speaker, message FROM (
                SELECT DISTINCT turn, COALESCE(speaker, 'Unknown') AS speaker, COALESCE(message, '') AS message
                FROM conversation_log
                WHERE pairing_id = $pairing_id AND session_id = $session_id AND turn BETWEEN $turn_start AND $turn_end
            )
            ORDER BY turn, CASE WHEN lower(speaker) = 'patient' THEN 0 ELSE 1 END
            """,
            {"pairing_id": pairing_id, "session_id": session_id, "turn_start": turn - 1, "turn_end": turn + 1},
        )
        return jsonify(records(df))

    # ------------------------------------------------------------------ summary
    @app.route("/api/dashboard-summary")
    @cached
    def dashboard_summary():
        cte, params = facts_filters()
        df = query(
            f"""
            {cte}
            SELECT COUNT(*) AS sessions, SUM(COALESCE(patient_turns, 0)) AS patient_turns, SUM(COALESCE(therapist_turns, 0)) AS therapist_turns,
                   COUNT(DISTINCT therapist_id) AS therapists, COUNT(DISTINCT patient_id) AS personas
            FROM filtered_sessions
            """,
            params,
        )
        row = {k: int(v or 0) for k, v in records(df)[0].items()}
        row["dialogue_turns"] = (row["patient_turns"] + row["therapist_turns"]) // 2
        return jsonify(row)

    # ------------------------------------------------------------------ crises
    @app.route("/api/crisis-events")
    @cached
    def crisis_events():
        joins, where_sql, params = log_filters("T")
        df = query(
            f"""
            SELECT T.classification, T.pairing_id, T.session_id, T.turn, pairings.therapist_id AS therapist_id,
                   personas.name AS patient_name, personas.subtype_name AS patient_subtype
            FROM crisis_eval_logs AS T {joins} {where_sql}
            """,
            params,
        )
        return jsonify(records(df))

    @app.route("/api/action-plan-adherence")
    @cached
    def action_plan_adherence():
        joins, where_sql, params = log_filters("T", ["pairings.therapist_id <> $excluded_therapist"])
        params["excluded_therapist"] = EXCLUDED_THERAPIST
        steps = [("Assess", "assess"), ("De-escalate", "de_escalate"), ("Recommend Services", "recommend_emergency_services"), ("Request Consultation", "request_human_consultation")]
        sql = "\nUNION ALL\n".join(
            f"SELECT '{label}' AS step, COUNT_IF(T.{column}) AS success_count, COUNT(*) AS total_count FROM action_plan_eval_logs AS T {joins} {where_sql}"
            for label, column in steps
        )
        rows = records(query(sql, params))
        for row in rows:
            row["percentage"] = (row["success_count"] / row["total_count"] * 100) if row["total_count"] else 0
        return jsonify(rows)

    @app.route("/api/overall-adherence")
    @cached
    def overall_adherence():
        joins, where_sql, params = log_filters("T", ["pairings.therapist_id <> $excluded_therapist"])
        params["excluded_therapist"] = EXCLUDED_THERAPIST
        row = records(query(
            f"""
            SELECT COUNT_IF(T.assess AND T.de_escalate AND T.recommend_emergency_services AND T.request_human_consultation) AS fully_adherent_count,
                   COUNT(*) AS total_count
            FROM action_plan_eval_logs AS T {joins} {where_sql}
            """,
            params,
        ))[0]
        percentage = row["fully_adherent_count"] / row["total_count"] * 100 if row["total_count"] else 0
        return jsonify({"percentage": float(percentage)})

    # ------------------------------------------------------------------ alliance & outcomes by therapist
    def by_therapist(table: str, select: str, order: str, exclude_material: bool) -> list[dict]:
        extra = ["sf.therapist_id <> $excluded_therapist"] if exclude_material else []
        cte, params = facts_filters(extra)
        params["excluded_therapist"] = EXCLUDED_THERAPIST
        df = query(
            f"""
            {cte}
            SELECT fs.therapist_id, {select}, COUNT(DISTINCT {SESSION_KEY.format(a='T')}) AS session_count
            FROM filtered_sessions AS fs
            JOIN {table} AS T ON fs.pairing_id = T.pairing_id AND fs.session_id = T.session_id
            GROUP BY fs.therapist_id
            ORDER BY {order}
            """,
            params,
        )
        return records(df)

    @app.route("/api/therapist-comparison")
    @cached
    def therapist_comparison():
        select = (
            f"AVG({SRS_TOTAL.format(a='T')}) AS average_srs_score, AVG(T.overall) AS avg_srs_overall, AVG(T.relationship) AS avg_srs_relationship, "
            "AVG(T.goals_and_topics) AS avg_srs_goals, AVG(T.approach_or_method) AS avg_srs_approach"
        )
        return jsonify(by_therapist("survey_srs_logs", select, "average_srs_score DESC", exclude_material=True))

    @app.route("/api/therapist-comparison-neq")
    @cached
    def therapist_comparison_neq():
        return jsonify(by_therapist("survey_neq_logs", "AVG(CAST(T.neq_total_severity_score AS DOUBLE)) AS avg_neq_score", "avg_neq_score DESC", exclude_material=False))

    @app.route("/api/therapist-comparison-sure")
    @cached
    def therapist_comparison_sure():
        return jsonify(by_therapist("survey_sure_logs", "AVG(CAST(T.total_sure_score AS DOUBLE)) AS avg_sure_score", "avg_sure_score DESC", exclude_material=False))

    @app.route("/api/therapist-comparison-wai")
    @cached
    def therapist_comparison_wai():
        select = (
            "AVG(CAST(T.composite_wai AS DOUBLE)) AS avg_wai_score, AVG(CAST(T.total_wai_task AS DOUBLE)) AS avg_wai_task, "
            "AVG(CAST(T.total_wai_bond AS DOUBLE)) AS avg_wai_bond, AVG(CAST(T.total_wai_goal AS DOUBLE)) AS avg_wai_goal"
        )
        return jsonify(by_therapist("survey_wai_logs", select, "avg_wai_score DESC", exclude_material=True))

    @app.route("/api/mi-global-profile")
    @cached
    def mi_global_profile():
        select = (
            "AVG(T.cultivating_change_talk_score) AS cultivating_change_talk, AVG(T.softening_sustain_talk_score) AS softening_sustain_talk, "
            "AVG(T.partnership_score) AS partnership, AVG(T.empathy_score) AS empathy"
        )
        return jsonify(by_therapist("mi_global_eval_logs", select, "fs.therapist_id", exclude_material=True))

    @app.route("/api/mi-global-metrics")
    @cached
    def mi_global_metrics():
        select = (
            "AVG(T.cultivating_change_talk_score) AS cultivating_change_talk, AVG(T.softening_sustain_talk_score) AS softening_sustain_talk, "
            "AVG(T.partnership_score) AS partnership, AVG(T.empathy_score) AS empathy"
        )
        results = []
        for row in by_therapist("mi_global_eval_logs", select, "fs.therapist_id", exclude_material=True):
            technical = (row["cultivating_change_talk"] + row["softening_sustain_talk"]) / 2 if row["cultivating_change_talk"] is not None and row["softening_sustain_talk"] is not None else None
            relational = (row["partnership"] + row["empathy"]) / 2 if row["partnership"] is not None and row["empathy"] is not None else None
            results.append({"therapist_id": row["therapist_id"], "technical_global": technical or 0.0, "relational_global": relational or 0.0, "session_count": row["session_count"] or 0})
        return jsonify(results)

    @app.route("/api/mi-behavior-metrics")
    @cached
    def mi_behavior_metrics():
        select = "AVG(T.percent_cr) AS percent_cr, AVG(T.r_q_ratio) AS r_q_ratio, AVG(T.percent_mi_adherent) AS percent_mi_adherent"
        results = []
        for row in by_therapist("mi_batch_behavior_eval_logs", select, "fs.therapist_id", exclude_material=True):
            results.append({
                "therapist_id": row["therapist_id"],
                "percent_cr": (row["percent_cr"] or 0.0) * 100,
                "r_q_ratio": row["r_q_ratio"] or 0.0,
                "percent_mi_adherent": (row["percent_mi_adherent"] or 0.0) * 100,
                "session_count": row["session_count"] or 0,
            })
        return jsonify(results)

    # ------------------------------------------------------------------ NEQ
    @app.route("/api/neq-aggregate-breakdown")
    @cached
    def neq_aggregate_breakdown():
        view = request.args.get("view", "therapist").lower()
        group = "fs.subtype_name" if view == "subtype" else "fs.therapist_id"
        cte, params = facts_filters()
        df = query(
            f"""
            {cte}
            SELECT {group} AS group_key, {group} AS group_label,
                   AVG(CAST(T.neq_total_effects_experienced AS DOUBLE)) AS avg_effects_experienced,
                   AVG(CAST(T.neq_effects_due_to_treatment AS DOUBLE)) AS avg_due_to_treatment,
                   AVG(CAST(T.neq_effects_due_to_other AS DOUBLE)) AS avg_due_to_other,
                   COUNT(DISTINCT {SESSION_KEY.format(a='T')}) AS session_count
            FROM filtered_sessions AS fs
            JOIN survey_neq_logs AS T ON fs.pairing_id = T.pairing_id AND fs.session_id = T.session_id
            GROUP BY group_key, group_label
            ORDER BY group_label
            """,
            params,
        )
        rows = []
        for row in records(df):
            if view == "subtype":
                label = row["group_label"] or "Unknown Subtype"
            else:
                label = therapist_labels().get(row["group_key"], row["group_label"] or row["group_key"] or "Unknown Therapist")
            rows.append({
                "group_key": row["group_key"],
                "label": label,
                "avg_effects_experienced": row["avg_effects_experienced"] or 0.0,
                "avg_due_to_treatment": row["avg_due_to_treatment"] or 0.0,
                "avg_due_to_other": row["avg_due_to_other"] or 0.0,
                "session_count": row["session_count"] or 0,
            })
        return jsonify(rows)

    @app.route("/api/neq-aggregate-totals")
    @cached
    def neq_aggregate_totals():
        cte, params = facts_filters()
        row = records(query(
            f"""
            {cte}
            SELECT AVG(CAST(T.neq_total_effects_experienced AS DOUBLE)) AS avg_effects_experienced,
                   AVG(CAST(T.neq_effects_due_to_treatment AS DOUBLE)) AS avg_due_to_treatment,
                   AVG(CAST(T.neq_effects_due_to_other AS DOUBLE)) AS avg_due_to_other
            FROM filtered_sessions AS fs
            JOIN survey_neq_logs AS T ON fs.pairing_id = T.pairing_id AND fs.session_id = T.session_id
            """,
            params,
        ))[0]
        return jsonify({k: v or 0.0 for k, v in row.items()})

    @app.route("/api/neq-session-trends")
    @cached
    def neq_session_trends():
        joins, where_sql, params = log_filters("neq", ["pairings.therapist_id <> $excluded_therapist"])
        params["excluded_therapist"] = EXCLUDED_THERAPIST
        averages = (
            "AVG(CAST(neq.neq_total_severity_score AS DOUBLE)) AS avg_neq_severity, "
            "AVG(CAST(neq.neq_total_effects_experienced AS DOUBLE)) AS avg_effects_experienced, "
            "AVG(CAST(neq.neq_effects_due_to_treatment AS DOUBLE)) AS avg_due_to_treatment, "
            "AVG(CAST(neq.neq_effects_due_to_other AS DOUBLE)) AS avg_due_to_other"
        )
        result = {}
        for key, column, alias in (("therapist", "pairings.therapist_id", "therapist_id"), ("patient", "personas.subtype_name", "subtype_name")):
            df = query(
                f"""
                SELECT neq.session_id AS session_id, {column} AS {alias}, {averages}
                FROM survey_neq_logs AS neq {joins} {where_sql}
                GROUP BY neq.session_id, {column}
                ORDER BY neq.session_id, {column}
                """,
                params,
            )
            result[key] = records(df)
        return jsonify(result)

    @app.route("/api/neq-question-summary")
    @cached
    def neq_question_summary():
        joins, where_sql, params = log_filters("T")
        structs = ", ".join(
            f"{{'question_number': {i}, 'experienced': T.question{i}_experienced, 'severity': T.question{i}_severity, 'cause': T.question{i}_cause}}"
            for i in range(1, C.NEQ_QUESTION_COUNT + 1)
        )
        params.update({"treatment_cause": C.NEQ_TREATMENT_CAUSE, "other_cause": C.NEQ_OTHER_CAUSE})
        df = query(
            f"""
            WITH unnested AS (
                SELECT UNNEST([{structs}], recursive := true)
                FROM survey_neq_logs AS T {joins} {where_sql}
            ),
            scored AS (
                SELECT question_number, COALESCE(experienced, FALSE) AS experienced, cause,
                       CASE severity WHEN 'Not at all' THEN 0 WHEN 'Slightly' THEN 1 WHEN 'Moderately' THEN 2 WHEN 'Very' THEN 3 WHEN 'Extremely' THEN 4 END AS severity_value
                FROM unnested
            )
            SELECT question_number, COUNT(*) AS total_responses, COUNT_IF(experienced) AS experienced_count,
                   AVG(CASE WHEN experienced THEN severity_value END) AS avg_severity_value,
                   COUNT_IF(experienced) / NULLIF(COUNT(*), 0) AS experienced_ratio,
                   COUNT_IF(experienced AND cause = $treatment_cause) / NULLIF(COUNT_IF(experienced), 0) AS treatment_ratio,
                   COUNT_IF(experienced AND cause = $other_cause) / NULLIF(COUNT_IF(experienced), 0) AS other_ratio
            FROM scored
            GROUP BY question_number
            ORDER BY question_number
            """,
            params,
        )
        rows = []
        for row in records(df):
            number = int(row["question_number"])
            rows.append({
                "question_number": number,
                "question_label": C.NEQ_QUESTION_LABELS.get(number, f"Question {number}"),
                "total_responses": row["total_responses"] or 0,
                "experienced_count": row["experienced_count"] or 0,
                "experienced_percentage": (row["experienced_ratio"] or 0.0) * 100,
                "average_severity": row["avg_severity_value"],
                "treatment_percentage": (row["treatment_ratio"] or 0.0) * 100,
                "other_percentage": (row["other_ratio"] or 0.0) * 100,
            })
        return jsonify(rows)

    # ------------------------------------------------------------------ SURE
    @app.route("/api/sure-domain-aggregates")
    @cached
    def sure_domain_aggregates():
        cte, params = facts_filters()
        domains = ", ".join(f"AVG(CAST(sure.{c} AS DOUBLE)) AS {c}" for c in SURE_DOMAIN_COLUMNS)
        result = {}
        for key, column, alias in (("therapist", "fs.therapist_id", "therapist_id"), ("patient", "fs.subtype_name", "subtype_name")):
            df = query(
                f"""
                {cte}
                SELECT {column} AS {alias}, {domains}, COUNT(DISTINCT {SESSION_KEY.format(a='sure')}) AS session_count
                FROM filtered_sessions AS fs
                JOIN survey_sure_logs AS sure ON fs.pairing_id = sure.pairing_id AND fs.session_id = sure.session_id
                GROUP BY {column} ORDER BY {column}
                """,
                params,
            )
            result[key] = records(df)
        return jsonify(result)

    def session_trends(table: str, alias: str, select: str, exclude_material: bool) -> dict:
        extra = ["sf.therapist_id <> $excluded_therapist"] if exclude_material else []
        cte, params = facts_filters(extra)
        params["excluded_therapist"] = EXCLUDED_THERAPIST
        result = {}
        for key, column, name in (("therapist", "fs.therapist_id", "therapist_id"), ("patient", "fs.subtype_name", "subtype_name")):
            df = query(
                f"""
                {cte}
                SELECT fs.session_id AS session_id, {column} AS {name}, {select}
                FROM filtered_sessions AS fs
                JOIN {table} AS {alias} ON fs.pairing_id = {alias}.pairing_id AND fs.session_id = {alias}.session_id
                GROUP BY fs.session_id, {column}
                ORDER BY fs.session_id, {column}
                """,
                params,
            )
            result[key] = records(df)
        return result

    @app.route("/api/sure-session-trends")
    @cached
    def sure_session_trends():
        return jsonify(session_trends("survey_sure_logs", "sure", "AVG(CAST(sure.total_sure_score AS DOUBLE)) AS avg_sure", exclude_material=False))

    @app.route("/api/sure-domain-session-trends")
    @cached
    def sure_domain_session_trends():
        domains = ", ".join(f"AVG(CAST(sure.{c} AS DOUBLE)) AS {c}" for c in SURE_DOMAIN_COLUMNS)
        return jsonify(session_trends("survey_sure_logs", "sure", domains, exclude_material=False))

    @app.route("/api/srs-session-component-trends")
    @cached
    def srs_session_component_trends():
        select = (
            "AVG(CAST(srs.overall AS DOUBLE)) AS avg_srs_overall, AVG(CAST(srs.relationship AS DOUBLE)) AS avg_srs_relationship, "
            "AVG(CAST(srs.goals_and_topics AS DOUBLE)) AS avg_srs_goals, AVG(CAST(srs.approach_or_method AS DOUBLE)) AS avg_srs_approach"
        )
        return jsonify(session_trends("survey_srs_logs", "srs", select, exclude_material=True))

    @app.route("/api/wai-session-component-trends")
    @cached
    def wai_session_component_trends():
        joins, where_sql, params = log_filters("wai", ["pairings.therapist_id <> $excluded_therapist"])
        params["excluded_therapist"] = EXCLUDED_THERAPIST
        select = (
            "AVG(CAST(wai.total_wai_task AS DOUBLE)) AS avg_wai_task, AVG(CAST(wai.total_wai_bond AS DOUBLE)) AS avg_wai_bond, "
            "AVG(CAST(wai.total_wai_goal AS DOUBLE)) AS avg_wai_goal"
        )
        result = {}
        for key, column, name in (("therapist", "pairings.therapist_id", "therapist_id"), ("patient", "personas.subtype_name", "subtype_name")):
            df = query(
                f"""
                SELECT wai.session_id AS session_id, {column} AS {name}, {select}
                FROM survey_wai_logs AS wai {joins} {where_sql}
                GROUP BY wai.session_id, {column}
                ORDER BY wai.session_id, {column}
                """,
                params,
            )
            result[key] = records(df)
        return jsonify(result)

    @app.route("/api/score-trends-over-sessions")
    @cached
    def score_trends_over_sessions():
        cte, params = facts_filters()
        df = query(
            f"""
            {cte}
            SELECT fs.session_id,
                   CASE WHEN GROUPING(fs.therapist_id) = 0 THEN 'therapist' WHEN GROUPING(fs.subtype_name) = 0 THEN 'subtype' ELSE 'summary' END AS dimension_type,
                   CASE WHEN GROUPING(fs.therapist_id) = 0 THEN fs.therapist_id END AS therapist_id,
                   CASE WHEN GROUPING(fs.subtype_name) = 0 THEN fs.subtype_name END AS subtype_name,
                   AVG({SRS_TOTAL.format(a='srs')}) AS avg_srs,
                   AVG(srs.overall) AS avg_srs_overall, AVG(srs.relationship) AS avg_srs_relationship,
                   AVG(srs.goals_and_topics) AS avg_srs_goals, AVG(srs.approach_or_method) AS avg_srs_approach,
                   AVG(sure.total_sure_score) AS avg_sure,
                   AVG(wai.composite_wai) AS avg_wai, AVG(wai.total_wai_task) AS avg_wai_task,
                   AVG(wai.total_wai_bond) AS avg_wai_bond, AVG(wai.total_wai_goal) AS avg_wai_goal
            FROM filtered_sessions AS fs
            JOIN survey_srs_logs AS srs ON fs.pairing_id = srs.pairing_id AND fs.session_id = srs.session_id
            LEFT JOIN survey_sure_logs AS sure ON fs.pairing_id = sure.pairing_id AND fs.session_id = sure.session_id
            LEFT JOIN survey_wai_logs AS wai ON fs.pairing_id = wai.pairing_id AND fs.session_id = wai.session_id
            GROUP BY GROUPING SETS ((fs.session_id), (fs.session_id, fs.therapist_id), (fs.session_id, fs.subtype_name))
            ORDER BY fs.session_id, dimension_type, therapist_id, subtype_name
            """,
            params,
        )
        rows = records(df)
        summary_columns = ["session_id", "avg_srs", "avg_srs_overall", "avg_srs_relationship", "avg_srs_goals", "avg_srs_approach", "avg_sure", "avg_wai", "avg_wai_task", "avg_wai_bond", "avg_wai_goal"]
        therapist_rows = [r for r in rows if r["dimension_type"] == "therapist" and r["therapist_id"] and r["therapist_id"] != EXCLUDED_THERAPIST]
        patient_rows = [r for r in rows if r["dimension_type"] == "subtype" and r["subtype_name"]]

        def pick(source, group, metric):
            return [{"session_id": r["session_id"], group: r[group], metric: r[metric]} for r in source if r[metric] is not None]

        return jsonify({
            "summary": [{c: r[c] for c in summary_columns} for r in rows if r["dimension_type"] == "summary"],
            "srs": {"therapist": pick(therapist_rows, "therapist_id", "avg_srs"), "patient": pick(patient_rows, "subtype_name", "avg_srs")},
            "wai": {"therapist": pick(therapist_rows, "therapist_id", "avg_wai"), "patient": pick(patient_rows, "subtype_name", "avg_wai")},
        })

    @app.route("/api/scores-by-patient-type")
    @cached
    def scores_by_patient_type():
        joins, where_sql, params = log_filters("srs")
        df = query(
            f"""
            SELECT personas.subtype_name,
                   AVG({SRS_TOTAL.format(a='srs')}) AS avg_srs, AVG(srs.overall) AS avg_srs_overall, AVG(srs.relationship) AS avg_srs_relationship,
                   AVG(srs.goals_and_topics) AS avg_srs_goals, AVG(srs.approach_or_method) AS avg_srs_approach,
                   AVG(CAST(sure.total_sure_score AS DOUBLE)) AS avg_sure,
                   AVG(CAST(wai.composite_wai AS DOUBLE)) AS avg_wai, AVG(CAST(wai.total_wai_task AS DOUBLE)) AS avg_wai_task,
                   AVG(CAST(wai.total_wai_bond AS DOUBLE)) AS avg_wai_bond, AVG(CAST(wai.total_wai_goal AS DOUBLE)) AS avg_wai_goal,
                   AVG(CAST(neq.neq_total_severity_score AS DOUBLE)) AS avg_neq,
                   COUNT(DISTINCT {SESSION_KEY.format(a='srs')}) AS session_count
            FROM survey_srs_logs AS srs
            {joins}
            LEFT JOIN survey_sure_logs AS sure ON srs.pairing_id = sure.pairing_id AND srs.session_id = sure.session_id
            LEFT JOIN survey_wai_logs AS wai ON srs.pairing_id = wai.pairing_id AND srs.session_id = wai.session_id
            LEFT JOIN survey_neq_logs AS neq ON srs.pairing_id = neq.pairing_id AND srs.session_id = neq.session_id
            {where_sql}
            GROUP BY personas.subtype_name
            ORDER BY personas.subtype_name
            """,
            params,
        )
        return jsonify(records(df))

    # ------------------------------------------------------------------ adverse outcomes & warning signs
    @app.route("/api/adverse-outcomes")
    @cached
    def adverse_outcomes():
        joins, where_sql, params = log_filters("T")
        df = query(
            f"""
            SELECT {SESSION_KEY.format(a='T')} AS session_key, T.event_type, MAX(CASE WHEN T.occurred THEN 1 ELSE 0 END) AS occurred_flag
            FROM adverse_events AS T {joins} {where_sql}
            GROUP BY session_key, T.event_type
            """,
            params,
        )
        if df.empty:
            return jsonify([])
        session_flags = df.groupby("session_key")["occurred_flag"].max()
        counts = df[df["occurred_flag"] == 1].groupby("event_type")["occurred_flag"].sum()
        result = {event: int(counts.get(event, 0)) for event, _ in ADVERSE_OUTCOME_DEFINITIONS}
        result["no_adverse_outcome"] = int((session_flags == 0).sum())
        result["total_sessions"] = int(df["session_key"].nunique())
        return jsonify([result])

    @app.route("/api/adverse-outcome-attributions")
    @cached
    def adverse_outcome_attributions():
        extra = ["T.attribution IS NOT NULL", "T.attribution <> ''"]
        events = request.args.getlist("events")
        joins, where_sql, params = log_filters("T", extra + (["T.event_type IN (SELECT UNNEST($events))"] if events else []))
        params["events"] = events
        df = query(f"SELECT T.attribution FROM adverse_events AS T {joins} {where_sql}", params)
        if df.empty:
            return jsonify([])
        mapping = {
            "Therapist's Actions": "Therapist Actions / Psychoeducation Material",
            "Psychoeducation Material": "Therapist Actions / Psychoeducation Material",
            "Treatment in General": "Treatment / Reading in General",
            "Reading in General": "Treatment / Reading in General",
        }
        counts = df["attribution"].map(lambda v: mapping.get(v, v)).value_counts()
        return jsonify([{"attribution": attribution, "count": int(count)} for attribution, count in counts.items()])

    @app.route("/api/equity-audit")
    @cached
    def equity_audit():
        allowed = allowed_values()
        events = [e for e in request.args.getlist("equity_event") if e in allowed["events"]]
        joins, where_sql, params = log_filters("ae", ["ae.event_type IN (SELECT UNNEST($equity_events))"] if events else [])
        params["equity_events"] = events
        df = query(
            f"""
            WITH session_level AS (
                SELECT pairings.therapist_id, personas.subtype_name, personas.state_of_change, ae.pairing_id, ae.session_id,
                       {SESSION_KEY.format(a='ae')} AS session_key, MAX(CASE WHEN ae.occurred THEN 1 ELSE 0 END) AS harm_flag
                FROM adverse_events AS ae {joins} {where_sql}
                GROUP BY pairings.therapist_id, personas.subtype_name, personas.state_of_change, ae.pairing_id, ae.session_id
            )
            SELECT therapist_id, subtype_name, state_of_change, COUNT(DISTINCT session_key) AS total_sessions, SUM(harm_flag) AS sessions_with_harm,
                   CAST(SUM(harm_flag) AS DOUBLE) / NULLIF(COUNT(DISTINCT session_key), 0) * 100 AS harm_rate
            FROM session_level
            GROUP BY therapist_id, subtype_name, state_of_change
            ORDER BY harm_rate DESC
            """,
            params,
        )
        rows = records(df)
        for row in rows:
            row["total_sessions"] = row["total_sessions"] or 0
            row["sessions_with_harm"] = int(row["sessions_with_harm"] or 0)
            row["therapist_label"] = get_therapist_label(row["therapist_id"])
        return jsonify(rows)

    @app.route("/api/patient-retention-by-session")
    @cached
    def patient_retention_by_session():
        cte, params = facts_filters()
        df = query(
            f"""
            {cte}
            SELECT fs.session_id,
                   COUNT(DISTINCT fs.pairing_id) AS active_patients,
                   COUNT(DISTINCT CASE WHEN ae.event_type = 'treatment_dropout' AND ae.occurred THEN fs.pairing_id END) AS dropouts,
                   COUNT(DISTINCT CASE WHEN ae.event_type = 'death_by_suicide' AND ae.occurred THEN fs.pairing_id END) AS suicides
            FROM filtered_sessions AS fs
            LEFT JOIN adverse_events AS ae ON fs.pairing_id = ae.pairing_id AND fs.session_id = ae.session_id
            GROUP BY fs.session_id
            ORDER BY fs.session_id
            """,
            params,
        )
        rows = records(df)
        for row in rows:
            row["continuing_patients"] = max(0, row["active_patients"] - row["dropouts"] - row["suicides"])
        return jsonify(rows)

    @app.route("/api/in-session-warning-signs")
    @cached
    def in_session_warning_signs():
        construct = request.args.get("construct", INTENSITY_COLUMNS[0])
        if construct not in WARNING_CONSTRUCTS:
            return jsonify({"error": "Invalid construct requested."}), 400
        joins, where_sql, params = log_filters("T", ["T.speaker = 'Patient'", f"T.{construct} IS NOT NULL"])
        df = query(
            f"""
            WITH patient_turns AS (
                SELECT T.pairing_id, T.session_id, T.turn, CAST(T.{construct} AS DOUBLE) AS construct_value, T.message AS patient_message,
                       {', '.join(f'T.{c}' for c in CHAIN_OF_THOUGHT_COLUMNS)},
                       ROW_NUMBER() OVER (PARTITION BY T.pairing_id, T.session_id, T.turn ORDER BY T.turn) AS row_num
                FROM conversation_log AS T {joins} {where_sql}
            ),
            therapist_turns AS (
                SELECT pairing_id, session_id, turn, message AS therapist_message,
                       ROW_NUMBER() OVER (PARTITION BY pairing_id, session_id, turn ORDER BY turn) AS row_num
                FROM conversation_log WHERE speaker = 'Therapist'
            )
            SELECT P.pairing_id, P.session_id, P.turn, P.construct_value, P.patient_message,
                   prev.therapist_message AS previous_therapist_message, prev.turn AS previous_therapist_turn,
                   {', '.join(f'P.{c}' for c in CHAIN_OF_THOUGHT_COLUMNS)}
            FROM (SELECT * FROM patient_turns WHERE row_num = 1) AS P
            LEFT JOIN (SELECT * FROM therapist_turns WHERE row_num = 1) AS prev
              ON prev.pairing_id = P.pairing_id AND prev.session_id = P.session_id AND prev.turn = P.turn - 1
            ORDER BY P.session_id, P.turn
            """,
            params,
        )
        results = []
        for row in records(df):
            results.append({
                "pairing_id": row["pairing_id"],
                "session_id": row["session_id"],
                "turn": row["turn"],
                "construct_value": row["construct_value"],
                "patient_message": row.get("patient_message") or "",
                "previous_therapist_message": row.get("previous_therapist_message") or "",
                "previous_therapist_turn": row.get("previous_therapist_turn"),
                "chain_of_thought": chain_of_thought(row),
            })
        return jsonify(results)

    return app
