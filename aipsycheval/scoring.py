"""Scoring for the validated instruments and MITI metrics.

Every function takes the flat row that is logged to CSV (or the raw LLM response)
and returns the derived score columns. The dashboard uses the same functions to
fill in scores for log files that do not already contain them.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

from .constants import MI_BEHAVIOR_CODES, NEQ_OTHER_CAUSE, NEQ_QUESTION_COUNT, NEQ_TREATMENT_CAUSE

# --- Substance Use Recovery Evaluator (SURE) ---
# Items are collapsed to 1-3 points. Section C (importance ratings) is not scored.
SURE_FREQUENCY_POINTS = {
    "Never": 3,
    "On 1 or 2 days": 3,
    "On 3 or 4 days": 2,
    "On 5 or 6 days": 1,
    "Every day": 1,
}
SURE_AMOUNT_POINTS = {
    "All of the time": 3,
    "Most of the time": 3,
    "A fair amount of the time": 2,
    "A little of the time": 1,
    "None of the time": 1,
}
SURE_DOMAINS = {
    "total_sure_drug_use": [f"sec_a_question_{i}" for i in range(1, 7)],
    "total_sure_self_care": [f"sec_b_question_{i}" for i in range(7, 12)],
    "total_sure_relationships": [f"sec_b_question_{i}" for i in range(12, 16)],
    "total_sure_material_resources": [f"sec_b_question_{i}" for i in range(16, 19)],
    "total_sure_outlook": [f"sec_b_question_{i}" for i in range(19, 22)],
}
SURE_SCORE_COLUMNS = list(SURE_DOMAINS) + ["total_sure_score"]

# --- Working Alliance Inventory (WAI, 36-item client form) ---
WAI_POINTS = {"Never": 1, "Rarely": 2, "Occasionally": 3, "Sometimes": 4, "Often": 5, "Very Often": 6, "Always": 7}
# Subscale items; negative numbers are reverse-scored (8 - rating).
# Note: the released dissertation dataset reverse-scored item 28 instead of item 29
# in the Bond subscale. New runs use the standard key below.
WAI_SUBSCALES = {
    "total_wai_task": [2, 4, -7, -11, 13, -15, 16, 18, 24, -31, -33, 35],
    "total_wai_bond": [-1, 5, 8, 17, 19, -20, 21, 23, 26, 28, -29, 36],
    "total_wai_goal": [-3, 6, -9, -10, -12, 14, 22, 25, -27, 30, 32, -34],
}
WAI_SCORE_COLUMNS = list(WAI_SUBSCALES) + ["composite_wai"]

# --- Session Rating Scale (SRS) ---
SRS_ITEMS = ["relationship", "goals_and_topics", "approach_or_method", "overall"]
SRS_SCORE_COLUMNS = ["total"]

# --- Negative Effects Questionnaire (NEQ) ---
NEQ_SEVERITY_POINTS = {"Not at all": 0, "Slightly": 1, "Moderately": 2, "Very": 3, "Extremely": 4}
NEQ_SCORE_COLUMNS = [
    "neq_total_effects_experienced",
    "neq_effects_due_to_treatment",
    "neq_effects_due_to_other",
    "neq_total_severity_score",
    "neq_avg_severity_of_experienced_effects",
]

# --- MITI 4.2.1 ---
MI_GLOBAL_SCORE_COLUMNS = ["technical_global", "relational_global"]
MI_BEHAVIOR_METRIC_COLUMNS = ["total_mi_adherent", "total_mi_non_adherent", "percent_mi_adherent", "percent_cr", "r_q_ratio"]
AFFIRM_CAP = 3


def is_true(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    try:
        return bool(value) and value == value  # NaN is truthy but not equal to itself
    except (TypeError, ValueError):
        return False


def score_sure(row: Mapping[str, Any]) -> dict:
    scores = {}
    for column, items in SURE_DOMAINS.items():
        total = 0
        for item in items:
            points = SURE_FREQUENCY_POINTS if item in ("sec_a_question_1", "sec_a_question_2", "sec_a_question_3") else SURE_AMOUNT_POINTS
            total += points.get(row.get(item), 0)
        scores[column] = total
    scores["total_sure_score"] = sum(scores[c] for c in SURE_DOMAINS)
    return scores


def score_wai(row: Mapping[str, Any]) -> dict:
    scores = {}
    for column, items in WAI_SUBSCALES.items():
        total = 0
        for item in items:
            rating = WAI_POINTS.get(row.get(f"question{abs(item)}"), 0)
            total += (8 - rating) if item < 0 and rating else rating
        scores[column] = total
    scores["composite_wai"] = sum(scores[c] for c in WAI_SUBSCALES)
    return scores


def score_srs(row: Mapping[str, Any]) -> dict:
    return {"total": round(sum(float(row.get(item) or 0) for item in SRS_ITEMS), 4)}


def score_neq(row: Mapping[str, Any]) -> dict:
    experienced = treatment = other = severity = 0
    for i in range(1, NEQ_QUESTION_COUNT + 1):
        if not is_true(row.get(f"question{i}_experienced")):
            continue
        experienced += 1
        cause = row.get(f"question{i}_cause")
        treatment += cause == NEQ_TREATMENT_CAUSE
        other += cause == NEQ_OTHER_CAUSE
        severity += NEQ_SEVERITY_POINTS.get(row.get(f"question{i}_severity"), 0)
    return {
        "neq_total_effects_experienced": experienced,
        "neq_effects_due_to_treatment": int(treatment),
        "neq_effects_due_to_other": int(other),
        "neq_total_severity_score": severity,
        "neq_avg_severity_of_experienced_effects": severity / experienced if experienced else 0.0,
    }


def flatten_neq_response(response: Mapping[str, Any]) -> dict:
    """Flatten the NEQ `question_responses` array into questionN_{experienced,severity,cause} columns."""
    flat = {}
    for item in response.get("question_responses") or []:
        match = re.search(r"\d+", str(item.get("question_id", "")))
        if not match:
            continue
        q = int(match.group())
        flat[f"question{q}_experienced"] = item.get("experienced")
        flat[f"question{q}_severity"] = item.get("severity")
        flat[f"question{q}_cause"] = item.get("cause")
    flat["other_incidents_or_effects"] = response.get("other_incidents_or_effects", "")
    return flat


def neq_is_complete(flat: Mapping[str, Any]) -> bool:
    return all(f"question{i}_experienced" in flat for i in range(1, NEQ_QUESTION_COUNT + 1))


def score_mi_global(row: Mapping[str, Any]) -> dict:
    def avg(a, b):
        try:
            return (float(row[a]) + float(row[b])) / 2
        except (KeyError, TypeError, ValueError):
            return None

    return {
        "technical_global": avg("cultivating_change_talk_score", "softening_sustain_talk_score"),
        "relational_global": avg("partnership_score", "empathy_score"),
    }


def normalize_behavior_counts(counts: Mapping[str, Any]) -> dict:
    """Map LLM keys (e.g. "Persuade with") to log column names and apply the Affirm cap."""
    normalized = {code: 0 for code in MI_BEHAVIOR_CODES}
    for key, value in counts.items():
        column = key.strip().replace(" ", "_")
        if column in normalized:
            normalized[column] = int(value or 0)
    normalized["AF"] = min(normalized["AF"], AFFIRM_CAP)
    return normalized


def score_mi_behavior(counts: Mapping[str, Any]) -> dict:
    """Summary metrics from MITI behavior counts (expects normalized column names)."""
    adherent = counts["Seek"] + counts["AF"] + counts["Emphasize"]
    non_adherent = counts["Confront"] + counts["Persuade"]
    reflections = counts["SR"] + counts["CR"]
    return {
        "total_mi_adherent": adherent,
        "total_mi_non_adherent": non_adherent,
        "percent_mi_adherent": adherent / (adherent + non_adherent) if adherent + non_adherent else 0.0,
        "percent_cr": counts["CR"] / reflections if reflections else 0.0,
        "r_q_ratio": reflections / counts["Q"] if counts["Q"] else 0.0,
    }


# Log file -> (scoring function, columns it produces). Used to fill in missing scores.
SCORERS = {
    "survey_sure_logs": (score_sure, SURE_SCORE_COLUMNS),
    "survey_wai_logs": (score_wai, WAI_SCORE_COLUMNS),
    "survey_srs_logs": (score_srs, SRS_SCORE_COLUMNS),
    "survey_neq_logs": (score_neq, NEQ_SCORE_COLUMNS),
    "mi_global_eval_logs": (score_mi_global, MI_GLOBAL_SCORE_COLUMNS),
    "mi_batch_behavior_eval_logs": (score_mi_behavior, MI_BEHAVIOR_METRIC_COLUMNS),
}


def add_missing_scores(table: str, df):
    """Compute derived score columns that are absent from a log DataFrame."""
    if table not in SCORERS or df.empty:
        return df
    scorer, columns = SCORERS[table]
    missing = [c for c in columns if c not in df.columns]
    if not missing:
        return df
    scored = df.apply(lambda row: scorer(row.to_dict()), axis=1, result_type="expand")
    for column in missing:
        df[column] = scored[column]
    return df
