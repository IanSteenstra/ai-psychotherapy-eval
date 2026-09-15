import pandas as pd
import pytest

from aipsycheval import scoring

from .conftest import DATASET_DIR


def load(name):
    return pd.read_csv(DATASET_DIR / name, keep_default_na=False, na_values=[""])


@pytest.mark.parametrize(
    "file, scorer, columns",
    [
        ("survey_sure_logs.csv", scoring.score_sure, scoring.SURE_SCORE_COLUMNS),
        ("survey_neq_logs.csv", scoring.score_neq, scoring.NEQ_SCORE_COLUMNS),
        ("survey_srs_logs_with_total.csv", scoring.score_srs, ["total"]),
        ("mi_global_eval_logs_with_globals.csv", scoring.score_mi_global, scoring.MI_GLOBAL_SCORE_COLUMNS),
        ("survey_wai_logs.csv", scoring.score_wai, ["total_wai_task", "total_wai_goal"]),
    ],
)
def test_scores_reproduce_released_dataset(file, scorer, columns):
    df = load(file)
    scored = df.apply(lambda row: scorer(row.to_dict()), axis=1, result_type="expand")
    for column in columns:
        assert (scored[column].astype(float) - df[column].astype(float)).abs().max() < 1e-9, column


def test_mi_behavior_metrics_reproduce_released_dataset():
    df = load("mi_batch_behavior_eval_logs.csv")
    for _, row in df.iterrows():
        counts = scoring.normalize_behavior_counts({code: row[code] for code in scoring.MI_BEHAVIOR_CODES if code in row})
        metrics = scoring.score_mi_behavior(counts)
        for column, value in metrics.items():
            assert abs(value - row[column]) < 1e-9, column


def test_wai_bond_uses_standard_reverse_items():
    # All "Always" (7): reverse-scored items 1, 20, 29 contribute 1 point each, the other nine 7 points.
    row = {f"question{i}": "Always" for i in range(1, 37)}
    assert scoring.score_wai(row)["total_wai_bond"] == 3 * 1 + 9 * 7


def test_behavior_counts_normalization_caps_affirmations():
    counts = scoring.normalize_behavior_counts({"Persuade with": 2, "AF": 9, "Q": 4, "SR": 2, "CR": 2})
    assert counts["Persuade_with"] == 2 and counts["AF"] == 3
    assert scoring.score_mi_behavior(counts)["r_q_ratio"] == 1.0


def test_flatten_neq_accepts_numeric_or_prefixed_ids():
    response = {"question_responses": [{"question_id": "question7", "experienced": True, "severity": "Very", "cause": "Other circumstances"}, {"question_id": "8", "experienced": False}]}
    flat = scoring.flatten_neq_response(response)
    assert flat["question7_severity"] == "Very" and flat["question8_experienced"] is False
    assert not scoring.neq_is_complete(flat)
