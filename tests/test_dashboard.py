import json

import pytest

from aipsycheval.dashboard.app import create_app

from .conftest import DATASET_DIR, run_simulation

ENDPOINTS = [
    "/", "/pairings", "/interventions", "/patient-personas", "/therapist-prompts",
    "/api/filters", "/api/pairings/filters", "/api/pairings?limit=5", "/api/interventions", "/api/interventions?crisis=with_crisis",
    "/api/dashboard-summary", "/api/crisis-events", "/api/action-plan-adherence", "/api/overall-adherence",
    "/api/therapist-comparison", "/api/therapist-comparison-neq", "/api/therapist-comparison-sure", "/api/therapist-comparison-wai",
    "/api/mi-global-profile", "/api/mi-global-metrics", "/api/mi-behavior-metrics",
    "/api/neq-aggregate-breakdown", "/api/neq-aggregate-breakdown?view=subtype", "/api/neq-aggregate-totals", "/api/neq-session-trends",
    "/api/neq-question-summary", "/api/sure-domain-aggregates", "/api/sure-session-trends", "/api/sure-domain-session-trends",
    "/api/srs-session-component-trends", "/api/wai-session-component-trends", "/api/score-trends-over-sessions",
    "/api/scores-by-patient-type", "/api/adverse-outcomes", "/api/adverse-outcome-attributions?events=treatment_dropout",
    "/api/equity-audit", "/api/equity-audit?equity_event=relapse_substance_use", "/api/patient-retention-by-session",
    "/api/in-session-warning-signs?construct=hopelessness_intensity&pairing=1",
    "/api/pairings/context?pairing=1", "/api/interventions/detail?pairing_id=1&session_id=1",
    "/api/transcript-snippet?pairing_id=1&session_id=1&turn=2", "/api/therapist-comparison?session=1&pairing=1,2",
]


def check_endpoints(client):
    for url in ENDPOINTS:
        response = client.get(url)
        assert response.status_code == 200, (url, response.get_data(as_text=True)[:300])
        if response.mimetype == "application/json":
            text = response.get_data(as_text=True)
            assert "NaN" not in text, url
            json.loads(text)


def test_dashboard_on_released_dataset():
    client = create_app(DATASET_DIR).test_client()
    check_endpoints(client)
    summary = client.get("/api/dashboard-summary").get_json()
    assert summary["sessions"] == 369 and summary["therapists"] == 6
    wai = {row["therapist_id"]: row for row in client.get("/api/therapist-comparison-wai").get_json()}
    assert "therapist_psych_material" not in wai and len(wai) == 5


def test_dashboard_on_new_run_with_custom_therapists(mock_config, tmp_path):
    run_simulation(mock_config())
    client = create_app(tmp_path / "run").test_client()
    check_endpoints(client)
    assert b'"mock_mi": "Mock MI"' in client.get("/").data
    assert {row["therapist_id"] for row in client.get("/api/therapist-comparison-sure").get_json()} == {"mock_mi", "mock_chat", "therapist_psych_material"}


def test_dashboard_on_empty_directory(tmp_path):
    client = create_app(tmp_path).test_client()
    for url in ENDPOINTS:
        assert client.get(url).status_code in (200, 404), url
