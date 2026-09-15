import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from aipsycheval.config import ConfigError, load_config
from aipsycheval.therapists import TherapistRequest, build_therapist, clean_therapist_response, split_material

from .conftest import write_config


def make_request(**overrides):
    base = dict(
        pairing_id=1, session_id=2, turn=3, therapist_id="t", patient_id="1", patient_message="I had a rough week.",
        history=[{"role": "patient", "content": "Hi."}, {"role": "therapist", "content": "Hello."}, {"role": "patient", "content": "I had a rough week."}],
        previous_sessions=[[{"role": "patient", "content": "First session."}]], previous_session_transcripts="--- Session 1 ---\nPatient: First session.",
    )
    base.update(overrides)
    return TherapistRequest(**base)


def test_http_therapist_posts_turn_and_reads_reply(tmp_path):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            received[-1]["_auth"] = self.headers.get("Authorization")
            body = json.dumps({"data": {"text": "That sounds hard. What happened?"}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        config = load_config(write_config(tmp_path, f"""
            output_dir: {tmp_path / 'out'}
            models: {{default: {{provider: mock}}}}
            therapists:
              - id: my_api
                type: http
                url: http://127.0.0.1:{server.server_port}/chat
                headers: {{Authorization: Bearer secret}}
                response_field: data.text
        """))
        therapist = build_therapist(config.therapists[0], config)
        reply = asyncio.run(therapist.respond(make_request(), {}))
    finally:
        server.shutdown()

    assert reply == "That sounds hard. What happened?"
    assert received[0]["message"] == "I had a rough week." and received[0]["_auth"] == "Bearer secret"
    assert received[0]["messages"][-1] == {"role": "user", "content": "I had a rough week."}
    assert received[0]["messages"][1]["role"] == "assistant"


def test_llm_template_therapist_renders_dissertation_prompt(tmp_path):
    config = load_config(write_config(tmp_path, """
        models: {default: {provider: mock}}
        therapists:
          - {id: t, type: llm, model: {provider: mock}, prompt_template: limited_prompt.txt}
    """))
    prompt = build_therapist(config.therapists[0], config).rendered_prompt(make_request())
    assert "Patient: Hi.\nTherapist: Hello.\nPatient: I had a rough week." in prompt
    assert 'Patient: "I had a rough week."' in prompt and "--- Session 1 ---" in prompt


def test_clean_therapist_response_strips_speaker_prefix():
    assert clean_therapist_response("  Dr. Anderson:  How are\n you?  ") == "How are you?"
    assert clean_therapist_response(None) == ""


def test_split_material_returns_requested_number_of_fragments():
    fragments = split_material(" ".join(f"w{i}" for i in range(100)), 8)
    assert len(fragments) == 8 and " ".join(fragments).split() == [f"w{i}" for i in range(100)]


@pytest.mark.parametrize(
    "body, message",
    [
        ("models: {default: {provider: mock}}\ntherapists: []", "at least one therapist"),
        ("models: {patient: {provider: mock}}\ntherapists: [{id: t, type: psych_material}]", "crisis_detector"),
        ("models: {default: {provider: nope}}\ntherapists: [{id: t, type: psych_material}]", "provider must be one of"),
        ("models: {default: {provider: mock}}\ntherapists: [{id: t, type: llm, model: {provider: mock}, bogus: 1}]", "unknown option"),
        ("models: {default: {provider: mock}}\ntherapists: [{id: t, type: llm, model: {provider: mock}, prompt_template: missing.txt}]", "File not found"),
        ("models: {default: {provider: openai, model: m, api_key: '${DEFINITELY_UNSET_VAR}'}}\ntherapists: [{id: t, type: psych_material}]", "DEFINITELY_UNSET_VAR"),
        ("modles: {}\ntherapists: [{id: t, type: psych_material}]", "Unknown top-level"),
    ],
)
def test_config_errors_are_clear(tmp_path, body, message):
    with pytest.raises(ConfigError, match=message):
        load_config(write_config(tmp_path, body))


def test_mi_models_not_required_for_material_only_runs(tmp_path):
    body = "models:\n  patient: mock\n  crisis_detector: mock\n  crisis_response_evaluator: mock\ntherapists: [{id: t, type: psych_material}]"
    assert load_config(write_config(tmp_path, body)).models["patient"].provider == "mock"


def test_bad_template_placeholder_is_reported(tmp_path):
    (tmp_path / "bad.txt").write_text("Respond as JSON like {reply: ...} to {patient_last_message}")
    config = load_config(write_config(tmp_path, "models: {default: mock}\ntherapists: [{id: t, type: llm, model: mock, prompt_template: bad.txt}]"))
    with pytest.raises(ConfigError, match="unknown placeholder"):
        build_therapist(config.therapists[0], config)
