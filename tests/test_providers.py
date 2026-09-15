"""Request/response handling for each provider, using fake HTTP transports (no network, no keys)."""

import asyncio
import json

import httpx
import pytest

from aipsycheval import llm
from aipsycheval.config import ModelSpec

SCHEMA = {
    "type": "object",
    "properties": {"score": {"type": "integer", "minimum": 1, "maximum": 5}, "reasoning": {"type": "string"}},
    "required": ["score", "reasoning"],
}


def anthropic_http_client(handler):
    """The Anthropic SDK ships its own httpx fork (httpx2) in recent versions."""
    try:
        import httpx2 as http
    except ImportError:
        http = httpx

    def adapt(request):
        response = handler(request)
        return http.Response(response.status_code, content=response.content, headers=dict(response.headers))

    return http.Client(transport=http.MockTransport(adapt))


def client_for(spec, provider):
    client = llm.LLMClient(spec, max_retries=3)
    client._provider = provider
    return client


def test_openai_structured_output_request(monkeypatch):
    seen = []

    def handler(request):
        body = json.loads(request.content)
        seen.append(body)
        content = json.dumps({"score": 4, "reasoning": "ok"})
        return httpx.Response(200, json={
            "id": "c1", "object": "chat.completion", "created": 0, "model": body["model"],
            "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": content}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
        })

    from openai import OpenAI

    spec = ModelSpec(provider="openai", model="gpt-test", api_key="sk-test", temperature=1.0)
    provider = llm.OpenAIProvider(spec)
    provider.client = OpenAI(api_key="sk-test", http_client=httpx.Client(transport=httpx.MockTransport(handler)))
    result = asyncio.run(client_for(spec, provider).complete_json("Rate it.", SCHEMA))

    assert result == {"score": 4, "reasoning": "ok"}
    assert seen[0]["response_format"]["type"] == "json_schema"
    assert seen[0]["response_format"]["json_schema"]["schema"] == SCHEMA
    assert seen[0]["temperature"] == 1.0
    assert llm.USAGE["openai:gpt-test"].input_tokens == 10


def test_openai_invalid_output_is_retried_then_validated():
    replies = iter(['{"score": 9, "reasoning": "out of range"}', "not json", '```json\n{"score": 2, "reasoning": "fixed"}\n```'])

    def handler(request):
        return httpx.Response(200, json={
            "id": "c", "object": "chat.completion", "created": 0, "model": "m",
            "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": next(replies)}}],
        })

    from openai import OpenAI

    spec = ModelSpec(provider="openai", model="m", api_key="k", base_url="http://localhost:1/v1", json_mode="prompt")
    provider = llm.OpenAIProvider(spec)
    provider.client = OpenAI(api_key="k", base_url=spec.base_url, http_client=httpx.Client(transport=httpx.MockTransport(handler)))
    assert asyncio.run(client_for(spec, provider).complete_json("Rate it.", SCHEMA)) == {"score": 2, "reasoning": "fixed"}


def test_anthropic_structured_output_request():
    seen = []

    def handler(request):
        body = json.loads(request.content)
        seen.append(body)
        return httpx.Response(200, json={
            "id": "msg_1", "type": "message", "role": "assistant", "model": body["model"],
            "content": [{"type": "text", "text": json.dumps({"score": 3, "reasoning": "fine"})}],
            "stop_reason": "end_turn", "stop_sequence": None, "usage": {"input_tokens": 7, "output_tokens": 3},
        })

    import anthropic

    spec = ModelSpec(provider="anthropic", model="claude-sonnet-5", api_key="k")
    provider = llm.AnthropicProvider(spec)
    provider.client = anthropic.Anthropic(api_key="k", http_client=anthropic_http_client(handler))
    assert asyncio.run(client_for(spec, provider).complete_json("Rate it.", SCHEMA)) == {"score": 3, "reasoning": "fine"}

    fmt = seen[0]["output_config"]["format"]
    assert fmt["type"] == "json_schema" and fmt["schema"]["additionalProperties"] is False
    assert seen[0]["max_tokens"] == llm.AnthropicProvider.DEFAULT_MAX_TOKENS
    assert "temperature" not in seen[0]


def test_anthropic_chat_messages_and_system_prompt():
    seen = []

    def handler(request):
        seen.append(json.loads(request.content))
        return httpx.Response(200, json={
            "id": "msg_1", "type": "message", "role": "assistant", "model": "m",
            "content": [{"type": "text", "text": "How are you feeling today?"}],
            "stop_reason": "end_turn", "stop_sequence": None, "usage": {"input_tokens": 1, "output_tokens": 1},
        })

    import anthropic

    spec = ModelSpec(provider="anthropic", model="m", api_key="k")
    provider = llm.AnthropicProvider(spec)
    provider.client = anthropic.Anthropic(api_key="k", http_client=anthropic_http_client(handler))
    messages = [{"role": "user", "content": "Hi."}, {"role": "assistant", "content": "Hello."}, {"role": "user", "content": "I drink a lot."}]
    reply = asyncio.run(client_for(spec, provider).complete_text(messages=messages, system="Be kind."))
    assert reply == "How are you feeling today?"
    assert seen[0]["system"] == "Be kind." and seen[0]["messages"] == messages


def test_gemini_structured_output_request():
    seen = []

    def handler(request):
        seen.append(json.loads(request.content))
        return httpx.Response(200, json={
            "candidates": [{"content": {"role": "model", "parts": [{"text": json.dumps({"score": 5, "reasoning": "great"})}]}, "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": 4, "candidatesTokenCount": 2},
        })

    from google import genai
    from google.genai import types

    spec = ModelSpec(provider="gemini", model="gemini-2.5-pro", api_key="k")
    provider = llm.GeminiProvider(spec)
    provider.client = genai.Client(api_key="k", http_options=types.HttpOptions(httpx_client=httpx.Client(transport=httpx.MockTransport(handler))))
    assert asyncio.run(client_for(spec, provider).complete_json("Rate it.", SCHEMA)) == {"score": 5, "reasoning": "great"}

    config = seen[0]["generationConfig"]
    assert config["responseMimeType"] == "application/json" and config["responseJsonSchema"] == SCHEMA
    assert {s["threshold"] for s in seen[0]["safetySettings"]} == {"BLOCK_NONE"}


def test_missing_api_key_fails_fast(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    client = llm.LLMClient(ModelSpec(provider="anthropic", model="m"))
    with pytest.raises(llm.LLMError, match="ANTHROPIC_API_KEY"):
        asyncio.run(client.complete_text(prompt="hi"))


@pytest.mark.parametrize("text", ['{"a": 1}', '```json\n{"a": 1}\n```', 'Sure! Here it is: {"a": 1} Hope that helps.'])
def test_parse_json_tolerates_wrappers(text):
    assert llm.parse_json(text) == {"a": 1}
