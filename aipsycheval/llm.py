"""Provider-agnostic LLM client with structured (JSON schema) output, validation, and retries.

Supported providers:
  openai     OpenAI, or any OpenAI-compatible server via `base_url` (Ollama, vLLM, OpenRouter, Together, ...)
  anthropic  Claude models
  gemini     Google Gemini (google-genai SDK)
  mock       Offline fake responses for trying the pipeline without API keys (results are meaningless)
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import os
import random
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from jsonschema import Draft7Validator

from .config import ModelSpec

logger = logging.getLogger(__name__)

Messages = list[dict[str, str]]  # [{"role": "user" | "assistant", "content": str}]


class LLMError(RuntimeError):
    """Raised when a model call fails after all retries (or with a non-retryable error)."""


class InvalidResponse(ValueError):
    """The model answered, but the answer was empty, truncated, or did not match the expected schema."""


@dataclass
class Usage:
    calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0


# Aggregated token usage per model, written to usage.json at the end of a run.
USAGE: dict[str, Usage] = defaultdict(Usage)


def _record_usage(spec: ModelSpec, input_tokens: int | None, output_tokens: int | None) -> None:
    usage = USAGE[spec.describe()]
    usage.calls += 1
    usage.input_tokens += input_tokens or 0
    usage.output_tokens += output_tokens or 0


# --------------------------------------------------------------------------------------
# Providers: each exposes generate(messages, system, schema, json_mode) -> str
# --------------------------------------------------------------------------------------


class OpenAIProvider:
    def __init__(self, spec: ModelSpec):
        from openai import OpenAI

        api_key = spec.api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            if not spec.base_url:
                raise LLMError("OPENAI_API_KEY is not set. Export it, add it to .env, or set api_key in the config.")
            api_key = "not-needed"  # local OpenAI-compatible servers usually ignore the key
        self.spec = spec
        self.client = OpenAI(api_key=api_key, base_url=spec.base_url)

    def generate(self, messages: Messages, system: str | None, schema: dict | None, json_mode: str) -> str:
        spec = self.spec
        kwargs: dict[str, Any] = {
            "model": spec.model,
            "messages": ([{"role": "system", "content": system}] if system else []) + messages,
        }
        if spec.temperature is not None:
            kwargs["temperature"] = spec.temperature
        if spec.max_tokens:
            kwargs["max_tokens" if spec.base_url else "max_completion_tokens"] = spec.max_tokens
        if schema is not None and json_mode == "schema":
            kwargs["response_format"] = {"type": "json_schema", "json_schema": {"name": "response", "schema": schema}}
        elif schema is not None and json_mode == "json_object":
            kwargs["response_format"] = {"type": "json_object"}
        kwargs.update(spec.extra)

        response = self.client.chat.completions.create(**kwargs)
        usage = getattr(response, "usage", None)
        _record_usage(spec, getattr(usage, "prompt_tokens", 0), getattr(usage, "completion_tokens", 0))
        if not response.choices:
            raise InvalidResponse("Empty response (no choices).")
        choice = response.choices[0]
        content = choice.message.content
        if choice.finish_reason == "length":
            raise InvalidResponse("Response was truncated (finish_reason=length). Increase max_tokens.")
        if not content:
            refusal = getattr(choice.message, "refusal", None)
            raise InvalidResponse(f"Empty response{f' (refusal: {refusal})' if refusal else ''}.")
        return content


class AnthropicProvider:
    DEFAULT_MAX_TOKENS = 8192

    def __init__(self, spec: ModelSpec):
        import anthropic

        api_key = spec.api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise LLMError("ANTHROPIC_API_KEY is not set. Export it, add it to .env, or set api_key in the config.")
        self.spec = spec
        self.client = anthropic.Anthropic(api_key=api_key, base_url=spec.base_url)

    @staticmethod
    def _strict_schema(schema: dict) -> dict:
        try:
            from anthropic.lib._parse._transform import transform_schema

            return transform_schema(copy.deepcopy(schema))
        except Exception:  # SDK internals moved; fall back to a minimal transform
            return _close_objects(copy.deepcopy(schema))

    def generate(self, messages: Messages, system: str | None, schema: dict | None, json_mode: str) -> str:
        spec = self.spec
        kwargs: dict[str, Any] = {
            "model": spec.model,
            "max_tokens": spec.max_tokens or self.DEFAULT_MAX_TOKENS,
            "messages": messages,
        }
        if system:
            kwargs["system"] = system
        extra = dict(spec.extra)
        extra_body = dict(extra.pop("extra_body", None) or {})
        if spec.temperature is not None:
            # The Messages SDK no longer exposes sampling parameters; send it raw for models that accept it.
            extra_body["temperature"] = spec.temperature
        if extra_body:
            kwargs["extra_body"] = extra_body
        if schema is not None and json_mode == "schema":
            kwargs["output_config"] = {"format": {"type": "json_schema", "schema": self._strict_schema(schema)}}
        kwargs.update(extra)

        response = self.client.messages.create(**kwargs)
        usage = getattr(response, "usage", None)
        _record_usage(spec, getattr(usage, "input_tokens", 0), getattr(usage, "output_tokens", 0))
        if response.stop_reason == "max_tokens":
            raise InvalidResponse("Response was truncated (stop_reason=max_tokens). Increase max_tokens.")
        if response.stop_reason == "refusal":
            raise InvalidResponse("The model declined to respond (stop_reason=refusal).")
        text = "".join(block.text for block in response.content if getattr(block, "type", None) == "text")
        if not text.strip():
            raise InvalidResponse("Empty response.")
        return text


class GeminiProvider:
    UNFILTERED_CATEGORIES = (
        "HARM_CATEGORY_HARASSMENT",
        "HARM_CATEGORY_HATE_SPEECH",
        "HARM_CATEGORY_SEXUALLY_EXPLICIT",
        "HARM_CATEGORY_DANGEROUS_CONTENT",
    )

    def __init__(self, spec: ModelSpec):
        from google import genai
        from google.genai import types

        api_key = spec.api_key or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise LLMError("GEMINI_API_KEY is not set. Export it, add it to .env, or set api_key in the config.")
        self.spec = spec
        self.types = types
        http_options = types.HttpOptions(base_url=spec.base_url) if spec.base_url else None
        self.client = genai.Client(api_key=api_key, http_options=http_options)

    def generate(self, messages: Messages, system: str | None, schema: dict | None, json_mode: str) -> str:
        spec, types = self.spec, self.types
        contents = [
            types.Content(role="model" if m["role"] == "assistant" else "user", parts=[types.Part(text=m["content"])])
            for m in messages
        ]
        config: dict[str, Any] = {}
        if spec.temperature is not None:
            config["temperature"] = spec.temperature
        if spec.max_tokens:
            config["max_output_tokens"] = spec.max_tokens
        if system:
            config["system_instruction"] = system
        if spec.safety_filters == "off":
            # The simulation discusses suicide, self-harm and substance use; default filters block many turns.
            config["safety_settings"] = [
                types.SafetySetting(category=category, threshold="BLOCK_NONE") for category in self.UNFILTERED_CATEGORIES
            ]
        if schema is not None and json_mode in ("schema", "json_object"):
            config["response_mime_type"] = "application/json"
            if json_mode == "schema":
                config["response_json_schema"] = schema
        config.update(spec.extra)

        response = self.client.models.generate_content(
            model=spec.model, contents=contents, config=types.GenerateContentConfig(**config)
        )
        usage = getattr(response, "usage_metadata", None)
        _record_usage(spec, getattr(usage, "prompt_token_count", 0), getattr(usage, "candidates_token_count", 0))
        text = response.text
        if not text:
            feedback = getattr(response, "prompt_feedback", None)
            finish = response.candidates[0].finish_reason if response.candidates else None
            raise InvalidResponse(f"Empty response (finish_reason={finish}, prompt_feedback={feedback}).")
        return text


class MockProvider:
    """Deterministic fake responses so the full pipeline can run offline."""

    PATIENT_LINES = [
        "I guess I drink more than I should, but it helps me unwind.",
        "Honestly, I'm not sure I'm ready to cut back yet.",
        "My family keeps bringing it up and it's getting old.",
        "I tried skipping a few nights last week. It was harder than I thought.",
        "Maybe I could stick to weekends. That feels doable.",
        "I don't really want to talk about that part.",
    ]
    THERAPIST_LINES = [
        "Thanks for sharing that. What does a typical evening look like for you?",
        "It sounds like drinking helps you cope with stress. What else helps?",
        "You've already noticed some changes you'd like to make. Tell me more.",
        "What would be one small step you could try this week?",
        "It makes sense to feel unsure. What matters most to you right now?",
    ]

    def __init__(self, spec: ModelSpec):
        self.spec = spec

    def generate(self, messages: Messages, system: str | None, schema: dict | None, json_mode: str) -> str:
        seed_text = (system or "") + "\n".join(m["content"] for m in messages) + json.dumps(schema or {}, sort_keys=True)
        rng = random.Random(hashlib.sha256(seed_text.encode("utf-8")).hexdigest())
        _record_usage(self.spec, len(seed_text) // 4, 50)
        if schema is None:
            return rng.choice(self.THERAPIST_LINES)
        return json.dumps(self._fake(schema, rng, key=""))

    # Rough event rates so demo runs look plausible (e.g. most pairings reach later sessions).
    OCCURRENCE_RATES = {"death_by_suicide": 0.003, "suicide_attempt": 0.01, "treatment_dropout": 0.03}

    def _fake(self, schema: dict, rng: random.Random, key: str, parent: str = "") -> Any:
        kind = schema.get("type")
        if "enum" in schema:
            options = schema["enum"]
            if key == "classification":
                return "No Crisis" if rng.random() < 0.92 else rng.choice([o for o in options if o != "No Crisis"])
            return rng.choice(options)
        if kind == "object":
            return {k: self._fake(v, rng, k, key) for k, v in schema.get("properties", {}).items()}
        if kind == "array":
            count = 32 if key == "question_responses" else 2
            items = [self._fake(schema.get("items", {}), rng, key, parent) for _ in range(count)]
            if key == "question_responses":
                for i, item in enumerate(items, start=1):
                    item["question_id"] = str(i)
            return items
        if kind == "boolean":
            if key == "occurred":
                return rng.random() < self.OCCURRENCE_RATES.get(parent, 0.08)
            return rng.random() < {"session_conclusion": 0.03, "experienced": 0.3}.get(key, 0.5)
        if kind == "integer":
            return rng.randint(schema.get("minimum", 0), schema.get("maximum", 5))
        if kind == "number":
            return round(rng.uniform(schema.get("minimum", 0), schema.get("maximum", 10)), 1)
        if kind == "string":
            if key == "response_formulation":
                return rng.choice(self.PATIENT_LINES)
            return f"Mock {key.replace('_', ' ') or 'text'}."
        return None


PROVIDER_CLASSES = {
    "openai": OpenAIProvider,
    "anthropic": AnthropicProvider,
    "gemini": GeminiProvider,
    "mock": MockProvider,
}
NATIVE_JSON_MODES = {
    "openai": {"schema", "json_object"},
    "anthropic": {"schema"},
    "gemini": {"schema", "json_object"},
    "mock": {"schema", "json_object"},
}


# --------------------------------------------------------------------------------------
# Client
# --------------------------------------------------------------------------------------


def _close_objects(schema: Any) -> Any:
    if isinstance(schema, dict):
        if schema.get("type") == "object":
            schema["additionalProperties"] = False
        for value in schema.values():
            _close_objects(value)
    elif isinstance(schema, list):
        for value in schema:
            _close_objects(value)
    return schema


def parse_json(text: str) -> Any:
    """Parse JSON from a model response, tolerating code fences or surrounding prose."""
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    if fenced:
        try:
            return json.loads(fenced.group(1))
        except json.JSONDecodeError:
            pass
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass
    raise InvalidResponse(f"Response is not valid JSON: {text[:200]!r}")


def _schema_instructions(schema: dict) -> str:
    return (
        "\n\nRespond with only a single JSON object (no markdown, no commentary) that conforms to this JSON schema:\n"
        + json.dumps(schema, indent=2)
    )


def _is_retryable(error: Exception) -> bool:
    if isinstance(error, (InvalidResponse, TimeoutError, ConnectionError)):
        return True
    if isinstance(error, (ImportError, TypeError, AttributeError, KeyError)):
        return False  # programming or installation errors will not fix themselves
    status = getattr(error, "status_code", None) or getattr(error, "code", None)
    if isinstance(status, int) and status in (400, 401, 403, 404, 422):
        return False
    return type(error).__name__ not in {"AuthenticationError", "PermissionDeniedError", "NotFoundError", "BadRequestError"}


async def with_retries(label: str, attempt_fn: Callable[[], Awaitable[Any]], max_retries: int, json_mode: str | None = None) -> Any:
    """Run an async call with exponential backoff. Configuration errors (bad key, unknown model) fail fast."""
    for attempt in range(1, max_retries + 1):
        try:
            return await attempt_fn()
        except LLMError:
            raise
        except Exception as error:  # noqa: BLE001 - provider SDKs raise many exception types
            if not _is_retryable(error):
                hint = ""
                if getattr(error, "status_code", None) == 400 and json_mode == "schema":
                    hint = " If the provider rejected the JSON schema, set `json_mode: prompt` for this model."
                raise LLMError(f"{label}: {error}{hint}") from error
            if attempt == max_retries:
                raise LLMError(f"{label}: failed after {attempt} attempts: {error}") from error
            delay = min(60.0, 2.0**attempt) + random.random()
            logger.warning("%s attempt %d/%d failed: %s (retrying in %.0fs)", label, attempt, max_retries, error, delay)
            await asyncio.sleep(delay)


class LLMClient:
    def __init__(self, spec: ModelSpec, max_retries: int = 4):
        self.spec = spec
        self.max_retries = max(1, max_retries)
        self._provider = None

    @property
    def provider(self):
        if self._provider is None:
            self._provider = PROVIDER_CLASSES[self.spec.provider](self.spec)
        return self._provider

    async def _call(self, label: str, fn: Callable[[], Any]) -> Any:
        return await with_retries(f"{label} [{self.spec.describe()}]", lambda: asyncio.to_thread(fn), self.max_retries, json_mode=self.spec.json_mode)

    async def complete_text(self, *, prompt: str | None = None, messages: Messages | None = None, system: str | None = None, label: str = "llm call") -> str:
        messages = messages if messages is not None else [{"role": "user", "content": prompt or ""}]
        return await self._call(label, lambda: self.provider.generate(messages, system, None, self.spec.json_mode))

    async def complete_json(self, prompt: str, schema: dict, *, validate: Callable[[dict], None] | None = None, label: str = "llm call") -> dict:
        json_mode = self.spec.json_mode
        if json_mode not in NATIVE_JSON_MODES[self.spec.provider]:
            json_mode = "prompt"
        if json_mode == "prompt":
            prompt = prompt + _schema_instructions(schema)
        validator = Draft7Validator(schema)
        messages = [{"role": "user", "content": prompt}]

        def run() -> dict:
            data = parse_json(self.provider.generate(messages, None, schema, json_mode))
            errors = sorted(validator.iter_errors(data), key=lambda e: list(e.path))
            if errors:
                first = errors[0]
                location = "/".join(str(p) for p in first.path) or "(root)"
                raise InvalidResponse(f"Response does not match schema at {location}: {first.message}")
            if validate is not None:
                validate(data)
            return data

        return await self._call(label, run)
