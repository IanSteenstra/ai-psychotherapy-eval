"""Therapist adapters: the AI systems under evaluation.

Every adapter receives a `TherapistRequest` for each turn and returns the therapist's reply as text.
Pick the adapter that matches how your system is exposed:

  llm             A model called through a provider SDK, with a prompt template or a system prompt
  http            Your own chatbot API; each turn is POSTed as JSON
  python          A Python function `respond(request) -> str` (sync or async)
  psych_material  Control condition that returns consecutive fragments of a psychoeducation booklet
  characterai     A Character.AI character (requires the optional PyCharacterAI package)
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .config import ConfigError, RunConfig, TherapistSpec
from .llm import LLMClient, LLMError, with_retries

THERAPIST_PREFIXES = ("Therapist (Dr. Anderson):", "Dr. Anderson:", "Therapist:")
END_OF_MATERIAL = "You have reached the end of the educational material."


@dataclass
class TherapistRequest:
    """Everything a therapist needs to produce its next reply."""

    pairing_id: int
    session_id: int
    turn: int
    therapist_id: str
    patient_id: str
    patient_message: str
    # Current session so far, ending with `patient_message`: [{"role": "patient" | "therapist", "content": str}]
    history: list[dict] = field(default_factory=list)
    # Earlier sessions, oldest first, each in the same format as `history`.
    previous_sessions: list[list[dict]] = field(default_factory=list)
    # Earlier sessions formatted as text, exactly as inserted into the dissertation prompts.
    previous_session_transcripts: str = ""

    def openai_messages(self) -> list[dict]:
        return [{"role": "user" if m["role"] == "patient" else "assistant", "content": m["content"]} for m in self.history]

    def to_json(self) -> dict:
        data = asdict(self)
        data["message"] = self.patient_message
        data["messages"] = self.openai_messages()
        return data


def clean_therapist_response(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    stripped = text.strip()
    for prefix in THERAPIST_PREFIXES:
        if stripped.startswith(prefix):
            stripped = stripped[len(prefix):].strip()
            break
    return " ".join(stripped.split())


class Therapist:
    def __init__(self, spec: TherapistSpec, config: RunConfig):
        self.spec = spec
        self.config = config
        self.max_retries = config.simulation.max_retries

    async def respond(self, request: TherapistRequest, state: dict) -> str:
        """Return the reply. `state` is a per-pairing dict persisted across restarts."""
        raise NotImplementedError

    def rendered_prompt(self, request: TherapistRequest) -> str | None:
        """The exact prompt sent for this turn (for prompt logging), if applicable."""
        return None

    async def close(self) -> None:
        pass


class LLMTherapist(Therapist):
    def __init__(self, spec: TherapistSpec, config: RunConfig):
        super().__init__(spec, config)
        self.client = LLMClient(spec.model, self.max_retries)
        self.template = spec.prompt_template.read_text(encoding="utf-8") if spec.prompt_template else None
        if self.template is not None:
            try:
                self.template.format(previous_session_transcripts="", current_session_transcript="", patient_last_message="")
            except (KeyError, IndexError, ValueError) as error:
                raise ConfigError(
                    f"therapist '{spec.id}': prompt template {spec.prompt_template} has an unknown placeholder ({error}). "
                    "Available: {previous_session_transcripts}, {current_session_transcript}, {patient_last_message}. "
                    "Write literal braces as {{ and }}."
                ) from error

    def rendered_prompt(self, request: TherapistRequest) -> str | None:
        if self.template is None:
            return None
        transcript = "\n".join(f"{m['role'].capitalize()}: {m['content']}" for m in request.history)
        return self.template.format(
            previous_session_transcripts=request.previous_session_transcripts,
            current_session_transcript=transcript,
            patient_last_message=request.patient_message,
        )

    async def respond(self, request: TherapistRequest, state: dict) -> str:
        label = f"therapist {self.spec.id} (pairing {request.pairing_id}, session {request.session_id}, turn {request.turn})"
        if self.template is not None:
            return await self.client.complete_text(prompt=self.rendered_prompt(request), label=label)
        system = self.spec.system_prompt or ""
        if self.spec.include_previous_sessions and request.previous_sessions:
            system += f"\n\n---\n[PREVIOUS SESSION TRANSCRIPTS]\n{request.previous_session_transcripts}"
        return await self.client.complete_text(messages=request.openai_messages(), system=system.strip() or None, label=label)


class HTTPTherapist(Therapist):
    async def respond(self, request: TherapistRequest, state: dict) -> str:
        import httpx

        async def attempt() -> str:
            async with httpx.AsyncClient(timeout=self.spec.timeout) as client:
                response = await client.post(self.spec.url, json=request.to_json(), headers=self.spec.headers)
            if response.status_code >= 400:
                error = RuntimeError(f"HTTP {response.status_code}: {response.text[:300]}")
                error.status_code = response.status_code if response.status_code in (400, 401, 403, 404) else None
                raise error
            return self._extract(response)

        label = f"therapist {self.spec.id} POST {self.spec.url}"
        return await with_retries(label, attempt, self.max_retries)

    def _extract(self, response) -> str:
        try:
            payload = response.json()
        except ValueError:
            return response.text
        if self.spec.response_field:
            value = payload
            for part in self.spec.response_field.split("."):
                value = value[int(part)] if isinstance(value, list) else value.get(part) if isinstance(value, dict) else None
            if not isinstance(value, str):
                raise LLMError(f"therapist {self.spec.id}: response_field '{self.spec.response_field}' not found in {str(payload)[:300]}")
            return value
        if isinstance(payload, str):
            return payload
        if isinstance(payload, dict):
            for key in ("reply", "response", "message", "content", "text", "output"):
                if isinstance(payload.get(key), str):
                    return payload[key]
        raise LLMError(f"therapist {self.spec.id}: could not find the reply in {str(payload)[:300]}. Set response_field in the config.")


class PythonTherapist(Therapist):
    def __init__(self, spec: TherapistSpec, config: RunConfig):
        super().__init__(spec, config)
        module_name, _, function_name = spec.function.partition(":")
        for path in (str(config.config_path.parent), str(Path.cwd())):
            if path not in sys.path:
                sys.path.insert(0, path)
        try:
            module = importlib.import_module(module_name)
            self.function = getattr(module, function_name)
        except (ImportError, AttributeError) as error:
            raise ConfigError(f"therapist '{spec.id}': could not import {spec.function}: {error}") from error

    async def respond(self, request: TherapistRequest, state: dict) -> str:
        async def attempt() -> str:
            if inspect.iscoroutinefunction(self.function):
                return await self.function(request)
            return await asyncio.to_thread(self.function, request)

        return await with_retries(f"therapist {self.spec.id} ({self.spec.function})", attempt, self.max_retries)


class PsychMaterialTherapist(Therapist):
    """Splits the booklet into equal word-count fragments, one per therapist turn across all sessions."""

    def __init__(self, spec: TherapistSpec, config: RunConfig):
        super().__init__(spec, config)
        count = config.simulation.sessions * config.simulation.max_turns_per_session
        self.snippets = split_material(spec.material_file.read_text(encoding="utf-8"), count)

    async def respond(self, request: TherapistRequest, state: dict) -> str:
        index = state.get("material_index", 0)
        if index >= len(self.snippets):
            return END_OF_MATERIAL
        state["material_index"] = index + 1
        return self.snippets[index]


def split_material(content: str, num_snippets: int) -> list[str]:
    words = content.split()
    if not words:
        return [""] * num_snippets
    words_per_snippet = max(1, len(words) // num_snippets)
    snippets = [" ".join(words[i : i + words_per_snippet]) for i in range(0, len(words), words_per_snippet)]
    while len(snippets) > num_snippets and len(snippets) > 1:
        last = snippets.pop()
        snippets[-1] += " " + last
    while len(snippets) < num_snippets:
        snippets.append("(End of material)")
    return snippets[:num_snippets]


class CharacterAITherapist(Therapist):
    def __init__(self, spec: TherapistSpec, config: RunConfig):
        super().__init__(spec, config)
        try:
            import PyCharacterAI  # noqa: F401
        except ImportError as error:
            raise ConfigError("characterai therapists need the optional dependency: pip install -e '.[characterai]'") from error
        self._client = None
        self._lock = asyncio.Lock()

    async def _get_client(self):
        async with self._lock:
            if self._client is None:
                from PyCharacterAI import get_client

                self._client = await get_client(token=self.spec.token)
            return self._client

    async def respond(self, request: TherapistRequest, state: dict) -> str:
        async def attempt() -> str:
            client = await self._get_client()
            if not state.get("characterai_chat_id"):
                chat, _ = await client.chat.create_chat(self.spec.character_id)
                state["characterai_chat_id"] = chat.chat_id
            answer = await client.chat.send_message(self.spec.character_id, state["characterai_chat_id"], request.patient_message)
            return answer.get_primary_candidate().text

        return await with_retries(f"therapist {self.spec.id} (Character.AI)", attempt, self.max_retries)

    async def close(self) -> None:
        if self._client is not None and hasattr(self._client, "close_session"):
            await self._client.close_session()


ADAPTERS = {
    "llm": LLMTherapist,
    "http": HTTPTherapist,
    "python": PythonTherapist,
    "psych_material": PsychMaterialTherapist,
    "characterai": CharacterAITherapist,
}


def build_therapist(spec: TherapistSpec, config: RunConfig) -> Therapist:
    return ADAPTERS[spec.type](spec, config)
