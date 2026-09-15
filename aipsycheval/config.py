"""Load and validate YAML run configurations."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .constants import DATA_DIR, DEFAULT_PERSONAS_FILE, PROMPT_DIR

PROVIDERS = ("openai", "anthropic", "gemini", "mock")
JSON_MODES = ("schema", "json_object", "prompt")
THERAPIST_TYPES = ("llm", "http", "python", "psych_material", "characterai")

# LLM roles used by the framework itself (not the therapist under evaluation).
ROLES = {
    "patient": "Simulated patient: dialogue turns, surveys (SURE, SRS, WAI, NEQ) and between-session reports",
    "crisis_detector": "Classifies every patient message for acute crises",
    "crisis_response_evaluator": "Scores the therapist's response to a detected crisis against the 4-step action plan",
    "mi_behavior_coder": "MITI 4.2.1 behavior counts for each session transcript",
    "mi_global_rater": "MITI 4.2.1 global ratings for each session transcript",
}
MI_ROLES = ("mi_behavior_coder", "mi_global_rater")

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


class ConfigError(ValueError):
    pass


@dataclass
class ModelSpec:
    provider: str
    model: str = ""
    api_key: str | None = None
    base_url: str | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    json_mode: str = "schema"
    safety_filters: str = "off"
    extra: dict = field(default_factory=dict)

    def describe(self) -> str:
        return f"{self.provider}:{self.model}" if self.model else self.provider


@dataclass
class TherapistSpec:
    id: str
    type: str
    label: str
    model: ModelSpec | None = None
    prompt_template: Path | None = None
    system_prompt: str | None = None
    include_previous_sessions: bool = True
    url: str | None = None
    headers: dict = field(default_factory=dict)
    response_field: str | None = None
    timeout: float = 120.0
    function: str | None = None
    material_file: Path | None = None
    character_id: str | None = None
    token: str | None = None

    @property
    def is_material(self) -> bool:
        return self.type == "psych_material"

    def describe(self) -> str:
        if self.type == "llm":
            return f"llm ({self.model.describe()})"
        if self.type == "http":
            return f"http ({self.url})"
        if self.type == "python":
            return f"python ({self.function})"
        return self.type


@dataclass
class SimulationSpec:
    sessions: int = 4
    max_turns_per_session: int = 48
    replicates: int = 1
    concurrency: int = 1
    max_retries: int = 4
    save_prompts: bool = False


@dataclass
class RunConfig:
    config_path: Path
    output_dir: Path
    simulation: SimulationSpec
    models: dict[str, ModelSpec]
    therapists: list[TherapistSpec]
    personas_file: Path
    patient_ids: list[str] | None
    pairings_file: Path | None
    prompts_dir: Path | None = None

    def model_for(self, role: str) -> ModelSpec:
        return self.models[role]

    @property
    def has_conversational_therapists(self) -> bool:
        return any(not t.is_material for t in self.therapists)


def _interpolate_env(value: Any) -> Any:
    if isinstance(value, str):
        def replace(match: re.Match) -> str:
            name, default = match.group(1), match.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is not None:
                return default
            raise ConfigError(f"Environment variable {name} is not set (referenced as ${{{name}}} in the config).")
        return _ENV_PATTERN.sub(replace, value)
    if isinstance(value, dict):
        return {k: _interpolate_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(v) for v in value]
    return value


def _resolve_path(value: str, base_dir: Path, fallback_dir: Path | None = None) -> Path:
    path = Path(os.path.expanduser(value))
    if path.is_absolute():
        candidates = [path]
    else:
        candidates = [base_dir / path, Path.cwd() / path]
        if fallback_dir is not None:
            candidates.append(fallback_dir / path)
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    searched = ", ".join(str(c) for c in candidates)
    raise ConfigError(f"File not found: {value} (looked in {searched})")


def parse_model_spec(raw: Any, where: str) -> ModelSpec:
    if isinstance(raw, str):
        provider, _, model = raw.partition(":")
        raw = {"provider": provider, "model": model}
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: expected a model mapping like {{provider: openai, model: gpt-4.1}} or a string 'openai:gpt-4.1'.")
    raw = dict(raw)
    provider = raw.pop("provider", None)
    if provider not in PROVIDERS:
        raise ConfigError(f"{where}: provider must be one of {', '.join(PROVIDERS)} (got {provider!r}).")
    spec = ModelSpec(provider=provider)
    for key in ("model", "api_key", "base_url", "temperature", "max_tokens", "json_mode", "safety_filters", "extra"):
        if key in raw:
            setattr(spec, key, raw.pop(key))
    if raw:
        raise ConfigError(f"{where}: unknown model option(s): {', '.join(raw)}")
    if provider != "mock" and not spec.model:
        raise ConfigError(f"{where}: 'model' is required for provider {provider}.")
    if spec.json_mode not in JSON_MODES:
        raise ConfigError(f"{where}: json_mode must be one of {', '.join(JSON_MODES)}.")
    if spec.safety_filters not in ("off", "default"):
        raise ConfigError(f"{where}: safety_filters must be 'off' or 'default'.")
    if not isinstance(spec.extra, dict):
        raise ConfigError(f"{where}: 'extra' must be a mapping of additional API parameters.")
    return spec


def _parse_therapist(raw: Any, index: int, base_dir: Path) -> TherapistSpec:
    where = f"therapists[{index}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: expected a mapping.")
    raw = dict(raw)
    therapist_id = str(raw.pop("id", "") or "")
    if not _ID_PATTERN.match(therapist_id):
        raise ConfigError(f"{where}: 'id' is required and may only contain letters, digits, '_', '-', '.'.")
    where = f"therapist '{therapist_id}'"
    kind = raw.pop("type", "llm")
    if kind not in THERAPIST_TYPES:
        raise ConfigError(f"{where}: type must be one of {', '.join(THERAPIST_TYPES)}.")
    spec = TherapistSpec(id=therapist_id, type=kind, label=str(raw.pop("label", therapist_id)))

    if kind == "llm":
        if "model" not in raw:
            raise ConfigError(f"{where}: 'model' is required for llm therapists.")
        spec.model = parse_model_spec(raw.pop("model"), f"{where}.model")
        template = raw.pop("prompt_template", None)
        system_prompt = raw.pop("system_prompt", None)
        system_prompt_file = raw.pop("system_prompt_file", None)
        if template and (system_prompt or system_prompt_file):
            raise ConfigError(f"{where}: use either prompt_template or system_prompt(_file), not both.")
        if template:
            spec.prompt_template = _resolve_path(template, base_dir, PROMPT_DIR)
        if system_prompt_file:
            spec.system_prompt = _resolve_path(system_prompt_file, base_dir).read_text(encoding="utf-8")
        elif system_prompt:
            spec.system_prompt = str(system_prompt)
        spec.include_previous_sessions = bool(raw.pop("include_previous_sessions", True))
    elif kind == "http":
        spec.url = raw.pop("url", None)
        if not spec.url:
            raise ConfigError(f"{where}: 'url' is required for http therapists.")
        spec.headers = {str(k): str(v) for k, v in (raw.pop("headers", None) or {}).items()}
        spec.response_field = raw.pop("response_field", None)
        spec.timeout = float(raw.pop("timeout", 120))
    elif kind == "python":
        spec.function = raw.pop("function", None)
        if not spec.function or ":" not in spec.function:
            raise ConfigError(f"{where}: 'function' is required, formatted as 'module.path:function_name'.")
    elif kind == "psych_material":
        material = raw.pop("material_file", "psych_edu_prompt.txt")
        spec.material_file = _resolve_path(material, base_dir, PROMPT_DIR)
    elif kind == "characterai":
        spec.character_id = raw.pop("character_id", None)
        spec.token = raw.pop("token", None)
        if not spec.character_id or not spec.token:
            raise ConfigError(f"{where}: 'character_id' and 'token' are required for characterai therapists.")

    if raw:
        raise ConfigError(f"{where}: unknown option(s) for type {kind}: {', '.join(raw)}")
    return spec


def load_config(path: str | Path, output_dir: str | Path | None = None) -> RunConfig:
    config_path = Path(path).resolve()
    if not config_path.exists():
        raise ConfigError(f"Config file not found: {path}")
    try:
        from dotenv import load_dotenv

        load_dotenv(Path.cwd() / ".env")
        load_dotenv(config_path.parent / ".env")
    except ImportError:
        pass

    with open(config_path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    raw = _interpolate_env(raw)
    base_dir = config_path.parent

    known = {"output_dir", "simulation", "models", "therapists", "patients", "pairings_file", "prompts_dir"}
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(f"Unknown top-level config key(s): {', '.join(sorted(unknown))}")

    sim_raw = dict(raw.get("simulation") or {})
    simulation = SimulationSpec()
    for key in list(sim_raw):
        if not hasattr(simulation, key):
            raise ConfigError(f"simulation: unknown option {key!r}")
        setattr(simulation, key, type(getattr(simulation, key))(sim_raw.pop(key)))
    if simulation.sessions < 1 or simulation.max_turns_per_session < 1:
        raise ConfigError("simulation: sessions and max_turns_per_session must be at least 1.")
    if simulation.concurrency < 1 or simulation.replicates < 1:
        raise ConfigError("simulation: concurrency and replicates must be at least 1.")

    therapists_raw = raw.get("therapists") or []
    if not therapists_raw:
        raise ConfigError("Config must define at least one therapist under 'therapists'.")
    therapists = [_parse_therapist(t, i, base_dir) for i, t in enumerate(therapists_raw)]
    ids = [t.id for t in therapists]
    duplicates = {i for i in ids if ids.count(i) > 1}
    if duplicates:
        raise ConfigError(f"Duplicate therapist id(s): {', '.join(sorted(duplicates))}")

    models_raw = dict(raw.get("models") or {})
    default = parse_model_spec(models_raw.pop("default"), "models.default") if "default" in models_raw else None
    models = {}
    for role, spec_raw in models_raw.items():
        if role not in ROLES:
            raise ConfigError(f"models: unknown role {role!r}. Valid roles: default, {', '.join(ROLES)}")
        models[role] = parse_model_spec(spec_raw, f"models.{role}")
    needs_mi = any(not t.is_material for t in therapists)
    for role in ROLES:
        if role in models:
            continue
        if default is not None:
            models[role] = default
        elif role not in MI_ROLES or needs_mi:
            raise ConfigError(f"models: no model configured for role '{role}' ({ROLES[role]}). Add it or set models.default.")

    patients_raw = raw.get("patients") or {}
    personas_file = DEFAULT_PERSONAS_FILE
    if patients_raw.get("personas_file"):
        personas_file = _resolve_path(patients_raw["personas_file"], base_dir, DATA_DIR)
    patient_ids = patients_raw.get("ids", "all")
    patient_ids = None if patient_ids in (None, "all") else [str(p) for p in patient_ids]

    pairings_file = _resolve_path(raw["pairings_file"], base_dir, DATA_DIR) if raw.get("pairings_file") else None

    prompts_dir = _resolve_path(raw["prompts_dir"], base_dir) if raw.get("prompts_dir") else None

    out = output_dir or raw.get("output_dir") or f"runs/{config_path.stem}"
    out_path = Path(os.path.expanduser(str(out)))
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path

    return RunConfig(
        config_path=config_path,
        output_dir=out_path,
        simulation=simulation,
        models=models,
        therapists=therapists,
        personas_file=personas_file,
        patient_ids=patient_ids,
        pairings_file=pairings_file,
        prompts_dir=prompts_dir,
    )
