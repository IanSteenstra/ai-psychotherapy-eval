import asyncio
import csv
import textwrap
from pathlib import Path

import pytest

from aipsycheval import Simulation, load_config
from aipsycheval import llm

REPO_ROOT = Path(__file__).resolve().parents[1]
DATASET_DIR = REPO_ROOT / "dataset"


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    """Retries sleep with exponential backoff; skip the waiting in tests."""
    async def instant(_seconds):
        return None

    monkeypatch.setattr(llm.asyncio, "sleep", instant)
    llm.USAGE.clear()


def write_config(tmp_path: Path, body: str, name: str = "config.yaml") -> Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return path


MOCK_CONFIG = """
output_dir: {output}
simulation:
  sessions: 2
  max_turns_per_session: 4
  concurrency: 3
  max_retries: 3
patients:
  ids: [1, 13]
models:
  default: {{provider: mock}}
therapists:
  - id: mock_mi
    label: Mock MI
    type: llm
    model: {{provider: mock}}
    prompt_template: ai_therapist_prompt.txt
  - id: mock_chat
    type: llm
    model: {{provider: mock}}
    system_prompt: You are a supportive counselor.
  - id: therapist_psych_material
    type: psych_material
"""


def run_simulation(config_path: Path) -> int:
    return asyncio.run(Simulation(load_config(config_path)).run())


def read_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@pytest.fixture
def mock_config(tmp_path):
    def make(output_name: str = "run") -> Path:
        return write_config(tmp_path, MOCK_CONFIG.format(output=tmp_path / output_name), f"{output_name}.yaml")

    return make
