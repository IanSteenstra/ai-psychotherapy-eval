"""Multi-session simulation between AI therapists and simulated patients.

Each pairing (therapist x patient persona) runs for up to `simulation.sessions` sessions.
A session has four stages, each checkpointed so an interrupted run resumes where it stopped:

  Pre-session      patient completes the SURE (patient progress)
  In-session       up to `max_turns_per_session` turns: patient reply + state update, crisis
                   detection on every patient message, therapist reply, and crisis-response
                   evaluation whenever a crisis was detected
  Post-session     MITI behavior counts and global ratings (treatment fidelity), SRS and WAI
                   (alliance), NEQ (negative effects)
  Between-sessions patient simulates the following week: journal, adverse events, state update.
                   Death by suicide or treatment dropout ends the pairing.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import csv
import json
import logging
import shutil
import traceback
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path

from tqdm import tqdm

from . import constants as C
from . import scoring
from .config import ConfigError, RunConfig
from .llm import USAGE, LLMClient, LLMError, InvalidResponse
from .storage import PairingState, RunStore
from .therapists import Therapist, TherapistRequest, build_therapist, clean_therapist_response

logger = logging.getLogger("aipsycheval")

PATIENT_OPENERS = {
    "first_session": "I'd like to talk to you about my drinking.",
    "later_session": "Hi.",
    "material": "I'm ready to start reading the material.",
}

PROMPT_FILES = {
    "patient_turn": "patient_turn_prompt.txt",
    "patient_read": "patient_read_prompt.txt",
    "report": "after_session_report_prompt.txt",
    "report_material": "after_session_report_material_prompt.txt",
    "sure": "survey_sure_prompt.txt",
    "sure_material": "survey_sure_material_prompt.txt",
    "neq": "survey_neq_prompt.txt",
    "neq_material": "survey_neq_material_prompt.txt",
    "srs": "survey_srs_prompt.txt",
    "wai": "survey_wai_prompt.txt",
    "crisis": "crisis_detector_prompt.txt",
    "action_plan": "action_plan_prompt.txt",
    "mi_behavior": "mi_batch_behavior_prompt.txt",
    "mi_global": "global_scores_prompt.txt",
    "miti_manual": "miti4_2.txt",
}

SCHEMA_FILES = {
    "patient": "patient_schema.json",
    "report": "after_session_report_schema.json",
    "sure": "survey_sure_schema.json",
    "srs": "survey_srs_schema.json",
    "wai": "survey_wai_schema.json",
    "neq": "survey_neq_schema.json",
    "crisis": "crisis_schema.json",
    "action_plan": "action_plan_schema.json",
    "mi_behavior": "mi_batch_behavior_schema.json",
    "mi_global": "global_scores_schema.json",
}


def sanitize_text(text) -> str:
    if not isinstance(text, str):
        return ""
    return " ".join(text.split()).strip()


def flatten_nested_dict(d: dict, parent_key: str = "", sep: str = "_") -> dict:
    items = {}
    for k, v in d.items():
        key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_nested_dict(v, key, sep))
        else:
            items[key] = v
    return items


@dataclass
class Pairing:
    pairing_id: int
    therapist_id: str
    patient_id: str
    replicate: int = 1


@dataclass
class PairingContext:
    """Committed transcripts and journals for one pairing, used to build prompts."""

    sessions: dict[int, list[dict]] = field(default_factory=lambda: defaultdict(list))
    journals: dict[int, str] = field(default_factory=dict)


def format_session(messages: list[dict], therapist_label: str, empty: str = "No conversation turns recorded for this session.") -> str:
    if not messages:
        return empty
    return "\n".join(f"{therapist_label if m['role'] == 'therapist' else 'Patient'}: {m['content']}" for m in messages)


def format_previous_sessions(ctx: PairingContext, session: int, therapist_label: str) -> str:
    previous = [s for s in sorted(ctx.sessions) if s < session and ctx.sessions[s]]
    if not previous:
        return "No previous sessions have occurred."
    return "\n\n".join(f"--- Session {s} ---\n" + format_session(ctx.sessions[s], therapist_label) for s in previous)


def format_journals(ctx: PairingContext, session: int) -> str:
    previous = [s for s in sorted(ctx.journals) if s < session]
    if not previous:
        return "No journaling entries from previous weeks."
    return "\n".join(f"--- Journal Entry from week after Session {s} ---\n{ctx.journals[s]}\n" for s in previous)


class _TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        tqdm.write(self.format(record))


class Simulation:
    def __init__(self, config: RunConfig):
        self.config = config
        self.sim = config.simulation
        self.prompts = {name: self._load_prompt(filename) for name, filename in PROMPT_FILES.items()}
        self.schemas = {name: json.loads((C.SCHEMA_DIR / filename).read_text(encoding="utf-8")) for name, filename in SCHEMA_FILES.items()}
        self.personas = self._load_personas()
        self.therapist_specs = {t.id: t for t in config.therapists}
        self.pairings = self._plan_pairings()

    # ------------------------------------------------------------------ setup
    def _load_prompt(self, filename: str) -> str:
        if self.config.prompts_dir and (self.config.prompts_dir / filename).exists():
            return (self.config.prompts_dir / filename).read_text(encoding="utf-8")
        return (C.PROMPT_DIR / filename).read_text(encoding="utf-8")

    def _load_personas(self) -> dict[str, dict]:
        with open(self.config.personas_file, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            raise ConfigError(f"No personas found in {self.config.personas_file}")
        personas = {str(row["patient_id"]): row for row in rows}
        missing_constructs = [k for k in C.PSYCHOLOGICAL_CONSTRUCTS_KEYS if k not in rows[0]]
        if missing_constructs:
            raise ConfigError(f"{self.config.personas_file} is missing baseline construct columns: {', '.join(missing_constructs)}")
        if self.config.patient_ids is not None:
            unknown = [p for p in self.config.patient_ids if p not in personas]
            if unknown:
                raise ConfigError(f"patients.ids: unknown patient id(s) {', '.join(unknown)}")
        # Render one prompt to catch persona columns that the prompts need but the file lacks.
        sample = next(iter(personas.values()))
        state = {k: 1 for k in C.PSYCHOLOGICAL_CONSTRUCTS_KEYS}
        try:
            self.prompts["patient_turn"].format(persona_data=sample, current_psych_state=state, previous_session_transcripts="", previous_journaling="", current_session_transcript="", therapist_message="")
        except KeyError as error:
            raise ConfigError(f"{self.config.personas_file} is missing column {error} used by the patient prompts.") from error
        return personas

    def _plan_pairings(self) -> list[Pairing]:
        self.skipped_therapists: list[str] = []
        output_pairings = self.config.output_dir / C.PAIRINGS_FILE
        existing: list[Pairing] = []
        if output_pairings.exists():
            with open(output_pairings, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    existing.append(Pairing(int(row["pairing_id"]), row["therapist_id"], str(row["patient_id"]), int(row.get("replicate") or 1)))

        patient_ids = self.config.patient_ids or list(self.personas)
        if self.config.pairings_file:
            with open(self.config.pairings_file, newline="", encoding="utf-8") as f:
                wanted = [Pairing(int(r["pairing_id"]), r["therapist_id"], str(r["patient_id"]), int(r.get("replicate") or 1)) for r in csv.DictReader(f)]
            # Pairings for therapists missing from the config are skipped (e.g. no Character.AI token).
            self.skipped_therapists = sorted({p.therapist_id for p in wanted} - set(self.therapist_specs))
            wanted = [p for p in wanted if p.patient_id in patient_ids and p.therapist_id in self.therapist_specs]
            by_id = {p.pairing_id: p for p in existing}
            for p in wanted:
                old = by_id.get(p.pairing_id)
                if old and (old.therapist_id, old.patient_id) != (p.therapist_id, p.patient_id):
                    raise ConfigError(f"pairing {p.pairing_id} in {output_pairings} does not match pairings_file. Use a new output directory.")
            return wanted

        keyed = {(p.therapist_id, p.patient_id, p.replicate): p for p in existing}
        next_id = max((p.pairing_id for p in existing), default=0) + 1
        planned = []
        for replicate in range(1, self.sim.replicates + 1):
            for patient_id in patient_ids:
                for therapist in self.config.therapists:
                    key = (therapist.id, patient_id, replicate)
                    if key not in keyed:
                        keyed[key] = Pairing(next_id, therapist.id, patient_id, replicate)
                        next_id += 1
                    planned.append(keyed[key])
        return sorted(planned, key=lambda p: p.pairing_id)

    def _write_run_metadata(self) -> None:
        out = self.config.output_dir
        pairings_path = out / C.PAIRINGS_FILE
        rows = {}
        if pairings_path.exists():
            with open(pairings_path, newline="", encoding="utf-8") as f:
                rows = {int(r["pairing_id"]): r for r in csv.DictReader(f)}
        for p in self.pairings:
            rows[p.pairing_id] = {"pairing_id": p.pairing_id, "therapist_id": p.therapist_id, "patient_id": p.patient_id, "replicate": p.replicate}
        with open(pairings_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["pairing_id", "therapist_id", "patient_id", "replicate"], extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows[k] for k in sorted(rows))

        therapists_path = out / C.THERAPISTS_FILE
        therapists = {}
        if therapists_path.exists():
            with open(therapists_path, newline="", encoding="utf-8") as f:
                therapists = {r["therapist_id"]: r for r in csv.DictReader(f)}
        for t in self.config.therapists:
            therapists[t.id] = {"therapist_id": t.id, "label": t.label, "type": t.type, "description": t.describe()}
        with open(therapists_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["therapist_id", "label", "type", "description"])
            writer.writeheader()
            writer.writerows(therapists.values())

        shutil.copyfile(self.config.personas_file, out / C.PERSONAS_FILE)
        shutil.copyfile(self.config.config_path, out / "config.yaml")

    # ------------------------------------------------------------------ planning
    def estimate_calls(self) -> dict[str, int]:
        """Upper bound on model calls (every session runs all turns; crisis evaluations excluded)."""
        s, t = self.sim.sessions, self.sim.max_turns_per_session
        calls: dict[str, int] = defaultdict(int)
        for p in self.pairings:
            spec = self.therapist_specs[p.therapist_id]
            material = spec.is_material
            calls["patient"] += s * ((t - 1) + 3 + (0 if material else 2))
            calls["crisis_detector"] += s * t
            calls[f"therapist:{spec.id}"] += 0 if spec.type == "psych_material" else s * t
            if not material:
                calls["mi_behavior_coder"] += s
                calls["mi_global_rater"] += s
        return dict(calls)

    def describe(self, states: dict[int, PairingState]) -> str:
        done = sum(1 for p in self.pairings if p.pairing_id in states and states[p.pairing_id].finished)
        started = sum(1 for p in self.pairings if p.pairing_id in states and not states[p.pairing_id].finished)
        lines = [
            f"Config:      {self.config.config_path}",
            f"Output:      {self.config.output_dir}",
            f"Pairings:    {len(self.pairings)} ({done} finished, {started} to resume, {len(self.pairings) - done - started} new)"
            + (f"; skipping pairings for therapists not in the config: {', '.join(self.skipped_therapists)}" if self.skipped_therapists else ""),
            f"Sessions:    {self.sim.sessions} per pairing, up to {self.sim.max_turns_per_session} turns each",
            f"Concurrency: {self.sim.concurrency} pairing(s) at a time",
            "Therapists:",
        ]
        lines += [f"  - {t.id} [{t.label}]: {t.describe()}" for t in self.config.therapists]
        lines.append("Framework models:")
        needs_mi = self.config.has_conversational_therapists
        lines += [f"  - {role}: {spec.describe()}" for role, spec in self.config.models.items() if needs_mi or role not in ("mi_behavior_coder", "mi_global_rater")]
        lines.append("Estimated model calls (upper bound):")
        lines += [f"  - {name}: {count:,}" for name, count in self.estimate_calls().items() if count]
        return "\n".join(lines)

    # ------------------------------------------------------------------ run
    async def run(self) -> int:
        """Run (or resume) the simulation. Returns the number of pairings that failed."""
        out = self.config.output_dir
        store = RunStore(out, save_prompts=self.sim.save_prompts)
        self.store = store
        states = store.load_states()
        removed = store.remove_uncommitted_rows(states)
        if removed:
            tqdm.write(f"Removed {removed} log row(s) written after the last checkpoint (from an interrupted run).")
        self._write_run_metadata()

        self.clients = {role: LLMClient(spec, self.sim.max_retries) for role, spec in self.config.models.items()}
        for role, client in self.clients.items():
            if role in ("mi_behavior_coder", "mi_global_rater") and not self.config.has_conversational_therapists:
                continue
            _ = client.provider  # fail fast on missing API keys / packages
        self.therapists: dict[str, Therapist] = {t.id: build_therapist(t, self.config) for t in self.config.therapists}

        contexts = self._load_contexts(store)
        todo = []
        for p in self.pairings:
            state = states.get(p.pairing_id)
            if state is None:
                state = PairingState(p.pairing_id, p.therapist_id, p.patient_id)
            if not state.finished:
                todo.append((p, state, contexts[p.pairing_id]))

        total_sessions = len(self.pairings) * self.sim.sessions
        already = sum(
            (self.sim.sessions if states[p.pairing_id].finished else states[p.pairing_id].session - 1)
            for p in self.pairings
            if p.pairing_id in states
        )
        self.progress = tqdm(total=total_sessions, initial=already, desc="Sessions", unit="session", dynamic_ncols=True)
        root = logging.getLogger()
        previous_handlers, previous_level = root.handlers, root.level
        handler = _TqdmLoggingHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        root.handlers, root.level = [handler], logging.WARNING

        loop = asyncio.get_running_loop()
        loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=self.sim.concurrency * 3 + 4))
        semaphore = asyncio.Semaphore(self.sim.concurrency)
        failures = 0
        try:
            results = await asyncio.gather(*(self._run_pairing_guarded(p, s, ctx, semaphore) for p, s, ctx in todo))
            failures = results.count(False)
        finally:
            root.handlers, root.level = previous_handlers, previous_level
            self.progress.close()
            for therapist in self.therapists.values():
                await therapist.close()
            self._write_usage(out)

        finished = sum(1 for s in store.load_states().values() if s.finished)
        tqdm.write(f"\nDone. {finished}/{len(self.pairings)} pairings finished; results in {out}")
        if failures:
            tqdm.write(f"{failures} pairing(s) failed (see errors above). Re-run the same command to retry them from their last checkpoint.")
        tqdm.write(f"View the results:  aipsycheval dashboard {out}")
        return failures

    def _write_usage(self, out: Path) -> None:
        path = out / "usage.json"
        totals = {}
        if path.exists():
            try:
                totals = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                totals = {}
        for model, usage in USAGE.items():
            entry = totals.setdefault(model, {"calls": 0, "input_tokens": 0, "output_tokens": 0})
            for key, value in asdict(usage).items():
                entry[key] += value
        USAGE.clear()
        path.write_text(json.dumps(totals, indent=2), encoding="utf-8")

    def _load_contexts(self, store: RunStore) -> dict[int, PairingContext]:
        contexts: dict[int, PairingContext] = defaultdict(PairingContext)
        for row in store.read_rows(C.CONVERSATION_LOG):
            role = "therapist" if row["speaker"] == "Therapist" else "patient"
            contexts[int(row["pairing_id"])].sessions[int(row["session_id"])].append({"role": role, "content": row["message"], "turn": int(row["turn"])})
        for row in store.read_rows(C.AFTER_SESSION_REPORT_LOG):
            contexts[int(row["pairing_id"])].journals[int(row["session_id"])] = row["journal_summary"]
        return contexts

    async def _run_pairing_guarded(self, pairing: Pairing, state: PairingState, ctx: PairingContext, semaphore: asyncio.Semaphore) -> bool:
        async with semaphore:
            try:
                await self._run_pairing(pairing, state, ctx)
                return True
            except (LLMError, InvalidResponse, ConfigError) as error:
                message = str(error)
            except Exception as error:  # noqa: BLE001 - keep other pairings running
                message = f"{type(error).__name__}: {error}"
                logger.error("Unexpected error in pairing %s:\n%s", pairing.pairing_id, traceback.format_exc())
            state.status = "failed"
            state.error = message
            self.store.save_state(state)
            tqdm.write(f"FAILED pairing {pairing.pairing_id} ({pairing.therapist_id} x patient {pairing.patient_id}), session {state.session}: {message}")
            return False

    async def _run_pairing(self, pairing: Pairing, state: PairingState, ctx: PairingContext) -> None:
        persona = self.personas[pairing.patient_id]
        if state.psych_state is None:
            state.psych_state = {k: int(float(persona[k])) for k in C.PSYCHOLOGICAL_CONSTRUCTS_KEYS}
        state.status, state.error = "in_progress", None
        while not state.finished:
            session = state.session
            await self._run_session(pairing, state, ctx, persona)
            self.progress.update(1 if state.session != session or not state.finished else self.sim.sessions - session + 1)
            if state.status == "terminated":
                tqdm.write(f"Pairing {pairing.pairing_id} ended after session {session}: {state.termination_reason.replace('_', ' ')}.")

    def _checkpoint(self, state: PairingState, stage: str) -> None:
        state.stage = stage
        self.store.save_state(state)

    async def _json(self, role: str, prompt: str, schema: str, pairing: Pairing, session: int, target: str, validate=None) -> dict:
        self.store.write_prompt(pairing.pairing_id, session, target, prompt)
        label = f"{role}/{target} (pairing {pairing.pairing_id}, session {session})"
        return await self.clients[role].complete_json(prompt, self.schemas[schema], validate=validate, label=label)

    async def _run_session(self, pairing: Pairing, state: PairingState, ctx: PairingContext, persona: dict) -> None:
        spec = self.therapist_specs[pairing.therapist_id]
        material = spec.is_material
        label = "Psychoeducation Material Fragment" if material else "Therapist"
        session = state.session
        stage = state.stage_index
        store = self.store

        previous_transcripts = format_previous_sessions(ctx, session, label)
        journals = format_journals(ctx, session)

        def patient_context(transcript: str) -> dict:
            return {
                "persona_data": persona,
                "current_psych_state": state.psych_state,
                "previous_session_transcripts": previous_transcripts,
                "previous_journaling": journals,
                "current_session_transcript": transcript,
            }

        # --- Pre-session: SURE ---
        if stage() < C.SESSION_STAGES.index("sure_done"):
            prompt = self.prompts["sure_material" if material else "sure"].format(**patient_context("Session has not started yet."))
            response = await self._json("patient", prompt, "sure", pairing, session, "sure_survey")
            store.append(C.SURE_SURVEY_LOG, [{"pairing_id": pairing.pairing_id, "session_id": session, **response, **scoring.score_sure(response)}])
            state.turn = 0
            self._checkpoint(state, "sure_done")

        # --- In-session: dialogue turns ---
        if stage() < C.SESSION_STAGES.index("turns_done"):
            await self._run_turns(pairing, state, ctx, persona, previous_transcripts, journals)

        transcript = format_session(ctx.sessions[session], label)

        # --- Post-session: fidelity, alliance, negative effects ---
        if not material:
            if stage() < C.SESSION_STAGES.index("mi_batch_behavior_done"):
                prompt = self.prompts["mi_behavior"].format(current_session_transcript=transcript, miti_manual=self.prompts["miti_manual"])
                response = await self._json("mi_behavior_coder", prompt, "mi_behavior", pairing, session, "mi_behavior_coding")
                counts = scoring.normalize_behavior_counts(response["behavior_code_counts"])
                row = {"pairing_id": pairing.pairing_id, "session_id": session, "reasoning": response.get("reasoning", ""), **counts, **scoring.score_mi_behavior(counts)}
                store.append(C.MI_BEHAVIOR_EVAL_LOG, [row])
                self._checkpoint(state, "mi_batch_behavior_done")

            if stage() < C.SESSION_STAGES.index("mi_global_done"):
                prompt = self.prompts["mi_global"].format(current_session_transcript=transcript, miti_manual=self.prompts["miti_manual"])
                flat = flatten_nested_dict(await self._json("mi_global_rater", prompt, "mi_global", pairing, session, "mi_global_scores"))
                store.append(C.MI_GLOBAL_EVAL_LOG, [{"pairing_id": pairing.pairing_id, "session_id": session, **flat, **scoring.score_mi_global(flat)}])
                self._checkpoint(state, "mi_global_done")

            if stage() < C.SESSION_STAGES.index("srs_done"):
                response = await self._json("patient", self.prompts["srs"].format(**patient_context(transcript)), "srs", pairing, session, "srs_survey")
                store.append(C.SRS_SURVEY_LOG, [{"pairing_id": pairing.pairing_id, "session_id": session, **response, **scoring.score_srs(response)}])
                self._checkpoint(state, "srs_done")

            if stage() < C.SESSION_STAGES.index("wai_done"):
                response = await self._json("patient", self.prompts["wai"].format(**patient_context(transcript)), "wai", pairing, session, "wai_survey")
                store.append(C.WAI_SURVEY_LOG, [{"pairing_id": pairing.pairing_id, "session_id": session, **response, **scoring.score_wai(response)}])
                self._checkpoint(state, "wai_done")
        elif stage() < C.SESSION_STAGES.index("wai_done"):
            # Fidelity and alliance measures do not apply to reading material.
            self._checkpoint(state, "wai_done")

        if stage() < C.SESSION_STAGES.index("neq_done"):
            def validate_neq(data: dict) -> None:
                if not scoring.neq_is_complete(scoring.flatten_neq_response(data)):
                    raise InvalidResponse("NEQ response does not answer all 32 questions.")

            prompt = self.prompts["neq_material" if material else "neq"].format(**patient_context(transcript))
            flat = scoring.flatten_neq_response(await self._json("patient", prompt, "neq", pairing, session, "neq_survey", validate=validate_neq))
            store.append(C.NEQ_SURVEY_LOG, [{"pairing_id": pairing.pairing_id, "session_id": session, **flat, **scoring.score_neq(flat)}])
            self._checkpoint(state, "neq_done")

        # --- Between sessions: the week after, adverse events ---
        prompt = self.prompts["report_material" if material else "report"].format(**patient_context(transcript))
        report = await self._json("patient", prompt, "report", pairing, session, "after_session_report")
        row = {
            "pairing_id": pairing.pairing_id,
            "session_id": session,
            "journal_summary": sanitize_text(report.get("journal_summary")),
            "state_change_justification": sanitize_text(report.get("internal_justification")),
            **report.get("state_update", {}),
        }
        events = report.get("adverse_event_selection", {})
        for event in C.ADVERSE_EVENT_KEYS:
            details = events.get(event, {})
            row[f"{event}_occurred"] = details.get("occurred", False)
            row[f"{event}_attribution"] = sanitize_text(details.get("attribution", "N/A"))
            row[f"{event}_internal_justification"] = sanitize_text(details.get("internal_justification", "N/A"))
        store.append(C.AFTER_SESSION_REPORT_LOG, [row])
        ctx.journals[session] = row["journal_summary"]

        state.psych_state = dict(report["state_update"])
        state.stage = "report_done"
        ended_by = next((event for event in C.TERMINATING_EVENTS if row[f"{event}_occurred"] is True), None)
        if ended_by:
            state.status, state.termination_reason = "terminated", ended_by
        elif session >= self.sim.sessions:
            state.status = "completed"
        else:
            state.session, state.stage, state.turn = session + 1, "start", 0
        store.save_state(state)

    async def _run_turns(self, pairing: Pairing, state: PairingState, ctx: PairingContext, persona: dict, previous_transcripts: str, journals: str) -> None:
        spec = self.therapist_specs[pairing.therapist_id]
        therapist = self.therapists[spec.id]
        material = spec.is_material
        label = "Psychoeducation Material Fragment" if material else "Therapist"
        session = state.session
        history = ctx.sessions[session]
        previous_sessions = [
            [{"role": m["role"], "content": m["content"]} for m in ctx.sessions[s]] for s in sorted(ctx.sessions) if s < session and ctx.sessions[s]
        ]
        store = self.store
        ids = {"pairing_id": pairing.pairing_id, "session_id": session}

        for turn in range(state.turn + 1, self.sim.max_turns_per_session + 1):
            psych_state = dict(state.psych_state)
            concluded = False
            conversation_rows, crisis_rows, action_rows = [], [], []

            # Patient
            if turn == 1 and not history:
                if material:
                    patient_message = PATIENT_OPENERS["material"]
                else:
                    patient_message = PATIENT_OPENERS["first_session" if session == 1 else "later_session"]
                conversation_rows.append({**ids, "turn": turn, "speaker": "Patient", "message": patient_message, "session_conclusion": False, **psych_state})
            else:
                prompt = self.prompts["patient_read" if material else "patient_turn"].format(
                    persona_data=persona,
                    current_psych_state=psych_state,
                    previous_session_transcripts=previous_transcripts,
                    previous_journaling=journals,
                    current_session_transcript=format_session(history, label),
                    therapist_message=history[-1]["content"] if history else "",
                )
                output = await self._json("patient", prompt, "patient", pairing, session, f"patient_turn_{turn}")
                cot = output["chain_of_thought"]
                concluded = bool(cot.get("session_conclusion", False))
                patient_message = sanitize_text(cot["response_formulation"])
                psych_state = dict(cot["state_update"])
                conversation_rows.append({
                    **ids, "turn": turn, "speaker": "Patient", "message": patient_message, "session_conclusion": concluded,
                    "appraisal_internal_reflection": cot["appraisal_internal_reflection"],
                    "internal_justification": cot["internal_justification"],
                    "goal": cot["selected_strategy"]["goal"],
                    "strategy": cot["selected_strategy"]["strategy"],
                    "tactic": cot["selected_strategy"]["tactic"],
                    **psych_state,
                })
            turn_history = history + [{"role": "patient", "content": patient_message, "turn": turn}]

            # Crisis detection (on the patient message) and the therapist reply run concurrently.
            previous_messages = "No previous messages in this session."
            if len(turn_history) >= 3:
                previous_messages = f"Patient: {sanitize_text(turn_history[-3]['content'])}\nTherapist: {sanitize_text(turn_history[-2]['content'])}"
            crisis_prompt = self.prompts["crisis"].format(previous_messages=previous_messages, patient_latest_message=patient_message)

            request = TherapistRequest(
                pairing_id=pairing.pairing_id,
                session_id=session,
                turn=turn,
                therapist_id=spec.id,
                patient_id=pairing.patient_id,
                patient_message=patient_message,
                history=[{"role": m["role"], "content": m["content"]} for m in turn_history],
                previous_sessions=previous_sessions,
                previous_session_transcripts=previous_transcripts,
            )
            therapist_prompt = therapist.rendered_prompt(request)
            if therapist_prompt is not None:
                store.write_prompt(pairing.pairing_id, session, f"therapist_{spec.id}_turn_{turn}", therapist_prompt)
            therapist_state = dict(state.therapist_state)
            crisis_info, raw_reply = await asyncio.gather(
                self._json("crisis_detector", crisis_prompt, "crisis", pairing, session, f"crisis_eval_turn_{turn}"),
                therapist.respond(request, therapist_state),
            )
            crisis_rows.append({**ids, "turn": turn, **crisis_info})

            therapist_message = clean_therapist_response(raw_reply)
            if not therapist_message:
                raise LLMError(f"therapist {spec.id} returned an empty reply (pairing {pairing.pairing_id}, session {session}, turn {turn}).")
            turn_history.append({"role": "therapist", "content": therapist_message, "turn": turn})
            conversation_rows.append({**ids, "turn": turn, "speaker": "Therapist", "message": therapist_message, "session_conclusion": concluded})

            # Crisis response evaluation
            if crisis_info["classification"] != C.NO_CRISIS:
                category = crisis_info["classification"]
                last_two = "\n".join(f"{m['role'].capitalize()}: {m['content']}" for m in turn_history[-2:])
                prompt = self.prompts["action_plan"].format(
                    crisis_category=category,
                    last_two_responses=last_two,
                    action_plan_text=C.ACTION_PLAN_DEFINITIONS.get(category, "No specific action plan defined."),
                )
                action_plan = await self._json("crisis_response_evaluator", prompt, "action_plan", pairing, session, f"action_plan_eval_turn_{turn}")
                action_rows.append({**ids, "turn": turn, **action_plan})

            # Commit the turn
            store.append(C.CONVERSATION_LOG, conversation_rows)
            store.append(C.CRISIS_EVAL_LOG, crisis_rows)
            store.append(C.ACTION_PLAN_EVAL_LOG, action_rows)
            history.extend(turn_history[len(history):])
            state.turn = turn
            state.psych_state = psych_state
            state.therapist_state = therapist_state
            if concluded:
                break
            store.save_state(state)

        self._checkpoint(state, "turns_done")
