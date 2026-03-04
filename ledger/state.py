from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class LedgerState:
    seen_paper_ids: set[str] = field(default_factory=set)
    last_run_started_at: str | None = None
    last_run_finished_at: str | None = None
    last_output_dir: str | None = None



def load_state(path: Path) -> LedgerState:
    if not path.exists():
        return LedgerState()

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    return LedgerState(
        seen_paper_ids=set(payload.get("seen_paper_ids", [])),
        last_run_started_at=payload.get("last_run_started_at"),
        last_run_finished_at=payload.get("last_run_finished_at"),
        last_output_dir=payload.get("last_output_dir"),
    )



def save_state(path: Path, state: LedgerState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    payload = {
        "seen_paper_ids": sorted(state.seen_paper_ids),
        "last_run_started_at": state.last_run_started_at,
        "last_run_finished_at": state.last_run_finished_at,
        "last_output_dir": state.last_output_dir,
    }
    with temp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    temp.replace(path)
