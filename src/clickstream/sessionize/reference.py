"""Reference sessionizer in plain Python (standard library only).

This is the streaming formulation: events arrive in time order, and we keep one small state
record per user (current session id, its start, the user's last event time). That's all a
gap-based session definition needs; there's no lookback beyond one event per user.

    for each event (in time order):
        state = open_sessions.get(user)
        if state is None or event_time - state.last > gap:
            start a new session (id = "<user_id>-<start epoch seconds>")
        else:
            extend state's session
        state.last = event_time

Ties (same user, same timestamp) are ordered by (source_file, source_row); they never change
session boundaries, only which row comes first.

Batch boundaries (one UTC day at a time) are handled by passing the open sessions that might
still continue into the next batch: users whose last event is within `gap` of the batch end.
Input to `sessionize_stream` must be sorted; the caller sorts each batch. Out-of-order arrival
*across* batches (late data) is a reprocessing concern handled by the pipeline, not here.

Times are integer microseconds since the Unix epoch, UTC.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

US = 1_000_000


@dataclass(slots=True)
class OpenSession:
    session_id: str
    start_us: int
    last_us: int


def session_id_for(user_id: int, start_us: int) -> str:
    return f"{user_id}-{start_us // US}"


def sessionize_stream(
    events: Iterable[tuple[int, int, str, int]],
    open_sessions: dict[int, OpenSession],
    gap_s: int,
) -> Iterator[tuple[str, int, str, int]]:
    """Assign sessions to time-ordered events.

    events: (user_id, event_time_us, source_file, source_row), sorted by
            (event_time_us, source_file, source_row).
    open_sessions: per-user state; updated in place (carry-in on entry, full state on exit).
    yields: (source_file, source_row, session_id, session_start_us) per event.
    """
    gap_us = gap_s * US
    prev_key = None
    for user_id, t, source_file, source_row in events:
        key = (t, source_file, source_row)
        if prev_key is not None and key < prev_key:
            raise ValueError(f"events not in time order at {source_file}:{source_row}")
        prev_key = key
        s = open_sessions.get(user_id)
        if s is None or t - s.last_us > gap_us:
            s = OpenSession(session_id_for(user_id, t), t, t)
            open_sessions[user_id] = s
        else:
            s.last_us = t
        yield source_file, source_row, s.session_id, s.start_us


def sessionize_batch(
    events: Iterable[tuple[int, int, str, int]],
    carry_in: dict[int, OpenSession],
    batch_end_us: int,
    gap_s: int,
) -> tuple[list[tuple[str, int, str, int]], dict[int, OpenSession]]:
    """Sessionize one batch (e.g. a UTC day) given the previous batch's carry-over.

    Sorts the batch first, so out-of-order rows *within* the batch are handled.
    Returns (assignments, carry_out) where carry_out holds the sessions that could still be
    extended by an event at or after `batch_end_us`.
    """
    ordered = sorted(events, key=lambda e: (e[1], e[2], e[3]))
    state = {u: OpenSession(s.session_id, s.start_us, s.last_us) for u, s in carry_in.items()}
    assignments = list(sessionize_stream(ordered, state, gap_s))
    horizon = batch_end_us - gap_s * US
    carry_out = {u: s for u, s in state.items() if s.last_us >= horizon}
    return assignments, carry_out
