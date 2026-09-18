"""
FA Max agent graph — a bounded tool-call loop over one claimed
fa_max_work_queue item (WP-T2-2).

Deterministic step consumption, not free-form LLM tool selection: a claimed
work item's payload carries an ordered ``steps`` list
(``[{"tool": <name>, "args": {...}}, ...]``), built by whatever enqueued the
work item (a graph, a task, an operator script). This mirrors the rest of
this codebase's own convention for agent routing — src.agents.cora.main_graph
routes on a plain dict lookup with the docstring "no LLM"; this loop is the
same idea applied to a sequence of tool calls instead of a single event
route.

EXPLICIT SCOPE DECISION (WP-T2-2 review round 5): the WP-T2-2 split doc's
prose describes Cora "receiving a task description" and "identifying which
tool to call from the registry" — read literally, that implies a live
LLM-driven task-to-tool planner. This module does NOT build that. Doing so
now would be new architecture invented mid-fix-cycle, not a bug fix, and
would contradict the one explicit "no LLM" precedent already set by
src.agents.cora.main_graph for the rest of this codebase's agent routing.
The caller-supplied ``steps`` plan (built by admin_router.py's
POST /fa-max/agent-tasks today, or a future automatic producer) IS this
WP's execution-plan contract — WP-T2-2 is hereby amended to state that
explicitly rather than leaving it an unstated gap against the split doc's
prose. A live task-to-tool LLM planner, if ever wanted, is new scope for a
future work package to design and task-analysis to settle — not something
to add here without that planning pass and the open questions it would
raise (which model, what tool-selection failure mode, how autonomy-tier
gating interacts with a planner's own tool choice).

The loop node repeatedly:

    pick next step -> look up tool in FA_MAX_TOOL_REGISTRY
        -> fa_max_tool_log.start_tool_call() [audit row BEFORE execution]
        -> call it -> fa_max_tool_log.finish_tool_call() [audit row updated]
        -> append result
    until steps are exhausted, a tool call fails/blocks, or
    config.agents.AgentsSettings.fa_max_agent_max_tool_calls is reached.

Checkpointed with the same generic Postgres checkpoint store Cora uses
(src.agents.checkpoint.checkpoint_saver — see that module; it is
graph-agnostic, keyed only by thread_id/checkpoint_ns, so this reuses it
directly rather than duplicating it). checkpoint_ns="fa_max" keeps this
graph's checkpoints in their own namespace within the same shared tables,
same reasoning as src.agents.cora.checkpointer's checkpoint_ns="cora".

On crash mid-loop, the work item's lease in fa_max_work_queue simply expires
and src.services.state_engine.reclaim_expired_work_items() returns it to
'available' for a future claim — no separate retry path here (see worker.py).
"""
from __future__ import annotations

import concurrent.futures
import logging
import time
from typing import Any, Callable, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.fa_max.tool_registry import FA_MAX_TOOL_REGISTRY, get_fa_max_tool
from src.services.fa_max_send_governance import GovernanceBlocked
from src.services.fa_max_tool_log import finish_tool_call, start_tool_call

logger = logging.getLogger(__name__)

# One dedicated worker per tool call, never reused across calls -- see
# _call_tool_with_timeout()'s docstring for why a fresh session/thread pair
# is used instead of sharing the node's own session with a background thread.
_TOOL_CALL_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="fa_max_tool_call",
)


class ToolCallTimeout(Exception):
    """Raised when a single tool call exceeds fa_max_agent_tool_timeout_seconds."""


class FaMaxAgentState(TypedDict, total=False):
    work_item_id: str
    agent_name: str
    steps: List[Dict[str, Any]]
    step_index: int
    tool_results: List[Dict[str, Any]]
    done: bool
    error: Optional[str]


def _call_tool(tool_name: str, args: Dict[str, Any], *, session, log_id: Optional[int] = None) -> Any:
    import inspect

    spec = get_fa_max_tool(tool_name)
    call_args = dict(args)
    # Uses inspect.signature() rather than a raw bytecode-varnames scan,
    # which would list every local variable a function body assigns, not
    # just its parameters -- a tool with a local variable happening to
    # share the `session` name (but not a parameter) would otherwise
    # silently receive an unexpected `session=` kwarg and raise TypeError
    # on every invocation.
    params = inspect.signature(spec.func).parameters
    if "session" in params:
        call_args["session"] = session
    # log_id (WP-T2-2 review fix): only a tool that declares this parameter
    # (currently just `send`) gets it -- it is this call's own durable
    # fa_max_tool_call_log row id, letting a send tool atomically confirm
    # via fa_max_tool_log.claim_send_attempt() that the agent loop hasn't
    # already given up on it (timed out) before causing a real side effect.
    if "log_id" in params:
        call_args["log_id"] = log_id
    return spec.func(**call_args)


def _reconcile_orphaned_call(
    *, log_id: int, tool_name: str, agent_name: Optional[str], work_item_id: Optional[str],
) -> "Callable[[concurrent.futures.Future], None]":
    """Build a Future.add_done_callback() target that reconciles a timed-out
    tool call's fa_max_tool_call_log row with its TRUE final outcome,
    whenever the orphaned thread actually finishes.

    Python cannot force-kill a thread, so a call that timed out from the
    agent loop's point of view may still complete later -- including a
    `send` tool whose side effect (an outbound message queued via
    relay.queue.enqueue) genuinely lands after the loop already logged a
    timeout and moved on. Rather than leaving that send permanently
    unaudited (or its audit row permanently lying that it errored), this
    callback fires when the future actually resolves -- possibly seconds or
    minutes after the loop gave up -- and updates the SAME log_id row
    (written by start_tool_call() before the call began) to the real
    status/output. finish_tool_call() may therefore be called twice for one
    log_id: once synchronously with status='error' when the timeout fires,
    and once here, later, with the true outcome -- last write wins, and the
    row is tagged so this is visible to anyone reading it.

    This does not and cannot PREVENT the late side effect (there is no way
    to abort an in-flight thread in Python) -- it closes the audit gap so
    the late effect is never silently invisible.

    KNOWN LIMITATION: this callback lives only in this process's memory. If
    the worker process itself crashes or restarts before the orphaned
    thread finishes (not just the tool call timing out -- the whole
    process going away), the callback never fires and the row is left at
    whatever finish_tool_call() wrote synchronously when the timeout first
    fired (status='error'). The actual SEND side effect is not lost in that
    scenario -- if the orphaned thread's `send` tool got past
    fa_max_tool_log.claim_send_attempt() before the crash, its
    relay.queue.enqueue() call already durably inserted the
    relay_approval_queue row (idempotency-keyed, so a retry of the same
    attempt cannot duplicate it) -- only this AUDIT row's final status can
    go stale. A periodic sweep that cross-references a stale/error
    fa_max_tool_call_log row's stored idempotency_key (in its redacted
    `input`) against relay_approval_queue would close this remaining gap;
    not built here -- flagged as a real, scoped-out follow-up rather than
    silently left unmentioned.
    """

    def _callback(fut: concurrent.futures.Future) -> None:
        from src.core.database import get_db_context

        exc = fut.exception()
        if exc is not None:
            output: Dict[str, Any] = {"error": str(exc)}
            status = "error"
        else:
            result = fut.result()
            output = dict(result) if isinstance(result, dict) else {"result": result}
            status = "success"
        output["_late_completion_after_timeout"] = True

        logger.warning(
            "fa_max.agent_graph: tool %r for work_item_id=%s completed AFTER its "
            "timeout was already logged (the agent loop had already moved on) -- "
            "reconciling fa_max_tool_call_log id=%s to status=%s",
            tool_name, work_item_id, log_id, status,
        )
        try:
            with get_db_context() as session:
                reconciled = finish_tool_call(
                    session=session, log_id=log_id, output=output,
                    duration_ms=None, status=status,
                )
            if not reconciled:
                logger.error(
                    "fa_max.agent_graph: failed to reconcile late completion for "
                    "tool %r log_id=%s work_item_id=%s -- a real side effect may "
                    "now be unaudited",
                    tool_name, log_id, work_item_id,
                )
        except Exception:
            logger.exception(
                "fa_max.agent_graph: reconciliation callback raised for tool %r "
                "log_id=%s work_item_id=%s",
                tool_name, log_id, work_item_id,
            )

    return _callback


def _call_tool_with_timeout(
    tool_name: str,
    args: Dict[str, Any],
    *,
    timeout_seconds: float,
    log_id: Optional[int] = None,
    agent_name: Optional[str] = None,
    work_item_id: Optional[str] = None,
) -> Any:
    """Run one tool call on its own DB session, in its own thread, bounded by
    fa_max_agent_tool_timeout_seconds.

    Deliberately does NOT share the calling node's own session with the
    worker thread: SQLAlchemy Session objects are not safe for concurrent
    use, and on a timeout the calling thread must be free to immediately log
    the failure and stop the loop without waiting on (or racing) whatever
    the abandoned worker thread is still doing. Python cannot force-kill a
    thread, so a timed-out call's underlying thread may keep running in the
    background against its OWN session/connection until it finishes or that
    connection is torn down independently -- this bounds the AGENT LOOP's
    wait, not the callee's actual execution. The work item's own lease
    (fa_max_work_queue, reclaim_expired_work_items) remains the outer safety
    net for a worker process that is well and truly stuck.

    On an actual timeout, if log_id is given, registers a done-callback
    (see _reconcile_orphaned_call) so a late-completing call is never
    silently unaudited -- see that function's docstring.

    log_id is ALSO passed straight through to _call_tool() (and from there,
    to any tool that declares a log_id parameter) whether or not a timeout
    ever happens -- the `send` tool uses it to durably confirm, right
    before it would cause a real side effect, that the agent loop hasn't
    already timed this attempt out (see fa_max_tool_log.claim_send_attempt
    and src.agents.fa_max.tool_registry.send).
    """
    from src.core.database import get_db_context

    def _run() -> Any:
        with get_db_context() as tool_session:
            return _call_tool(tool_name, args, session=tool_session, log_id=log_id)

    future = _TOOL_CALL_EXECUTOR.submit(_run)
    try:
        return future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError as exc:
        if log_id is not None:
            future.add_done_callback(
                _reconcile_orphaned_call(
                    log_id=log_id, tool_name=tool_name,
                    agent_name=agent_name, work_item_id=work_item_id,
                )
            )
        raise ToolCallTimeout(
            f"tool {tool_name!r} exceeded {timeout_seconds}s timeout"
        ) from exc


def _node_tool_step(state: FaMaxAgentState) -> FaMaxAgentState:
    from config.agents import get_agents_settings
    from src.core.database import get_db_context

    settings = get_agents_settings()
    max_calls = settings.fa_max_agent_max_tool_calls
    tool_timeout_seconds = settings.fa_max_agent_tool_timeout_seconds
    step_index = state.get("step_index", 0)
    steps = state.get("steps", [])
    agent_name = state["agent_name"]
    work_item_id = state.get("work_item_id")
    results = list(state.get("tool_results", []))

    if step_index >= len(steps) or step_index >= max_calls:
        return {"done": True}

    step = steps[step_index]
    tool_name = step.get("tool")
    args = step.get("args", {}) or {}

    if tool_name not in FA_MAX_TOOL_REGISTRY:
        logger.warning("fa_max.agent_graph: unknown tool %r at step %d — stopping", tool_name, step_index)
        results.append({"tool": tool_name, "status": "error", "error": "unknown_tool"})
        return {"tool_results": results, "step_index": step_index + 1, "done": True, "error": "unknown_tool"}

    # Audit BEFORE execution, not only after: a durable 'in_progress' row is
    # written before the tool runs at all, so a record of this call exists
    # before any side effect (e.g. a `send` tool queuing an outbound
    # message) can occur -- closing the gap where a post-hoc-only log write
    # meant a real send could land before, or without, any audit trail. If
    # even this start-write fails, refuse to run the tool at all: an
    # unauditable call is worse than a work item that stays claimed for its
    # lease to expire and retry.
    with get_db_context() as start_session:
        log_id = start_tool_call(
            session=start_session,
            agent_name=agent_name,
            tool_name=tool_name,
            input=args,
            work_item_id=work_item_id,
        )

    if log_id is None:
        logger.error(
            "fa_max.agent_graph: could not write in_progress audit row for tool %r "
            "work_item_id=%s — refusing to execute unaudited, stopping loop fail-closed",
            tool_name, work_item_id,
        )
        results.append({"tool": tool_name, "status": "error", "output": None, "error": "audit_log_write_failed"})
        return {
            "tool_results": results,
            "step_index": step_index + 1,
            "done": True,
            "error": "audit_log_write_failed",
        }

    start = time.monotonic()
    was_timeout = False
    try:
        output = _call_tool_with_timeout(
            tool_name, args, timeout_seconds=tool_timeout_seconds,
            log_id=log_id, agent_name=agent_name, work_item_id=work_item_id,
        )
        status = "success"
        error = None
    except GovernanceBlocked as exc:
        output = {"error": str(exc)}
        status = "blocked"
        error = str(exc)
    except ToolCallTimeout as exc:
        # The row this timeout is about to mark 'error' may still be
        # overwritten later by _reconcile_orphaned_call() (registered inside
        # _call_tool_with_timeout) with the call's TRUE outcome once the
        # orphaned thread actually finishes -- this is the best-available
        # value at the moment the loop gives up, not a claim that the call
        # never had an effect. was_timeout makes the finish_tool_call() call
        # below CONDITIONAL (require_status='in_progress') -- WP-T2-2 review
        # round 5 fix: if fa_max_tool_log.claim_send_attempt() already
        # promoted this row to 'claimed' (the orphaned send tool durably
        # confirmed it was still live and proceeded), this write must not
        # clobber that signal back to 'error'.
        was_timeout = True
        logger.error(
            "fa_max.agent_graph: tool %r timed out after %ss for work_item_id=%s",
            tool_name, tool_timeout_seconds, work_item_id,
        )
        output = {"error": str(exc)}
        status = "error"
        error = str(exc)
    except Exception as exc:  # noqa: BLE001 - must not crash the loop; logged below
        logger.exception(
            "fa_max.agent_graph: tool %r raised for work_item_id=%s", tool_name, work_item_id,
        )
        output = {"error": str(exc)}
        status = "error"
        error = str(exc)
    duration_ms = int((time.monotonic() - start) * 1000)

    # Fail-closed on the audit write itself (not just on the tool call):
    # WP-T2-2's Done-When line is "every tool call logged," not "every tool
    # call logged unless the log write itself fails." finish_tool_call()
    # never raises (a logging failure must not crash mid-call), so its
    # return value is the only signal this loop has that the durable audit
    # row didn't get its final state. Continuing past that failure would
    # silently leave the row stuck at 'in_progress' forever -- stopping the
    # loop lets the work item's lease-driven reclaim retry it from scratch.
    with get_db_context() as log_session:
        logged = finish_tool_call(
            session=log_session,
            log_id=log_id,
            output=output if isinstance(output, dict) else {"result": output},
            duration_ms=duration_ms,
            status=status,
            require_status="in_progress" if was_timeout else None,
        )

    if not logged:
        logger.error(
            "fa_max.agent_graph: audit log finish-write failed for tool %r log_id=%s "
            "work_item_id=%s — stopping loop fail-closed rather than leaving the row "
            "stuck at 'in_progress'",
            tool_name, log_id, work_item_id,
        )
        results.append({"tool": tool_name, "status": status, "output": output, "error": error})
        return {
            "tool_results": results,
            "step_index": step_index + 1,
            "done": True,
            "error": "audit_log_write_failed",
        }

    results.append({"tool": tool_name, "status": status, "output": output, "error": error})
    new_step_index = step_index + 1
    done = status != "success" or new_step_index >= len(steps) or new_step_index >= max_calls

    return {
        "tool_results": results,
        "step_index": new_step_index,
        "done": done,
        "error": error if status != "success" else state.get("error"),
    }


def _route_continue(state: FaMaxAgentState) -> str:
    return END if state.get("done") else "tool_step"


def build_fa_max_agent_graph() -> StateGraph:
    g = StateGraph(FaMaxAgentState)
    g.add_node("tool_step", _node_tool_step)
    g.add_edge(START, "tool_step")
    g.add_conditional_edges("tool_step", _route_continue, {"tool_step": "tool_step", END: END})
    return g


def run_fa_max_agent(
    *, work_item_id: str, agent_name: str, steps: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compile and run one work item's bounded tool-call loop, checkpointed
    by thread_id=work_item_id so a crash mid-loop can resume from the last
    completed step on the next claim of the same item (before its lease
    expires and reclaim_expired_work_items() offers it up again)."""
    from src.agents.checkpoint import checkpoint_saver

    builder = build_fa_max_agent_graph()
    initial: FaMaxAgentState = {
        "work_item_id": work_item_id,
        "agent_name": agent_name,
        "steps": steps,
        "step_index": 0,
        "tool_results": [],
        "done": False,
        "error": None,
    }
    with checkpoint_saver() as saver:
        graph = builder.compile(checkpointer=saver)
        final = graph.invoke(
            initial,
            config={"configurable": {"thread_id": f"fa_max_work:{work_item_id}", "checkpoint_ns": "fa_max"}},
        )
        return dict(final)


def run_fa_max_agent_no_checkpoint(
    *, work_item_id: str, agent_name: str, steps: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Checkpointer-free variant for tests/CLI use."""
    graph = build_fa_max_agent_graph().compile()
    final = graph.invoke({
        "work_item_id": work_item_id, "agent_name": agent_name, "steps": steps,
        "step_index": 0, "tool_results": [], "done": False, "error": None,
    })
    return dict(final)
