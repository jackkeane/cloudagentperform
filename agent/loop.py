import json
import re
import time
from dataclasses import dataclass, field

from agent.llm import LLMError
from agent.tools import TOOL_NAMES, TOOL_SCHEMAS, run_tool
from core.models import (EV_MESSAGE, EV_TOOL_CALL, EV_TOOL_RESULT, FAILED,
                         R_MAX_STEPS, R_MODEL, SUCCEEDED)

SYSTEM_PROMPT = (
    "You are an autonomous engineering agent working inside an isolated "
    "Linux sandbox. The project to work on is in /workspace (your cwd). "
    "Use the provided tools to complete the user's task. Write deliverable "
    "files under /workspace/output/. When the task is complete, reply with "
    "a short final summary and no tool calls.\n"
    "Rules:\n"
    "1. Make one tool call at a time and wait for its result before you "
    "decide the next step.\n"
    "2. Tool calls do not share a shell session: environment variables, "
    "`cd` and command output never carry over from one call to the next. "
    "Never put $VAR or backticks in write_file content hoping an earlier "
    "command filled them in — write the literal text you want to appear in "
    "the file, copied from a tool result you have already seen.\n"
    "3. If a command exits non-zero or prints nothing, do not repeat it "
    "unchanged. Run a simpler variant (for example plain "
    "`grep -rn TODO .`) to find out why.\n"
    "4. Keep reasoning brief; the context window is small.")

# Reasoning models (Qwen3, DeepSeek-R1, ...) emit <think> blocks that are
# worth nothing to the next turn but cost hundreds of tokens each. Drop them
# before the message goes back into the context. Also drops an unterminated
# trailing block, which is how a truncated reasoning turn arrives.
_THINK = re.compile(r"<think>.*?</think>\s*|<think>.*\Z", re.DOTALL)

_NUDGE = ("Your last reply contained no visible answer and no tool call. "
          "Either call a tool to make progress, or state the final result "
          "in plain text.")


def strip_reasoning(text: str | None) -> str:
    return _THINK.sub("", text or "").strip()


@dataclass
class Outcome:
    status: str
    reason: str | None = None
    summary: str | None = None
    usage: dict = field(default_factory=dict)
    transcript: list = field(default_factory=list)


class Stopped(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def run_agent(prompt, sandbox, llm, emit, should_stop, *, max_steps=20,
              tool_timeout=30, step_delay_ms=0) -> Outcome:
    messages = [{"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt}]
    usage_total = {"prompt_tokens": 0, "completion_tokens": 0}

    def check():
        reason = should_stop()
        if reason:
            raise Stopped(reason)

    for _step in range(max_steps):
        check()
        try:
            result = llm.chat(messages, TOOL_SCHEMAS)
        except LLMError as exc:
            return Outcome(FAILED, R_MODEL, str(exc), usage_total, messages)
        for k in usage_total:
            usage_total[k] += result.usage.get(k, 0)

        text = strip_reasoning(result.text)

        if not result.tool_calls:
            if not text:  # reasoning-only turn: not an answer, don't call it one
                messages.append({"role": "assistant", "content": ""})
                messages.append({"role": "user", "content": _NUDGE})
                continue
            emit(EV_MESSAGE, {"text": text, "final": True})
            messages.append({"role": "assistant", "content": text})
            return Outcome(SUCCEEDED, None, text, usage_total, messages)

        messages.append({"role": "assistant", "content": text,
                         "tool_calls": [
                             {"id": c.id, "type": "function", "function": {
                                 "name": c.name,
                                 "arguments": json.dumps(c.arguments or {})}}
                             for c in result.tool_calls]})
        if text:
            emit(EV_MESSAGE, {"text": text, "final": False})

        for call in result.tool_calls:
            check()
            if step_delay_ms:  # demo pacing knob, default off
                time.sleep(step_delay_ms / 1000)
            emit(EV_TOOL_CALL, {"call_id": call.id, "name": call.name,
                                "arguments": call.arguments or {}})
            if call.parse_error:
                payload = {"ok": False, "exit_code": None, "truncated": False,
                           "output": "invalid tool arguments:"
                                     f" {call.parse_error}"}
            elif call.name not in TOOL_NAMES:
                payload = {"ok": False, "exit_code": None, "truncated": False,
                           "output": f"unknown tool: {call.name!r};"
                                     f" available: {sorted(TOOL_NAMES)}"}
            else:
                try:
                    payload = run_tool(sandbox, call.name,
                                       call.arguments or {}, tool_timeout)
                except (KeyError, TypeError) as exc:
                    payload = {"ok": False, "exit_code": None,
                               "truncated": False,
                               "output": "invalid arguments for"
                                         f" {call.name}: {exc!r}"}
            emit(EV_TOOL_RESULT,
                 {"call_id": call.id, "name": call.name, **payload})
            messages.append({"role": "tool", "tool_call_id": call.id,
                             "content": json.dumps(payload)})

    return Outcome(FAILED, R_MAX_STEPS,
                   f"gave up after {max_steps} steps", usage_total, messages)
