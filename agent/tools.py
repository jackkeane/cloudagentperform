import shlex

MAX_TOOL_OUTPUT = 50_000

def _fn(name, desc, props, required):
    return {"type": "function", "function": {
        "name": name, "description": desc,
        "parameters": {"type": "object", "properties": props,
                       "required": required}}}

TOOL_SCHEMAS = [
    _fn("bash", "Run a bash command in the sandbox (cwd /workspace).",
        {"command": {"type": "string"}}, ["command"]),
    _fn("read_file", "Read a text file (relative to /workspace).",
        {"path": {"type": "string"}}, ["path"]),
    _fn("write_file", "Create/overwrite a text file; parents are created. "
        "Deliverables belong under output/.",
        {"path": {"type": "string"}, "content": {"type": "string"}},
        ["path", "content"]),
    _fn("list_dir", "List a directory (relative to /workspace).",
        {"path": {"type": "string"}}, ["path"]),
]
TOOL_NAMES = {t["function"]["name"] for t in TOOL_SCHEMAS}

def truncate_head(text, limit=MAX_TOOL_OUTPUT):
    if len(text) <= limit:
        return text, False
    return text[:limit] + f"\n...[truncated {len(text) - limit} chars]", True

def truncate_tail(text, limit=MAX_TOOL_OUTPUT):
    if len(text) <= limit:
        return text, False
    return f"[truncated {len(text) - limit} chars]...\n" + text[-limit:], True

def run_tool(sandbox, name, args, tool_timeout) -> dict:
    """Execute one allowlisted tool. Truncation policy is per tool intent:
    read_file/list_dir keep the head, bash keeps the tail (errors live at
    the end of shell output). Raises KeyError for unknown tools (the loop
    checks TOOL_NAMES first) and KeyError/TypeError for bad args."""
    if name == "bash":
        r = sandbox.exec(args["command"], timeout=tool_timeout)
        out, truncated = truncate_tail(r.output)
        if r.timed_out:
            out += f"\n[tool timed out after {tool_timeout}s]"
        return {"ok": r.exit_code == 0 and not r.timed_out,
                "exit_code": r.exit_code, "output": out,
                "truncated": truncated}
    if name == "read_file":
        try:
            content = sandbox.read_file(args["path"],
                                        max_bytes=MAX_TOOL_OUTPUT)
        except FileNotFoundError:
            return {"ok": False, "exit_code": None, "truncated": False,
                    "output": f"file not found: {args['path']}"}
        out, truncated = truncate_head(content)
        return {"ok": True, "exit_code": None, "output": out,
                "truncated": truncated}
    if name == "write_file":
        sandbox.write_file(args["path"], args["content"])
        return {"ok": True, "exit_code": None, "truncated": False,
                "output": f"wrote {len(args['content'])} chars"
                          f" to {args['path']}"}
    if name == "list_dir":
        r = sandbox.exec(f"ls -la {shlex.quote(args['path'])}",
                         timeout=tool_timeout)
        out, truncated = truncate_head(r.output)
        return {"ok": r.exit_code == 0, "exit_code": r.exit_code,
                "output": out, "truncated": truncated}
    raise KeyError(name)
