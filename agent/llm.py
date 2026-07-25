import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

import httpx


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: dict | None
    parse_error: str | None = None

@dataclass
class ChatResult:
    text: str | None
    tool_calls: list[ToolCall]
    usage: dict

class LLMError(Exception):
    pass

class LLMProvider(ABC):
    @abstractmethod
    def describe(self) -> dict: ...
    @abstractmethod
    def chat(self, messages: list[dict], tools: list[dict]) -> ChatResult: ...

class OpenAICompatProvider(LLMProvider):
    def __init__(self, base_url, model, api_key="", timeout=60.0,
                 max_retries=3, backoff_base=1.0, transport=None):
        self.base_url = base_url.rstrip("/")
        self.model = model
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.http = httpx.Client(timeout=timeout, headers=headers,
                                 transport=transport)
        self.max_retries = max_retries
        self.backoff_base = backoff_base

    def describe(self):
        return {"mode": "real", "model": self.model,
                "base_url": self.base_url}

    def preflight(self) -> list[str]:
        resp = self.http.get(f"{self.base_url}/models")
        resp.raise_for_status()
        names = [m["id"] for m in resp.json().get("data", [])]
        if self.model not in names:
            raise LLMError(
                f"model {self.model!r} not served; endpoint offers {names}")
        return names

    def chat(self, messages, tools):
        payload: dict = {"model": self.model, "messages": messages}
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"
        last: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                resp = self.http.post(f"{self.base_url}/chat/completions",
                                      json=payload)
                if resp.status_code >= 500:
                    raise LLMError(f"server error {resp.status_code}")
                resp.raise_for_status()
                return self._parse(resp.json())
            except (httpx.TransportError, LLMError) as exc:
                last = exc
                if attempt < self.max_retries - 1:
                    time.sleep(self.backoff_base * (2 ** attempt))
        raise LLMError(f"llm failed after {self.max_retries} tries: {last}")

    @staticmethod
    def _parse(body) -> ChatResult:
        msg = body["choices"][0]["message"]
        calls = []
        for tc in msg.get("tool_calls") or []:
            fn = tc.get("function", {})
            args, perr = None, None
            try:
                args = json.loads(fn.get("arguments") or "{}")
                if not isinstance(args, dict):
                    raise ValueError("arguments is not a JSON object")
            except (json.JSONDecodeError, ValueError) as exc:
                args, perr = None, str(exc)
            calls.append(ToolCall(id=tc.get("id", ""), name=fn.get("name", ""),
                                  arguments=args, parse_error=perr))
        usage = body.get("usage") or {}
        return ChatResult(
            text=msg.get("content"), tool_calls=calls,
            usage={"prompt_tokens": usage.get("prompt_tokens", 0),
                   "completion_tokens": usage.get("completion_tokens", 0)})
