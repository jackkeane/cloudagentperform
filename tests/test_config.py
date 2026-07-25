import os

from core.config import load_config

def test_defaults(monkeypatch):
    for k in list(os.environ):
        if k.startswith(("CAP_", "LLM_")):
            monkeypatch.delenv(k)
    cfg = load_config()
    assert cfg.max_concurrency == 2
    assert cfg.lease_ttl == 15
    assert cfg.llm_mode == "mock"
    assert cfg.sandbox_backend == "docker"

def test_env_override(monkeypatch):
    monkeypatch.setenv("CAP_LEASE_TTL", "3")
    monkeypatch.setenv("LLM_MODE", "real")
    cfg = load_config()
    assert cfg.lease_ttl == 3
    assert cfg.llm_mode == "real"
