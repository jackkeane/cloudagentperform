import json

import pytest

from agent.mock import MockProvider, TrajectoryMismatch, fixture_sha256

TRAJ = "tests/fixtures/tiny_trajectory.json"
FIXTURE = "fixtures/demo-repo"

def test_replays_steps_in_order_ignoring_messages():
    p = MockProvider(TRAJ, FIXTURE)
    s1 = p.chat([{"role": "user", "content": "anything"}], tools=[])
    assert s1.tool_calls[0].name == "bash"
    s2 = p.chat([], tools=[])
    assert s2.tool_calls[0].name == "write_file"
    s3 = p.chat([], tools=[])
    assert s3.tool_calls == [] and "report" in s3.text

def test_exhausted_trajectory_raises():
    p = MockProvider(TRAJ, FIXTURE)
    for _ in range(3):
        p.chat([], tools=[])
    with pytest.raises(TrajectoryMismatch):
        p.chat([], tools=[])

def test_describe_exposes_replay_provenance():
    d = MockProvider(TRAJ, FIXTURE).describe()
    assert d["mode"] == "mock" and d["model"].startswith("replay:")

def test_pinned_hash_mismatch_refuses_to_load(tmp_path):
    traj = {"recorded_from": {"model": "m", "date": "2026-07-25",
                              "fixture_sha256": "0" * 64},
            "steps": [{"tool_calls": [], "text": "hi"}]}
    path = tmp_path / "t.json"
    path.write_text(json.dumps(traj))
    with pytest.raises(TrajectoryMismatch, match="hash"):
        MockProvider(str(path), FIXTURE)

def test_fixture_sha256_is_stable_and_content_sensitive(tmp_path):
    (tmp_path / "a.txt").write_text("one")
    h1 = fixture_sha256(str(tmp_path))
    assert h1 == fixture_sha256(str(tmp_path))
    (tmp_path / "a.txt").write_text("two")
    assert fixture_sha256(str(tmp_path)) != h1
