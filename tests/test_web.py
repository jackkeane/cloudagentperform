import dataclasses

import pytest
from fastapi.testclient import TestClient

from core.config import load_config


@pytest.fixture
def client(tmp_path):
    from api.main import create_app
    cfg = dataclasses.replace(
        load_config(), db_path=str(tmp_path / "t.db"),
        artifacts_dir=str(tmp_path / "artifacts"),
        redis_url="redis://localhost:6379/15")
    app = create_app(cfg)
    return TestClient(app)


def test_index_serves_html_with_submit_control_and_provenance(client):
    r = client.get("/")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    body = r.text
    assert 'id="prompt"' in body           # submit control: prompt input
    assert 'id="submit-btn"' in body       # submit control: submit button
    assert 'id="provenance"' in body       # provenance placeholder
