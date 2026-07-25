import pytest
import redis as redis_lib

REDIS_URL = "redis://localhost:6379/15"

@pytest.fixture
def bus():
    from core.queuebus import QueueBus
    try:
        r = redis_lib.Redis.from_url(REDIS_URL)
        r.ping()
    except Exception:
        pytest.fail("Redis required for this test: docker compose up -d redis")
    r.flushdb()
    return QueueBus(REDIS_URL)

@pytest.fixture
def store(tmp_path):
    from core.store import TaskStore
    return TaskStore(str(tmp_path / "t.db"))

@pytest.fixture(scope="session")
def sandbox_image():
    import docker as docker_lib
    try:
        client = docker_lib.from_env()
        client.ping()
    except Exception:
        pytest.fail("Docker required for this test (daemon not reachable)")
    client.images.build(path=".", dockerfile="sandbox.Dockerfile",
                        tag="cap-sandbox")
    return "cap-sandbox"

@pytest.fixture
def provider(sandbox_image):
    from sandbox.docker_provider import DockerSandboxProvider
    p = DockerSandboxProvider(image=sandbox_image)
    yield p
    p.gc(set())  # drop every cap-labeled container left behind
