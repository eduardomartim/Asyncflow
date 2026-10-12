"""Integration tests for the v1 Tasks API, against the real PostgreSQL
started by docker-compose.yml. Each task created here uses task_type
"test_*" so `cleanup_db` (conftest.py) can find and remove it afterwards.
"""
import uuid

from tests.conftest import unique_idempotency_key


async def test_create_task_success(api_client):
    resp = await api_client.post(
        "/tasks",
        params={"task_type": "test_create"},
        json={"foo": "bar"},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "queued"
    assert body["task_type"] == "test_create"
    uuid.UUID(body["id"])  # raises if not a valid UUID


async def test_create_task_rejects_bad_task_type(api_client):
    # task_type is validated against a conservative charset (no spaces/slashes).
    resp = await api_client.post(
        "/tasks",
        params={"task_type": "not a valid type!"},
        json={},
    )
    assert resp.status_code == 422


async def test_create_task_rejects_non_object_payload(api_client):
    resp = await api_client.post(
        "/tasks",
        params={"task_type": "test_badpayload"},
        content=b'"just a string, not an object"',
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 422


async def test_idempotent_create_returns_same_task(api_client):
    key = unique_idempotency_key()
    first = await api_client.post(
        "/tasks",
        params={"task_type": "test_idem", "idempotency_key": key},
        json={"n": 1},
    )
    assert first.status_code == 201
    first_id = first.json()["id"]

    second = await api_client.post(
        "/tasks",
        params={"task_type": "test_idem", "idempotency_key": key},
        json={"n": 2},  # different body -- must still dedupe on the key
    )
    assert second.status_code == 200  # 200, not 201: no new resource was created
    assert second.json()["id"] == first_id
    assert "already submitted" in second.json()["message"]


async def test_get_task_by_id(api_client):
    created = await api_client.post(
        "/tasks", params={"task_type": "test_get"}, json={"x": 1}
    )
    task_id = created.json()["id"]

    resp = await api_client.get(f"/tasks/{task_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == task_id
    assert body["task_type"] == "test_get"
    assert body["status"] == "queued"
    assert body["attempts"] == 0


async def test_get_nonexistent_task_returns_404(api_client):
    resp = await api_client.get(f"/tasks/{uuid.uuid4()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Task not found"


async def test_list_tasks_filters_by_status_and_paginates(api_client):
    for _ in range(3):
        await api_client.post(
            "/tasks", params={"task_type": "test_list_status"}, json={}
        )

    resp = await api_client.get(
        "/tasks", params={"status": "queued", "page": 1, "size": 2}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"] == 1
    assert body["size"] == 2
    assert len(body["items"]) == 2
    assert all(item["status"] == "queued" for item in body["items"])

    resp_page2 = await api_client.get(
        "/tasks", params={"status": "queued", "page": 2, "size": 2}
    )
    page2_ids = {item["id"] for item in resp_page2.json()["items"]}
    page1_ids = {item["id"] for item in body["items"]}
    assert page1_ids.isdisjoint(page2_ids)  # no overlap between pages
