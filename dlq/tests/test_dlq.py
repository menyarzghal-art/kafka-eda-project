import json
from unittest import mock

from django.test import TestCase

from dlq.agent import verified_suggestion
from dlq.models import DeadLetter
from dlq.pipeline import process_with_dlq
from dlq.utils import ReplayRefused, replay, store_dead_letter

GOOD = {"event_id": "b3f1c2de-7a4e-4c1b-9d52-0a1f6c3e8e11", "event_type": "OrderCreated", "customer_id": 42,
        "order_id": 17, "total": "129.90", "items": [{"sku": "SHOE-42", "qty": 1}],
        "created_at": "2026-11-18T10:30:00Z"}


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None, headers=None):
        self.sent.append((topic, key, value, dict(headers or [])))

    def flush(self, timeout=0):
        pass


class PipelineTests(TestCase):
    def test_valid_event_uses_handler_status(self):
        p = FakeProducer()
        assert process_with_dlq("billing", json.dumps(GOOD), b"42", lambda e: "processed", p) == "processed"
        assert p.sent == []

    def test_invalid_event_goes_to_dlq_without_retry(self):
        p, calls = FakeProducer(), []
        raw = b'{"event_id": "abc", "customer_id": "oops"}'
        assert process_with_dlq("billing", raw, "42", lambda e: calls.append(1), p) == "dlq"
        topic, key, value, headers = p.sent[0]
        assert topic == "orders.DLQ" and value == raw and calls == []
        assert headers["source_group"] == b"billing" and b"customer_id" in headers["error"]

    def test_three_attempts_then_dlq(self):
        p, calls = FakeProducer(), []

        def boom(e):
            calls.append(1)
            raise RuntimeError("SKU inconnu")
        assert process_with_dlq("inventory", json.dumps(GOOD), "42", boom, p) == "dlq"
        assert len(calls) == 3 and b"RuntimeError" in p.sent[0][3]["stacktrace"]

    def test_transient_failure_recovers(self):
        state = {"n": 0}

        def flaky(e):
            state["n"] += 1
            if state["n"] < 2:
                raise RuntimeError("temporaire")
            return "processed"
        p = FakeProducer()
        assert process_with_dlq("billing", json.dumps(GOOD), "42", flaky, p) == "processed" and p.sent == []


class DeadLetterTests(TestCase):
    def _dl(self, payload):
        return store_dead_letter(payload.encode(), b"42", [("error", b"boom"), ("source_group", b"billing")])

    def test_listener_stores_message(self):
        dl = self._dl("{}")
        assert DeadLetter.objects.count() == 1 and dl.source_group == "billing" and dl.status == "PENDING"

    def test_replay_valid_publishes_with_key_and_marks_replayed(self):
        dl, p = self._dl("{}"), FakeProducer()
        replay(dl, json.dumps(GOOD), p)
        dl.refresh_from_db()
        assert p.sent[0][0] == "orders" and p.sent[0][1] == "42" and dl.status == "REPLAYED"

    def test_replay_invalid_is_refused(self):
        dl, p = self._dl("{}"), FakeProducer()
        with self.assertRaises(ReplayRefused):
            replay(dl, json.dumps({**GOOD, "total": "-5"}), p)
        assert p.sent == [] and DeadLetter.objects.get().status == "PENDING"


class AgentAndViewsTests(TestCase):
    BAD_FIXABLE = json.dumps({**GOOD, "customer_id": "42", "order_id": "17", "total": "12,50"})

    def test_stub_fix_passes_pydantic(self):
        ok, text = verified_suggestion(self.BAD_FIXABLE, "", "")
        assert ok and json.loads(text)["total"] == "12.50"

    def test_unfixable_is_rejected(self):
        ok, errors = verified_suggestion('{"event_id": "abc", "customer_id": "oops"}', "", "")
        assert not ok and errors

    def test_dashboard_flow(self):
        dl = store_dead_letter(self.BAD_FIXABLE.encode(), b"42", [("error", b"boom"), ("source_group", b"billing")])
        assert self.client.get("/dlq/").status_code == 200
        assert self.client.get(f"/dlq/{dl.pk}/").status_code == 200
        r = self.client.post(f"/dlq/{dl.pk}/suggest/")
        dl.refresh_from_db()
        assert r.status_code == 200 and json.loads(dl.suggestion)["total"] == "12.50"
        fake = FakeProducer()
        with mock.patch("dlq.views.get_producer", return_value=fake):
            r = self.client.post(f"/dlq/{dl.pk}/replay/", {"payload": dl.suggestion})
        dl.refresh_from_db()
        assert r.status_code == 200 and dl.status == "REPLAYED" and fake.sent[0][0] == "orders"

    def test_dashboard_refuses_bad_replay(self):
        dl = store_dead_letter(b"{}", b"1", [])
        fake = FakeProducer()
        with mock.patch("dlq.views.get_producer", return_value=fake):
            r = self.client.post(f"/dlq/{dl.pk}/replay/", {"payload": "{}"})
        assert r.status_code == 200 and fake.sent == []

    def test_api_for_n8n(self):
        dl = store_dead_letter(self.BAD_FIXABLE.encode(), b"42", [])
        r = self.client.post(f"/dlq/api/{dl.pk}/suggest/")
        assert r.json()["valid"] is True
