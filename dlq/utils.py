import json
import logging
import traceback
import urllib.request

from confluent_kafka import Producer
from django.conf import settings
from pydantic import ValidationError

from .models import DeadLetter
from .schemas import OrderEvent

log = logging.getLogger(__name__)
_producer = None


class ReplayRefused(Exception):
    pass


def get_producer():
    global _producer
    if _producer is None:                                # créé à la demande (pas à l'import)
        _producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP})
    return _producer


def send_to_dlq(raw_value, key, error, group, producer=None):
    """Publie le message d'origine dans orders.DLQ avec headers error / source_group / stacktrace."""
    producer = producer or get_producer()
    stack = "".join(traceback.format_exception(error))[-3000:]
    producer.produce("orders.DLQ", key=key or None, value=raw_value,
                     headers=[("error", str(error)[:500].encode()),
                              ("source_group", group.encode()),
                              ("stacktrace", stack.encode())])
    producer.flush(10)


def store_dead_letter(value, key, headers) -> DeadLetter:
    """Appelé par le listener : enregistre le message de orders.DLQ dans la table."""
    h = {k: (v.decode("utf-8", "replace") if v else "") for k, v in (headers or [])}
    text = value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value)
    dl = DeadLetter.objects.create(
        payload=text, key=(key.decode() if isinstance(key, bytes) else key) or "",
        error=h.get("error", "inconnue"), stacktrace=h.get("stacktrace", ""),
        source_group=h.get("source_group", "inconnu"))
    notify_n8n(dl)
    return dl


def notify_n8n(dl: DeadLetter):
    url = getattr(settings, "N8N_WEBHOOK_URL", "")
    if not url:
        return
    body = json.dumps({"dlq_id": dl.pk, "source_group": dl.source_group, "error": dl.error}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=3)
    except Exception as exc:                             # l'alerte ne doit jamais casser le listener
        log.warning("n8n indisponible: %s", exc)


def validate_payload(text):
    """Retourne (OrderEvent | None, [erreurs lisibles])."""
    try:
        return OrderEvent.model_validate_json(text), []
    except ValidationError as exc:
        return None, [f"{'.'.join(map(str, e['loc'])) or 'json'}: {e['msg']}" for e in exc.errors()]


def replay(dl: DeadLetter, payload_text: str, producer=None):
    """Rejoue UNIQUEMENT un JSON validé par Pydantic, dans le topic orders, avec la clé customer_id."""
    event, errors = validate_payload(payload_text)
    if event is None:
        raise ReplayRefused("; ".join(errors))
    producer = producer or get_producer()
    producer.produce("orders", key=str(event.customer_id), value=payload_text)
    producer.flush(10)
    dl.status = DeadLetter.REPLAYED
    dl.save(update_fields=["status"])
