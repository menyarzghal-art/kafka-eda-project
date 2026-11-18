"""Fonction à appeler par P3 dans run_consumer : validation Pydantic + 3 essais + DLQ."""
import json
import time

from django.conf import settings
from pydantic import ValidationError

from .schemas import OrderEvent
from .utils import send_to_dlq


def process_with_dlq(group, raw, key, handler, producer=None) -> str:
    """handler(event_dict) = fonction de P3 (ProcessedEvent + action). Retourne son statut ou 'dlq'."""
    key_s = key.decode() if isinstance(key, bytes) else (key or "")
    try:
        data = json.loads(raw)
        OrderEvent.model_validate(data)                  # événement invalide = poison, pas de retry
    except (ValueError, ValidationError) as exc:
        send_to_dlq(raw, key_s, exc, group, producer)
        return "dlq"
    last = None
    for attempt in range(getattr(settings, "MAX_RETRIES", 3)):
        try:
            return handler(data)
        except Exception as exc:                         # noqa: BLE001
            last = exc
            time.sleep(getattr(settings, "DLQ_RETRY_DELAY", 0))
    send_to_dlq(raw, key_s, last, group, producer)
    return "dlq"
