"""Agent IA local : il SUGGÈRE, Pydantic DÉCIDE."""
import json
import re
import urllib.request
from datetime import datetime, timezone
from uuid import uuid4

from django.conf import settings

from .schemas import OrderEvent
from .utils import validate_payload


def suggest_fix(raw: str, error: str, stacktrace: str):
    """Retourne un dict candidat ou None."""
    if getattr(settings, "AGENT_MODE", "stub") == "ollama":
        return _ollama(raw, error, stacktrace)
    return _stub(raw)


def verified_suggestion(raw, error, stacktrace):
    """(True, json_str) si la suggestion passe Pydantic, sinon (False, [erreurs])."""
    cand = suggest_fix(raw, error, stacktrace)
    if cand is None:
        return False, ["l'agent n'a rien proposé"]
    text = json.dumps(cand, ensure_ascii=False)
    event, errors = validate_payload(text)
    return (True, json.dumps(event.model_dump(mode="json"), indent=2)) if event else (False, errors)


def _ollama(raw, error, stacktrace):
    prompt = ("Tu répares un message Kafka invalide. Réponds UNIQUEMENT avec le JSON corrigé.\n"
              f"Schéma: {json.dumps(OrderEvent.model_json_schema())}\nMessage: {raw}\n"
              f"Erreur: {error}\nStack trace:\n{stacktrace[-1500:]}")
    req = urllib.request.Request(
        f"{getattr(settings, 'OLLAMA_URL', 'http://localhost:11434')}/api/generate",
        data=json.dumps({"model": getattr(settings, "OLLAMA_MODEL", "llama3.2"), "prompt": prompt,
                         "stream": False, "format": "json"}).encode(),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(json.loads(r.read())["response"])
    except Exception:
        return None


def _stub(raw):
    """Agent déterministe de secours (démo sans GPU) : corrige les fautes classiques."""
    try:
        d = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(d, dict):
        return None
    if isinstance(d.get("total"), str):
        d["total"] = re.sub(r"[^\d.]", "", d["total"].replace(",", "."))
    for k in ("customer_id", "order_id"):
        if isinstance(d.get(k), str) and d[k].isdigit():
            d[k] = int(d[k])
    for item in d["items"] if isinstance(d.get("items"), list) else []:
        if isinstance(item, dict) and item.get("sku") == "POISON":
            item["sku"] = "UNKNOWN-SKU-REVIEW"
    d.setdefault("event_id", str(uuid4()))
    d.setdefault("event_type", "OrderCreated")
    d.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    return d
