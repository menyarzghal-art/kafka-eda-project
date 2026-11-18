import json
import uuid

from confluent_kafka import Consumer
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from consumers.models import ProcessedEvent


def action_billing(e):
    print(f"[FACTURE] {e['total']} pour le client {e['customer_id']}")
    points = float(e["total"]) / 10
    print(f"[FIDÉLITÉ] +{points:.1f} points pour le client {e['customer_id']}")


ACTIONS = {
    "inventory": lambda e: print(f"[STOCK] réserver le stock pour la commande {e['order_id']}"),
    "billing": action_billing,
    "notification": lambda e: print(f"[NOTIF] email au client {e['customer_id']}"),
}


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("group")

    def handle(self, *args, **opts):
        group = opts["group"]
        c = Consumer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c.subscribe(["orders"])
        print(f"consumer {group} démarré")

        while True:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue

            try:
                event = json.loads(msg.value())
                event["event_id"] = str(uuid.UUID(event["event_id"]))
            except (ValueError, KeyError, TypeError) as err:
                print(f"[{group}] message invalide ignoré : {err}")
                c.commit(msg)
                continue

            with transaction.atomic():
                _, created = ProcessedEvent.objects.get_or_create(
                    event_id=event["event_id"], consumer_group=group)
                if created:
                    ACTIONS[group](event)
                else:
                    print(f"[{group}] doublon {event['event_id']} ignoré")

            c.commit(msg)