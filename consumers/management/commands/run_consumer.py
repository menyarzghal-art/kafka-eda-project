import json
import uuid

from confluent_kafka import Consumer
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from consumers.models import ProcessedEvent
from dlq.utils import send_to_dlq

MAX_ATTEMPTS = 3


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

            # 1) message invalide : direct en DLQ
            try:
                event = json.loads(msg.value())
                event["event_id"] = str(uuid.UUID(event["event_id"]))
            except (ValueError, KeyError, TypeError) as err:
                print(f"[{group}] message invalide -> DLQ : {err}")
                send_to_dlq(msg.value(), msg.key(), err, group)
                c.commit(msg)
                continue

            # 2) traitement avec 3 essais, puis DLQ
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    with transaction.atomic():
                        _, created = ProcessedEvent.objects.get_or_create(
                            event_id=event["event_id"], consumer_group=group)
                        if created:
                            ACTIONS[group](event)
                        else:
                            print(f"[{group}] doublon {event['event_id']} ignoré")
                    break
                except Exception as err:
                    print(f"[{group}] échec {attempt}/{MAX_ATTEMPTS} : {err}")
                    if attempt == MAX_ATTEMPTS:
                        send_to_dlq(msg.value(), msg.key(), err, group)

            c.commit(msg)
