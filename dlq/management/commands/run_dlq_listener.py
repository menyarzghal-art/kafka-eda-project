from confluent_kafka import Consumer
from django.conf import settings
from django.core.management.base import BaseCommand

from dlq.utils import store_dead_letter


class Command(BaseCommand):
    help = "Lit orders.DLQ, enregistre dans DeadLetter et alerte n8n"

    def handle(self, *args, **opts):
        c = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP, "group.id": "dlq-listener",
                      "auto.offset.reset": "earliest", "enable.auto.commit": False})
        c.subscribe(["orders.DLQ"])
        self.stdout.write("Listener DLQ démarré")
        while True:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            dl = store_dead_letter(msg.value(), msg.key(), msg.headers())
            c.commit(msg)                                # commit APRÈS l'enregistrement en base
            self.stdout.write(f"DLQ #{dl.pk} ({dl.source_group}) : {dl.error[:80]}")
