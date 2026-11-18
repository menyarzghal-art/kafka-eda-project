import json
import time
from confluent_kafka import Producer
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from orders.models import OutboxEvent


class Command(BaseCommand):
    help = "Lit les événements outbox non envoyés et les publie dans Kafka"

    def handle(self, *args, **opts):
        producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP})
        self.stdout.write("Relay démarré...")
        while True:
            with transaction.atomic():
                events = list(
                    OutboxEvent.objects.select_for_update(skip_locked=True)
                    .filter(published_at__isnull=True)
                    .order_by("created_at")[:100]
                )
                for e in events:
                    producer.produce(e.topic, key=e.key,
                                     value=json.dumps(e.payload))
                if events:
                    producer.flush()  # attendre la confirmation de Kafka
                    for e in events:
                        e.published_at = timezone.now()
                        e.save()
                    self.stdout.write(f"{len(events)} événement(s) publié(s)")
            time.sleep(1)