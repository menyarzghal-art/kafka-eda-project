import uuid
from django.db import transaction
from .models import Order, OutboxEvent


def create_order(customer_id, total, items, fail=False):
    with transaction.atomic():  # tout ou rien
        order = Order.objects.create(customer_id=customer_id, total=total)
        event_id = uuid.uuid4()
        OutboxEvent.objects.create(
            id=event_id,
            topic="orders",
            key=str(customer_id),
            payload={
                "event_id": str(event_id),
                "event_type": "OrderCreated",
                "customer_id": customer_id,
                "order_id": order.id,
                "total": str(total),
                "items": items,
                "created_at": order.created_at.isoformat(),
            },
        )
        if fail:
            raise Exception("Erreur simulée !")
    return order