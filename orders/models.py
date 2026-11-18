import uuid
from django.db import models


class Order(models.Model):
    customer_id = models.IntegerField()
    total = models.DecimalField(max_digits=10, decimal_places=2)
    status = models.CharField(max_length=20, default="CREATED")
    created_at = models.DateTimeField(auto_now_add=True)


class OutboxEvent(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    topic = models.CharField(max_length=100, default="orders")
    key = models.CharField(max_length=100)
    payload = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    published_at = models.DateTimeField(null=True, blank=True)