from django.db import models


class ProcessedEvent(models.Model):
    event_id = models.UUIDField()
    consumer_group = models.CharField(max_length=50)
    processed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("event_id", "consumer_group")