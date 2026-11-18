from django.db import models


class DeadLetter(models.Model):
    PENDING, REPLAYED = "PENDING", "REPLAYED"
    payload = models.TextField()                         # message brut
    key = models.CharField(max_length=100, blank=True)
    error = models.TextField()
    stacktrace = models.TextField(blank=True)
    source_group = models.CharField(max_length=50)
    status = models.CharField(max_length=20, default=PENDING)
    suggestion = models.TextField(blank=True)            # JSON proposé par l'agent (déjà validé)
    created_at = models.DateTimeField(auto_now_add=True)
