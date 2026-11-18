import random
from django.core.management.base import BaseCommand
from orders.services import create_order


class Command(BaseCommand):
    help = "Crée une commande de test"

    def add_arguments(self, parser):
        parser.add_argument("--fail", action="store_true",
                            help="Simule une erreur pour montrer le rollback")

    def handle(self, *args, **opts):
        customer = random.randint(1, 5)
        order = create_order(customer, "129.90",
                             [{"sku": "SHOE-42", "qty": 1}],
                             fail=opts["fail"])
        self.stdout.write(f"Commande {order.id} créée pour le client {customer}")