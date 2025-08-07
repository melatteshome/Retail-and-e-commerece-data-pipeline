import random
from datetime import timezone
from faker import Faker
from faker.providers import BaseProvider
import pandas as pd


EVENT_TYPES = ["page_view", "add_to_cart", "checkout", "purchase" , "Order Completed", "Product Shared", "Product Reviewed", "select_promotion", "Products Searched", "Product List Viewed", "Product Clicked"]
EVENT_PROBS = [0.70, 0.15, 0.10, 0.05]


class ClickStreamProvider(BaseProvider):
    _customer_ids: list[int] | None = None
    _product_ids:  list[int] | None = None

    # ---------- one-time setup ----------
    @classmethod
    def configure(cls, customers, products) -> None:
        cls._customer_ids = list(customers)
        cls._product_ids  = list(products)

    # ---------- public API ----------
    def click_event(self) -> dict:
        if self._customer_ids is None or self._product_ids is None:
            raise RuntimeError(
                "ClickStreamProvider not configured. "
                "Call ClickStreamProvider.configure(customer_ids, product_ids) first."
            )

        return {
            "user_id":    random.choice(self._customer_ids),
            "product_id": random.choice(self._product_ids),
            "event_type": random.choices(EVENT_TYPES, weights=EVENT_PROBS, k=1)[0],
            "timestamp":  self.generator.date_time_between(
                              start_date="-30d", end_date="now", tzinfo=timezone.utc
                          ).isoformat(),
        }

