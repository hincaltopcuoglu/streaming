"""Event blueprints shared between the API and the Spark streaming job.

These are plain dataclasses so both the FastAPI service (Pydantic conversion
in the API layer) and the Spark job (row -> dict -> typed object) can use
the same vocabulary. Inheritance + polymorphism is preserved:

  - ClickEvent is the base. A "click" is anything that is not a purchase.
  - PurchaseEvent extends it and adds `amount`. It also flips
    `is_purchase()` to always return True and exposes event_type="purchase".

Serialization rules (kept identical to the original project so existing
Kafka consumers keep working):

  - to_dict() returns only JSON-safe primitives.
  - The Kafka payload does NOT include event_type — downstream readers
    detect purchase vs click by inspecting the `action` field, which is
    how the original producer always worked.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict


@dataclass
class ClickEvent:
    user_id: int
    url: str
    action: str
    session_id: str
    timestamp: int  # epoch seconds

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def is_purchase(self) -> bool:
        return self.action == "purchase"

    def event_type(self) -> str:
        return "click"


@dataclass
class PurchaseEvent(ClickEvent):
    amount: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        d["amount"] = self.amount
        return d

    def is_purchase(self) -> bool:
        return True

    def event_type(self) -> str:
        return "purchase"
