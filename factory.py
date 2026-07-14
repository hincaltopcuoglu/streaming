"""EventFactory rebuilds typed event objects from raw dictionaries.
Kafka messages arrive as JSON-decoded dicts. Before any OOP logic runs
(e.g. `event.is_purchase()`), those dicts need to be turned back into
ClickEvent / PurchaseEvent instances.
This module centralizes that decision: any caller can ask for
`EventFactory.from_dict(d)` and get back the correct subtype without
caring how the choice is made.
Decision rule (mirrors what our producer writes):
    action == "purchase"  ->  PurchaseEvent
    anything else          ->  ClickEvent
"""


from models import ClickEvent, PurchaseEvent

class EventFactory:
    @staticmethod
    def from_dict(data):
        if data.get("action") == "purchase":
            return PurchaseEvent(
                user_id = data["user_id"],
                url = data["url"],
                action = data["action"],
                session_id = data["session_id"],
                timestamp = data["timestamp"],
                amount = data.get("amount",0.0),
                )

        return ClickEvent(
            user_id = data["user_id"], 
            url = data["url"], 
            action = data["action"],
            session_id  = data["session_id"],
            timestamp= data["timestamp"] 
        )