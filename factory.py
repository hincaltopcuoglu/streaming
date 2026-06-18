from models import ClickEvent, PurchaseEvent

class EventFactory:
    @staticmethod
    def from_dict(data):
        if data.get("event_type") == "purchase":
            return PurchaseEvent(
                user_id=data["user_id"],
                url=data["url"],
                action=data["action"],
                session_id=data["session_id"],
                timestamp=data["timestamp"],
                amount=data["amount"]
                )

        return ClickEvent(
            user_id = data["user_id"], 
            url = data["url"], 
            action = data["action"],
            session_id  = data["session_id"],
            timestamp= data["timestamp"] 
        )