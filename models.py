class ClickEvent:
    def __init__(self, user_id, url, action, session_id, timestamp):
        self.user_id = user_id
        self.url = url
        self.action = action
        self.session_id = session_id
        self.timestamp = timestamp

    def to_dict(self):
        return {
            "user_id" : self.user_id,
            "url" : self.url,
            "action" : self.action,
            "session_id" : self.session_id,
            "timestamp" : self.timestamp,
        }

    def is_purchase(self):
        
        return self.action == "purchase"

    def event_type(self):
        return "click"


class PurchaseEvent(ClickEvent):
    def __init__(self, user_id, url, action, session_id, timestamp, amount):
        super().__init__(user_id, url, action, session_id, timestamp)
        self.amount = amount


    def to_dict(self):
        return {
            "user_id" : self.user_id,
            "url" : self.url,
            "action" : self.action,
            "session_id" : self.session_id,
            "timestamp" : self.timestamp,
            "amount": self.amount,
        }

    def is_purchase(self):
        return True

    def event_type(self):
        return "purchase"