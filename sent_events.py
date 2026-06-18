from producer import EventProducer
from models import ClickEvent, PurchaseEvent
import time

p = EventProducer(topic="clickstream_v2")

# Session s1: 2 clicks then 1 purchase
p.send(ClickEvent(user_id=1, url="/home", action="click", session_id="s1", timestamp=int(time.time())))
time.sleep(0.1)
p.send(ClickEvent(user_id=1, url="/products", action="click", session_id="s1", timestamp=int(time.time())))
time.sleep(0.1)
p.send(PurchaseEvent(user_id=1, url="/checkout", action="purchase", session_id="s1", timestamp=int(time.time()), amount=49.99))

# Session s2: just 1 click, no purchase
time.sleep(0.1)
p.send(ClickEvent(user_id=2, url="/home", action="click", session_id="s2", timestamp=int(time.time())))

# Session s3: 1 click then purchase
time.sleep(0.1)
p.send(ClickEvent(user_id=3, url="/home", action="click", session_id="s3", timestamp=int(time.time())))
time.sleep(0.1)
p.send(PurchaseEvent(user_id=3, url="/buy", action="purchase", session_id="s3", timestamp=int(time.time()), amount=99.99))

# Session s4: just clicks, no purchase
time.sleep(0.1)
p.send(ClickEvent(user_id=4, url="/home", action="click", session_id="s4", timestamp=int(time.time())))
time.sleep(0.1)
p.send(ClickEvent(user_id=4, url="/about", action="click", session_id="s4", timestamp=int(time.time())))

p.close()
print("Done!")