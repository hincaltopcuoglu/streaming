from producer import EventProducer
from models import PurchaseEvent
import time

p = EventProducer(topic="clickstream_v2") # default topic is now clickstream_v2

p.send(PurchaseEvent(1, "/buy", "purchase", "s1", int(time.time()), 49.99))
time.sleep(1)
p.send(PurchaseEvent(2, "/buy", "purchase", "s2", int(time.time()), 20.00))
time.sleep(1)
p.send(PurchaseEvent(1, "/buy", "purchase", "s1", int(time.time()), 99.99))

p.close()
print("Done!")