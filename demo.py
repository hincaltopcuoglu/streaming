"""
Online learning canli simulasyon — v6 (distribution shift + drift).

Phase 1 (0-30s)  : Normal — buyuk session = purchase, kucuk = no purchase
Phase 2 (30-60s) : Drift  — sinyal zayifliyor (buyuk session artik daha az purchase ediyor)
Phase 3 (60-90s) : Shift  — yon tersine: az click = purchase, cok click = no purchase
Phase 4 (90-120s): Drift  — yeni sinyal de zayifliyor
Phase 5 (120-180s): Stabil yeni dağılım

Ekran HER 10 SANIYEDE BIR guncellenir.
"""

import json
import random
import time
import urllib.request
import urllib.error
import os


def send_event(payload):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "http://localhost:30080/events",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def get_weights():
    with urllib.request.urlopen("http://localhost:30080/metrics/weights", timeout=5) as r:
        return json.loads(r.read())


def predict(clicks, time_page):
    url = "http://localhost:30080/predictions?clicks_in_session={0}&time_on_page={1}".format(clicks, time_page)
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.loads(r.read())["purchase_probability"]


def bar(p, length=20):
    fill = max(0, min(length, int(round(p * length))))
    return "#" * fill + "." * (length - fill)


def clear():
    os.system("cls" if os.name == "nt" else "clear")


def get_phase(elapsed):
    """Zamana gore phase ve signal bilgisi don."""
    if elapsed < 30:
        return ("PHASE 1: Normal — buyuk click = purchase",
                12, 20, 2, 5, 1.0, "buyuk")
    elif elapsed < 60:
        # Drift: buyuk session'da purchase olasiligi 0.5'e dusuyor
        return ("PHASE 2: Drift  — sinyal zayifliyor (buyuk session %50 purchase)",
                12, 20, 2, 5, 0.5, "buyuk")
    elif elapsed < 90:
        # Shift: yon tersine
        return ("PHASE 3: SHIFT  — kucuk click = purchase, buyuk = no purchase",
                12, 20, 2, 5, 1.0, "kucuk")
    elif elapsed < 120:
        # Drift yeni sinyalde
        return ("PHASE 4: Drift  — yeni sinyal zayifliyor",
                12, 20, 2, 5, 0.5, "kucuk")
    else:
        # Stabil yeni
        return ("PHASE 5: Stabil yeni dagilim — kucuk = purchase",
                12, 20, 2, 5, 1.0, "kucuk")


def main():
    print("=== Online learning v6 — distribution shift + drift ===")
    print("Phase 1 (0-30s)  : Normal")
    print("Phase 2 (30-60s) : Drift (sinyal zayifliyor)")
    print("Phase 3 (60-90s) : SHIFT (yon tersine)")
    print("Phase 4 (90-120s): Drift yeni sinyalde")
    print("Phase 5 (120s+) : Stabil yeni")
    print()
    print("Ekran her 10 saniyede guncellenir. Durdurmak icin Ctrl+C")
    print()

    start = time.time()
    rounds = 0
    last_print = 0
    last_w = None

    while time.time() - start < 180:
        rounds += 1
        sid = "sess_{0}_{1}".format(int(start), rounds)
        elapsed = int(time.time() - start)

        # Phase bilgisi
        phase_text, big_min, big_max, small_min, small_max, buy_prob, positive_class = get_phase(elapsed)

        # Session tipine karar ver
        # positive_class="buyuk" ise buyuk session purchase eder
        # positive_class="kucuk" ise kucuk session purchase eder
        if positive_class == "buyuk":
            # Buyuk session purchase eder, kucuk etmez
            if rounds % 2 == 0:
                n_clicks = random.randint(big_min, big_max)
                will_buy = random.random() < buy_prob
            else:
                n_clicks = random.randint(small_min, small_max)
                will_buy = False
        else:  # "kucuk"
            # Kucuk session purchase eder, buyuk etmez
            if rounds % 2 == 0:
                n_clicks = random.randint(small_min, small_max)
                will_buy = random.random() < buy_prob
            else:
                n_clicks = random.randint(big_min, big_max)
                will_buy = False

        session_start_ts = int(time.time())

        for i in range(n_clicks):
            send_event({
                "user_id": 1,
                "url": "/page",
                "action": "click",
                "session_id": sid,
                "timestamp": session_start_ts + i,
            })
            time.sleep(0.005)

        if will_buy:
            send_event({
                "user_id": 1,
                "url": "/checkout",
                "action": "purchase",
                "session_id": sid,
                "timestamp": session_start_ts + n_clicks,
                "amount": 99.99,
            })

        # Her 10 saniyede ekran bas
        elapsed = int(time.time() - start)
        if elapsed - last_print >= 10:
            clear()
            print("=== t={0}s  toplam_session={1} ===".format(elapsed, rounds))
            print("  {0}".format(phase_text))
            print()

            try:
                w = get_weights()
                if w["exists"]:
                    wc = w["weights"]["clicks_in_session"]
                    wt = w["weights"]["time_on_page"]
                    bb = w["intercept"]
                    uc = w["update_count"]
                    ub = w["last_batch_size"]

                    print("  Model  update=#{0}  batch={1}".format(uc, ub))
                    print("    w_clicks = {0:+.5f}    w_time = {1:+.5f}    b = {2:+.5f}".format(wc, wt, bb))

                    if last_w is not None:
                        dwc = wc - last_w[0]
                        dwt = wt - last_w[1]
                        dbb = bb - last_w[2]
                        print("    delta:    dwc={0:+.5f}      dwt={1:+.5f}      db={2:+.5f}".format(dwc, dwt, dbb))
                    last_w = (wc, wt, bb)
                else:
                    print("  (model henuz olusmadi)")
            except (urllib.error.URLError, urllib.error.HTTPError) as e:
                print("  weights hatasi: {0}".format(e))

            print()
            print("  Predictions  (clicks, time) -> p")
            print("  +-------+-------+-------+------------------------+")
            print("  | clicks|  time |   p   | bar                    |")
            print("  +-------+-------+-------+------------------------+")
            for c, t in [(1, 2), (3, 4), (5, 6), (7, 8), (10, 12), (15, 16), (20, 22)]:
                try:
                    p = predict(c, t)
                    print("  | {0:>5} | {1:>5} | {2:+.4f} | {3} |".format(c, t, p, bar(p)))
                except (urllib.error.URLError, urllib.error.HTTPError) as e:
                    print("  | {0:>5} | {1:>5} | ERR   | {2}".format(c, t, e))
            print("  +-------+-------+-------+------------------------+")

            last_print = elapsed

        time.sleep(0.05)

    print()
    print("=== Demo bitti ({0} round, {1}s) ===".format(rounds, int(time.time() - start)))


if __name__ == "__main__":
    main()