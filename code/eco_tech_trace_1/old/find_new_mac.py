import pyshark
import pandas as pd

capture = pyshark.FileCapture("dump_2025-07-02_00-41-04.pcap")
first_seen = {}


def get_info():
    df = pd.DataFrame(list(first_seen.items()), columns=["mac", "timestamp"])
    df["minute"] = (df["timestamp"] // 60) * 60
    df["minute_dt"] = pd.to_datetime(df["minute"], unit="s")

    result = df.groupby("minute_dt").size()

    print(result)


for i, pkt in enumerate(capture):
    if 'ETH' in pkt:

        ts = float(pkt.sniff_timestamp)
        
        for mac in [pkt.eth.src, pkt.eth.dst]:
            if mac not in first_seen:
                first_seen[mac] = ts

    if i % 10000 == 0:
        print(i, len(first_seen))
        get_info()