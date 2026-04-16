PROM = "http://10.220.212.49:9090"
QUERY_GPU_WH = 'outlet10Wh{instance="127.0.0.1:9116",job="Eaton ePDU 2",target="10.212.220.16"}'
QUERY_GPU_WATTS = 'outlet10Watts{instance="127.0.0.1:9116",job="Eaton ePDU 2",target="10.212.220.16"}'

import requests
import pandas as pd

def fetch_prom_ts(start, end, step, query):
    start = start.tz_convert("UTC")
    end = end.tz_convert("UTC")
    
    start_str = start.isoformat().replace("+00:00", "Z")
    end_str = end.isoformat().replace("+00:00", "Z")

    params = {
        "query": query,
        "start": start_str,
        "end":   end_str,
        "step":  step
    }

    
    r = requests.get(f"{PROM}/api/v1/query_range", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()["data"]["result"]
    
    rows = []
    for series in data:
        labels = series["metric"]
        for ts, value in series["values"]:
            row = dict(labels)
            row["timestamp"] = pd.to_datetime(float(ts), unit="s", utc=True)
            row["value"] = float(value)
            rows.append(row)
    return rows

def calc_gauge_sum(df):
    df = df.sort_values("timestamp")
    df["dt"] = df["timestamp"].diff().dt.total_seconds()
    df["avg_power"] = (df["value"] + df["value"].shift()) / 2
    energy_wh = (df["avg_power"] * df["dt"] / 3600).sum()
    return energy_wh
    
def fetch_gpu_wh(start, end, step):
    if step.endswith('s'):
        step_int_sec = int(step[0:-1])
    wh_ts = fetch_prom_ts(start, end, step, QUERY_GPU_WH)
    watts_ts = fetch_prom_ts(start, end, step, QUERY_GPU_WATTS)
    wh_counter = wh_ts[-1]["value"] - wh_ts[0]["value"]
    wh_gauge_sum = pd.DataFrame(watts_ts)["value"].sum()*step_int_sec/3600
    return (wh_counter, wh_gauge_sum, calc_gauge_sum(pd.DataFrame(watts_ts)), (end-start).value / 1000 )