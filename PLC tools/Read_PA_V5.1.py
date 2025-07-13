import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

BASE_URL = os.getenv("BASE_URL")
if not BASE_URL:
    raise ValueError("BASE_URL is not set in the .env file.")
BASE_URL = os.getenv("BASE_URL")


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.72'
}

SORTS = {
    1: ("T09:30:00.000Z&endTime=", "T15:29:59.999Z&locale=en-US", "Sunrise"),
    2: ("T15:30:00.000Z&endTime=", "T22:29:59.999Z&locale=en-US", "Day"),
    3: ("T22:30:00.000Z&endTime=", "T03:29:59.999Z&locale=en-US", "Twilight"),
    4: ("T03:30:00.000Z&endTime=", "T09:29:59.999Z&locale=en-US", "Night"),
    5: ("T06:00:00.000Z&endTime=", "T15:29:59.999Z&locale=en-US", "Mon-Sunrise")
}

PMODS = [
    "399", "398", "146", "152", "82", "151", "84", "153", "397", "172", "409", "97",
    "428", "95", "402", "96", "401", "94", "85", "110", "90", "109", "106", "143", "168",
    "139", "108", "140", "105", "144", "123", "141", "88", "142", "121", "124", "403",
    "430", "400", "176", "207", "390", "177", "212", "214", "173", "183", "185", "184",
    "186", "411", "412", "421", "420", "217", "419", "213", "215"
]

def get_shift_dates(day, sort):
    today = datetime.today()
    date_shift = timedelta(days=day)
    if sort == 4:
        date_start = (today - timedelta(days=day - 1)).date()
        date_end = (today - timedelta(days=day - 1)).date()
    elif sort == 3:
        date_start = (today - date_shift).date()
        date_end = (today - timedelta(days=day - 1)).date()
    else:
        date_start = (today - date_shift).date()
        date_end = date_start
    return str(date_start), str(date_end)

def fetch_data(start_date, end_date, sort, page):
    mod = PMODS[page]
    suffix = f"{SORTS[sort][0]}{end_date}{SORTS[sort][1]}"
    url1 = f"{base_url}/{mod}/statistics/aggregatedreadrate/shift/device?startTime={start_date}{suffix}"
    url2 = f"{base_url}/{mod}/statistics/aggregatedreadrate/shift?startTime={start_date}{suffix}"
    r1 = requests.get(url1)
    r2 = requests.get(url2)
    return r1.json() if r1.ok else None, r2.json() if r2.ok else None

def parallel_fetch(start_date, end_date, sort, max_workers=58):
    results1, results2 = [None] * 58, [None] * 58
    def worker(i):
        results1[i], results2[i] = fetch_data(start_date, end_date, sort, i)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(worker, range(58))
    return results1, results2

def reorder_data(json1, json2):
    sorted1, sorted2 = [None] * 58, [None] * 58
    for i in range(58):
        if json1[i] and isinstance(json1[i], list):
            idx = PMODS.index(json1[i][0]["systemId"])
            sorted1[idx] = json1[i][0]
            sorted2[idx] = json2[i][0] if json2[i] and isinstance(json2[i], list) else None
    return sorted1, sorted2

def map_read_rates(device_count, reads, rates):
    if device_count == 17:
        order = [6,7,8,9,10,11,12,13,14,15,16,0,1,2,3,4,5]
    elif device_count == 6:
        order = [0,1,2,3,4,5]
    else:
        order = [1,2,3,4,0]
    mapped_reads = [reads[i] for i in order if i < len(reads)]
    mapped_rates = [rates[i] for i in order if i < len(rates)]
    return mapped_reads, mapped_rates

def main():
    day = int(input("Enter day (0 = today, 1 = yesterday, etc.): "))
    today = datetime.today()
    df_date = today - timedelta(days=day)
    day_of_week = df_date.weekday()

    if day_of_week in (5, 6):
        print("No data for Saturday or Sunday. Exiting...")
        return

    sort_ids = [1,2,3,4] if day_of_week != 0 else [5,2,3,4]
    all_dfs = []

    for sort in sort_ids:
        start_date, end_date = get_shift_dates(day, sort)
        print(f"Fetching data for sort: {SORTS[sort][2]} ({start_date})")
        data1, data2 = parallel_fetch(start_date, end_date, sort)
        sorted1, sorted2 = reorder_data(data1, data2)

        rows, index = [], []
        for i, mod_data in enumerate(sorted1):
            if not mod_data or not mod_data.get("deviceStatistics"):
                continue

            system_name = mod_data["systemName"]
            count = mod_data["validObjectCount"]
            stats = mod_data["deviceStatistics"]
            device_count = len(stats)

            reads = [s["statistics"][-1]["conditionCount"] for s in stats]
            rates = [count] + [s["statistics"][-1]["readRate"] for s in stats]

            mapped_reads, mapped_rates = map_read_rates(device_count, reads, rates)
            tote_stat = sorted2[i]["statistics"][-1]["conditionCount"] if sorted2[i] else "N/A"

            rows.extend([mapped_reads, mapped_rates, ["", tote_stat] + [""] * (len(mapped_rates) - 2)])
            index.extend([system_name, "", "Totes"])

        if rows:
            df = pd.DataFrame(rows, index=index, columns=[" Volume ", SORTS[sort][2] + " - " + start_date] + [str(i) for i in range(1, len(rows[0])-1)])
            all_dfs.append((SORTS[sort][2], df))

    if all_dfs:
        output_path = "ScanData.xlsx"
        with pd.ExcelWriter(output_path) as writer:
            for sheet_name, df in all_dfs:
                df.to_excel(writer, sheet_name=sheet_name)
        print(f"Data written to {output_path}")
    else:
        print("No valid data to write.")

if __name__ == "__main__":
    main()
