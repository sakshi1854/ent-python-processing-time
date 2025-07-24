import os
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from urllib.parse import quote
import json
import re

# ------------------------
#  SAP Credentials & URLs (via environment variables)
# ------------------------
SAP_USERNAME = os.environ.get("SAP_USERNAME")
SAP_PASSWORD = os.environ.get("SAP_PASSWORD")
SAP_BASE_URL = os.environ.get("SAP_BASE_URL")

IFLOW_URL = os.environ.get("IFLOW_URL")
IFLOW_USERNAME = os.environ.get("IFLOW_USERNAME")
IFLOW_PASSWORD = os.environ.get("IFLOW_PASSWORD")

# ------------------------
#  Validate all required env vars
# ------------------------
if not all([SAP_USERNAME, SAP_PASSWORD, SAP_BASE_URL, IFLOW_URL, IFLOW_USERNAME, IFLOW_PASSWORD]):
    raise RuntimeError("❌ One or more required environment variables are missing.")

# ------------------------
#  Time range: past 24 hours (UTC)
# ------------------------
end_time = datetime.utcnow()
start_time = end_time - timedelta(days=1)
start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")

# ------------------------
#  OData Query Setup
# ------------------------
filter_str = f"LogStart ge datetime'{start_str}' and LogEnd le datetime'{end_str}'"
encoded_filter = quote(filter_str)
initial_url = f"{SAP_BASE_URL}/MessageProcessingLogs?$format=json&$select=IntegrationFlowName,LogStart,LogEnd,MessageGuid&$filter={encoded_filter}"

# ------------------------
#  Fetch Data via Pagination
# ------------------------
all_results = []
next_url = initial_url

while next_url:
    print(f"📡 Requesting: {next_url}")
    response = requests.get(next_url, auth=HTTPBasicAuth(SAP_USERNAME, SAP_PASSWORD))

    if response.status_code == 200:
        data = response.json()
        results = data.get("d", {}).get("results", [])
        all_results.extend(results)

        next_link = data.get("d", {}).get("__next")
        if next_link:
            next_url = next_link if next_link.startswith("http") else f"{SAP_BASE_URL}/{next_link}"
        else:
            next_url = None
    else:
        print(f"❌ Request failed with status code: {response.status_code}")
        print(response.text)
        break

print(f"\n✅ All records collected: {len(all_results)}")

# ------------------------
#  Duration Calculation
# ------------------------
def parse_log_date(date_str):
    match = re.search(r'/Date\((\d+)\)/', date_str)
    return int(match.group(1)) if match else None

duration_records = []
for entry in all_results:
    start_ms = parse_log_date(entry["LogStart"])
    end_ms = parse_log_date(entry["LogEnd"])
    if start_ms is None or end_ms is None:
        continue
    duration = end_ms - start_ms
    duration_records.append({
        "IntegrationFlowName": entry["IntegrationFlowName"],
        "MessageGuid": entry["MessageGuid"],
        "DurationMs": duration,
        "LogStart": start_ms,
        "LogEnd": end_ms
    })

# ------------------------
#  Max Duration per iFlow
# ------------------------
max_durations = {}
for record in duration_records:
    name = record["IntegrationFlowName"]
    if name not in max_durations or record["DurationMs"] > max_durations[name]["DurationMs"]:
        max_durations[name] = record

# ------------------------
#  Top 5 iFlows by Duration
# ------------------------
top_5 = sorted(max_durations.values(), key=lambda x: x["DurationMs"], reverse=True)[:5]

print("\n📊 Top 5 Integration Flows by Max Processing Time:")
for idx, entry in enumerate(top_5, 1):
    print(f"\n#{idx}")
    print(f"Integration Flow: {entry['IntegrationFlowName']}")
    print(f"Message GUID    : {entry['MessageGuid']}")
    print(f"Duration (ms)   : {entry['DurationMs']}")

# ------------------------
#  Send to CPI iFlow Endpoint
# ------------------------
payload = {
    "Top5IflowsByDuration": top_5
}

print(f"\n🚀 Sending data to CPI iFlow: {IFLOW_URL}")
post_response = requests.post(
    url=IFLOW_URL,
    auth=HTTPBasicAuth(IFLOW_USERNAME, IFLOW_PASSWORD),
    headers={"Content-Type": "application/json"},
    data=json.dumps(payload)
)

if post_response.status_code in [200, 201, 202]:
    print(f"\n✅ Successfully sent to CPI iFlow. Status Code: {post_response.status_code}")
    print(post_response.text)
else:
    print(f"\n❌ Failed to send to CPI iFlow. Status Code: {post_response.status_code}")
    print(post_response.text)
