from src.utils.helper import filter_uuid, extract_uuids_from_logs
from src.logs import fetch_logs

logs = fetch_logs(hours=2, limit=1000, page=1, page_size=50)

for i in logs['logs']:
    print(i['message'])