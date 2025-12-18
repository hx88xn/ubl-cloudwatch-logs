from src.utils.helper import filter_uuid, extract_uuids_from_logs
from src.logs import fetch_logs

logs = fetch_logs(hours=1, limit=100, page=1, page_size=50)

print(logs)