from src.utils.helper import filter_uuid
from src.logs import fetch_logs

logs = fetch_logs(hours=1, limit=10, page=1, page_size=50)
print(logs)
uuid_map = filter_uuid(logs)
print(uuid_map)