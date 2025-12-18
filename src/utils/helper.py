from collections import defaultdict
import re

UUID4_REGEX = re.compile(
    r"\[([0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12})\]"
)

def filter_uuid(logs):
    uuid_map = defaultdict(list)
    for log_line in logs:
        uuids = UUID4_REGEX.findall(log_line['message'])
        for uuid in uuids:
            uuid_map[uuid].append(log_line)
    return uuid_map

def extract_uuids_from_logs(logs):
    uuids = set()
    for log_line in logs:
        found_uuids = UUID4_REGEX.findall(log_line['message'])
        uuids.update(found_uuids)
    return sorted(list(uuids))