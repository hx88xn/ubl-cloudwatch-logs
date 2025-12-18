# from collections import defaultdict
# import re

# UUID4_REGEX = re.compile(
#     r"\b[0-9a-fA-F]{8}-"
#     r"[0-9a-fA-F]{4}-"
#     r"4[0-9a-fA-F]{3}-"
#     r"[89abAB][0-9a-fA-F]{3}-"
#     r"[0-9a-fA-F]{12}\b"
# )

# def filter_uuid(logs):
#     uuid_map = defaultdict(list)
#     for log_line in logs:
#         uuids = UUID4_REGEX.findall(log_line)
#         for uuid in uuids:
#             uuid_map[uuid].append(log_line)
#     return uuid_map