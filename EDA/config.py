from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
FIG_DIR = BASE_DIR / "figures"

API_BASE = "https://api.openf1.org/v1"

# 2026 Monaco GP, as used in the CALIBRE report.
MEETING_KEY = 1286
RACE_SESSION_KEY = 11299

# Reported Monaco session keys. The downloader also discovers these
# dynamically from the OpenF1 sessions endpoint.
REPORTED_SESSION_KEYS = [11292, 11293, 11294, 11295, 11299]

# For the EDA in the report:
RED_FLAG_HOLD_THRESHOLD_S = 1000
RESTART_THRESHOLD_S = 50

# Race-state windows used in the report's pit-lane EDA.
# The race had SC periods on laps 60-65 and 66-68; the report groups
# pit passages as laps 58-61 and 65-66 around those periods.
SC1_LAPS = range(58, 62)
SC2_LAPS = range(65, 67)

# The report identifies these drive-throughs.
DRIVE_THROUGH_LAPS = {9, 72}

for p in [RAW_DIR, PROCESSED_DIR, FIG_DIR]:
    p.mkdir(parents=True, exist_ok=True)
