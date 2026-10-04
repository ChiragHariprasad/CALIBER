# CALIBRE — EDA Reproduction

This mini-project reproduces the EDA behind the CALIBRE PPT using the public
OpenF1 historical API.

## Source

OpenF1 historical data is available from 2023 onward and is free to access.
CALIBRE uses it as replayable historical input.

## Exact workflow

### 1. Install dependencies

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
# source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Download Monaco data

```bash
python 01_download_monaco.py
```

This discovers the sessions for meeting key 1286 and downloads the core
endpoints. It also downloads supporting race-session datasets.

### 3. Recompute the EDA

```bash
python 02_eda_metrics.py
```

This prints:

- pit-lane record counts and medians
- track-limit deletion counts and corner concentration
- pit-speeding event-to-notice latency

It also writes CSV files under `data/processed/`.

### 4. Generate the PPT charts

```bash
python 03_make_eda_charts.py
```

Charts are saved under `figures/`.

## Important interpretation

Some CALIBRE report numbers come from public OpenF1 data, while others are
from the CALIBRE simulation/timing-system twin.

OpenF1 does NOT publish the raw Official Timing System loop-crossing data or
the configured loop distance. Therefore:

- `86` race pit records = real OpenF1 data
- `198` track-limit deletions = derived from race-control data
- `5.8–6.6 min` penalty latency = derived by joining public pit + race-control data
- `423 simulated passages` = CALIBRE simulation, NOT a raw OpenF1 count
- `2692 cm`, `2615 cm`, +2.9% timing bias = case/simulation parameters, NOT
  fields returned by the OpenF1 API

The report says one of six race pit-speeding penalties cannot be tied to a
single passage from public data, so the latency chart intentionally shows
only valid public-data matches.

## Reproducibility note

The CALIBRE report states that the Monaco data was pulled on 30 September
2026. If OpenF1 later corrects historical records, your computed values can
differ slightly. Keep the downloaded raw JSON beside your presentation/code
as the experiment snapshot.

## What this proves in a viva

You can show:

OpenF1 API
  -> raw JSON cache
  -> cleaning/classification
  -> derived EDA metrics
  -> matplotlib figures
  -> PPT

That is a materially stronger demonstration than presenting static numbers.
