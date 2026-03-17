# INOVUES LL97 Weekly Deed Digest

Internal prospecting tool. Every Monday, pulls NYC deed transfers from ACRIS,
matches against the LL97 benchmarking dataset, and emails matched buildings
(ranked by fine exposure) to the INOVUES team.

## How it works
1. Calls Infotool `/api/weekly-feed` to get recent deed transfers
2. For each BBL, calls Infotool `/api/lookup` to get the PLUTO address
3. Fuzzy-matches that address against the LL97 dataset
4. Sends an HTML digest email ranked by LL97 fine exposure

## Environment Variables (set in Railway)
| Variable | Value |
|---|---|
| `INFOTOOL_URL` | `https://web-production-84068.up.railway.app` |
| `GMAIL_USER` | `stephanketterermba@gmail.com` |
| `GMAIL_APP_PASS` | Gmail App Password |
| `RECIPIENT` | `sketterer@inovues.com` |
| `LOOKBACK_DAYS` | `45` (covers ACRIS lag) |

## Schedule
Every Monday at 8:00 AM EST (`0 13 * * 1` UTC)

## Data
- `data/LL97_cleaned_reduced_columns.csv` — LL97 benchmarking dataset
- `sent_tracker.json` — auto-created, tracks which BBLs have been sent
