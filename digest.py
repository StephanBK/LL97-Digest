"""
INOVUES LL97 Weekly Deed Transfer Digest
-----------------------------------------
Every Monday:
1. Pulls deed transfers from the Infotool weekly feed (past 7 days)
2. For each transfer, queries PLUTO via Infotool to get the address
3. Fuzzy-matches that address against the LL97 dataset
4. Sends an HTML email with matched buildings ranked by fine exposure
"""

import os
import re
import json
import smtplib
import requests
import pandas as pd
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────────────────────────────
INFOTOOL_URL   = os.getenv("INFOTOOL_URL", "https://web-production-84068.up.railway.app")
GMAIL_USER     = os.getenv("GMAIL_USER", "stephanketterermba@gmail.com")
GMAIL_APP_PASS = os.getenv("GMAIL_APP_PASS", "vbkn rrmk kaet jnzk")
RECIPIENT      = os.getenv("RECIPIENT", "sketterer@inovues.com")
LL97_PATH      = os.getenv("LL97_PATH", "data/LL97_cleaned_reduced_columns.csv")
TRACKER_PATH   = os.getenv("TRACKER_PATH", "sent_tracker.json")
LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "45"))  # 45 to cover ACRIS lag

# ── ADDRESS NORMALIZATION ─────────────────────────────────────────────────────
ABBREV = {
    r'\bST\.?\b': 'STREET', r'\bAVE?\.?\b': 'AVENUE', r'\bBLVD\.?\b': 'BOULEVARD',
    r'\bDR\.?\b': 'DRIVE',  r'\bPL\.?\b': 'PLACE',    r'\bRD\.?\b': 'ROAD',
    r'\bLN\.?\b': 'LANE',   r'\bCT\.?\b': 'COURT',    r'\bPKWY\.?\b': 'PARKWAY',
    r'\bHWY\.?\b': 'HIGHWAY',r'\bTER\.?\b': 'TERRACE', r'\bCIR\.?\b': 'CIRCLE',
    r'\bW\b': 'WEST',       r'\bE\b': 'EAST',
    r'\bN\b': 'NORTH',      r'\bS\b': 'SOUTH',
}

def normalize(addr):
    if not addr:
        return ""
    s = str(addr).upper().strip()
    # Remove ordinal suffixes from street numbers: 81ST -> 81, 42ND -> 42, 33RD -> 33, 14TH -> 14
    s = re.sub(r'(\d+)(ST|ND|RD|TH)\b', r'\1', s)
    # Expand abbreviations
    for pattern, replacement in ABBREV.items():
        s = re.sub(pattern, replacement, s)
    # Remove punctuation except hyphens (needed for Queens addresses like 41-34)
    s = re.sub(r'[^\w\s\-]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# ── LOAD LL97 ─────────────────────────────────────────────────────────────────
def load_ll97():
    df = pd.read_csv(LL97_PATH)
    # Exclude multifamily only
    df = df[df['Primary Property Type - Self Selected'] != 'Multifamily Housing'].copy()
    df['addr_norm'] = df['Address 1'].apply(normalize)
    lookup = {}
    for _, row in df.iterrows():
        key = row['addr_norm']
        if key and key not in lookup:
            lookup[key] = row.to_dict()
    print(f"LL97 loaded: {len(df)} non-multifamily buildings, {len(lookup)} unique addresses")
    return lookup


# ── TRACKER ───────────────────────────────────────────────────────────────────
def load_tracker():
    if Path(TRACKER_PATH).exists():
        with open(TRACKER_PATH) as f:
            return set(json.load(f).get("sent_bbls", []))
    return set()

def save_tracker(sent_bbls):
    with open(TRACKER_PATH, "w") as f:
        json.dump({"sent_bbls": list(sent_bbls), "last_run": datetime.now(timezone.utc).isoformat()}, f, indent=2)


# ── FETCH WEEKLY FEED ─────────────────────────────────────────────────────────
def fetch_feed():
    print(f"Fetching weekly feed (last {LOOKBACK_DAYS} days)...")
    r = requests.get(f"{INFOTOOL_URL}/api/weekly-feed",
                     params={"days": LOOKBACK_DAYS}, timeout=60)
    r.raise_for_status()
    data = r.json()
    txns = data.get("transactions", [])
    print(f"Feed returned {len(txns)} deed transfers")
    return txns


# ── RESOLVE BBL → PLUTO ADDRESS ───────────────────────────────────────────────
def resolve_bbl(bbl):
    """Query Infotool to get PLUTO address and building info for a BBL.
    For condo units (lot >= 1000), also try the base lot (lot 0001)."""
    def _lookup(b):
        try:
            r = requests.get(f"{INFOTOOL_URL}/api/lookup",
                             params={"bbl": b}, timeout=25)
            if r.status_code == 200:
                data = r.json()
                building = data.get("building_info", {})
                if building.get("address"):
                    return {
                        "address": building.get("address", ""),
                        "bldgclass": building.get("bldgclass", ""),
                        "yearbuilt": building.get("yearbuilt", ""),
                        "numfloors": building.get("numfloors", ""),
                        "bldgarea": building.get("bldgarea", ""),
                        "zipcode": building.get("zipcode", ""),
                    }
        except Exception as e:
            print(f"  BBL {b} lookup error: {e}")
        return {}

    result = _lookup(bbl)
    if result:
        return result

    # If no address, try base lot (for condo units lot >= 1000)
    if len(bbl) == 10:
        lot = int(bbl[6:])
        if lot >= 1000:
            base_bbl = bbl[:6] + "0001"
            result = _lookup(base_bbl)
            if result:
                return result

    return {}


# ── MATCH TRANSACTIONS AGAINST LL97 ──────────────────────────────────────────
def match_transactions(transactions, ll97_lookup, sent_bbls):
    """
    For each transaction:
    1. Skip if BBL already sent
    2. Resolve BBL -> PLUTO address
    3. Try to match against LL97 lookup
    4. If matched, enrich and return
    """
    matched = []
    seen_bbls = set()

    for txn in transactions:
        bbl = txn.get("bbl", "")
        if not bbl or bbl in seen_bbls:
            continue
        seen_bbls.add(bbl)

        if bbl in sent_bbls:
            print(f"  BBL {bbl}: already sent, skipping")
            continue

        print(f"  Resolving BBL {bbl}...", end=" ")
        pluto = resolve_bbl(bbl)
        pluto_addr = pluto.get("address", "")
        norm = normalize(pluto_addr)

        if not norm:
            print("no PLUTO address")
            continue

        ll97_row = ll97_lookup.get(norm)
        if ll97_row is None:
            print(f"no LL97 match ({pluto_addr})")
            continue

        print(f"MATCH → {pluto_addr} | {ll97_row['Primary Property Type - Self Selected']}")
        matched.append({
            "bbl": bbl,
            "address": pluto_addr or txn.get("buyer_address", ""),
            "borough": txn.get("borough_name", ""),
            "buyer": txn.get("buyer", ""),
            "buyer_address": txn.get("buyer_address", ""),
            "seller": txn.get("seller", ""),
            "sale_amount": txn.get("sale_amount", ""),
            "transaction_type": txn.get("transaction_type", ""),
            "recorded_date": (txn.get("recorded_date") or "")[:10],
            "doc_type": txn.get("doc_type", ""),
            "acris_url": txn.get("acris_url", ""),
            # PLUTO data
            "bldgclass": pluto.get("bldgclass", ""),
            "yearbuilt": pluto.get("yearbuilt", ""),
            "numfloors": pluto.get("numfloors", ""),
            "bldgarea": pluto.get("bldgarea", ""),
            "zipcode": pluto.get("zipcode", ""),
            # LL97 data
            "property_name": ll97_row.get("Property Name", ""),
            "property_type": ll97_row.get("Primary Property Type - Self Selected", ""),
            "year_built_ll97": ll97_row.get("Year Built", ""),
            "energy_star": ll97_row.get("ENERGY STAR Score", ""),
            "site_eui": ll97_row.get("Site EUI (kBtu/ft²)", ""),
            "ghg": ll97_row.get("Total (Location-Based) GHG Emissions (Metric Tons CO2e)", ""),
            "gfa": ll97_row.get("Property GFA - Self-Reported (ft²)", ""),
            "fine_2024": ll97_row.get("Fine_2024-2029", 0),
            "fine_2030": ll97_row.get("Fine_2030-2034", 0),
            "fine_2035": ll97_row.get("Fine_2035-2039", 0),
        })

    # Rank by fine_2024 desc, then fine_2030 desc
    matched.sort(key=lambda x: (-(x['fine_2024'] or 0), -(x['fine_2030'] or 0)))
    return matched


# ── FORMAT CURRENCY ───────────────────────────────────────────────────────────
def fmt_money(val):
    try:
        v = float(val)
        if v >= 1_000_000:
            return f"${v/1_000_000:.1f}M"
        elif v >= 1_000:
            return f"${v/1_000:.0f}K"
        elif v > 0:
            return f"${v:,.0f}"
        else:
            return "—"
    except:
        return "—"

def fmt_num(val, suffix=""):
    try:
        v = float(val)
        return f"{v:,.0f}{suffix}"
    except:
        return "—"


# ── BUILD HTML EMAIL ──────────────────────────────────────────────────────────
def build_email(matched, week_str):
    n = len(matched)
    subject = f"LL97 Weekly Deed Digest — {n} Match{'es' if n != 1 else ''} — {week_str}"

    # Fine badge color
    def fine_color(val):
        try:
            v = float(val)
            if v > 500_000:  return "#c0392b"   # red — big fine
            elif v > 50_000: return "#e67e22"   # orange
            elif v > 0:      return "#f39c12"   # yellow
            else:            return "#27ae60"   # green — no fine yet
        except:
            return "#95a5a6"

    cards = ""
    for i, b in enumerate(matched, 1):
        fc_now  = fine_color(b['fine_2024'])
        fc_2030 = fine_color(b['fine_2030'])
        has_fine_now = float(b['fine_2024'] or 0) > 0

        fine_badge = f"""
        <div style="display:inline-block;background:{fc_now};color:#fff;
             font-size:11px;font-weight:700;padding:3px 10px;border-radius:3px;
             letter-spacing:0.5px;margin-bottom:8px;">
          {'🔥 FINE NOW' if has_fine_now else '⚠ FINE 2030+'}
        </div>""" if (float(b['fine_2024'] or 0) > 0 or float(b['fine_2030'] or 0) > 0) else ""

        google_maps_url = f"https://www.google.com/maps/search/?api=1&query={requests.utils.quote(b['address'] + ' New York NY')}"
        acris_link = f'<a href="{b["acris_url"]}" style="color:#2980b9;text-decoration:none;">View deed →</a>' if b.get("acris_url") else ""

        cards += f"""
        <div style="background:#fff;border:1px solid #dde4ea;border-radius:8px;
             margin-bottom:20px;overflow:hidden;">

          <!-- Card Header -->
          <div style="background:#1a2e4a;padding:14px 20px;">
            <div style="color:#8faec8;font-size:11px;font-weight:700;
                 letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px;">
              #{i} · {b['property_type']} · {b['borough']}
            </div>
            <div style="color:#fff;font-size:17px;font-weight:700;line-height:1.3;">
              {b['address']}{(', ' + b['zipcode']) if b['zipcode'] else ''}
            </div>
            {f'<div style="color:#8faec8;font-size:12px;margin-top:4px;">{b["property_name"]}</div>' if b['property_name'] and b['property_name'] != b['address'] else ''}
          </div>

          <!-- Card Body -->
          <div style="padding:16px 20px;">
            {fine_badge}

            <!-- Fine exposure row -->
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="margin-bottom:14px;border-collapse:collapse;">
              <tr>
                <td width="33%" style="text-align:center;padding:10px 6px;
                    border:1px solid #eaeef2;border-radius:4px 0 0 4px;background:#f8fafc;">
                  <div style="font-size:10px;color:#7f8c8d;font-weight:600;
                       text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">
                    Fine 2024–2029
                  </div>
                  <div style="font-size:18px;font-weight:700;color:{fc_now};">
                    {fmt_money(b['fine_2024'])}
                  </div>
                </td>
                <td width="33%" style="text-align:center;padding:10px 6px;
                    border:1px solid #eaeef2;border-top:1px solid #eaeef2;
                    border-bottom:1px solid #eaeef2;background:#f8fafc;">
                  <div style="font-size:10px;color:#7f8c8d;font-weight:600;
                       text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">
                    Fine 2030–2034
                  </div>
                  <div style="font-size:18px;font-weight:700;color:{fc_2030};">
                    {fmt_money(b['fine_2030'])}
                  </div>
                </td>
                <td width="33%" style="text-align:center;padding:10px 6px;
                    border:1px solid #eaeef2;border-radius:0 4px 4px 0;background:#f8fafc;">
                  <div style="font-size:10px;color:#7f8c8d;font-weight:600;
                       text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">
                    Fine 2035–2039
                  </div>
                  <div style="font-size:18px;font-weight:700;color:{fine_color(b['fine_2035'])};">
                    {fmt_money(b['fine_2035'])}
                  </div>
                </td>
              </tr>
            </table>

            <!-- Building stats -->
            <table width="100%" cellpadding="0" cellspacing="4"
                   style="margin-bottom:14px;font-size:12px;">
              <tr>
                <td style="color:#7f8c8d;width:140px;">Building Class</td>
                <td style="color:#2c3e50;font-weight:600;">{b['bldgclass'] or '—'}</td>
                <td style="color:#7f8c8d;width:140px;">Year Built</td>
                <td style="color:#2c3e50;font-weight:600;">{b['yearbuilt'] or b['year_built_ll97'] or '—'}</td>
              </tr>
              <tr>
                <td style="color:#7f8c8d;">Floor Area (sqft)</td>
                <td style="color:#2c3e50;font-weight:600;">{fmt_num(b['gfa'] or b['bldgarea'])}</td>
                <td style="color:#7f8c8d;">Floors</td>
                <td style="color:#2c3e50;font-weight:600;">{b['numfloors'] or '—'}</td>
              </tr>
              <tr>
                <td style="color:#7f8c8d;">ENERGY STAR</td>
                <td style="color:#2c3e50;font-weight:600;">{b['energy_star'] or '—'}</td>
                <td style="color:#7f8c8d;">Site EUI</td>
                <td style="color:#2c3e50;font-weight:600;">{fmt_num(b['site_eui'], ' kBtu/ft²')}</td>
              </tr>
            </table>

            <!-- Deed info -->
            <div style="background:#f0f4f8;border-radius:6px;padding:12px 14px;
                 font-size:12px;margin-bottom:10px;">
              <div style="font-weight:700;color:#1a2e4a;margin-bottom:8px;">
                📋 Deed Transfer — {b['recorded_date']} — {b['transaction_type']}
                &nbsp;&nbsp;{acris_link}
              </div>
              <table width="100%" cellpadding="0" cellspacing="4">
                <tr>
                  <td style="color:#7f8c8d;width:60px;">Buyer</td>
                  <td style="color:#2c3e50;font-weight:600;">{b['buyer'] or '—'}</td>
                  <td style="color:#7f8c8d;width:80px;">Sale Price</td>
                  <td style="color:#2c3e50;font-weight:600;">{fmt_money(b['sale_amount'])}</td>
                </tr>
                <tr>
                  <td style="color:#7f8c8d;">Seller</td>
                  <td colspan="3" style="color:#2c3e50;">{b['seller'] or '—'}</td>
                </tr>
              </table>
            </div>

            <!-- Action link -->
            <div style="text-align:right;">
              <a href="{google_maps_url}"
                 style="font-size:11px;color:#7f8c8d;text-decoration:none;">
                📍 View on Google Maps →
              </a>
            </div>
          </div>
        </div>"""

    no_matches_msg = """
    <div style="text-align:center;padding:40px;color:#7f8c8d;">
      <div style="font-size:36px;margin-bottom:12px;">🔍</div>
      <div style="font-size:16px;font-weight:600;color:#2c3e50;margin-bottom:8px;">
        No LL97 matches this week
      </div>
      <div style="font-size:13px;">
        No deed transfers this week matched buildings in the LL97 dataset.
        Check back next Monday.
      </div>
    </div>""" if not matched else ""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#eef2f7;font-family:Arial,sans-serif;">
<div style="max-width:640px;margin:0 auto;padding:20px 0 40px;">

  <!-- Header -->
  <div style="background:#1a2e4a;border-radius:8px 8px 0 0;padding:28px 32px 24px;">
    <div style="color:#4ecdc4;font-size:11px;font-weight:700;letter-spacing:2px;
         text-transform:uppercase;margin-bottom:8px;">INOVUES · Internal Prospecting</div>
    <div style="color:#fff;font-size:22px;font-weight:700;margin-bottom:6px;">
      LL97 Weekly Deed Digest
    </div>
    <div style="color:#8faec8;font-size:13px;">
      Week of {week_str} &nbsp;·&nbsp;
      {n} LL97 match{'es' if n != 1 else ''} found &nbsp;·&nbsp;
      Ranked by fine exposure
    </div>
  </div>

  <!-- Body -->
  <div style="background:#f4f7fb;padding:20px 24px;">
    {'<div style="background:#fff3cd;border:1px solid #ffc107;border-radius:6px;padding:12px 16px;margin-bottom:20px;font-size:13px;color:#856404;"><strong>Note:</strong> These buildings just changed hands — perfect timing to reach the new owner about LL97 compliance.</div>' if matched else ''}
    {cards}
    {no_matches_msg}
  </div>

  <!-- Footer -->
  <div style="background:#1a2e4a;border-radius:0 0 8px 8px;padding:16px 32px;
       text-align:center;color:#5a7a9a;font-size:11px;">
    INOVUES Internal Use Only &nbsp;·&nbsp;
    Data: NYC ACRIS + MapPLUTO + LL97 Benchmarking &nbsp;·&nbsp;
    <a href="https://web-production-84068.up.railway.app" style="color:#4ecdc4;text-decoration:none;">
      Open Ownership Tool →
    </a>
  </div>

</div>
</body>
</html>"""

    return subject, html


# ── SEND EMAIL ────────────────────────────────────────────────────────────────
def send_email(subject, html_body):
    import socket
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_USER
    msg["To"]      = RECIPIENT
    msg.attach(MIMEText(html_body, "html"))

    # Force IPv4 — Railway sometimes fails on IPv6 for smtp.gmail.com
    addr_info = socket.getaddrinfo("smtp.gmail.com", 465, socket.AF_INET, socket.SOCK_STREAM)
    if not addr_info:
        raise Exception("Could not resolve smtp.gmail.com to IPv4")
    ipv4_addr = addr_info[0][4][0]
    print(f"Connecting to smtp.gmail.com via IPv4: {ipv4_addr}")

    with smtplib.SMTP_SSL(ipv4_addr, 465) as server:
        server.ehlo("localhost")
        server.login(GMAIL_USER, GMAIL_APP_PASS)
        server.sendmail(GMAIL_USER, RECIPIENT, msg.as_string())
    print(f"Email sent to {RECIPIENT}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"INOVUES LL97 Digest — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    ll97_lookup = load_ll97()
    sent_bbls   = load_tracker()
    transactions = fetch_feed()

    print(f"\nMatching {len(transactions)} transfers against LL97...")
    matched = match_transactions(transactions, ll97_lookup, sent_bbls)

    week_str = datetime.now(timezone.utc).strftime("%b %d, %Y")
    subject, html = build_email(matched, week_str)

    print(f"\nFound {len(matched)} matches. Building email...")
    send_email(subject, html)

    # Update tracker
    new_sent = sent_bbls | {b["bbl"] for b in matched}
    save_tracker(new_sent)
    print(f"Tracker updated: {len(new_sent)} BBLs total")
    print("Done.")


if __name__ == "__main__":
    main()
