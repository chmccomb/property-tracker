"""
Gmail → MLS Listing Ingestion
-------------------------------
Polls a Gmail inbox for Paragon MLS alert emails and synthesises rows in the
same format as a Paragon CSV export (Testing.csv).  New rows are appended to
data/raw/mls_incremental.csv, which you can then feed to refresh.py.

Architecture:
    Gmail (Paragon MLS alerts)
        → gmail_ingest.py   [this script]
        → data/raw/mls_incremental.csv
        → python refresh.py --csv data/raw/mls_incremental.csv

Prerequisites:
    pip install google-auth google-auth-oauthlib google-api-python-client beautifulsoup4

One-time setup:
    1. Go to https://console.cloud.google.com
    2. Create a project → Enable "Gmail API"
    3. Credentials → Create OAuth 2.0 Client ID → Desktop app
    4. Download the JSON and save as  credentials/gmail_credentials.json
    5. Run this script once to authorize — a browser window will open.
       The token is saved to  credentials/gmail_token.json  for future runs.

Usage:
    python scripts/gmail_ingest.py                 # check inbox, append new rows
    python scripts/gmail_ingest.py --since 7       # only look at emails from last 7 days
    python scripts/gmail_ingest.py --label Paragon # use a specific Gmail label
    python scripts/gmail_ingest.py --dry-run       # parse emails but don't write CSV

IMPORTANT — Paragon email format:
    The HTML parser below (parse_paragon_email) is written for the typical
    Paragon MLS alert format as of 2024.  If your Paragon portal uses a
    custom template the field selectors may need adjustment.

    To debug: run with --dry-run and inspect the printed field extraction.
    Forward a sample alert to yourself and check the raw HTML with:
        python scripts/gmail_ingest.py --dump-sample "Subject line of email"
"""

import argparse
import base64
import csv
import json
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT         = Path(__file__).resolve().parent.parent
CREDS_DIR    = ROOT / "credentials"
CREDS_FILE   = CREDS_DIR / "gmail_credentials.json"
TOKEN_FILE   = CREDS_DIR / "gmail_token.json"
OUTPUT_CSV   = ROOT / "data" / "raw" / "mls_incremental.csv"

# Gmail search query — adjust label/sender to match your Paragon alert setup
DEFAULT_LABEL  = "INBOX"
PARAGON_SENDER = "email@paragonmessaging.com"  # Paragon MLS alert sender

# Scopes (read-only is sufficient)
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# ── CSV field names matching Paragon export (Testing.csv columns) ────────────
# Fields we can extract from email; the rest will be blank (pipeline handles None)
FIELDNAMES = [
    "MLS #", "Address", "Unit Number", "Zip", "Area", "Block", "Lot",
    "Complex Name", "Type", "Class", "Status",
    "Bedrooms", "Total # Full Baths", "Total # Half Baths", "Approx Sq Ft",
    "Asking Price", "Sold Price", "Original List Price",
    "Closing Date", "Listing Date", "Days On Market",
    "Monthly Maintenance Fee", "Taxes", "Assessed Value",
    "Year Built", "Parking", "Outdoor Space", "Floor Number",
    "Short Sale (Y/N)", "Bank Owned Y/N",
    "Y Coordinates", "X Coordinates",
    "_source",   # internal: "gmail_alert" — stripped before pipeline
    "_email_id", # internal: Gmail message ID for deduplication
]

BLANK_ROW = {f: "" for f in FIELDNAMES}


# ── Gmail authentication ──────────────────────────────────────────────────────

def get_gmail_service():
    """Authenticate with Gmail API, caching token in credentials/gmail_token.json."""
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError:
        print("ERROR: Gmail API libraries not installed.")
        print("  Run:  pip install google-auth google-auth-oauthlib google-api-python-client")
        sys.exit(1)

    if not CREDS_FILE.exists():
        print(f"ERROR: Credentials file not found at {CREDS_FILE}")
        print()
        print("Setup instructions:")
        print("  1. Go to https://console.cloud.google.com")
        print("  2. Create project → Enable 'Gmail API'")
        print("  3. Credentials → OAuth 2.0 Client ID → Desktop app → Download JSON")
        print(f"  4. Save the file as {CREDS_FILE}")
        sys.exit(1)

    CREDS_DIR.mkdir(exist_ok=True)
    creds = None

    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_FILE.write_text(creds.to_json())
        print(f"Token saved to {TOKEN_FILE}")

    return build("gmail", "v1", credentials=creds)


# ── Email fetching ────────────────────────────────────────────────────────────

def search_emails(service, since_days: int, label: str) -> list[str]:
    """Return list of message IDs matching Paragon alerts."""
    since_date = (date.today() - timedelta(days=since_days)).strftime("%Y/%m/%d")
    query = f"from:{PARAGON_SENDER} after:{since_date}"
    if label and label != "INBOX":
        query += f" label:{label}"

    print(f"Gmail query: {query}")
    result = service.users().messages().list(userId="me", q=query, maxResults=500).execute()
    messages = result.get("messages", [])
    print(f"Found {len(messages)} matching emails")
    return [m["id"] for m in messages]


def get_email_body(service, msg_id: str) -> tuple[str, str]:
    """Return (subject, html_body) for a message."""
    msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
    subject = ""
    for header in msg["payload"].get("headers", []):
        if header["name"] == "Subject":
            subject = header["value"]
            break

    html_body = _extract_html(msg["payload"])
    return subject, html_body


def _extract_html(payload: dict) -> str:
    """Recursively extract HTML body from Gmail message payload."""
    mime_type = payload.get("mimeType", "")
    body_data = payload.get("body", {}).get("data", "")

    if mime_type == "text/html" and body_data:
        return base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")

    for part in payload.get("parts", []):
        result = _extract_html(part)
        if result:
            return result
    return ""


# ── Paragon email parser ──────────────────────────────────────────────────────

def parse_paragon_email(subject: str, html: str) -> list[dict]:
    """
    Parse a Paragon MLS alert email and return a list of property dicts.

    Uses <span id="displayMlsNum"> as the anchor for each listing, then
    walks up ancestors to find the full listing container (the <td> that
    also includes the status badge and all field labels).
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("ERROR: beautifulsoup4 not installed.  Run: pip install beautifulsoup4")
        sys.exit(1)

    soup = BeautifulSoup(html, "html.parser")
    properties = []

    STATUS_KEYWORDS = {"active", "sold", "under contract", "pending", "expired", "withdrawn"}

    def listing_container(span_el):
        """
        Walk up from the <span id="displayMlsNum"> until we find a <td>
        ancestor whose text contains a known status keyword.  This is the
        full per-listing block including status, address, price, and fields.
        """
        node = span_el
        for _ in range(25):
            node = node.parent
            if node is None:
                break
            if node.name == "td":
                t = node.get_text(separator=" ", strip=True).lower()
                if any(kw in t for kw in STATUS_KEYWORDS):
                    return node
        return None

    for mls_span in soup.find_all("span", id="displayMlsNum"):
        mls_num = mls_span.get_text(strip=True)
        if not re.match(r"^\d{7,10}$", mls_num):
            continue

        block = listing_container(mls_span)
        if block is None:
            continue

        # Use pipe-separated text for reliable field extraction
        block_text = block.get_text(separator="|", strip=True)
        tokens = [t.strip() for t in block_text.split("|") if t.strip()]

        row = dict(BLANK_ROW)
        row["_source"] = "gmail_alert"
        row["MLS #"] = mls_num

        # ── Status ────────────────────────────────────────────────────────
        # Paragon format: "Active - ACTIVE" or "New|Active - ACTIVE" etc.
        status_match = re.search(
            r"\b(Active|Sold|Under Contract|Pending|Expired|Withdrawn)\b",
            block_text, re.I)
        row["Status"] = status_match.group(1).upper() if status_match else ""

        # ── Address ───────────────────────────────────────────────────────
        # First token matching "<number> <STREET NAME>"
        for tok in tokens:
            if re.match(r"^\d+\s+[A-Z]", tok) and len(tok) < 80:
                # strip trailing unit if present ("272 TERRACE AVE UNIT 2A")
                addr = re.sub(r"\s+(?:UNIT|APT|#)\s*\S+$", "", tok, flags=re.I).strip()
                row["Address"] = addr
                # Unit: anything after UNIT/APT/#
                unit_m = re.search(r"(?:UNIT|APT|#)\s*(\S+)$", tok, re.I)
                if unit_m:
                    row["Unit Number"] = unit_m.group(1)
                break

        # ── ZIP ───────────────────────────────────────────────────────────
        zip_match = re.search(r"\b(07\d{3})\b", block_text)
        if zip_match:
            row["Zip"] = zip_match.group(1)

        # ── Property type ─────────────────────────────────────────────────
        type_match = re.search(
            r"\b(Condominium|Townhouse|Single.Family|Multi.Family|Co.Op|Land)\b",
            block_text, re.I)
        if type_match:
            row["Type"] = type_match.group(1)

        # ── Price ─────────────────────────────────────────────────────────
        price_match = re.search(r"\$([\d,]+)", block_text)
        if price_match:
            price_str = "$" + price_match.group(1)
            if row["Status"] == "SOLD":
                row["Sold Price"] = price_str
            else:
                row["Asking Price"] = price_str

        # ── Field extraction using label→next-token pattern ───────────────
        # tokens looks like: [..., "Bedrooms:", "3", "Total Bathrooms:", "2", ...]
        for i, tok in enumerate(tokens):
            val = tokens[i + 1] if i + 1 < len(tokens) else ""
            tok_lower = tok.lower().rstrip(":")
            if tok_lower == "bedrooms":
                row["Bedrooms"] = val
            elif tok_lower == "total bathrooms":
                row["Total # Full Baths"] = val
            elif tok_lower in ("approx sq ft", "approx. sq ft", "sq ft"):
                row["Approx Sq Ft"] = val.replace(",", "")
            elif tok_lower == "days on market":
                row["Days On Market"] = val
            elif tok_lower in ("year built",):
                row["Year Built"] = val
            elif tok_lower in ("monthly maintenance", "hoa", "maintenance"):
                row["Monthly Maintenance Fee"] = val

        # ── Closing / listing date ────────────────────────────────────────
        date_matches = re.findall(r"\d{1,2}/\d{1,2}/\d{4}", block_text)
        if date_matches:
            if row["Status"] == "SOLD":
                row["Closing Date"] = date_matches[-1]
            else:
                row["Listing Date"] = date_matches[0]

        properties.append(row)

    return properties


# ── Deduplication ─────────────────────────────────────────────────────────────

def load_seen_ids(output_path: Path) -> set[str]:
    """Return set of _email_id values already written to the output CSV."""
    if not output_path.exists():
        return set()
    seen = set()
    with open(output_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            eid = row.get("_email_id", "").strip()
            if eid:
                seen.add(eid)
    return seen


def load_seen_mls(output_path: Path) -> set[str]:
    """Return set of MLS # values already written to the output CSV."""
    if not output_path.exists():
        return set()
    seen = set()
    with open(output_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mls = row.get("MLS #", "").strip()
            if mls:
                seen.add(mls)
    return seen


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Poll Gmail for Paragon MLS alerts and append rows to mls_incremental.csv"
    )
    parser.add_argument("--since", type=int, default=30,
                        help="Look back N days in Gmail (default: 30)")
    parser.add_argument("--label", default=DEFAULT_LABEL,
                        help="Gmail label to search (default: INBOX)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse emails but do not write CSV")
    parser.add_argument("--dump-sample", metavar="SUBJECT",
                        help="Print raw HTML for an email matching this subject (for debugging)")
    args = parser.parse_args()

    service = get_gmail_service()

    if args.dump_sample:
        msg_ids = search_emails(service, since_days=90, label=args.label)
        for mid in msg_ids:
            subj, html = get_email_body(service, mid)
            if args.dump_sample.lower() in subj.lower():
                print(f"=== {subj} ===\n{html[:5000]}")
                break
        else:
            print(f"No email found with subject containing: {args.dump_sample!r}")
        return

    # ── Fetch and parse ───────────────────────────────────────────────────
    msg_ids = search_emails(service, since_days=args.since, label=args.label)
    if not msg_ids:
        print("No new MLS alerts found.")
        return

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    seen_ids  = load_seen_ids(OUTPUT_CSV)
    seen_mls  = load_seen_mls(OUTPUT_CSV)

    new_rows: list[dict] = []
    for mid in msg_ids:
        subject, html = get_email_body(service, mid)
        if not html:
            continue
        props = parse_paragon_email(subject, html)
        # Deduplicate by MLS # (not email_id) so multi-listing emails don't get
        # partially skipped when only some listings were captured in a prior run.
        fresh = []
        for prop in props:
            mls = prop.get("MLS #", "").strip()
            if mls and mls not in seen_mls:
                prop["_email_id"] = mid
                seen_mls.add(mls)
                fresh.append(prop)
        new_rows.extend(fresh)
        if props:
            print(f"  Parsed {len(props)} listing(s) from: {subject[:60]}"
                  + (f" ({len(fresh)} new)" if fresh else " (all already seen)"))

    print(f"\nNew rows to append: {len(new_rows)}")

    if args.dry_run:
        print("\n[DRY RUN] Sample of parsed fields:")
        for row in new_rows[:3]:
            for k, v in row.items():
                if v:
                    print(f"  {k}: {v}")
            print("  ---")
        return

    if not new_rows:
        print("Nothing new to write.")
        return

    # ── Append to CSV ─────────────────────────────────────────────────────
    write_header = not OUTPUT_CSV.exists()
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)

    print(f"Appended {len(new_rows)} rows → {OUTPUT_CSV}")
    print(f"\nNext step:")
    print(f"  python refresh.py --csv {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
