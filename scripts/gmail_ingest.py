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
PARAGON_SENDER = "noreply@paragonrels.com"    # typical Paragon MLS sender

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

    Paragon alert emails typically contain a table or series of divs with
    property details.  This parser handles the most common formats.

    If your Paragon portal has a custom template, inspect the raw HTML and
    update the selectors below.  The --dump-sample flag prints the HTML.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("ERROR: beautifulsoup4 not installed.  Run: pip install beautifulsoup4")
        sys.exit(1)

    soup = BeautifulSoup(html, "html.parser")
    properties = []

    # ── Strategy 1: Paragon "Property Card" table layout ─────────────────
    # Each listing is usually in a <table> or <div> block identified by an
    # MLS number.  We look for cells/divs that label their content.

    def text(el) -> str:
        return el.get_text(separator=" ", strip=True) if el else ""

    def find_labelled(container, label: str) -> str:
        """Find a value next to a label cell/span in the container."""
        label_lower = label.lower()
        for el in container.find_all(["td", "th", "span", "div", "strong", "b"]):
            el_text = el.get_text(strip=True).lower().rstrip(":").strip()
            if el_text == label_lower:
                # Try sibling
                nxt = el.find_next_sibling()
                if nxt:
                    return text(nxt)
                # Try parent's next sibling
                parent_nxt = el.parent.find_next_sibling() if el.parent else None
                if parent_nxt:
                    return text(parent_nxt)
        return ""

    # ── Try to find per-listing blocks ───────────────────────────────────
    # Common patterns: each listing wrapped in a <tr> group or a <div class="listing">
    listing_blocks = (
        soup.find_all("div", class_=re.compile(r"listing|property|result", re.I))
        or soup.find_all("table", class_=re.compile(r"listing|property", re.I))
    )

    if not listing_blocks:
        # Fallback: treat entire email as one listing (single-listing alert)
        listing_blocks = [soup]

    for block in listing_blocks:
        row = dict(BLANK_ROW)
        row["_source"] = "gmail_alert"

        # MLS number — usually formatted as "MLS#: 24012345" or "#24012345"
        mls_match = re.search(r"(?:MLS\s*#?:?\s*|#)(\d{7,10})", text(block), re.I)
        row["MLS #"] = mls_match.group(1) if mls_match else ""

        # Skip blocks without an MLS number (navigation/footer blocks)
        if not row["MLS #"]:
            continue

        # Status — "Active", "Sold", "Under Contract", etc.
        status_match = re.search(
            r"\b(Active|Sold|Under Contract|Pending|Expired|Withdrawn)\b",
            text(block), re.I)
        row["Status"] = status_match.group(1).upper() if status_match else ""

        # Address — look for a strong/h3 element with a number-word-word pattern
        for tag in block.find_all(["h2", "h3", "h4", "strong", "b", "a"]):
            t = text(tag)
            if re.match(r"^\d+\s+\w", t) and len(t) < 80:
                row["Address"] = t.split(",")[0].strip()
                break

        # Price — first dollar amount
        price_match = re.search(r"\$[\d,]+", text(block))
        if price_match:
            price_str = price_match.group(0)
            if row["Status"] == "SOLD":
                row["Sold Price"] = price_str
            else:
                row["Asking Price"] = price_str

        # Beds / baths — "3 BD / 2 BA" or "Beds: 3" etc.
        bd_match = re.search(r"(\d)\s*(?:BD|Bed|BR)\b", text(block), re.I)
        ba_match = re.search(r"(\d)\s*(?:BA|Bath|Full Bath)\b", text(block), re.I)
        if bd_match: row["Bedrooms"] = bd_match.group(1)
        if ba_match: row["Total # Full Baths"] = ba_match.group(1)

        # Sqft
        sqft_match = re.search(r"([\d,]+)\s*(?:sq\.?\s*ft|sqft|SF)\b", text(block), re.I)
        if sqft_match:
            row["Approx Sq Ft"] = sqft_match.group(1).replace(",", "")

        # Closing / listing date
        date_matches = re.findall(r"\d{1,2}/\d{1,2}/\d{4}", text(block))
        if date_matches:
            if row["Status"] == "SOLD" and len(date_matches) >= 1:
                row["Closing Date"] = date_matches[-1]
            elif len(date_matches) >= 1:
                row["Listing Date"] = date_matches[0]

        # ZIP — 5-digit code
        zip_match = re.search(r"\b(07\d{3})\b", text(block))
        if zip_match: row["Zip"] = zip_match.group(1)

        # HOA
        hoa_match = re.search(r"(?:HOA|Maint|Maintenance)[^\$]*\$([\d,]+)", text(block), re.I)
        if hoa_match: row["Monthly Maintenance Fee"] = "$" + hoa_match.group(1)

        # DOM
        dom_match = re.search(r"(\d+)\s*(?:DOM|Days on Market|days)", text(block), re.I)
        if dom_match: row["Days On Market"] = dom_match.group(1)

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
    seen_ids = load_seen_ids(OUTPUT_CSV)

    new_rows: list[dict] = []
    for mid in msg_ids:
        if mid in seen_ids:
            continue
        subject, html = get_email_body(service, mid)
        if not html:
            continue
        props = parse_paragon_email(subject, html)
        for prop in props:
            prop["_email_id"] = mid
        new_rows.extend(props)
        print(f"  Parsed {len(props)} listing(s) from: {subject[:60]}")

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
