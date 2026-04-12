"""One-off: dump raw HTML of the first Paragon alert email for parser debugging."""
import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from gmail_ingest import get_gmail_service, get_email_body

service = get_gmail_service()
result = service.users().messages().list(
    userId="me",
    q="from:email@paragonmessaging.com subject:'Activity notification'",
    maxResults=1,
).execute()
msgs = result.get("messages", [])
if not msgs:
    print("No matching emails found.")
    sys.exit(1)

subj, html = get_email_body(service, msgs[0]["id"])
from bs4 import BeautifulSoup
soup = BeautifulSoup(html, "html.parser")
# Remove style/script tags
for tag in soup(["style", "script"]):
    tag.decompose()
text = soup.get_text(separator="\n", strip=True)
# Filter to non-empty lines
lines = [l for l in text.splitlines() if l.strip()]
print("\n".join(lines))
