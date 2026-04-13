"""Transform monolithic unified_dashboard.html → slim version served by API.

Reads dashboard/unified_dashboard.html, strips the 5.9MB inline MARKETS data
blob (line 597), injects a fetch-wrapped bootstrap that pulls from /api/all,
writes the result to app/static/dashboard.html.

One-shot: run after pipeline refreshes. `python -m app.build_dashboard`.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "dashboard" / "unified_dashboard.html"
OUT = ROOT / "app" / "static" / "dashboard.html"

# Replaces the `const MARKETS = {...}` line plus moves the initial
# rebuildActiveData() call inside a fetch callback so scoring and rendering
# happen after data arrives.
BOOTSTRAP = """
// Data is fetched from the API now; MARKETS is populated after load.
let MARKETS = {jc:{properties:[],blocks:[],overall_trend:{},trends:{}},
               hoboken:{properties:[],blocks:[],overall_trend:{},trends:{}},
               weehawken:{properties:[],blocks:[],overall_trend:{},trends:{}}};
window.__marketsReady = fetch('/api/all')
  .then(r => r.json())
  .then(data => { MARKETS = data; })
  .catch(err => { console.error('MARKETS fetch failed', err); });
"""

INIT_HOOK = """
// Defer init until MARKETS resolves.
window.__marketsReady.then(() => {
  rebuildActiveData();
  if (typeof renderAllActive === 'function') renderAllActive();
});
"""


def main():
    if not SRC.exists():
        print(f"ERROR: source HTML not found at {SRC}", file=sys.stderr)
        sys.exit(1)

    lines = SRC.read_text().splitlines(keepends=True)

    # Sanity-check line 597 is indeed the MARKETS blob.
    idx = 596  # 0-based
    if not lines[idx].lstrip().startswith("const MARKETS"):
        # Fall back: find it by scan.
        for i, ln in enumerate(lines):
            if ln.lstrip().startswith("const MARKETS = {"):
                idx = i
                break
        else:
            print("ERROR: could not locate const MARKETS line", file=sys.stderr)
            sys.exit(1)

    orig_size = sum(len(l) for l in lines)
    orig_markets_size = len(lines[idx])
    lines[idx] = BOOTSTRAP

    # Replace the bare `rebuildActiveData();` that runs at module top level.
    # It's the one NOT indented inside a function.
    for i, ln in enumerate(lines):
        if ln.strip() == "rebuildActiveData();" and not ln.startswith(" "):
            lines[i] = INIT_HOOK
            break

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("".join(lines))
    new_size = OUT.stat().st_size
    print(f"Source:  {SRC} ({orig_size/1024:.0f} KB, MARKETS blob {orig_markets_size/1024:.0f} KB)")
    print(f"Output:  {OUT} ({new_size/1024:.0f} KB)")
    print(f"Reduction: {(1 - new_size/orig_size)*100:.1f}%")


if __name__ == "__main__":
    main()
