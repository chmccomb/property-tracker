# Claude Code Instructions — JC Heights Analysis

This project is a real estate market analysis tool for Jersey City Heights built on Paragon MLS data.

## Project Context

- **Main deliverable**: `dashboard/jc_heights_dashboard_v2.html` — single-file HTML, ~2MB, with JSON embedded inline
- **Data pipeline**: `scripts/jc_heights_clean.py` → `scripts/jc_heights_emerging.py` → embedded into HTML
- **Raw data**: Paragon MLS 51-column export format (Hudson County)

## Architecture

The dashboard is a single HTML file with all JS, CSS, and data inline. No build step, no server required. Data is embedded as a JSON blob inside a `<script>` tag.

Key JS functions to know:
- `computeDealScore(p)` — main scoring function, returns `{ deal, pricePct, adjBlockPsf }`
- `blockQualityScore(est, emg)` — null-safe block quality, weights est 35% / emg 65%
- `getBedMatchedBlockPsf(blockKey, bedsNum, fallback)` — tries 24mo, 36mo, then fallback
- `renderScorerResults(d)` — renders deal scorer card; handles appreciation since sale date
- `_scoreActive(idx)` — scores active listings via cache, bypasses address lookup

## Scoring Formula

```
blockQuality = est_score * 0.35 + emg_score * 0.65   (null-safe)
priceScore   = 100 - clamp((adjPsf / adjBlockPsf - 1) * 100 * 2, -50, 50) + 50
dealScore    = blockQuality * 0.60 + priceScore * 0.40
```

Amenity-adjusted block PSF strips the average block amenity mix and re-applies the property's actual amenities:
- Parking: +2% (1BR), +14.2% (2BR), +3.4% (3BR)
- Outdoor: +4.3%
- Size curve: SIZE_CURVE constant in dashboard JS

HOA capitalization: `monthly_hoa * 12 / 0.07` subtracted from price before PSF calc.

## Common Tasks

**Add a new data field from MLS:**
1. Add parsing in `scripts/jc_heights_clean.py`
2. Aggregate/expose in `scripts/jc_heights_emerging.py`
3. Add to the property card render in `renderProperty()` or `renderScorerResults()` in the dashboard

**Refresh data after new Paragon export:**
1. Replace `data/raw/MLS_export.csv`
2. Run `python scripts/jc_heights_clean.py`
3. Run `python scripts/jc_heights_emerging.py`
4. Embed new `data/processed/dashboard_data_v2.json` into dashboard HTML

**Debugging deal scores:**
- Check `refPsfSource` field — tells you which PSF fallback was used (24mo/36mo/block)
- Check block reliability flag: `n_sales < 10` means weak signal
- Check `multi_address_warning` — block spanning multiple buildings skews median

## Known Issues / Technical Debt

1. **Block splitting**: Tax blocks span multiple buildings. Block 2801 includes 4 different streets, which skews its CAGR/median. Future improvement: split blocks by street or building.
2. **Score compression**: Min-max normalization of sub-signals then averaging compresses composite scores toward center. Re-normalization applied but still not full 0-100 range in practice.
3. **Data refresh is manual**: New Paragon exports require manual script re-run and HTML rebuild. Consider automating with a refresh script.
4. **Single-file HTML size**: At ~2MB it's approaching limits for easy editing. Consider splitting data embed into a `fetch()` call from a separate JSON file.

## Data Schema (dashboard_data_v2.json)

```json
{
  "properties": [...],   // all transactions + active listings
  "blocks": [...]        // block-level aggregates with scores
}
```

Property fields: `address, unit, beds, baths, sqft, price, psf, status, closing_date, block, block_psf, block_cagr, est_score, emg_score, parking, outdoor, hoa, ...`

Block fields: `block, n_sales, median_psf, block_cagr, est_score, emg_score, cagr_score, dom_score, stl_score, assess_score, inv_score, ...`
