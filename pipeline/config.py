"""
City configuration registry.

Each city's config captures the differences that previously required separate
scripts.  The unified clean.py and emerging.py read this config to adapt their
behaviour per municipality.
"""

CITY_CONFIGS = {
    "jc_heights": {
        "name": "JC Heights",
        "city_filter": "JC, Heights",
        "geocode_suffix": ", Jersey City, NJ",
        "modiiv_muni": "0906",
        "radius_miles": 0.30,
        "affordability_threshold": 700,
        "default_zip": "07307",

        # Block key strategy
        "blockstreet_min": 5,
        "complex_fallback": False,
        "complex_min": 5,
        "zip_normalize": False,
        "norm_block_leading_zeros": False,

        # Parking detection
        "parking_strong_extra": "",
        "parking_neg_pattern": (
            r'\b(no\s+parking|street\s+parking\s+only)\b'
        ),

        # Output file prefixes (within data_dir)
        "output_prefix": "jc_heights",

        # Scoring (identical across cities today, but per-city override possible)
        "score_weights": {
            "block_cagr":          0.30,
            "dom_trend":           0.20,
            "sale_to_list_ratio":  0.20,
            "assess_delta":        0.15,
            "inventory_trend":     0.15,
        },
        "cagr_window_start": 2022,
        "cagr_window_end":   2026,
    },

    "hoboken": {
        "name": "Hoboken",
        "city_filter": "Hoboken",
        "geocode_suffix": ", Hoboken, NJ",
        "modiiv_muni": "0905",
        "radius_miles": 0.20,
        "affordability_threshold": 900,
        "default_zip": "07030",

        "blockstreet_min": 5,
        "complex_fallback": True,
        "complex_min": 5,
        "zip_normalize": True,
        "norm_block_leading_zeros": True,

        "parking_strong_extra": r"|deeded\s+garage",
        "parking_neg_pattern": (
            r'\b(no\s+parking|street\s+parking\s+only|optional\s+parking|'
            r'rental\s+parking|off-?site\s+parking)\b'
        ),

        "output_prefix": "hoboken",

        "score_weights": {
            "block_cagr":          0.30,
            "dom_trend":           0.20,
            "sale_to_list_ratio":  0.20,
            "assess_delta":        0.15,
            "inventory_trend":     0.15,
        },
        "cagr_window_start": 2022,
        "cagr_window_end":   2026,
    },

    "weehawken": {
        "name": "Weehawken",
        "city_filter": "Weehawken",
        "geocode_suffix": ", Weehawken, NJ",
        "modiiv_muni": "0911",
        "radius_miles": 0.20,
        "affordability_threshold": 700,
        "default_zip": "07086",

        "blockstreet_min": 5,
        "complex_fallback": True,
        "complex_min": 5,
        "zip_normalize": True,
        "norm_block_leading_zeros": True,

        "parking_strong_extra": r"|deeded\s+garage",
        "parking_neg_pattern": (
            r'\b(no\s+parking|street\s+parking\s+only|street\s+parking\s+permits?|'
            r'permit\s+parking\s+only|optional\s+parking|rental\s+parking|'
            r'off-?site\s+parking)\b'
        ),

        "output_prefix": "weehawken",

        "score_weights": {
            "block_cagr":          0.30,
            "dom_trend":           0.20,
            "sale_to_list_ratio":  0.20,
            "assess_delta":        0.15,
            "inventory_trend":     0.15,
        },
        "cagr_window_start": 2022,
        "cagr_window_end":   2026,
    },
}


# MLS City column value → config key mapping (for auto-filtering combined exports)
CITY_COLUMN_MAP = {}
for key, cfg in CITY_CONFIGS.items():
    CITY_COLUMN_MAP[cfg["city_filter"]] = key
