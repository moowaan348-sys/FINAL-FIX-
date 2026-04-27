"""
Country-name / alias → ISO-2 expansion for free-text search.

Given what the user typed, return:
  • a set of ISO-2 codes (preferred — match against `country` / `card_country`)
  • plus free-text patterns (fallback partial matches on whatever text the
    raw-line country field might contain, e.g. "UNITED")

Covers common aliases like UK, USA, Britain, Holland, etc.
"""
from __future__ import annotations

from typing import Set, Tuple, List


# Alias (uppercased, stripped) -> ISO-2
ALIASES: dict = {
    # United Kingdom
    'UK': 'GB', 'GB': 'GB', 'GBR': 'GB', 'ENG': 'GB', 'EN': 'GB',
    'BRITAIN': 'GB', 'GREAT BRITAIN': 'GB',
    'UNITED KINGDOM': 'GB', 'UNITED-KINGDOM': 'GB',
    'ENGLAND': 'GB', 'SCOTLAND': 'GB', 'WALES': 'GB',
    'NORTHERN IRELAND': 'GB',

    # United States
    'US': 'US', 'USA': 'US', 'U.S.': 'US', 'U.S.A.': 'US',
    'UNITED STATES': 'US', 'UNITED-STATES': 'US',
    'UNITED STATES OF AMERICA': 'US', 'AMERICA': 'US',

    # Canada
    'CA': 'CA', 'CAN': 'CA', 'CANADA': 'CA',

    # Germany
    'DE': 'DE', 'DEU': 'DE', 'GER': 'DE', 'GERMANY': 'DE', 'DEUTSCHLAND': 'DE',

    # France
    'FR': 'FR', 'FRA': 'FR', 'FRANCE': 'FR',

    # Italy
    'IT': 'IT', 'ITA': 'IT', 'ITALY': 'IT', 'ITALIA': 'IT',

    # Spain
    'ES': 'ES', 'ESP': 'ES', 'SPAIN': 'ES', 'ESPANA': 'ES', 'ESPAÑA': 'ES',

    # Netherlands
    'NL': 'NL', 'NLD': 'NL', 'NETHERLANDS': 'NL', 'HOLLAND': 'NL',
    'THE NETHERLANDS': 'NL',

    # Australia
    'AU': 'AU', 'AUS': 'AU', 'AUSTRALIA': 'AU',

    # Ireland
    'IE': 'IE', 'IRL': 'IE', 'IRELAND': 'IE', 'EIRE': 'IE',

    # Portugal
    'PT': 'PT', 'PRT': 'PT', 'PORTUGAL': 'PT',

    # Belgium
    'BE': 'BE', 'BEL': 'BE', 'BELGIUM': 'BE',

    # Switzerland
    'CH': 'CH', 'CHE': 'CH', 'SWITZERLAND': 'CH', 'SWISS': 'CH',

    # Austria
    'AT': 'AT', 'AUT': 'AT', 'AUSTRIA': 'AT',

    # Sweden / Norway / Denmark / Finland / Iceland
    'SE': 'SE', 'SWE': 'SE', 'SWEDEN': 'SE', 'SVERIGE': 'SE',
    'NO': 'NO', 'NOR': 'NO', 'NORWAY': 'NO', 'NORGE': 'NO',
    'DK': 'DK', 'DNK': 'DK', 'DENMARK': 'DK', 'DANMARK': 'DK',
    'FI': 'FI', 'FIN': 'FI', 'FINLAND': 'FI', 'SUOMI': 'FI',
    'IS': 'IS', 'ISL': 'IS', 'ICELAND': 'IS',

    # Japan / China / Korea / India / Singapore / Hong Kong
    'JP': 'JP', 'JPN': 'JP', 'JAPAN': 'JP', 'NIHON': 'JP',
    'CN': 'CN', 'CHN': 'CN', 'CHINA': 'CN',
    'KR': 'KR', 'KOR': 'KR', 'KOREA': 'KR',
    'SOUTH KOREA': 'KR', 'REPUBLIC OF KOREA': 'KR',
    'IN': 'IN', 'IND': 'IN', 'INDIA': 'IN',
    'SG': 'SG', 'SGP': 'SG', 'SINGAPORE': 'SG',
    'HK': 'HK', 'HKG': 'HK', 'HONG KONG': 'HK', 'HONGKONG': 'HK',
    'TW': 'TW', 'TWN': 'TW', 'TAIWAN': 'TW',
    'TH': 'TH', 'THA': 'TH', 'THAILAND': 'TH',
    'MY': 'MY', 'MYS': 'MY', 'MALAYSIA': 'MY',
    'PH': 'PH', 'PHL': 'PH', 'PHILIPPINES': 'PH',
    'ID': 'ID', 'IDN': 'ID', 'INDONESIA': 'ID',
    'VN': 'VN', 'VNM': 'VN', 'VIETNAM': 'VN',

    # Latin America
    'MX': 'MX', 'MEX': 'MX', 'MEXICO': 'MX',
    'BR': 'BR', 'BRA': 'BR', 'BRAZIL': 'BR', 'BRASIL': 'BR',
    'AR': 'AR', 'ARG': 'AR', 'ARGENTINA': 'AR',
    'CL': 'CL', 'CHL': 'CL', 'CHILE': 'CL',
    'CO': 'CO', 'COL': 'CO', 'COLOMBIA': 'CO',
    'PE': 'PE', 'PER': 'PE', 'PERU': 'PE',

    # Europe (misc)
    'PL': 'PL', 'POL': 'PL', 'POLAND': 'PL', 'POLSKA': 'PL',
    'CZ': 'CZ', 'CZE': 'CZ', 'CZECH': 'CZ', 'CZECHIA': 'CZ',
    'CZECH REPUBLIC': 'CZ',
    'RO': 'RO', 'ROU': 'RO', 'ROMANIA': 'RO',
    'GR': 'GR', 'GRC': 'GR', 'GREECE': 'GR',
    'HU': 'HU', 'HUN': 'HU', 'HUNGARY': 'HU',
    'BG': 'BG', 'BGR': 'BG', 'BULGARIA': 'BG',
    'RU': 'RU', 'RUS': 'RU', 'RUSSIA': 'RU',
    'UA': 'UA', 'UKR': 'UA', 'UKRAINE': 'UA',
    'TR': 'TR', 'TUR': 'TR', 'TURKEY': 'TR', 'TÜRKIYE': 'TR',

    # Middle East / Africa
    'IL': 'IL', 'ISR': 'IL', 'ISRAEL': 'IL',
    'AE': 'AE', 'ARE': 'AE', 'UAE': 'AE',
    'UNITED ARAB EMIRATES': 'AE', 'DUBAI': 'AE',
    'SA': 'SA', 'SAU': 'SA', 'SAUDI': 'SA', 'SAUDI ARABIA': 'SA',
    'QA': 'QA', 'QAT': 'QA', 'QATAR': 'QA',
    'ZA': 'ZA', 'ZAF': 'ZA', 'SOUTH AFRICA': 'ZA',
    'NG': 'NG', 'NGA': 'NG', 'NIGERIA': 'NG',
    'EG': 'EG', 'EGY': 'EG', 'EGYPT': 'EG',
    'KE': 'KE', 'KEN': 'KE', 'KENYA': 'KE',
    'MA': 'MA', 'MAR': 'MA', 'MOROCCO': 'MA',

    # Oceania
    'NZ': 'NZ', 'NZL': 'NZ', 'NEW ZEALAND': 'NZ',

    # Cyprus / Malta / Luxembourg
    'CY': 'CY', 'CYP': 'CY', 'CYPRUS': 'CY',
    'MT': 'MT', 'MLT': 'MT', 'MALTA': 'MT',
    'LU': 'LU', 'LUX': 'LU', 'LUXEMBOURG': 'LU',
}


def expand_country_query(user_input: str) -> Tuple[Set[str], List[str]]:
    """Return (iso2_codes, free_patterns) to search.

    • `iso2_codes`: set of exact ISO-2 codes to match against country fields
    • `free_patterns`: list of substrings to regex-match against the raw
      `country` text (fallback for weird uploads with full country names).
    """
    raw = (user_input or '').strip()
    if not raw:
        return set(), []
    up = raw.upper()
    codes: Set[str] = set()
    patterns: List[str] = []

    # Try whole-string alias first (e.g. "UNITED KINGDOM")
    if up in ALIASES:
        codes.add(ALIASES[up])

    # If input is exactly 2 letters, also accept it literally (unknown ISO-2)
    if len(up) == 2 and up.isalpha():
        codes.add(up)

    # If we haven't matched anything yet, split into tokens and alias each
    if not codes:
        for token in up.replace('.', ' ').replace('-', ' ').split():
            if token in ALIASES:
                codes.add(ALIASES[token])

    # Always also keep the raw text as a free-text regex pattern so partial
    # uploads like "UNITED" still surface results that store the full name.
    patterns.append(raw)
    return codes, patterns


__all__ = ['ALIASES', 'expand_country_query', 'iso2_to_name']


# Reverse mapping: ISO-2 → canonical full name.
# Derived from ALIASES: for each iso2, pick the longest alias that's pure letters
# (to prefer "UNITED KINGDOM" over "UK").
def _build_iso2_names():
    best = {}
    for name, iso in ALIASES.items():
        if not all(c.isalpha() or c in ' ' for c in name):
            continue
        # Skip 2-3 letter codes, we want full names
        if len(name.replace(' ', '')) <= 3:
            continue
        cur = best.get(iso)
        if cur is None or len(name) > len(cur):
            best[iso] = name
    return best


_ISO2_NAMES = _build_iso2_names()

# Canonical overrides for countries where the longest-alias heuristic picks
# a sub-region (e.g. GB → "Northern Ireland" instead of "United Kingdom").
_ISO2_NAMES.update({
    'GB': 'United Kingdom',
    'US': 'United States',
    'AE': 'United Arab Emirates',
    'KR': 'South Korea',
    'CZ': 'Czech Republic',
    'NL': 'Netherlands',
})


def iso2_to_name(iso2: str) -> str:
    """Return a human-readable country name for an ISO-2 code.
    Falls back to the ISO-2 code itself if no mapping is known."""
    if not iso2:
        return ''
    up = str(iso2).strip().upper()
    return _ISO2_NAMES.get(up, up)
