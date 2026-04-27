"""Bulk line parser for pipe-separated card lines.

Supports TWO common formats (auto-detected per line):

  1) Combined expiry (12-field):
     number|mm/yy|cvv|name|address|city|state|zip|country|phone|email|…
       0     1    2    3     4       5    6    7    8       9   10

  2) Split expiry (13-field):
     number|mm|yy|cvv|name|address|city|state|zip|country|phone|email|…
       0   1  2   3   4     5       6    7    8    9      10   11

Detection: if parts[1] contains '/', '-', or is longer than 4 chars, treat as
combined; otherwise as split.
"""
from typing import List, Tuple, Dict, Any
import hashlib
import re


_MMYY_RE = re.compile(r'^\s*(\d{1,2})\s*[/\-\. ]\s*(\d{2,4})\s*$')


def _norm_year(y: str) -> str:
    y = y.strip()
    if len(y) == 2 and y.isdigit():
        return '20' + y
    return y


def _parse_combined_expiry(token: str) -> Tuple[str, str]:
    """Return (mm, yyyy) from something like '06/28', '6-28', '06 / 2028'."""
    m = _MMYY_RE.match(token)
    if not m:
        return '', ''
    mm = m.group(1).zfill(2)
    yy = _norm_year(m.group(2))
    return mm, yy


def parse_bulk_lines(text: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    records: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for idx, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        parts = [p.strip() for p in line.split('|')]
        if len(parts) < 3:
            errors.append({'line_no': idx, 'reason': f'only {len(parts)} fields', 'raw': line})
            continue
        number = parts[0]
        if not number.isdigit() or len(number) < 6:
            errors.append({'line_no': idx, 'reason': 'invalid card number', 'raw': line})
            continue

        # Detect which format this line uses
        p1 = parts[1]
        combined = any(ch in p1 for ch in ('/', '-', ' ')) or len(p1) > 4

        if combined:
            mm, yyyy = _parse_combined_expiry(p1)
            if not mm or not yyyy:
                errors.append({'line_no': idx, 'reason': f'bad expiry {p1!r}', 'raw': line})
                continue
            rest_start = 2       # parts[2] onwards = cvv|name|...
        else:
            mm = parts[1].zfill(2) if parts[1].isdigit() else parts[1]
            yyyy = _norm_year(parts[2]) if len(parts) > 2 else ''
            if not yyyy:
                errors.append({'line_no': idx, 'reason': 'missing year', 'raw': line})
                continue
            rest_start = 3       # parts[3] onwards = cvv|name|...

        # Re-index the remainder so both layouts land in the same slots below.
        rem = parts[rest_start:]
        # Pad to ensure we have at least 9 remaining positions (cvv..email)
        while len(rem) < 9:
            rem.append('')

        cvv     = rem[0]
        name    = rem[1]
        address = rem[2]
        city    = rem[3]
        state   = rem[4]
        zip_    = rem[5]
        country = rem[6].upper()
        phone   = rem[7]
        email   = rem[8]

        record = {
            'number': number,
            'bin': number[:6],
            'exp_month': mm,
            'exp_year': yyyy,
            'cvv': cvv,
            'name': name,
            'address': address,
            'city': city,
            'state': state,
            'zip': zip_,
            'country': country,
            'phone': phone,
            'email': email,
            'raw_line': line,
            'dedupe_key': hashlib.sha1(f'{number}|{mm}|{yyyy}|{cvv}'.encode()).hexdigest(),
        }
        records.append(record)
    return records, errors
