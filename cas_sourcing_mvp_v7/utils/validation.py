from __future__ import annotations

import re

CAS_PATTERN = re.compile(r"^\d{2,7}-\d{2}-\d$")


def is_valid_cas(cas_number: str) -> bool:
    cas = cas_number.strip()
    if not CAS_PATTERN.match(cas):
        return False
    digits = cas.replace("-", "")
    check_digit = int(digits[-1])
    body = digits[:-1][::-1]
    total = sum((i + 1) * int(d) for i, d in enumerate(body))
    return total % 10 == check_digit
