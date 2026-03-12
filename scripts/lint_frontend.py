from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT / "web"
INDEX_HTML = WEB_DIR / "index.html"

ID_PATTERN = re.compile(r"""id\s*=\s*["']([^"']+)["']""", re.IGNORECASE)


def collect_duplicate_ids(html_text: str) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for match in ID_PATTERN.finditer(html_text):
        value = match.group(1).strip()
        if not value:
            continue
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def main() -> int:
    errors: list[str] = []

    if not INDEX_HTML.exists():
        errors.append(f"missing required frontend entrypoint: {INDEX_HTML}")
    else:
        index_text = INDEX_HTML.read_text(encoding="utf-8")
        duplicates = collect_duplicate_ids(index_text)
        if duplicates:
            errors.append(f"duplicate HTML ids in web/index.html: {', '.join(duplicates)}")

        required_ids = {
            "searchInput",
            "searchResultsPop",
            "releasedGamesGrid",
            "seasonalSaleTopGrid",
            "seasonalSaleLowerGrid",
            "topPlayedList",
            "trendingList",
            "alertsList",
        }
        for element_id in sorted(required_ids):
            if element_id not in index_text:
                errors.append(f"missing required id in index.html: {element_id}")

        if "Released Games" in index_text:
            errors.append('stale label found: "Released Games" (expected "All Games")')
        if "All Games" not in index_text:
            errors.append('missing expected label: "All Games"')

    if errors:
        for message in errors:
            print(f"[frontend-lint] {message}")
        return 1

    print("[frontend-lint] ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
