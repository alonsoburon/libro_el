"""Shorten callout titles and move excess text to the body.

Titles should be 3-8 words, like a heading. The rest goes to the body.

Rules:
1. If title is already <= 60 chars, leave it alone.
2. If title starts with "Don't ..." -- keep "Don't [verb] [object]" (up to ~5-6 words).
3. If title contains backtick code -- split before the first backtick if that gives a reasonable title.
4. Otherwise, split at first period, colon, or dash that's <= 60 chars.
5. Fallback: take first 6 words.
"""

import re
from pathlib import Path

TYPST_DIR = Path("/home/alonso/code/libro_el/typst")
CALLOUT_TYPES = ("ecl-tip", "ecl-warning", "ecl-danger", "ecl-info")
MAX_TITLE = 60


def shorten_title(title: str, body: str) -> tuple[str, str]:
    """Shorten title, prepend excess to body."""
    if len(title) <= MAX_TITLE:
        return title, body

    # Strategy 1: "Don't X Y" patterns -- keep the imperative
    dont_match = re.match(r"^(Don't \w+ [\w`_\\*]+(?:\s+[\w`_\\*]+)?)", title)
    if dont_match and len(dont_match.group(1)) <= MAX_TITLE:
        new_title = dont_match.group(1)
        overflow = title[len(new_title):].strip()
        if overflow:
            body = overflow + " " + body if body else overflow
        return new_title, body

    # Strategy 2: split before first backtick if reasonable
    bt = title.find('`')
    if 10 < bt < MAX_TITLE:
        candidate = title[:bt].rstrip()
        # Make sure we're not cutting mid-word awkwardly
        if len(candidate) >= 10:
            overflow = title[bt:].strip()
            body = overflow + " " + body if body else overflow
            return candidate, body

    # Strategy 3: split at first sentence boundary (. or :) under limit
    for sep in ['. ', ': ', ' -- ']:
        pos = title.find(sep)
        if 0 < pos <= MAX_TITLE:
            new_title = title[:pos + (1 if sep == '. ' else 0)].strip()
            overflow = title[pos + len(sep):].strip()
            if overflow:
                body = overflow + " " + body if body else overflow
            return new_title, body

    # Strategy 4: take first 6 words
    words = title.split()
    if len(words) > 6:
        new_title = " ".join(words[:6])
        overflow = " ".join(words[6:])
        body = overflow + " " + body if body else overflow
        return new_title, body

    return title, body


def process_file(path: Path) -> int:
    content = path.read_text(encoding="utf-8")
    count = 0

    for ctype in CALLOUT_TYPES:
        prefix = f'#{ctype}("'
        i = 0
        while True:
            pos = content.find(prefix, i)
            if pos < 0:
                break

            # Parse title string
            title_start = pos + len(prefix)
            j = title_start
            while j < len(content):
                if content[j] == '\\' and j + 1 < len(content):
                    j += 2
                    continue
                if content[j] == '"':
                    break
                j += 1
            title_end = j
            title_raw = content[title_start:title_end]

            # Unescape title
            title = title_raw.replace('\\"', '"').replace('\\\\', '\\')

            # Find body: ")[body]"
            rest_start = title_end + 1  # after closing "
            # Skip ")[
            if content[rest_start:rest_start+2] == ')[':
                body_start = rest_start + 2
                # Find matching ]
                depth = 1
                k = body_start
                while k < len(content) and depth > 0:
                    if content[k] == '[':
                        depth += 1
                    elif content[k] == ']':
                        depth -= 1
                    k += 1
                body_end = k - 1
                body = content[body_start:body_end]

                # Shorten
                new_title, new_body = shorten_title(title, body)

                if new_title != title or new_body != body:
                    # Re-escape title
                    new_title_esc = new_title.replace('\\', '\\\\').replace('"', '\\"')
                    old_full = content[pos:k]
                    new_full = f'#{ctype}("{new_title_esc}")[{new_body}]'
                    content = content[:pos] + new_full + content[k:]
                    count += 1
                    i = pos + len(new_full)
                else:
                    i = k
            else:
                i = rest_start

    if count > 0:
        path.write_text(content, encoding="utf-8")

    return count


if __name__ == "__main__":
    total = 0
    for f in sorted(TYPST_DIR.glob("ch*.typ")):
        n = process_file(f)
        if n:
            print(f"  {f.name}: {n} titles shortened")
            total += n
    print(f"\nTotal: {total} titles shortened")
