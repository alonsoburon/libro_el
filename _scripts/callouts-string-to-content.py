"""Convert callouts from two-string to string+content format.

From: #ecl-warning("Title here", "Body with `code` and stuff.")
To:   #ecl-warning("Title here")[Body with `code` and stuff.]

The title stays as a string (plain text, no markup needed).
The body becomes a content block (supports `code`, *bold*, links, etc.).
"""

import re
from pathlib import Path

TYPST_DIR = Path("/home/alonso/code/libro_el/typst")
CALLOUT_TYPES = ("ecl-tip", "ecl-warning", "ecl-danger", "ecl-info")


def unescape_string(s: str) -> str:
    """Unescape Typst string escapes back to raw content."""
    s = s.replace('\\"', '"')
    s = s.replace('\\\\', '\\')
    return s


def process_file(path: Path) -> int:
    content = path.read_text(encoding="utf-8")
    count = 0

    for ctype in CALLOUT_TYPES:
        # Match: #ecl-TYPE("title", "body")
        # Need to handle escaped quotes inside the strings
        prefix = f"#{ctype}("
        i = 0
        while True:
            pos = content.find(prefix, i)
            if pos < 0:
                break

            # Parse the first string argument (title)
            after_prefix = pos + len(prefix)
            if after_prefix >= len(content) or content[after_prefix] != '"':
                i = after_prefix
                continue

            # Find end of first string
            j = after_prefix + 1
            while j < len(content):
                if content[j] == '\\' and j + 1 < len(content):
                    j += 2  # skip escaped char
                    continue
                if content[j] == '"':
                    break
                j += 1
            title_end = j  # position of closing "

            title_raw = content[after_prefix+1:title_end]

            # Skip ", " separator
            sep_start = title_end + 1
            # Find the start of second string
            while sep_start < len(content) and content[sep_start] in ' ,':
                sep_start += 1

            if sep_start >= len(content) or content[sep_start] != '"':
                i = sep_start
                continue

            # Find end of second string
            k = sep_start + 1
            while k < len(content):
                if content[k] == '\\' and k + 1 < len(content):
                    k += 2
                    continue
                if content[k] == '"':
                    break
                k += 1
            body_end = k

            body_raw = content[sep_start+1:body_end]

            # Skip closing )
            close_paren = body_end + 1
            if close_paren < len(content) and content[close_paren] == ')':
                # Build replacement
                title = unescape_string(title_raw)
                body = unescape_string(body_raw)

                # Re-escape title for string (only quotes and backslashes)
                title_esc = title.replace('\\', '\\\\').replace('"', '\\"')

                old = content[pos:close_paren+1]
                new = f'#{ctype}("{title_esc}")[{body}]'
                content = content[:pos] + new + content[close_paren+1:]
                count += 1
                i = pos + len(new)
            else:
                i = close_paren

    if count > 0:
        path.write_text(content, encoding="utf-8")

    return count


if __name__ == "__main__":
    total = 0
    for f in sorted(TYPST_DIR.glob("ch*.typ")):
        n = process_file(f)
        if n:
            print(f"  {f.name}: {n}")
            total += n
    print(f"\nTotal: {total} callouts converted to string+content format")
