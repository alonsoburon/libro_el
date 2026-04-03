"""Convert all ecl-callout blocks from bracket syntax to string arguments.

From: #ecl-warning[#strong[Title.] Body text with `code` and stuff.]
To:   #ecl-warning("Title.", "Body text with `code` and stuff.")

Also handles blocks WITHOUT #strong title (just body text):
From: #ecl-warning[Body text here.]
To:   #ecl-warning("Body text here.", "")
  -> Then splits first sentence as title.
"""

import re
from pathlib import Path

TYPST_DIR = Path("/home/alonso/code/libro_el/typst")
CALLOUT_TYPES = ("ecl-tip", "ecl-warning", "ecl-danger", "ecl-info")


def find_matching_bracket(text: str, start: int) -> int:
    """Find the position of the ] that closes the [ at start."""
    depth = 0
    i = start
    while i < len(text):
        if text[i] == '[':
            depth += 1
        elif text[i] == ']':
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def escape_for_string(s: str) -> str:
    """Escape characters for Typst string literals."""
    s = s.replace('\\', '\\\\')
    s = s.replace('"', '\\"')
    return s


def split_title_body(text: str) -> tuple[str, str]:
    """Split content that has #strong[Title] Body into (title, body)."""
    # Pattern: #strong[Title] Body
    m = re.match(r'#strong\[(.+?)\]\s*(.*)', text, re.DOTALL)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # No #strong -- split on first sentence boundary
    text = text.strip()
    # Try: first period + space + uppercase
    m2 = re.match(r'^(.+?\.)\s+([A-Z`].*)', text, re.DOTALL)
    if m2 and len(m2.group(1)) < 100:
        return m2.group(1).strip(), m2.group(2).strip()

    # Try: first colon + space
    m3 = re.match(r'^(.+?:)\s+(.*)', text, re.DOTALL)
    if m3 and len(m3.group(1)) < 80:
        return m3.group(1).strip(), m3.group(2).strip()

    # Fallback: first 6-8 words as title
    words = text.split()
    if len(words) > 8:
        return " ".join(words[:7]), " ".join(words[7:])

    return text, ""


def process_file(path: Path) -> int:
    content = path.read_text(encoding="utf-8")
    result = []
    i = 0
    count = 0

    while i < len(content):
        # Look for #ecl-TYPE[
        matched = False
        for ctype in CALLOUT_TYPES:
            prefix = f"#{ctype}["
            if content[i:i+len(prefix)] == prefix:
                # Found a callout -- find the matching ]
                bracket_start = i + len(prefix) - 1  # position of [
                bracket_end = find_matching_bracket(content, bracket_start)
                if bracket_end < 0:
                    break  # unclosed bracket, skip

                inner = content[bracket_start+1:bracket_end]
                title, body = split_title_body(inner)

                # Build the new format with string arguments
                title_esc = escape_for_string(title)
                body_esc = escape_for_string(body)
                result.append(f'#{ctype}("{title_esc}", "{body_esc}")')

                i = bracket_end + 1
                count += 1
                matched = True
                break

        if not matched:
            result.append(content[i])
            i += 1

    if count > 0:
        path.write_text("".join(result), encoding="utf-8")

    return count


if __name__ == "__main__":
    total = 0
    for f in sorted(TYPST_DIR.glob("ch*.typ")):
        n = process_file(f)
        if n:
            print(f"  {f.name}: {n} callouts converted")
            total += n
    print(f"\nTotal: {total} callouts converted to string arguments")
