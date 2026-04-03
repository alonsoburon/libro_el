"""Split callout blocks into title + body format.

Converts:
  #ecl-warning[
    Title text Rest of the content here.
  ]

To:
  #ecl-warning[Title text][Rest of the content here.]
"""

import re
from pathlib import Path

TYPST_DIR = Path("/home/alonso/code/libro_el/typst")

# Match #ecl-TYPE[\n  content\n]
CALLOUT_RE = re.compile(
    r'#(ecl-(?:tip|warning|danger|info))\[\s*\n\s*(.*?)\n\]',
    re.DOTALL,
)


def split_title_body(text: str) -> tuple[str, str]:
    """Split the first sentence from the rest as title vs body.

    The title is everything up to and including the first period that's
    followed by a space and an uppercase letter, or the first period
    followed by a space and a backtick, or the first period at the end.
    """
    text = text.strip()

    # Try splitting at first ". " followed by uppercase or backtick or quote
    # This catches: "Title sentence. Body starts here."
    m = re.match(r'^(.+?(?:\.|:))\s+([A-Z`"\'#].*)', text, re.DOTALL)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # Try splitting at first " -- " as a fallback title separator
    if " -- " in text:
        parts = text.split(" -- ", 1)
        if len(parts[0]) < 80:  # reasonable title length
            return parts[0].strip(), parts[1].strip()

    # No clear split point -- use first ~8 words as title
    words = text.split()
    if len(words) > 8:
        title = " ".join(words[:8])
        body = " ".join(words[8:])
        return title, body

    # Very short -- entire text is the title, no body
    return text, ""


def convert_callout(match: re.Match) -> str:
    func = match.group(1)
    content = match.group(2).strip()

    title, body = split_title_body(content)

    if body:
        return f'#{func}[{title}][{body}]'
    else:
        return f'#{func}[{title}][]'


def process_file(path: Path) -> int:
    content = path.read_text(encoding="utf-8")
    new_content, count = CALLOUT_RE.subn(convert_callout, content)

    if new_content != content:
        path.write_text(new_content, encoding="utf-8")

    return count


if __name__ == "__main__":
    total = 0
    for f in sorted(TYPST_DIR.glob("ch*.typ")):
        count = process_file(f)
        if count:
            print(f"  {f.name}: {count} callouts split")
            total += count
    print(f"\nTotal: {total} callouts converted to title+body format")
