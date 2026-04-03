"""Convert callout blocks in Typst chapter files to semantic callout functions.

Handles two source formats:
1. #quote(block: true)[/* Type: */ ...] → #ecl-type[...]
2. #block(fill: gruvbox.bg0, ..., stroke: (left: 3pt + gruvbox.yellow_d))[...] → #ecl-warning[...]

Leaves #quote(block: true)[#strong[One-liner:] ...] untouched.
"""

import re
from pathlib import Path

TYPST_DIR = Path("/home/alonso/code/libro_el/typst")

# Map callout markers to function names
CALLOUT_MAP = {
    "Warning": "ecl-warning",
    "Tip": "ecl-tip",
    "Danger": "ecl-danger",
    "Info": "ecl-info",
    "Example": "ecl-info",  # By Corridor examples use info style
}


def fix_imports(content: str) -> str:
    """Update import line to include callout functions."""
    old_import = '#import "theme.typ": gruvbox, ecl-theme'
    new_import = '#import "theme.typ": gruvbox, ecl-theme, ecl-tip, ecl-warning, ecl-danger, ecl-info'
    return content.replace(old_import, new_import)


def convert_quote_callouts(content: str) -> str:
    """Convert #quote(block: true)[/* Type: */ ...] to #ecl-type[...]."""
    for marker, func in CALLOUT_MAP.items():
        # Pattern: #quote(block: true)[\n/* Type: */ content\n]
        # The content may span multiple lines
        pattern = (
            r'#quote\(block: true\)\[\s*\n'
            r'/\* ' + marker + r': \*/ (.*?)\n'
            r'\]'
        )
        replacement = f'#{func}[\n  \\1\n]'
        content = re.sub(pattern, replacement, content, flags=re.DOTALL)

        # Also handle single-line variant (no leading newline)
        pattern2 = (
            r'#quote\(block: true\)\[\s*'
            r'/\* ' + marker + r': \*/ (.*?)\n'
            r'\]'
        )
        replacement2 = f'#{func}[\n  \\1\n]'
        content = re.sub(pattern2, replacement2, content, flags=re.DOTALL)

    return content


def convert_block_callouts(content: str) -> str:
    """Convert #block(fill: gruvbox.bg0, ..., stroke: yellow_d)[...] to #ecl-warning[...]."""
    # Pattern for the manual block callouts with yellow border
    pattern = (
        r'#block\(fill: gruvbox\.bg0, inset: 12pt, radius: 4pt, '
        r'stroke: \(left: 3pt \+ gruvbox\.yellow_d\)\)\[\s*\n'
        r'(.*?)\n'
        r'\s*\]'
    )

    def replace_block(match):
        body = match.group(1).strip()
        # Remove trailing backslash if present
        if body.endswith('\\'):
            body = body[:-1].rstrip()
        return f'#ecl-warning[\n  {body}\n]'

    content = re.sub(pattern, replace_block, content, flags=re.DOTALL)
    return content


def process_file(path: Path) -> tuple[int, int]:
    """Process a single .typ file. Returns (quotes_converted, blocks_converted)."""
    original = path.read_text(encoding="utf-8")

    # Count before
    quotes_before = original.count("/* Warning: */") + original.count("/* Tip: */") + \
                    original.count("/* Danger: */") + original.count("/* Info: */") + \
                    original.count("/* Example: */")
    blocks_before = original.count("stroke: (left: 3pt + gruvbox.yellow_d)")

    content = original
    content = fix_imports(content)
    content = convert_quote_callouts(content)
    content = convert_block_callouts(content)

    # Count after
    quotes_after = content.count("/* Warning: */") + content.count("/* Tip: */") + \
                   content.count("/* Danger: */") + content.count("/* Info: */") + \
                   content.count("/* Example: */")
    blocks_after = content.count("stroke: (left: 3pt + gruvbox.yellow_d)")

    quotes_converted = quotes_before - quotes_after
    blocks_converted = blocks_before - blocks_after

    if content != original:
        path.write_text(content, encoding="utf-8")

    return quotes_converted, blocks_converted


if __name__ == "__main__":
    total_quotes = 0
    total_blocks = 0

    for f in sorted(TYPST_DIR.glob("ch*.typ")):
        q, b = process_file(f)
        total_quotes += q
        total_blocks += b
        if q or b:
            print(f"  {f.name}: {q} quote callouts, {b} block callouts converted")

    print(f"\nTotal: {total_quotes} quote callouts, {total_blocks} block callouts converted")

    # Report remaining
    for f in sorted(TYPST_DIR.glob("ch*.typ")):
        content = f.read_text(encoding="utf-8")
        remaining_quotes = sum(1 for m in re.finditer(r'/\* (Warning|Tip|Danger|Info|Example): \*/', content))
        remaining_blocks = content.count("stroke: (left: 3pt + gruvbox.yellow_d)")
        if remaining_quotes or remaining_blocks:
            print(f"  REMAINING in {f.name}: {remaining_quotes} quote markers, {remaining_blocks} yellow blocks")
