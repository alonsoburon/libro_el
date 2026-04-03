"""Convert Obsidian markdown chapters to single Typst files per chapter.

Concatenates all patterns per chapter in numeric order, strips frontmatter,
runs pandoc md→typst, then fixes Obsidian-specific syntax.
"""

import subprocess
import re
from pathlib import Path

REPO = Path("/home/alonso/code/libro_el")
OUT = REPO / "typst"

CHAPTERS = [
    ("00-front-matter", "ch00-front-matter"),
    ("01-foundations-and-archetypes", "ch01-foundations"),
    ("02-full-replace-patterns", "ch02-full-replace"),
    ("03-incremental-patterns", "ch03-incremental"),
    ("04-load-strategies", "ch04-load-strategies"),
    ("05-conforming-playbook", "ch05-conforming"),
    ("06-operating-the-pipeline", "ch06-operating"),
    ("07-serving-the-destination", "ch07-serving"),
    ("08-appendix", "ch08-appendix"),
]


def strip_frontmatter(text: str) -> str:
    """Remove YAML frontmatter between --- markers."""
    if text.startswith("---"):
        end = text.find("---", 3)
        if end != -1:
            return text[end + 3:].lstrip("\n")
    return text


def fix_obsidian_callouts(text: str) -> str:
    """Convert raw Obsidian callout remnants to Typst boxes.

    Pandoc leaves callouts as literal \[!type\] text.
    Convert to Typst #block with colored sidebar.
    """
    # Map callout types to Typst styling
    callout_map = {
        "warning": ("orange", "Warning"),
        "tip": ("green", "Tip"),
        "danger": ("red", "Danger"),
        "info": ("blue", "Info"),
        "example": ("gray", "Example"),
    }

    # Pattern: #quote(block: true)[\n\[!type\] ... content ... \n]
    # or collapsed: \[!type\]-
    def replace_callout_block(match):
        callout_type = match.group(1).lower()
        title = match.group(2).strip() if match.group(2) else ""
        content = match.group(3).strip()
        color, default_title = callout_map.get(callout_type, ("gray", "Note"))
        display_title = title if title else default_title
        return f'#block(fill: luma(245), inset: 12pt, radius: 4pt)[\n  *{display_title}*\\\n  {content}\n]'

    # Match pandoc's quote blocks containing callout markers
    text = re.sub(
        r'#quote\(block: true\)\[\s*\\?\[!(\w+)\\?\][-]?\s*(.*?)\n(.*?)\n\]',
        replace_callout_block,
        text,
        flags=re.DOTALL,
    )

    # Simpler pattern: standalone \[!type\] lines
    for ctype, (color, title) in callout_map.items():
        text = re.sub(
            rf'\\?\[!{ctype}\\?\][-]?\s*',
            f'/* {title}: */ ',
            text,
        )

    return text


def fix_wikilinks(text: str) -> str:
    """Convert Obsidian wikilink remnants to Typst labels.

    Pandoc leaves wikilinks as literal [[path|display]] text.
    Convert to just the display text (cross-references will be
    handled when we set up Typst labels properly).
    """
    # [[path|display]] -> display
    text = re.sub(r'\\\[\\\[([^\]|]+)\|([^\]]+)\\\]\\\]', r'\2', text)
    # [[path]] -> path (last segment)
    text = re.sub(r'\\\[\\\[([^\]]+)\\\]\\\]', lambda m: m.group(1).split('/')[-1], text)
    return text


def fix_mermaid(text: str) -> str:
    """Replace mermaid blocks with a TODO comment.

    Mermaid diagrams need to be converted to Typst diagrams manually
    or rendered as SVGs.
    """
    text = re.sub(
        r'```mermaid\n.*?```',
        '// TODO: Convert mermaid diagram to Typst or embed as SVG',
        text,
        flags=re.DOTALL,
    )
    return text


def convert_chapter(src_dir: str, out_name: str):
    """Concatenate all .md files in a chapter dir and convert to .typ."""
    src_path = REPO / src_dir
    if not src_path.exists():
        print(f"  SKIP: {src_dir} (not found)")
        return

    # Get all .md files sorted by name
    md_files = sorted(src_path.glob("*.md"))
    if not md_files:
        print(f"  SKIP: {src_dir} (no .md files)")
        return

    # Concatenate with frontmatter stripped
    combined = []
    for f in md_files:
        content = f.read_text(encoding="utf-8")
        content = strip_frontmatter(content)
        combined.append(content)
        combined.append("\n\n---\n\n")  # page break hint between patterns

    combined_md = "\n".join(combined)

    # Fix mermaid before pandoc (pandoc doesn't handle it)
    combined_md = fix_mermaid(combined_md)

    # Run pandoc
    result = subprocess.run(
        ["pandoc", "-f", "markdown", "-t", "typst", "--wrap=none"],
        input=combined_md,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"  ERROR: {src_dir}: {result.stderr}")
        return

    typst = result.stdout

    # Post-process
    typst = fix_obsidian_callouts(typst)
    typst = fix_wikilinks(typst)

    # Write output
    out_path = OUT / f"{out_name}.typ"
    out_path.write_text(typst, encoding="utf-8")
    print(f"  OK: {out_name}.typ ({len(md_files)} patterns, {len(typst)} chars)")


if __name__ == "__main__":
    print("Converting chapters to Typst...\n")
    for src_dir, out_name in CHAPTERS:
        convert_chapter(src_dir, out_name)
    print("\nDone. Output in typst/")
