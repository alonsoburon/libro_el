#!/usr/bin/env python3
"""
Swap SVG colors between dark and light themes, or generate adaptive ePub SVGs.

Usage:
  python svg-theme.py light typst/diagrams/    # generate light variants in-place
  python svg-theme.py dark  typst/diagrams/    # generate dark variants in-place
  python svg-theme.py epub  typst/diagrams/ out/epub/diagrams/  # adaptive SVGs for ePub
"""

import sys, os, re, shutil
from pathlib import Path

# Gruvbox dark → light color mapping (bidirectional)
DARK_TO_LIGHT = {
    # Surfaces
    "#1d2021": "#ffffff",  # bg (white for print)
    "#181825": "#ffffff",  # bg (catppuccin variant from ecl-charts.js)
    "#282828": "#f9f5d7",  # surface
    "#3c3836": "#ebdbb2",  # border-dim / table header
    "#504945": "#d5c4a1",  # border
    "#665c54": "#bdae93",  # border heavy

    # Text
    "#ebdbb2": "#3c3836",  # fg
    "#fbf1c7": "#282828",  # fg-bright
    "#d5c4a1": "#504945",  # fg-subtle
    "#bdae93": "#665c54",  # fg-quote
    "#a89984": "#7c6f64",  # fg-dim
    "#928374": "#928374",  # gray (same)
    "#7c6f64": "#a89984",  # bg4/fg4 swap

    # Accents: bright → dark variants for light theme
    "#fb4934": "#cc241d",  # red
    "#b8bb26": "#98971a",  # green
    "#fabd2f": "#d79921",  # yellow
    "#83a598": "#458588",  # blue
    "#d3869b": "#b16286",  # purple
    "#8ec07c": "#689d6a",  # aqua
    "#fe8019": "#d65d0e",  # orange

    # Accent darks stay the same or darken further for light
    "#cc241d": "#9d0006",  # red-accent
    "#98971a": "#79740e",  # green-accent
    "#d79921": "#b57614",  # yellow-accent
    "#458588": "#076678",  # blue-accent
    "#b16286": "#8f3f71",  # purple-accent
    "#689d6a": "#427b58",  # aqua-accent
    "#d65d0e": "#af3a03",  # orange-accent
}

LIGHT_TO_DARK = {v: k for k, v in DARK_TO_LIGHT.items()}

# Catppuccin colors from ecl-charts.js that also need mapping
CATPPUCCIN_TO_GRUVBOX_DARK = {
    "#181825": "#1d2021",
    "#313244": "#3c3836",
    "#585b70": "#504945",
    "#a6adc8": "#a89984",
    "#6c7086": "#928374",
    "#cdd6f4": "#ebdbb2",
    "#ccc":    "#ebdbb2",
}


def detect_theme(svg_content):
    """Detect if SVG is dark or light themed."""
    if "#1d2021" in svg_content or "#181825" in svg_content:
        return "dark"
    elif "#fbf1c7" in svg_content or "#ffffff" in svg_content:
        return "light"
    return "unknown"


def swap_colors(svg_content, color_map):
    """Replace hex colors in SVG using a mapping. Uses placeholders to avoid
    sequential replacement collisions (e.g. A→B then B→C)."""
    # Phase 1: replace all known colors with unique placeholders
    placeholders = {}
    for i, (old, new) in enumerate(color_map.items()):
        ph = f"__ECL_PLACEHOLDER_{i:03d}__"
        placeholders[ph] = new
        svg_content = svg_content.replace(f'"{old}"', f'"{ph}"')
        svg_content = svg_content.replace(f"'{old}'", f"'{ph}'")
    # Phase 2: replace placeholders with final colors
    for ph, new in placeholders.items():
        svg_content = svg_content.replace(f'"{ph}"', f'"{new}"')
        svg_content = svg_content.replace(f"'{ph}'", f"'{new}'")
    return svg_content


def to_static(svg_content, target_theme):
    """Convert SVG to static colors for a target theme."""
    current = detect_theme(svg_content)

    # First normalize catppuccin to gruvbox-dark
    if "#181825" in svg_content:
        svg_content = swap_colors(svg_content, CATPPUCCIN_TO_GRUVBOX_DARK)
        current = "dark"

    if current == target_theme:
        return svg_content
    elif current == "dark" and target_theme == "light":
        return swap_colors(svg_content, DARK_TO_LIGHT)
    elif current == "light" and target_theme == "dark":
        return swap_colors(svg_content, LIGHT_TO_DARK)
    else:
        print(f"  Warning: unknown theme, skipping", file=sys.stderr)
        return svg_content


def to_adaptive(svg_content):
    """Inject CSS media queries for ePub adaptive dark/light mode.
    Light is the default; dark activates via prefers-color-scheme."""

    # First ensure we have the light version as base
    light_svg = to_static(svg_content, "light")

    # Build the CSS style block with dark overrides
    css_pairs = []
    for light_hex, dark_hex in LIGHT_TO_DARK.items():
        css_pairs.append(f'    [fill="{light_hex}"] {{ fill: {dark_hex}; }}')
        css_pairs.append(f'    [stroke="{light_hex}"] {{ stroke: {dark_hex}; }}')

    style_block = (
        '<style>\n'
        '  @media (prefers-color-scheme: dark) {\n'
        + '\n'.join(css_pairs) +
        '\n  }\n'
        '</style>\n'
    )

    # Inject style block right after <svg ...>
    light_svg = re.sub(r'(<svg[^>]*>)', r'\1\n' + style_block, light_svg, count=1)
    return light_svg


def process_directory(mode, src_dir, dst_dir=None):
    src = Path(src_dir)
    dst = Path(dst_dir) if dst_dir else src

    if dst != src:
        dst.mkdir(parents=True, exist_ok=True)

    for svg_file in sorted(src.glob("*.svg")):
        content = svg_file.read_text()
        out_path = dst / svg_file.name

        if mode == "epub":
            result = to_adaptive(content)
        else:
            result = to_static(content, mode)

        out_path.write_text(result)
        print(f"  {svg_file.name} → {mode}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    mode = sys.argv[1]  # "dark", "light", or "epub"
    src_dir = sys.argv[2]
    dst_dir = sys.argv[3] if len(sys.argv) > 3 else None

    assert mode in ("dark", "light", "epub"), f"Unknown mode: {mode}"
    process_directory(mode, src_dir, dst_dir)
    print(f"Done: {mode}")
