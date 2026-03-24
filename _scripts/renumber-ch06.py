"""Shift 0602-0614 → 0603-0615 to free up 0602 for the health table pattern."""

from pathlib import Path

REPO = Path("/home/alonso/code/libro_el")
CH06 = REPO / "06-operating-the-pipeline"

# (old_number, slug, new_number) — reverse order to avoid collisions
RENAMES = [
    (14, "recovery-from-corruption", 15),
    (13, "reconciliation-patterns", 14),
    (12, "duplicate-detection", 13),
    (11, "partial-failure-recovery", 12),
    (10, "backfill-strategies", 11),
    (9, "extraction-status-gates", 10),
    (8, "data-contracts", 9),
    (7, "tiered-freshness", 8),
    (6, "source-system-etiquette", 7),
    (5, "scheduling-and-dependencies", 6),
    (4, "alerting-and-notifications", 5),
    (3, "sla-management", 4),
    (2, "cost-monitoring", 3),
]

md_files = list(REPO.rglob("*.md"))

# Step 1: Update all references in content (files still have old names)
for old_num, slug, new_num in RENAMES:
    old_name = f"06{old_num:02d}-{slug}"
    new_name = f"06{new_num:02d}-{slug}"
    old_prefix = f"06{old_num:02d}"
    new_prefix = f"06{new_num:02d}"

    for md_file in md_files:
        try:
            content = md_file.read_text(encoding="utf-8")
        except (FileNotFoundError, UnicodeDecodeError):
            continue
        updated = content
        # Full slug (covers paths and display text like |0602-cost-monitoring]])
        updated = updated.replace(old_name, new_name)
        # Bare number in wikilink display text: |0602]]
        updated = updated.replace(f"|{old_prefix}]]", f"|{new_prefix}]]")
        if updated != content:
            md_file.write_text(updated, encoding="utf-8")
            print(f"  refs: {md_file.relative_to(REPO)}")

# Step 2: Rename files on disk
for old_num, slug, new_num in RENAMES:
    old_path = CH06 / f"06{old_num:02d}-{slug}.md"
    new_path = CH06 / f"06{new_num:02d}-{slug}.md"
    if old_path.exists():
        old_path.rename(new_path)
        print(f"  file: {old_path.name} -> {new_path.name}")
    else:
        print(f"  MISSING: {old_path.name}")

print("\nDone. 0602 is free.")
