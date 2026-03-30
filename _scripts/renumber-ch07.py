"""Delete 0704 (merged into 0702) and shift 0705-0708 → 0704-0707."""

from pathlib import Path

REPO = Path("/home/alonso/code/libro_el")
CH07 = REPO / "07-serving-the-destination"

# (old_number, slug, new_number) — forward order is fine since we deleted 0704 first
RENAMES = [
    (5, "query-patterns-for-analysts", 4),
    (6, "cost-optimization-by-engine", 5),
    (7, "point-in-time-from-events", 6),
    (8, "schema-naming-conventions", 7),
]

md_files = list(REPO.rglob("*.md"))

# Step 0: Delete old 0704
old_0704 = CH07 / "0704-clustering-and-pruning.md"
if old_0704.exists():
    old_0704.unlink()
    print(f"  deleted: {old_0704.name}")

# Step 1: Update all references in content (files still have old names)
for old_num, slug, new_num in RENAMES:
    old_name = f"07{old_num:02d}-{slug}"
    new_name = f"07{new_num:02d}-{slug}"
    old_prefix = f"07{old_num:02d}"
    new_prefix = f"07{new_num:02d}"

    for md_file in md_files:
        try:
            content = md_file.read_text(encoding="utf-8")
        except (FileNotFoundError, UnicodeDecodeError):
            continue
        updated = content
        # Full slug (covers paths and display text)
        updated = updated.replace(old_name, new_name)
        # Bare number in wikilink display text: |0705]]
        updated = updated.replace(f"|{old_prefix}]]", f"|{new_prefix}]]")
        if updated != content:
            md_file.write_text(updated, encoding="utf-8")
            print(f"  refs: {md_file.relative_to(REPO)}")

# Step 2: Also remove references to old 0704-clustering-and-pruning
old_0704_slug = "0704-clustering-and-pruning"
new_0702_slug = "0702-partitioning-for-consumers"
for md_file in md_files:
    try:
        content = md_file.read_text(encoding="utf-8")
    except (FileNotFoundError, UnicodeDecodeError):
        continue
    if old_0704_slug in content:
        # Replace references to the old 0704 with the merged 0702
        updated = content.replace(
            "07-serving-the-destination/0704-clustering-and-pruning|0704-clustering-and-pruning",
            "07-serving-the-destination/0702-partitioning-for-consumers|0702-partitioning-clustering-and-pruning"
        ).replace(
            "07-serving-the-destination/0704-clustering-and-pruning|0704",
            "07-serving-the-destination/0702-partitioning-for-consumers|0702"
        ).replace(
            "0704-clustering-and-pruning",
            "0702-partitioning-for-consumers"
        )
        if updated != content:
            md_file.write_text(updated, encoding="utf-8")
            print(f"  0704->0702: {md_file.relative_to(REPO)}")

# Step 3: Rename files on disk
for old_num, slug, new_num in RENAMES:
    old_path = CH07 / f"07{old_num:02d}-{slug}.md"
    new_path = CH07 / f"07{new_num:02d}-{slug}.md"
    if old_path.exists():
        old_path.rename(new_path)
        print(f"  file: {old_path.name} -> {new_path.name}")
    else:
        print(f"  MISSING: {old_path.name}")

print("\nDone. 0704 merged into 0702, 0705-0708 shifted to 0704-0707.")
