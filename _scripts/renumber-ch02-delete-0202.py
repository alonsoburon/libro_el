"""Delete 0202-snapshot-append and shift 0203-0210 → 0202-0209."""

from pathlib import Path

REPO = Path("/home/alonso/code/libro_el")
CH02 = REPO / "02-full-replace-patterns"

# (old_number, slug, new_number) — forward order is safe here (shifting down)
RENAMES = [
    (3, "partition-swap", 2),
    (4, "staging-swap", 3),
    (5, "scoped-full-replace", 4),
    (6, "rolling-window-replace", 5),
    (7, "sparse-table-extraction", 6),
    (8, "activity-driven-extraction", 7),
    (9, "hash-based-change-detection", 8),
    (10, "partial-column-loading", 9),
]

md_files = list(REPO.rglob("*.md"))

# Step 0: Delete 0202-snapshot-append.md
old_file = CH02 / "0202-snapshot-append.md"
if old_file.exists():
    old_file.unlink()
    print(f"  DELETED: {old_file.name}")

# Step 1: Remove all references to 0202-snapshot-append from content
for md_file in md_files:
    try:
        content = md_file.read_text(encoding="utf-8")
    except (FileNotFoundError, UnicodeDecodeError):
        continue
    updated = content
    # Remove wikilink references like [[02-full-replace-patterns/0202-snapshot-append|0202-snapshot-append]]
    # and [[02-full-replace-patterns/0202-snapshot-append|0202]]
    # These need manual review, so just flag them
    if "0202-snapshot-append" in updated or "0202-snapshot" in updated:
        print(f"  HAS 0202 REF: {md_file.relative_to(REPO)}")

# Step 2: Update all references in content (files still have old names)
for old_num, slug, new_num in RENAMES:
    old_name = f"02{old_num:02d}-{slug}"
    new_name = f"02{new_num:02d}-{slug}"
    old_prefix = f"02{old_num:02d}"
    new_prefix = f"02{new_num:02d}"

    for md_file in md_files:
        try:
            content = md_file.read_text(encoding="utf-8")
        except (FileNotFoundError, UnicodeDecodeError):
            continue
        updated = content
        # Full slug (covers paths and display text)
        updated = updated.replace(old_name, new_name)
        # Bare number in wikilink display text: |0203]]
        updated = updated.replace(f"|{old_prefix}]]", f"|{new_prefix}]]")
        if updated != content:
            md_file.write_text(updated, encoding="utf-8")
            print(f"  refs: {md_file.relative_to(REPO)} ({old_prefix} -> {new_prefix})")

# Step 3: Rename files on disk (forward order — shifting down, no collisions)
for old_num, slug, new_num in RENAMES:
    old_path = CH02 / f"02{old_num:02d}-{slug}.md"
    new_path = CH02 / f"02{new_num:02d}-{slug}.md"
    if old_path.exists():
        old_path.rename(new_path)
        print(f"  file: {old_path.name} -> {new_path.name}")
    else:
        print(f"  MISSING: {old_path.name}")

print("\nDone. 0202-snapshot-append deleted, 0203-0210 shifted to 0202-0209.")
