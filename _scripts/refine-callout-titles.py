"""Second pass: refine callout titles to be punchy 3-8 word headings.

Rules:
1. "Don't X Y Z W..." -> "Don't X Y" or "Don't X Y Z" (keep the verb + direct object)
2. Two-sentence titles -> first sentence only (before the second capital letter after a space)
3. Titles ending with articles/prepositions -> trim them
4. Max 50 chars unless it's a clean phrase
"""

import re
from pathlib import Path

TYPST_DIR = Path("/home/alonso/code/libro_el/typst")
CALLOUT_TYPES = ("ecl-tip", "ecl-warning", "ecl-danger", "ecl-info")
MAX_TITLE = 50

# Words that shouldn't end a title
BAD_ENDINGS = {'a', 'an', 'the', 'of', 'in', 'on', 'at', 'to', 'for', 'by',
               'and', 'or', 'but', 'with', 'from', 'your', 'is', 'are',
               'its', 'as', 'that', 'this', 'their', 'every', 'any'}


def refine_title(title: str, body: str) -> tuple[str, str]:
    """Make title punchier, overflow goes to body."""
    orig_title = title

    # Already short enough and looks like a real title
    if len(title) <= MAX_TITLE and title[-1] in '.!?':
        return title, body

    # Split compound titles: "X Y Z Second sentence here"
    # Find where a second sentence starts (capital after space, not after `)
    m = re.match(r'^(.+?[.!?:])(\s+[A-Z].*)', title)
    if m and len(m.group(1)) >= 15 and len(m.group(1)) <= MAX_TITLE:
        new_title = m.group(1).strip()
        overflow = m.group(2).strip()
        body = overflow + " " + body if body else overflow
        return new_title, body

    # For "Don't" patterns - keep verb + object
    dont_m = re.match(r"^(Don't \w+\s+\S+(?:\s+\S+)?)", title)
    if dont_m:
        candidate = dont_m.group(1)
        # Trim trailing bad words
        words = candidate.split()
        while len(words) > 3 and words[-1].lower().rstrip('.,') in BAD_ENDINGS:
            words.pop()
        new_title = " ".join(words)
        overflow = title[len(candidate):].strip()
        # If we trimmed more from candidate, add those words too
        if len(new_title) < len(candidate):
            extra = candidate[len(new_title):].strip()
            overflow = extra + " " + overflow if overflow else extra
        if overflow:
            body = overflow + " " + body if body else overflow
        return new_title, body

    # General: if too long, try splitting at first period/colon
    for sep in ['. ', ': ']:
        pos = title.find(sep)
        if 10 < pos <= MAX_TITLE:
            new_title = title[:pos+1].strip()
            overflow = title[pos+2:].strip()
            if overflow:
                body = overflow + " " + body if body else overflow
            return new_title, body

    # Fallback: take first 5 words, trim bad endings
    if len(title) > MAX_TITLE:
        words = title.split()
        take = min(5, len(words))
        while take > 2 and words[take-1].lower().rstrip('.,') in BAD_ENDINGS:
            take -= 1
        new_title = " ".join(words[:take])
        overflow = " ".join(words[take:])
        if overflow:
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
            title = title_raw.replace('\\"', '"').replace('\\\\', '\\')

            rest_start = title_end + 1
            if content[rest_start:rest_start+2] == ')[':
                body_start = rest_start + 2
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

                new_title, new_body = refine_title(title, body)

                if new_title != title or new_body != body:
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
            print(f"  {f.name}: {n}")
            total += n
    print(f"\nTotal: {total} titles refined")
