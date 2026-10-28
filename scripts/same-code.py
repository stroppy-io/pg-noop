#!/usr/bin/env python3
"""Compare two revisions of the Rust sources with comments removed.

A comment-only change must leave the code identical: this strips every comment
(line, block, doc) from both sides, collapses whitespace, and reports any file
whose remaining text differs. It is the check that a "no behaviour change" claim
is true.

    scripts/same-code.py <base-ref> [<tip-ref>]     # tip defaults to the worktree
"""

import subprocess
import sys


def rust_code(src: str) -> str:
    """The file with comments removed and whitespace collapsed.

    A lexer, not a regex: `//` inside a string is not a comment, a raw string can
    hold anything, and a lifetime is not a character literal.
    """
    out = []
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        # Line comment.
        if src.startswith("//", i):
            i = src.find("\n", i)
            if i < 0:
                break
            continue
        # Block comment, which nests.
        if src.startswith("/*", i):
            depth = 1
            i += 2
            while i < n and depth:
                if src.startswith("/*", i):
                    depth += 1
                    i += 2
                elif src.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            continue
        # Raw string: r"…", r#"…"#, br#"…"#
        m = i
        if src.startswith("br", m) or src.startswith("rb", m):
            m += 2
        elif c == "r":
            m += 1
        if m > i and m < n and src[m] in '#"':
            hashes = 0
            while m < n and src[m] == "#":
                hashes += 1
                m += 1
            if m < n and src[m] == '"':
                end = src.find('"' + "#" * hashes, m + 1)
                end = n if end < 0 else end + 1 + hashes
                out.append(src[i:end])
                i = end
                continue
        # Byte string / normal string.
        if c == '"' or src.startswith('b"', i):
            if src.startswith('b"', i):
                out.append("b")
                i += 1
            start = i
            i += 1
            while i < n:
                if src[i] == "\\":
                    i += 2
                    continue
                if src[i] == '"':
                    i += 1
                    break
                i += 1
            out.append(src[start:i])
            continue
        # Character literal against a lifetime: '\n' is one, 'a is not.
        if c == "'":
            j = i + 1
            if j < n and src[j] == "\\":
                end = src.find("'", j + 1)
                end = n if end < 0 else end + 1
                out.append(src[i:end])
                i = end
                continue
            if j + 1 < n and src[j + 1] == "'":
                out.append(src[i : j + 2])
                i = j + 2
                continue
        out.append(c)
        i += 1

    return "".join("".join(out).split())


def file_at(ref: str, path: str) -> str:
    if ref == "worktree":
        with open(path, encoding="utf-8") as handle:
            return handle.read()
    return subprocess.run(
        ["git", "show", f"{ref}:{path}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def rust_files(ref: str) -> list:
    if ref == "worktree":
        found = subprocess.run(
            ["git", "ls-files", "*.rs"], check=True, capture_output=True, text=True
        ).stdout.split()
        import glob

        for pattern in ("src/**/*.rs", "src/*.rs", "tests/*.rs"):
            for path in glob.glob(pattern, recursive=True):
                if path not in found:
                    found.append(path)
        return sorted(set(found))
    return sorted(
        subprocess.run(
            ["git", "ls-tree", "-r", "--name-only", ref],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.split("\n")
    )


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    base = sys.argv[1]
    tip = sys.argv[2] if len(sys.argv) > 2 else "worktree"

    base_files = [p for p in rust_files(base) if p.endswith(".rs")]
    tip_files = [p for p in rust_files(tip) if p.endswith(".rs")]
    bad = []
    for path in sorted(set(base_files) | set(tip_files)):
        a = rust_code(file_at(base, path)) if path in base_files else None
        b = rust_code(file_at(tip, path)) if path in tip_files else None
        if a != b:
            bad.append(path)

    if bad:
        print(f"CODE CHANGED in {len(bad)} file(s):")
        for path in bad:
            print(f"  {path}")
        return 1
    print(f"same code: {len(tip_files)} file(s), comments only")
    return 0


if __name__ == "__main__":
    sys.exit(main())
