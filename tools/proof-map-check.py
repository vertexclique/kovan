#!/usr/bin/env python3
"""Check that the reclamation model's code map (proofs/tla/reclaim/README.md) names every atomic
operation of the code and nothing else.

The four files of the reclamation (guard.rs, slot.rs, reclaim.rs, retired.rs), outside their
#[cfg(test)] items, are scanned for every atomic operation: the std atomic methods, `fence(`, and
the WordPair methods. The `## Code map` table has one row per model label:

    | label | function | code | kind | step |

`code` holds references `file.rs:N` or `file.rs:N-M`; `kind` is
  atomic    the row models atomic operations: every reference holds at least one, and every
            operation is covered by exactly one atomic row;
  control   the row models control flow or a model-only step: its references may hold no atomic
            operation (it is declared as not one);
  abstract  the row names atomic operations the model deliberately has no step for (a constant
            word, a debug build, an allocation): they count as covered, and the step column says why.

Fails, listing every offender, when an operation is covered by no row or by two atomic rows, when
an atomic row's reference holds no operation, or when a row is malformed. A reference to a file
outside the four (atomic.rs, node.rs) is checked for holding an operation but needs no coverage.
    python3 tools/proof-map-check.py
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "kovan" / "src"
README = ROOT / "proofs" / "tla" / "reclaim" / "README.md"
COVERED = ["guard.rs", "slot.rs", "reclaim.rs", "retired.rs"]
EXTRA = ["atomic.rs", "node.rs"]
KINDS = {"atomic", "control", "abstract", "split"}
MODEL = ROOT / "proofs" / "tla" / "reclaim" / "Reclaim.tla"
# Items skipped as the tests are: the targets without native 128-bit atomics run a fallback pair
# that proofs/tla/reclaim/README.md says is not modelled.
SKIPPED_ATTRS = [
    r"#\[cfg\(test\)\]",
    r"#\[cfg\(not\(any\(target_arch = \"x86_64\", target_arch = \"aarch64\", target_arch = \"s390x\"\)\)\)\]",
]

STD = r"(?:load|store|swap|exchange|compare_exchange|compare_exchange_weak|fetch_add|fetch_sub|fetch_or|fetch_and|fetch_xor|fetch_update)"
PAIR = r"(?:load_lo|load_hi|store_lo|store_hi|exchange_lo|compare_exchange_lo|compare_exchange_hi)"
# Class A: the primitive operations. Class B: the calls of the methods that wrap one (a call site
# is a step of the model, the body of the wrapper is covered once).
OP_A = re.compile(r"\.(?:%s|%s)\s*\(|(?<![\w.])fence\s*\(" % (STD, PAIR))
OP_B = re.compile(
    r"(?<!\w)(?:epoch|slow_counter|max_threads|thread_slots|get_refs_node|batch_next|get_slot_info)\s*\("
    r"|(?<!\w)(?:advance_epoch|inc_slow|dec_slow|alloc_tid|release_tid|set_batch_next|set_slot_info)\s*\("
    r"|\.(?:activate|leave)\(\s*[^)\s]"
)
FN_DEF = re.compile(r"\bfn\s+\w+")


def strip_code(line):
    """The line without its comment and string literals (the operations are in code only)."""
    line = re.sub(r'"(?:\\.|[^"\\])*"', '""', line)
    return line.split("//", 1)[0]


def code_lines(text):
    """(line number, code) of every line outside the #[cfg(test)] items and the comments."""
    out = []
    lines = text.split("\n")
    i = 0
    in_block = False
    while i < len(lines):
        raw = lines[i]
        if any(re.match(r"\s*" + a, raw) for a in SKIPPED_ATTRS):
            # skip the attribute's item: to the close of its brackets, then its `;`, `,` or `}`
            depth = 0
            j = i + 1
            started = False
            while j < len(lines):
                s = strip_code(lines[j])
                if s.strip().startswith("#["):
                    j += 1
                    continue
                for ch in s:
                    if ch in "([{":
                        depth += 1
                        started = True
                    elif ch in ")]}":
                        depth -= 1
                end = s.rstrip()
                if depth <= 0 and (end.endswith(";") or end.endswith(",") or (started and end.endswith("}"))):
                    break
                j += 1
            i = j + 1
            continue
        s = raw
        if in_block:
            if "*/" in s:
                s = s.split("*/", 1)[1]
                in_block = False
            else:
                i += 1
                continue
        if "/*" in s and "*/" not in s.split("/*", 1)[1]:
            s = s.split("/*", 1)[0]
            in_block = True
        out.append((i + 1, strip_code(s)))
        i += 1
    return out


def operations(name):
    """{line: "A" | "B"} of the atomic operations of src/<name>."""
    ops = {}
    for no, code in code_lines((SRC / name).read_text()):
        if FN_DEF.search(code):
            continue
        if OP_A.search(code):
            ops[no] = "A"
        elif OP_B.search(code):
            ops[no] = "B"
    return ops


def parse_map():
    text = README.read_text().split("\n")
    start = next((i for i, l in enumerate(text) if l.strip() == "## Code map"), None)
    if start is None:
        sys.exit("proof-map-check: README.md has no `## Code map` section")
    rows, errors = [], []
    i = start + 1
    while i < len(text) and not text[i].startswith("## "):
        l = text[i]
        if l.startswith("|") and not l.startswith("|---") and not l.startswith("| label"):
            cells = [c.strip() for c in l.strip().strip("|").split("|")]
            if len(cells) != 5:
                errors.append(f"README.md:{i+1}: a map row has {len(cells)} cells, not 5: {l[:60]}")
            else:
                label, func, code, kind, step = cells
                labels = [x.strip().strip("`") for x in label.split(",")]
                if kind not in KINDS:
                    errors.append(f"README.md:{i+1}: `{label}` kind `{kind}` is not one of {sorted(KINDS)}")
                refs = []
                for m in re.finditer(r"`([\w]+\.rs):(\d+)(?:-(\d+))?`", code):
                    a = int(m.group(2))
                    b = int(m.group(3) or a)
                    refs.append((m.group(1), a, b))
                stripped = re.sub(r"`[\w]+\.rs:\d+(?:-\d+)?`", "", code)
                if stripped.replace(",", "").strip() != "":
                    errors.append(f"README.md:{i+1}: `{label}` code cell holds more than file:line references: {code}")
                if kind != "control" and not refs:
                    errors.append(f"README.md:{i+1}: `{label}` is {kind} and names no code line")
                rows.append((i + 1, labels, kind, refs))
        i += 1
    return rows, errors


def model_labels():
    """The labels of the PlusCal algorithm in Reclaim.tla."""
    text = MODEL.read_text()
    a = text.index("(* --algorithm")
    b = text.index("end algorithm")
    return set(re.findall(r"^([A-Za-z][A-Za-z0-9_]*):", text[a:b], re.M))


def main():
    ops = {n: operations(n) for n in COVERED + EXTRA}
    rows, errors = parse_map()
    owners = {(n, o): [] for n in COVERED for o in ops[n]}
    for no, labels, kind, refs in rows:
        name = ", ".join(labels)
        for f, a, b in refs:
            if f not in ops:
                errors.append(f"README.md:{no}: `{name}` names {f}, which is not a scanned file")
                continue
            if b < a:
                errors.append(f"README.md:{no}: `{name}` has an empty range {f}:{a}-{b}")
            held = [o for o in ops[f] if a <= o <= b]
            if kind == "control":
                for o in held:
                    errors.append(f"README.md:{no}: `{name}` is control but {f}:{o} is an atomic operation")
                continue
            if not held:
                errors.append(f"README.md:{no}: `{name}` ({kind}) names {f}:{a}-{b}, which holds no atomic operation")
            for o in held:
                if (f, o) in owners:
                    owners[(f, o)].append((no, name, kind))
    for (f, o), own in sorted(owners.items()):
        who = ", ".join(f"`{l}` ({k}, README.md:{n})" for n, l, k in own)
        if not own:
            errors.append(f"kovan/src/{f}:{o}: an atomic operation no map row covers")
        elif ops[f][o] == "B":
            continue
        elif any(k == "split" for _, _, k in own):
            if len(own) < 2 or any(k != "split" for _, _, k in own):
                errors.append(f"kovan/src/{f}:{o}: split rows must be two or more and alone: {who}")
        elif len(own) > 1:
            errors.append(f"kovan/src/{f}:{o}: an atomic operation covered by {len(own)} rows: {who}")
    in_model = model_labels()
    in_map = {l for _, labels, _, _ in rows for l in labels if not l.startswith("(")}
    for l in sorted(in_model - in_map):
        errors.append(f"Reclaim.tla: label `{l}` has no map row")
    for l in sorted(in_map - in_model):
        errors.append(f"README.md: map row `{l}` is no label of Reclaim.tla")
    if errors:
        print("proof-map-check: the code map of proofs/tla/reclaim/README.md does not match kovan/src:")
        for e in errors:
            print("  " + e)
        print(f"{len(errors)} offender(s)")
        return 1
    total = sum(len(ops[n]) for n in COVERED)
    print(f"proof-map-check: {total} atomic operations in {len(COVERED)} files, {len(rows)} map rows, {len(in_model)} labels, all matched")
    return 0


if __name__ == "__main__":
    sys.exit(main())
