#!/usr/bin/env python3
"""Checks that every public operation of every kovan crate has a criterion bench.

    python3 tools/bench-coverage.py            every crate; exit status 1 if one lacks a bench
    python3 tools/bench-coverage.py -p kovan   one crate
    python3 tools/bench-coverage.py --list     also print what each crate exposes

The public API comes from rustdoc's JSON (nightly), so it is what a user can call: free functions,
the inherent methods of public types, and the methods of the traits a crate defines. An operation
counts as benched when one of the crate's bench files calls it: `Type::name(`, `.name(`,
`.name::<` or `$T::name(` (a macro the file instantiates) with the type named in that file, or
`name(` for a free function. The few items a
bench cannot sensibly call are listed in EXEMPT with the reason, and nowhere else.
"""
import argparse
import json
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CRATES = ["kovan", "kovan-queue", "kovan-map", "kovan-channel", "kovan-stm", "kovan-mvcc"]

# (crate, "Type::method" or "function") -> why no bench calls it.
EXEMPT = {}


def rustdoc_json(crate):
    env = {**os.environ, "RUSTFLAGS": "-C target-feature=+cmpxchg16b"}
    out = subprocess.run(
        ["cargo", "+nightly", "rustdoc", "-q", "-p", crate, "--lib", "--",
         "-Z", "unstable-options", "--output-format", "json"],
        cwd=ROOT, env=env, check=False, capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"rustdoc failed for {crate}:\n{out.stderr}")
    with open(os.path.join(ROOT, "target", "doc", crate.replace("-", "_") + ".json")) as f:
        return json.load(f)


def public_api(doc):
    """(kind, owner, name) for every public operation of the crate itself."""
    index, paths = doc["index"], doc["paths"]
    local = {i for i, p in paths.items() if p.get("crate_id") == 0}
    api = set()

    def public(item):
        return item.get("visibility") == "public"

    # Functions that belong to an impl or a trait are methods, never free functions.
    owned = set()
    for item in index.values():
        inner = item.get("inner", {})
        for key in ("impl", "trait"):
            if key in inner:
                owned.update(str(m) for m in inner[key].get("items", []))

    for item_id, item in index.items():
        if item_id not in local or not public(item):
            continue
        inner = item.get("inner", {})
        name = item.get("name")
        if "function" in inner and item_id not in owned:
            api.add(("fn", "", name))
        for kind in ("struct", "enum", "union"):
            if kind in inner:
                for impl_id in inner[kind].get("impls", []):
                    impl = index.get(str(impl_id), {}).get("inner", {}).get("impl")
                    if not impl or impl.get("trait") is not None or impl.get("blanket_impl"):
                        continue
                    for m in impl.get("items", []):
                        method = index.get(str(m))
                        if method and public(method) and "function" in method.get("inner", {}):
                            api.add(("method", name, method["name"]))
        if "trait" in inner:
            for m in inner["trait"].get("items", []):
                method = index.get(str(m))
                if method and "function" in method.get("inner", {}):
                    api.add(("trait", name, method["name"]))
    return api


def bench_sources(crate):
    d = os.path.join(ROOT, crate, "benches")
    out = []
    if os.path.isdir(d):
        for root, _, files in os.walk(d):
            for fname in sorted(files):
                if fname.endswith(".rs"):
                    with open(os.path.join(root, fname)) as f:
                        out.append(f.read())
    return out


def covered(kind, owner, name, sources):
    n = re.escape(name)
    for src in sources:
        if kind == "fn":
            if re.search(rf"\b{n}\s*(::<[^()]*>)?\s*\(", src):
                return True
            continue
        o = re.escape(owner)
        # `$Map::name(` in a macro the file instantiates for the types it names.
        call = re.search(
            rf"(\.{n}\s*(::<[^()]*>)?\s*\(|\b{o}\s*(::<[^()]*>)?::{n}\s*\(|\b{o}\s*>::{n}\s*\(|\$\w+\s*(::<[^()]*>)?::{n}\s*\()",
            src)
        if call and (kind == "trait" or re.search(rf"\b{re.escape(owner)}\b", src)):
            return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-p", dest="crates", action="append")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()
    missing_any = False
    for crate in args.crates or CRATES:
        api = public_api(rustdoc_json(crate))
        sources = bench_sources(crate)
        missing = []
        for kind, owner, name in sorted(api):
            key = f"{owner}::{name}" if owner else name
            if (crate, key) in EXEMPT:
                continue
            if not covered(kind, owner, name, sources):
                missing.append(key)
            elif args.list:
                print(f"  {crate}: {key}")
        print(f"{crate}: {len(api) - len(missing)} of {len(api)} public operations benched")
        for key in missing:
            print(f"  missing: {key}")
        missing_any |= bool(missing)
    sys.exit(1 if missing_any else 0)


if __name__ == "__main__":
    main()
