#!/usr/bin/env python3
"""Fail if any crate in Cargo.lock was published less than 24 hours ago.

Run: python3 codegen/check_crate_age.py [--hours N]
"""

import argparse
import datetime
import json
import re
import sys
import urllib.request
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--hours", type=float, default=24)
args = parser.parse_args()

lock = (Path(__file__).resolve().parent.parent / "Cargo.lock").read_text()
packages = re.findall(
    r'\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"\nsource = "registry', lock
)
now = datetime.datetime.now(datetime.timezone.utc)
too_new = []
for name, version in packages:
    req = urllib.request.Request(
        f"https://crates.io/api/v1/crates/{name}/{version}",
        headers={"User-Agent": "kernel-rust-sdk crate age check"},
    )
    created = json.load(urllib.request.urlopen(req))["version"]["created_at"]
    published = datetime.datetime.fromisoformat(created.replace("Z", "+00:00"))
    if now - published < datetime.timedelta(hours=args.hours):
        too_new.append(f"{name} {version} (published {created})")

if too_new:
    print(f"crates published less than {args.hours:g} hours ago:")
    for line in too_new:
        print("  " + line)
    print("pin older versions with: cargo update -p <name> --precise <version>")
    sys.exit(1)
print(f"{len(packages)} crates checked; all older than {args.hours:g} hours")
