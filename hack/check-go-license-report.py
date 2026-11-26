"""Fail closed on unreviewed linked Go license classifications."""
import csv
import sys

ALLOWED = {"Apache-2.0", "BSD-2-Clause", "BSD-3-Clause", "MIT", "ISC"}
PROJECT = "github.com/misospace/courier"

with open(sys.argv[1], newline="", encoding="utf-8") as report:
    rows = list(csv.reader(report))

if not rows or not any(len(row) == 3 and row[0] == PROJECT for row in rows):
    sys.exit("Missing Courier license record in Go license report")

unreviewed = []
for row in rows:
    if len(row) != 3:
        unreviewed.append(row)
    elif row[0] == PROJECT:
        if row[2] not in {"AGPL-3.0", "AGPL-3.0-only"}:
            unreviewed.append(row)
    elif row[2] not in ALLOWED:
        unreviewed.append(row)

if unreviewed:
    for row in unreviewed:
        print("Unreviewed license:", row, file=sys.stderr)
    sys.exit(1)
print(f"Reviewed license classifications for {len(rows)} linked Go records")
