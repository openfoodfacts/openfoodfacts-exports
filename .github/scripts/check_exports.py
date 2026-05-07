#!/usr/bin/env python3
"""Check that all Open Food Facts exports exist, have reasonable sizes, and
are recent enough.

Checks:
1. JSONL source files served at static.openfoodfacts.org (and sister sites).
2. Generated Parquet files on Hugging Face Hub.
3. Mobile-app dump on AWS S3 (optional – skipped if not publicly accessible).

Exit code:
  0 – all checks passed
  1 – one or more checks failed
"""

import argparse
import datetime
import sys
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Files that are older than this are reported as stale.
MAX_AGE_HOURS = 48

# Conservative minimum sizes (bytes). If a file is smaller it is almost
# certainly corrupt or empty.
MIN_SIZES: dict[str, int] = {
    # Generated Parquet exports
    "food.parquet": 500_000_000,  # 500 MB
    "beauty.parquet": 10_000_000,  # 10 MB
    "prices.parquet": 50_000_000,  # 50 MB
    # Mobile-app TSV dump
    "openfoodfacts-mobile-dump-products.tsv.gz": 50_000_000,  # 50 MB
    # JSONL source exports
    "openfoodfacts-products.jsonl.gz": 5_000_000_000,  # 5 GB
    "openbeautyfacts-products.jsonl.gz": 50_000_000,  # 50 MB
    "openpetfoodfacts-products.jsonl.gz": 5_000_000,  # 5 MB
    "openproductsfacts-products.jsonl.gz": 1_000_000,  # 1 MB
}

# Hugging Face dataset repos and the files within them that this project
# generates.
HF_REPOS: dict[str, list[str]] = {
    "openfoodfacts/product-database": ["food.parquet", "beauty.parquet"],
    "openfoodfacts/open-prices": ["prices.parquet"],
}

# Direct HTTP URLs for JSONL source files and the mobile-app dump.
HTTP_URLS: list[tuple[str, str]] = [
    (
        "OFF JSONL",
        "https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz",
    ),
    (
        "OBF JSONL",
        "https://static.openbeautyfacts.org/data/openbeautyfacts-products.jsonl.gz",
    ),
    (
        "OPFF JSONL",
        "https://static.openpetfoodfacts.org/data/openpetfoodfacts-products.jsonl.gz",
    ),
    (
        "OPF JSONL",
        "https://static.openproductsfacts.org/data/openproductsfacts-products.jsonl.gz",
    ),
    (
        "Mobile dump",
        "https://openfoodfacts-ds.s3.amazonaws.com/openfoodfacts-mobile-dump-products.tsv.gz",
    ),
]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ExportCheck:
    name: str
    description: str
    size_bytes: Optional[int] = None
    last_modified: Optional[datetime.datetime] = None
    min_size_bytes: Optional[int] = None
    error: Optional[str] = None
    # Whether the file is required (False = warning only)
    required: bool = True

    @property
    def exists(self) -> bool:
        return self.error is None

    @property
    def size_ok(self) -> bool:
        if not self.exists:
            return False
        if self.min_size_bytes is None or self.size_bytes is None:
            # No size information available – skip this check
            return True
        return self.size_bytes >= self.min_size_bytes

    @property
    def age_ok(self) -> bool:
        if not self.exists or self.last_modified is None:
            # No date information available – skip this check
            return True
        age = datetime.datetime.now(tz=datetime.timezone.utc) - self.last_modified
        return age.total_seconds() / 3600 <= MAX_AGE_HOURS

    @property
    def ok(self) -> bool:
        return self.exists and self.size_ok and self.age_ok

    def status_icon(self) -> str:
        if self.ok:
            return "✅"
        if not self.required:
            return "⚠️"
        return "❌"

    def format_size(self) -> str:
        if self.size_bytes is None:
            return "N/A"
        size = float(self.size_bytes)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"

    def format_age(self) -> str:
        if self.last_modified is None:
            return "N/A"
        age = datetime.datetime.now(tz=datetime.timezone.utc) - self.last_modified
        hours = age.total_seconds() / 3600
        if hours < 24:
            return f"{hours:.1f}h ago"
        return f"{age.days}d {int(hours % 24)}h ago"


# ---------------------------------------------------------------------------
# Check helpers
# ---------------------------------------------------------------------------


def http_head_check(
    name: str,
    description: str,
    url: str,
    min_size_bytes: Optional[int] = None,
    required: bool = True,
    timeout: int = 30,
) -> ExportCheck:
    """Perform an HTTP HEAD request and collect size / last-modified info."""
    check = ExportCheck(
        name=name,
        description=description,
        min_size_bytes=min_size_bytes,
        required=required,
    )
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()

        cl = resp.headers.get("Content-Length")
        check.size_bytes = int(cl) if cl else None

        lm = resp.headers.get("Last-Modified")
        if lm:
            check.last_modified = parsedate_to_datetime(lm).astimezone(
                datetime.timezone.utc
            )
    except Exception as exc:
        check.error = str(exc)
    return check


def hf_repo_checks(repo_id: str, filenames: list[str]) -> list[ExportCheck]:
    """Query the Hugging Face Hub tree API and return one check per file."""
    url = f"https://huggingface.co/api/datasets/{repo_id}/tree/main"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        entries = {e["path"]: e for e in resp.json() if e.get("type") == "file"}
    except Exception as exc:
        # Return a failed check for every expected file
        return [
            ExportCheck(
                name=f"{repo_id}/{fn}",
                description=f"HF Hub: {repo_id} → {fn}",
                error=str(exc),
                min_size_bytes=MIN_SIZES.get(fn),
            )
            for fn in filenames
        ]

    checks: list[ExportCheck] = []
    for fn in filenames:
        check = ExportCheck(
            name=fn,
            description=f"HF Hub: {repo_id} → {fn}",
            min_size_bytes=MIN_SIZES.get(fn),
        )
        if fn not in entries:
            check.error = f"File not found in HF repo {repo_id}"
        else:
            entry = entries[fn]
            check.size_bytes = entry.get("size")
            last_commit = entry.get("lastCommit") or {}
            date_str = last_commit.get("date")
            if date_str:
                check.last_modified = datetime.datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                )
        checks.append(check)
    return checks


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def generate_report(checks: list[ExportCheck]) -> tuple[str, bool]:
    """Return (markdown_report, all_required_ok)."""
    now = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    all_ok = all(c.ok for c in checks if c.required)

    lines = [
        f"## Daily Export Check — {now}",
        "",
        f"**Overall status:** {'✅ All checks passed' if all_ok else '❌ Some checks failed'}",
        "",
        "| Status | Export | Size | Last modified | Notes |",
        "|--------|--------|------|---------------|-------|",
    ]

    for c in checks:
        icon = c.status_icon()
        size_str = c.format_size()
        age_str = c.format_age()
        notes: list[str] = []
        if c.error:
            notes.append(f"Error: {c.error}")
        if c.exists and not c.size_ok and c.min_size_bytes is not None:
            min_mb = c.min_size_bytes / 1_000_000
            notes.append(f"Size below minimum ({min_mb:.0f} MB)")
        if c.exists and not c.age_ok:
            notes.append(f"File older than {MAX_AGE_HOURS}h")
        lines.append(
            f"| {icon} | {c.description} | {size_str} | {age_str} | {'; '.join(notes)} |"
        )

    lines += [
        "",
        f"*Stale threshold: {MAX_AGE_HOURS} hours.*",
        "*This report is auto-generated by the [Check Exports workflow](../../actions/workflows/check-exports.yml).*",
    ]
    return "\n".join(lines), all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-file",
        metavar="PATH",
        help="Write the markdown report to this file (default: stdout)",
    )
    args = parser.parse_args()

    checks: list[ExportCheck] = []

    # 1. Hugging Face Hub exports
    for repo_id, filenames in HF_REPOS.items():
        checks.extend(hf_repo_checks(repo_id, filenames))

    # 2. Direct HTTP checks (JSONL source files + mobile dump)
    for label, url in HTTP_URLS:
        filename = url.rsplit("/", 1)[-1]
        # Mobile dump is optional – it may not be publicly accessible
        required = filename != "openfoodfacts-mobile-dump-products.tsv.gz"
        checks.append(
            http_head_check(
                name=filename,
                description=label,
                url=url,
                min_size_bytes=MIN_SIZES.get(filename),
                required=required,
            )
        )

    report, all_ok = generate_report(checks)

    if args.output_file:
        with open(args.output_file, "w") as fh:
            fh.write(report)
        print(f"Report written to {args.output_file}")
    else:
        print(report)

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
