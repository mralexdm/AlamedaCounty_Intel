#!/usr/bin/env python3
"""
NextKey Home Offer — Alameda County Motivated Seller Lead Scraper
=================================================================

Pulls recently recorded motivated-seller / financial-distress public records from
Alameda County, California, normalizes + classifies them, enriches them with
parcel/assessor/open-data address information, scores each lead, and exports
CRM-ready files for cold-call outreach.

Primary source (clerk / recorder index):
    https://www.acgov.org/auditor/clerk/opr/

Enrichment sources:
    https://data.acgov.org                          (Alameda County Open Data Hub / ArcGIS)
    https://www.acassessor.org/homeowners/assessors-records/
    https://www.acassessor.org/homeowners/parcel-viewer/

Run:
    python src/scraper.py            # live scrape
    python src/scraper.py --demo     # sample leads (no live portal)

Outputs:
    data/output.json                 # structured dataset (read by dashboard/index.html)
    exports/ghl_export.csv           # CRM import file

Author: NextKey Home Offer / MADM
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

# --- Third-party (see requirements.txt). Imported defensively so the module still
#     loads (and logs a clear message) if an optional dependency is missing. -------
try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore

try:
    from bs4 import BeautifulSoup  # noqa: F401  (used by static-page parsing helpers)
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

# Playwright is only required if the clerk portal needs real browser automation.
# Guarded so the rest of the pipeline can still run (e.g. enrichment-only / tests).
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except Exception:  # pragma: no cover
    async_playwright = None  # type: ignore
    PWTimeoutError = Exception  # type: ignore
    PLAYWRIGHT_AVAILABLE = False


# =============================================================================
# CONFIG
# =============================================================================

# /src/scraper.py -> parents[1] == project root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# --- Output destinations --------------------------------------------------------
# Primary JSON dataset (read by dashboard/index.html via ../data/output.json).
OUTPUT_JSON_PATHS = [
    PROJECT_ROOT / "data" / "output.json",
]
OUTPUT_CSV_PATH = PROJECT_ROOT / "exports" / "ghl_export.csv"
LOG_DIR = PROJECT_ROOT / "logs"

# --- Runtime knobs (env-overridable so GitHub Actions / cron can tune them) ------
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30"))
PAGE_TIMEOUT_MS = int(os.environ.get("PAGE_TIMEOUT_MS", "45000"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_BACKOFF = float(os.environ.get("RETRY_BACKOFF", "2.0"))  # exponential base seconds
REQUEST_PAUSE = float(os.environ.get("REQUEST_PAUSE", "1.5"))  # polite delay between hits
HEADLESS = os.environ.get("HEADLESS", "1") not in ("0", "false", "False")
MAX_RESULTS_PER_CATEGORY = int(os.environ.get("MAX_RESULTS_PER_CATEGORY", "500"))

USER_AGENT = os.environ.get(
    "SCRAPER_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 NextKeyLeadBot/1.0",
)

# --- Source endpoints -----------------------------------------------------------
CLERK_PORTAL_URL = "https://www.acgov.org/auditor/clerk/opr/"
SOURCE_LABEL = "Alameda County public records"

# Selectors / form hooks for the OPR index search. These are intentionally
# heuristic + centralized: the scraper tries each candidate in order and logs
# loudly if none match (portal-structure-change detection). Adjust after
# inspecting the live page. See README "Adapting to the live portal".
CLERK_SELECTORS = {
    # Links/buttons that lead from the landing page into the actual index search.
    "enter_search_candidates": [
        "a:has-text('Search')",
        "a:has-text('Official Public Records')",
        "a:has-text('Index Search')",
        "a:has-text('Accept')",
        "a:has-text('Agree')",
        "button:has-text('Accept')",
        "button:has-text('I Agree')",
    ],
    # Date range inputs on the search form.
    "date_from_candidates": [
        "input[name*='from' i]",
        "input[id*='from' i]",
        "input[name*='begin' i]",
        "input[name*='start' i]",
    ],
    "date_to_candidates": [
        "input[name*='to' i]",
        "input[id*='to' i]",
        "input[name*='end' i]",
    ],
    # Document-type filter (select or text input).
    "doctype_candidates": [
        "select[name*='doc' i]",
        "select[id*='type' i]",
        "input[name*='doctype' i]",
        "input[name*='document' i]",
    ],
    # Submit / search button.
    "submit_candidates": [
        "input[type='submit']",
        "button[type='submit']",
        "button:has-text('Search')",
        "input[value*='Search' i]",
    ],
    # Results table rows.
    "results_table_candidates": [
        "table.results tr",
        "table#results tr",
        "table[summary*='result' i] tr",
        "div.search-results tr",
        "table tr",
    ],
}

# Alameda County Open Data Hub (ArcGIS). The parcel layer URL frequently changes;
# keep it configurable. The scraper will skip enrichment gracefully if unreachable.
# Example shape: https://<host>/arcgis/rest/services/<svc>/FeatureServer/<n>/query
ASSESSOR_PARCEL_ARCGIS_URL = os.environ.get(
    "ASSESSOR_PARCEL_ARCGIS_URL",
    # Best-effort default; override with a verified layer from data.acgov.org.
    "https://services.arcgis.com/Alameda/parcels/FeatureServer/0/query",
)
# Field names within the parcel layer (override to match the real schema).
PARCEL_FIELDS = {
    "owner": os.environ.get("PARCEL_FIELD_OWNER", "OwnerName"),
    "apn": os.environ.get("PARCEL_FIELD_APN", "APN"),
    "situs": os.environ.get("PARCEL_FIELD_SITUS", "SitusAddress"),
    "situs_city": os.environ.get("PARCEL_FIELD_SITUS_CITY", "SitusCity"),
    "situs_zip": os.environ.get("PARCEL_FIELD_SITUS_ZIP", "SitusZip"),
    "mail": os.environ.get("PARCEL_FIELD_MAIL", "MailAddress"),
    "mail_city": os.environ.get("PARCEL_FIELD_MAIL_CITY", "MailCity"),
    "mail_state": os.environ.get("PARCEL_FIELD_MAIL_STATE", "MailState"),
    "mail_zip": os.environ.get("PARCEL_FIELD_MAIL_ZIP", "MailZip"),
}

# =============================================================================
# LEAD TYPE TAXONOMY
# =============================================================================
# Each category maps Alameda County document-name variants -> a normalized
# motivated-seller category, a human label, and the motivation flag(s) it raises.
# `patterns` are matched case-insensitively against the recorded document type.
# Order matters: more specific categories first.

@dataclass
class Category:
    cat: str
    cat_label: str
    patterns: list[str]
    flags: list[str]
    pre_foreclosure: bool = False  # used by the scoring model

CATEGORIES: list[Category] = [
    Category(
        cat="notice_trustee_sale",
        cat_label="Notice of Trustee Sale",
        patterns=[r"notice of trustee'?s? sale", r"\bnots\b", r"trustee'?s? sale",
                  r"notice of sale"],
        flags=["Notice of trustee sale", "Pre-foreclosure"],
        pre_foreclosure=True,
    ),
    Category(
        cat="notice_of_default",
        cat_label="Notice of Default",
        patterns=[r"notice of default", r"\bnod\b", r"default.*election to sell"],
        flags=["Notice of default", "Pre-foreclosure"],
        pre_foreclosure=True,
    ),
    Category(
        cat="lis_pendens",
        cat_label="Lis Pendens / Pre-Foreclosure",
        patterns=[r"lis pendens", r"notice of pendency", r"pendency of action",
                  r"notice of action"],
        flags=["Lis pendens", "Pre-foreclosure"],
        pre_foreclosure=True,
    ),
    Category(
        cat="tax_lien",
        cat_label="Tax Lien / Tax Default",
        patterns=[r"tax lien", r"tax default", r"tax deed", r"delinquent tax",
                  r"notice of power to sell", r"irs lien", r"internal revenue",
                  r"federal tax lien", r"state tax lien", r"franchise tax",
                  r"corporate tax"],
        flags=["Tax lien"],
    ),
    Category(
        cat="mechanic_lien",
        cat_label="Mechanic's Lien",
        patterns=[r"mechanic'?s? lien", r"claim of lien", r"construction lien"],
        flags=["Mechanic lien"],
    ),
    Category(
        cat="hoa_lien",
        cat_label="HOA / Assessment Lien",
        patterns=[r"\bhoa\b", r"homeowners? association", r"assessment lien",
                  r"notice of delinquent assessment", r"common interest"],
        flags=["HOA lien"],
    ),
    Category(
        cat="judgment_lien",
        cat_label="Judgment / Abstract of Judgment",
        patterns=[r"abstract of judgment", r"judgment lien", r"certified judgment",
                  r"domestic judgment", r"sister state judgment", r"\bjudgment\b"],
        flags=["Judgment lien"],
    ),
    Category(
        cat="federal_lien",
        cat_label="Federal / IRS / Corporate Lien",
        patterns=[r"federal lien", r"notice of federal", r"corporate lien"],
        flags=["Tax lien"],
    ),
    Category(
        cat="general_lien",
        cat_label="General Lien",
        patterns=[r"\blien\b"],  # catch-all; runs late so specific liens win first
        flags=["Judgment lien"],
    ),
    Category(
        cat="probate",
        cat_label="Probate / Estate",
        patterns=[r"probate", r"estate of", r"affidavit of death", r"decree of distribution",
                  r"\bdeceased\b", r"administrator", r"executor", r"letters testamentary"],
        flags=["Probate / estate"],
    ),
]

# Document types to actively request from the portal (mapped to county wording in
# the search step). Used as search hints / filters where the portal supports it.
SEARCH_DOCTYPE_HINTS = [
    "LIS PENDENS",
    "NOTICE OF DEFAULT",
    "NOTICE OF TRUSTEE SALE",
    "TAX LIEN",
    "FEDERAL TAX LIEN",
    "ABSTRACT OF JUDGMENT",
    "MECHANICS LIEN",
    "MECHANIC LIEN",
    "LIEN",
    "PROBATE",
]

# Flags that count as "distress flags" for scoring (excludes meta signals like
# "New this week" / "Absentee owner" / "LLC / corp owner").
DISTRESS_FLAGS = {
    "Lis pendens", "Pre-foreclosure", "Notice of default", "Notice of trustee sale",
    "Judgment lien", "Tax lien", "Mechanic lien", "HOA lien", "Probate / estate",
}
PRE_FORECLOSURE_FLAGS = {"Lis pendens", "Pre-foreclosure", "Notice of default",
                         "Notice of trustee sale"}

BUSINESS_SUFFIXES = {
    "LLC", "L.L.C", "INC", "INC.", "CORP", "CORPORATION", "CO", "COMPANY", "LP",
    "LLP", "LTD", "TRUST", "PARTNERS", "ASSOCIATES", "HOLDINGS", "GROUP", "ENTERPRISES",
    "PROPERTIES", "INVESTMENTS", "BANK", "N.A", "NA", "FUND", "CAPITAL", "REALTY",
}
NAME_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V", "JR.", "SR."}


# =============================================================================
# LOGGING
# =============================================================================

def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("nextkey_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    try:
        fh = logging.FileHandler(LOG_DIR / "scrape.log", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception:
        pass  # never let logging setup crash the run

    return logger

log = setup_logging()

# A dedicated "skipped records" trail (per spec: log skipped records with reasons).
SKIPPED: list[dict[str, str]] = []

def skip_record(reason: str, ctx: Any = None) -> None:
    entry = {"reason": reason, "context": _short(ctx)}
    SKIPPED.append(entry)
    log.warning("SKIPPED record (%s): %s", reason, _short(ctx))

def _short(obj: Any, n: int = 200) -> str:
    try:
        s = json.dumps(obj, default=str) if not isinstance(obj, str) else obj
    except Exception:
        s = str(obj)
    return s if len(s) <= n else s[: n - 1] + "…"


# =============================================================================
# RETRY HELPERS
# =============================================================================

def with_retries(what: str = "operation") -> Callable:
    """Decorator: retry a *sync* callable up to MAX_RETRIES with backoff."""
    def deco(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc: Optional[Exception] = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    wait = RETRY_BACKOFF * attempt
                    log.warning("%s failed (attempt %d/%d): %s — retrying in %.1fs",
                                what, attempt, MAX_RETRIES, exc, wait)
                    time.sleep(wait)
            log.error("%s failed after %d attempts: %s", what, MAX_RETRIES, last_exc)
            return None
        return wrapper
    return deco

async def async_retry(coro_factory: Callable, what: str = "operation",
                      default: Any = None) -> Any:
    """Retry an *async* operation. `coro_factory` must return a fresh awaitable."""
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return await coro_factory()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = RETRY_BACKOFF * attempt
            log.warning("%s failed (attempt %d/%d): %s — retrying in %.1fs",
                        what, attempt, MAX_RETRIES, exc, wait)
            await asyncio.sleep(wait)
    log.error("%s failed after %d attempts: %s", what, MAX_RETRIES, last_exc)
    return default


# =============================================================================
# DATA CLEANING / NORMALIZATION UTILITIES
# =============================================================================

_WS_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s,&.'-]")

def norm_ws(s: Optional[str]) -> str:
    if not s:
        return ""
    return _WS_RE.sub(" ", str(s)).strip()

def strip_punct(s: str) -> str:
    return _PUNCT_RE.sub("", s)

def clean_text(s: Optional[str]) -> str:
    return norm_ws(s).replace(" ", " ")

_DATE_FORMATS = [
    "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%y",
    "%b %d, %Y", "%B %d, %Y", "%d-%b-%Y", "%m.%d.%Y",
]

def parse_date(value: Optional[str]) -> str:
    """Best-effort -> ISO 'YYYY-MM-DD'. Returns raw string if unparseable."""
    if not value:
        return ""
    raw = clean_text(value)
    if not raw:
        return ""
    # Already ISO?
    if re.match(r"^\d{4}-\d{2}-\d{2}", raw):
        return raw[:10]
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Try to pull a date substring out of noisier text.
    m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", raw)
    if m:
        return parse_date(m.group(1))
    log.debug("Could not parse date '%s' — keeping raw", raw)
    return raw  # keep raw value rather than dropping the record

_AMOUNT_RE = re.compile(r"-?\$?\s*([\d,]+(?:\.\d{1,2})?)")

def parse_amount(value: Optional[str]) -> Optional[float]:
    """Parse a monetary value -> float. Returns None if not a usable number."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw or raw in {"-", "N/A", "NA", "$0", "$0.00"}:
        return None
    m = _AMOUNT_RE.search(raw)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except (ValueError, AttributeError):
        return None


def is_business_name(name: str) -> bool:
    if not name:
        return False
    upper = strip_punct(name).upper()
    # Estate / trust style owners aren't real person names — treat as non-person so
    # First/Last stay blank and the full owner string is preserved.
    if re.search(r"\bESTATE OF\b|\bTRUST\b|\bLIVING TRUST\b|\bFAMILY TRUST\b", upper):
        return True
    tokens = re.split(r"[\s,]+", upper)
    return any(tok in BUSINESS_SUFFIXES for tok in tokens if tok)


def normalize_owner(raw_name: Optional[str]) -> dict[str, Any]:
    """
    Normalize an owner string and produce lookup variants.

    Returns a dict:
        {
          "display":  cleaned display name,
          "is_business": bool,
          "first": str, "last": str, "middle": str, "suffix": str,
          "variants": ["FIRST LAST", "LAST FIRST", "LAST, FIRST", ...]
        }
    """
    name = clean_text(raw_name).upper()
    name = name.replace("\t", " ")
    if not name:
        return {"display": "", "is_business": False, "first": "", "last": "",
                "middle": "", "suffix": "", "variants": []}

    business = is_business_name(name)
    if business:
        compact = norm_ws(name)
        # Distinguish a genuine corporate entity from an estate/trust so the
        # "LLC / corp owner" flag is only applied to actual companies.
        is_estate = bool(re.search(r"\bESTATE OF\b|\bTRUST\b", compact.upper()))
        return {"display": compact, "is_business": True, "is_corp": not is_estate,
                "first": "", "last": "", "middle": "", "suffix": "",
                "variants": [compact]}

    # Handle "LAST, FIRST MIDDLE" vs "FIRST MIDDLE LAST".
    suffix = ""
    work = name.replace(".", "")
    # extract suffix
    tokens = [t for t in re.split(r"[\s,]+", work) if t]
    if tokens and tokens[-1] in {s.replace(".", "") for s in NAME_SUFFIXES}:
        suffix = tokens.pop()

    first = middle = last = ""
    if "," in name:
        # "LAST, FIRST MIDDLE"
        last_part, _, rest = name.partition(",")
        last = norm_ws(strip_punct(last_part))
        rest_tokens = [t for t in re.split(r"\s+", norm_ws(rest)) if t and t not in NAME_SUFFIXES]
        if rest_tokens:
            first = rest_tokens[0]
            if len(rest_tokens) > 1:
                middle = " ".join(rest_tokens[1:])
    else:
        ptokens = [t for t in tokens]
        if len(ptokens) == 1:
            last = ptokens[0]
        elif len(ptokens) == 2:
            first, last = ptokens
        else:
            first = ptokens[0]
            last = ptokens[-1]
            middle = " ".join(ptokens[1:-1])

    first, middle, last = (norm_ws(x) for x in (first, middle, last))
    display = norm_ws(f"{first} {middle} {last} {suffix}").title()

    variants = set()
    if first and last:
        variants.add(f"{first} {last}")
        variants.add(f"{last} {first}")
        variants.add(f"{last}, {first}")
        if middle:
            variants.add(f"{first} {middle} {last}")
            variants.add(f"{last}, {first} {middle}")
            # middle initial form
            variants.add(f"{first} {middle[0]} {last}")
    elif last:
        variants.add(last)
    variants = {norm_ws(v) for v in variants if v}

    return {
        "display": display or norm_ws(name).title(),
        "is_business": False,
        "first": first.title(), "last": last.title(),
        "middle": middle.title(), "suffix": suffix.title(),
        "variants": sorted(variants),
    }


def classify_doc_type(doc_type: str) -> Optional[Category]:
    """Map a recorded document type string to one of our motivated-seller categories."""
    if not doc_type:
        return None
    hay = doc_type.lower()
    for category in CATEGORIES:
        for pat in category.patterns:
            if re.search(pat, hay):
                return category
    return None


# =============================================================================
# RAW RECORD MODEL
# =============================================================================

@dataclass
class LeadRecord:
    doc_num: str = ""
    doc_type: str = ""
    filed: str = ""
    cat: str = ""
    cat_label: str = ""
    owner: str = ""
    grantee: str = ""
    amount: str = ""
    legal: str = ""
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = "CA"
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = ""
    mail_zip: str = ""
    phone: str = ""
    email: str = ""
    clerk_url: str = ""
    flags: list[str] = field(default_factory=list)
    score: int = 0

    # internal / not exported to JSON record schema
    _owner_norm: dict = field(default_factory=dict, repr=False)
    _amount_val: Optional[float] = field(default=None, repr=False)

    def to_json(self) -> dict:
        d = asdict(self)
        d.pop("_owner_norm", None)
        d.pop("_amount_val", None)
        return d

    def dedupe_key(self) -> tuple:
        return (
            (self.doc_num or "").strip().upper(),
            (self.filed or "").strip(),
            norm_ws(self.owner).upper(),
        )


# =============================================================================
# CLERK PORTAL SCRAPER (Playwright, async)
# =============================================================================

class ClerkScraper:
    """
    Resilient scraper for the Alameda County Official Public Records (OPR) index.

    Strategy:
      1. Open the portal landing page.
      2. Click through any disclaimer / "Accept" / "Search" entry point.
      3. Locate the date-range + document-type search form heuristically.
      4. Submit one search per document-type hint for the lookback window.
      5. Parse result rows into LeadRecord stubs.

    Every step is wrapped so that an unexpected page structure produces a clear log
    line and an empty result set rather than an exception.
    """

    def __init__(self, date_from: datetime, date_to: datetime):
        self.date_from = date_from
        self.date_to = date_to
        self.records: list[LeadRecord] = []

    async def run(self) -> list[LeadRecord]:
        if not PLAYWRIGHT_AVAILABLE:
            log.error("Playwright is not installed — cannot drive the clerk portal. "
                      "Install with: pip install playwright && playwright install chromium")
            return []

        async with async_playwright() as pw:
            browser = None
            try:
                browser = await pw.chromium.launch(headless=HEADLESS)
                context = await browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={"width": 1366, "height": 900},
                )
                page = await context.new_page()
                page.set_default_timeout(PAGE_TIMEOUT_MS)

                ok = await async_retry(lambda: self._open_portal(page),
                                       what="open clerk portal", default=False)
                if not ok:
                    log.error("Could not open / enter the clerk portal search. "
                              "Verify CLERK_PORTAL_URL and CLERK_SELECTORS against the live site.")
                    return []

                for hint in SEARCH_DOCTYPE_HINTS:
                    recs = await async_retry(
                        lambda h=hint: self._search_doctype(page, h),
                        what=f"search '{hint}'", default=[],
                    )
                    if recs:
                        self.records.extend(recs)
                    await asyncio.sleep(REQUEST_PAUSE)

            except Exception as exc:  # noqa: BLE001
                log.error("Clerk scraper aborted: %s", exc)
            finally:
                if browser:
                    try:
                        await browser.close()
                    except Exception:
                        pass

        log.info("Clerk scraper collected %d raw row(s) before classification.",
                 len(self.records))
        return self.records

    async def _open_portal(self, page) -> bool:
        await page.goto(CLERK_PORTAL_URL, wait_until="domcontentloaded")
        await asyncio.sleep(1.0)
        # Click through any disclaimer / entry links we can find.
        for sel in CLERK_SELECTORS["enter_search_candidates"]:
            try:
                loc = page.locator(sel).first
                if await loc.count() and await loc.is_visible():
                    await loc.click(timeout=5000)
                    await page.wait_for_load_state("domcontentloaded")
                    await asyncio.sleep(1.0)
                    log.info("Clicked portal entry element: %s", sel)
                    break
            except Exception:
                continue
        # Confirm we landed on something resembling a search form.
        has_form = await self._first_match(page, CLERK_SELECTORS["date_from_candidates"]) is not None
        if not has_form:
            log.warning("No recognizable date-range search form found after entry. "
                        "Portal structure may have changed.")
        return True  # we still let the per-doctype search try (and log) individually

    async def _first_match(self, page, selectors: list[str]):
        for sel in selectors:
            try:
                loc = page.locator(sel).first
                if await loc.count():
                    return loc
            except Exception:
                continue
        return None

    async def _search_doctype(self, page, doctype: str) -> list[LeadRecord]:
        df = self.date_from.strftime("%m/%d/%Y")
        dt = self.date_to.strftime("%m/%d/%Y")

        date_from = await self._first_match(page, CLERK_SELECTORS["date_from_candidates"])
        date_to = await self._first_match(page, CLERK_SELECTORS["date_to_candidates"])
        if not date_from:
            log.warning("Search form not available for '%s' (no date input). Skipping.", doctype)
            return []

        try:
            await date_from.fill(df)
            if date_to:
                await date_to.fill(dt)
        except Exception as exc:
            log.warning("Could not fill date range for '%s': %s", doctype, exc)

        # Document-type filter (select or text).
        dtype_loc = await self._first_match(page, CLERK_SELECTORS["doctype_candidates"])
        if dtype_loc is not None:
            try:
                tag = await dtype_loc.evaluate("el => el.tagName.toLowerCase()")
                if tag == "select":
                    try:
                        await dtype_loc.select_option(label=re.compile(doctype, re.I))
                    except Exception:
                        # Fall back to partial value match
                        await dtype_loc.select_option(label=doctype.title())
                else:
                    await dtype_loc.fill(doctype)
            except Exception as exc:
                log.debug("Doc-type filter not applied for '%s': %s", doctype, exc)

        submit = await self._first_match(page, CLERK_SELECTORS["submit_candidates"])
        if submit is None:
            log.warning("No submit button found for '%s'.", doctype)
            return []
        try:
            await submit.click()
            await page.wait_for_load_state("networkidle", timeout=PAGE_TIMEOUT_MS)
        except PWTimeoutError:
            log.debug("networkidle timeout after submitting '%s' (continuing).", doctype)
        await asyncio.sleep(1.0)

        return await self._parse_results(page, doctype)

    async def _parse_results(self, page, doctype: str) -> list[LeadRecord]:
        rows = []
        for sel in CLERK_SELECTORS["results_table_candidates"]:
            try:
                loc = page.locator(sel)
                count = await loc.count()
                if count and count > 1:
                    rows = await loc.all()
                    break
            except Exception:
                continue
        if not rows:
            log.info("No result rows parsed for '%s' (empty result set or unknown layout).",
                     doctype)
            return []

        out: list[LeadRecord] = []
        for row in rows[: MAX_RESULTS_PER_CATEGORY]:
            try:
                cells = await row.locator("td").all_inner_texts()
            except Exception:
                continue
            cells = [clean_text(c) for c in cells]
            if not any(cells):
                continue
            rec = self._row_to_record(cells, row, doctype)
            if rec:
                out.append(rec)
        log.info("Parsed %d row(s) for doc-type '%s'.", len(out), doctype)
        return out

    def _row_to_record(self, cells: list[str], row, doctype_hint: str) -> Optional[LeadRecord]:
        """
        Heuristic column mapping. County result tables vary; we look for the most
        date-like, number-like, and name-like cells rather than assuming positions.
        """
        if len(cells) < 2:
            return None
        rec = LeadRecord()
        # Document number: first cell that looks like an instrument number.
        for c in cells:
            if re.search(r"\d{4,}", c) and not re.search(r"[/-]\d{2,4}$", c):
                rec.doc_num = c
                break
        # Filed date: first date-like cell.
        for c in cells:
            if re.search(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", c) or re.match(r"\d{4}-\d{2}-\d{2}", c):
                rec.filed = parse_date(c)
                break
        # Doc type: cell containing letters and matching a category, else the hint.
        rec.doc_type = next(
            (c for c in cells if classify_doc_type(c)),
            doctype_hint.title(),
        )
        # Owner / grantor: the longest mostly-alphabetic cell.
        alpha_cells = sorted(
            [c for c in cells if re.search(r"[A-Za-z]", c) and not classify_doc_type(c)],
            key=len, reverse=True,
        )
        if alpha_cells:
            rec.owner = alpha_cells[0]
            if len(alpha_cells) > 1:
                rec.grantee = alpha_cells[1]
        rec.clerk_url = CLERK_PORTAL_URL
        if not rec.doc_num and not rec.owner:
            skip_record("clerk row had neither doc number nor owner", cells)
            return None
        return rec


# =============================================================================
# ENRICHMENT (Assessor / Open Data)
# =============================================================================

class AssessorEnricher:
    """
    Enrich lead records with property + mailing address from Alameda County
    open data (ArcGIS parcel layer). Degrades gracefully if the endpoint is
    unreachable or the schema differs — records simply pass through un-enriched.
    """

    def __init__(self):
        self.session = None
        if requests is not None:
            self.session = requests.Session()
            self.session.headers.update({"User-Agent": USER_AGENT})
        self._cache: dict[str, Optional[dict]] = {}

    def enrich(self, rec: LeadRecord) -> LeadRecord:
        if self.session is None:
            return rec
        owner_norm = rec._owner_norm or normalize_owner(rec.owner)
        variants = owner_norm.get("variants") or []
        if not variants:
            return rec

        parcel = None
        for variant in variants:
            parcel = self._lookup_owner(variant)
            if parcel:
                break

        if not parcel:
            return rec

        attrs = parcel.get("attributes", parcel)
        rec.prop_address = rec.prop_address or clean_text(attrs.get(PARCEL_FIELDS["situs"]))
        rec.prop_city = rec.prop_city or clean_text(attrs.get(PARCEL_FIELDS["situs_city"]))
        rec.prop_zip = rec.prop_zip or clean_text(attrs.get(PARCEL_FIELDS["situs_zip"]))
        rec.mail_address = rec.mail_address or clean_text(attrs.get(PARCEL_FIELDS["mail"]))
        rec.mail_city = rec.mail_city or clean_text(attrs.get(PARCEL_FIELDS["mail_city"]))
        rec.mail_state = rec.mail_state or clean_text(attrs.get(PARCEL_FIELDS["mail_state"]))
        rec.mail_zip = rec.mail_zip or clean_text(attrs.get(PARCEL_FIELDS["mail_zip"]))
        if rec.prop_address and not rec.prop_state:
            rec.prop_state = "CA"
        return rec

    @with_retries("ArcGIS parcel lookup")
    def _query_arcgis(self, where: str) -> Optional[dict]:
        params = {
            "where": where,
            "outFields": "*",
            "f": "json",
            "returnGeometry": "false",
            "resultRecordCount": 1,
        }
        resp = self.session.get(ASSESSOR_PARCEL_ARCGIS_URL, params=params,
                                timeout=HTTP_TIMEOUT)
        if resp.status_code == 429:
            raise RuntimeError("rate limited (429)")
        resp.raise_for_status()
        data = resp.json()
        feats = data.get("features") or []
        return feats[0] if feats else None

    def _lookup_owner(self, owner_variant: str) -> Optional[dict]:
        key = owner_variant.upper().strip()
        if key in self._cache:
            return self._cache[key]
        # Parameterize safely-ish: ArcGIS where clause with escaped quotes + UPPER().
        safe = key.replace("'", "''")
        field = PARCEL_FIELDS["owner"]
        where = f"UPPER({field}) LIKE '%{safe}%'"
        result = self._query_arcgis(where)
        self._cache[key] = result
        time.sleep(0.4)  # be polite to the open-data endpoint
        return result


# =============================================================================
# SCORING
# =============================================================================

def score_record(rec: LeadRecord, new_this_week: bool) -> int:
    """Motivated-seller score (0-100) per the NextKey scoring model."""
    score = 30  # base
    flags = set(rec.flags)

    distress = flags & DISTRESS_FLAGS
    score += 10 * len(distress)

    is_pre_fc = bool(flags & PRE_FORECLOSURE_FLAGS)
    if is_pre_fc and len(distress) >= 2:
        score += 20

    amt = rec._amount_val
    if amt is not None:
        if amt > 100_000:
            score += 15
        if amt > 50_000:
            score += 10

    if new_this_week:
        score += 5
        if "New this week" not in rec.flags:
            rec.flags.append("New this week")

    full_mail = bool(rec.mail_address and (rec.mail_city or rec.mail_zip))
    if full_mail:
        score += 5
    if rec.prop_address:
        score += 5

    # Absentee owner: mailing address differs from property address.
    if rec.mail_address and rec.prop_address:
        if _addr_key(rec.mail_address) != _addr_key(rec.prop_address):
            score += 5
            if "Absentee owner" not in rec.flags:
                rec.flags.append("Absentee owner")

    return max(0, min(100, score))


def _addr_key(addr: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (addr or "").lower())


# =============================================================================
# PIPELINE
# =============================================================================

def classify_and_flag(rec: LeadRecord) -> Optional[LeadRecord]:
    """Apply category classification + base flags. Returns None to drop non-target rows."""
    category = classify_doc_type(rec.doc_type) or classify_doc_type(rec.cat_label)
    if category is None:
        skip_record("doc type did not match any motivated-seller category", rec.doc_type)
        return None
    rec.cat = category.cat
    rec.cat_label = category.cat_label
    flags = set(rec.flags) | set(category.flags)

    rec._owner_norm = normalize_owner(rec.owner)
    if rec._owner_norm.get("is_corp"):
        flags.add("LLC / corp owner")
    if rec._owner_norm.get("display"):
        rec.owner = rec._owner_norm["display"]

    rec._amount_val = parse_amount(rec.amount)
    if rec._amount_val is not None:
        rec.amount = f"{rec._amount_val:.2f}"

    rec.filed = parse_date(rec.filed)
    rec.flags = sorted(flags)
    return rec


def dedupe(records: list[LeadRecord]) -> list[LeadRecord]:
    seen: dict[tuple, LeadRecord] = {}
    for rec in records:
        key = rec.dedupe_key()
        if key in seen:
            # Merge: prefer the record with more populated address fields.
            existing = seen[key]
            if _completeness(rec) > _completeness(existing):
                seen[key] = rec
            log.debug("Deduped record %s", key)
            continue
        seen[key] = rec
    return list(seen.values())


def _completeness(rec: LeadRecord) -> int:
    fields = [rec.prop_address, rec.mail_address, rec.amount, rec.phone, rec.email,
              rec.legal, rec.grantee]
    return sum(1 for f in fields if f)


# =============================================================================
# OUTPUT WRITERS
# =============================================================================

def write_json_outputs(records: list[LeadRecord], date_from: datetime,
                       date_to: datetime) -> dict:
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_LABEL,
        "date_range": {
            "from": date_from.strftime("%Y-%m-%d"),
            "to": date_to.strftime("%Y-%m-%d"),
        },
        "total": len(records),
        "with_address": sum(1 for r in records if r.prop_address or r.mail_address),
        "records": [r.to_json() for r in records],
    }
    for path in OUTPUT_JSON_PATHS:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            log.info("Wrote %d records -> %s", len(records), path)
        except Exception as exc:  # noqa: BLE001
            log.error("Failed writing JSON to %s: %s", path, exc)
    return payload


CSV_COLUMNS = [
    "First Name", "Last Name", "Full Name", "Phone", "Email",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]

def write_csv_export(records: list[LeadRecord]) -> None:
    OUTPUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for r in records:
        owner_norm = r._owner_norm or normalize_owner(r.owner)
        if owner_norm.get("is_business"):
            first, last = "", ""
        else:
            first, last = owner_norm.get("first", ""), owner_norm.get("last", "")
        rows.append({
            "First Name": first,
            "Last Name": last,
            "Full Name": r.owner,
            "Phone": r.phone,
            "Email": r.email,
            "Mailing Address": r.mail_address,
            "Mailing City": r.mail_city,
            "Mailing State": r.mail_state,
            "Mailing Zip": r.mail_zip,
            "Property Address": r.prop_address,
            "Property City": r.prop_city,
            "Property State": r.prop_state,
            "Property Zip": r.prop_zip,
            "Lead Type": r.cat_label,
            "Document Type": r.doc_type,
            "Date Filed": r.filed,
            "Document Number": r.doc_num,
            "Amount/Debt Owed": r.amount,
            "Seller Score": r.score,
            "Motivated Seller Flags": "; ".join(r.flags),
            "Source": SOURCE_LABEL,
            "Public Records URL": r.clerk_url,
        })

    try:
        if pd is not None:
            pd.DataFrame(rows, columns=CSV_COLUMNS).to_csv(
                OUTPUT_CSV_PATH, index=False, encoding="utf-8-sig")
        else:
            with OUTPUT_CSV_PATH.open("w", newline="", encoding="utf-8-sig") as fh:
                writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
                writer.writeheader()
                writer.writerows(rows)
        log.info("Wrote CSV export (%d rows) -> %s", len(rows), OUTPUT_CSV_PATH)
    except Exception as exc:  # noqa: BLE001
        log.error("Failed writing CSV: %s", exc)


def write_empty_outputs(date_from: datetime, date_to: datetime) -> None:
    """Ensure valid, importable empty files exist even when nothing was found."""
    write_json_outputs([], date_from, date_to)
    write_csv_export([])


# =============================================================================
# DEMO MODE
# =============================================================================
# Generates realistic Alameda County sample records so the team can preview the
# full JSON + CRM CSV format before the live portal selectors are calibrated.
# These rows run through the SAME classify -> score -> export pipeline as live
# data (enrichment is skipped because demo addresses are already embedded).

def _demo_raw_records(date_to: datetime) -> list[LeadRecord]:
    d = lambda days: (date_to - timedelta(days=days)).strftime("%m/%d/%Y")
    samples = [
        LeadRecord(
            doc_num="2026-198432", doc_type="Notice of Trustee's Sale", filed=d(2),
            owner="GARCIA, MARIA ELENA", grantee="WELLS FARGO BANK NA",
            amount="$432,500.00", legal="LOT 14 BLK 3 TRACT 4521",
            prop_address="1428 92nd Ave", prop_city="Oakland", prop_zip="94603",
            mail_address="1428 92nd Ave", mail_city="Oakland", mail_state="CA",
            mail_zip="94603", clerk_url=CLERK_PORTAL_URL),
        LeadRecord(
            doc_num="2026-198501", doc_type="Notice of Default", filed=d(1),
            owner="THOMPSON, ROBERT J JR", grantee="QUALITY LOAN SERVICE CORP",
            amount="$58,900.00", legal="UNIT 7 CONDO PLAN 88-221",
            prop_address="355 Vernon St Apt 7", prop_city="Oakland", prop_zip="94610",
            mail_address="PO Box 1180", mail_city="Pleasanton", mail_state="CA",
            mail_zip="94566", clerk_url=CLERK_PORTAL_URL),
        LeadRecord(
            doc_num="2026-197905", doc_type="Federal Tax Lien", filed=d(4),
            owner="NGUYEN, DAVID", grantee="INTERNAL REVENUE SERVICE",
            amount="$112,340.00", legal="LOT 22 TRACT 6610",
            prop_address="2901 Fruitvale Ave", prop_city="Oakland", prop_zip="94602",
            mail_address="2901 Fruitvale Ave", mail_city="Oakland", mail_state="CA",
            mail_zip="94602", clerk_url=CLERK_PORTAL_URL),
        LeadRecord(
            doc_num="2026-198120", doc_type="Abstract of Judgment", filed=d(5),
            owner="WILLIAMS, SANDRA K", grantee="MIDLAND FUNDING LLC",
            amount="$24,615.00", legal="LOT 9 BLK 12 TRACT 1188",
            prop_address="540 Bellevue Ave", prop_city="Hayward", prop_zip="94541",
            mail_address="1900 Webster St Unit 410", mail_city="San Francisco",
            mail_state="CA", mail_zip="94115", clerk_url=CLERK_PORTAL_URL),
        LeadRecord(
            doc_num="2026-198777", doc_type="Mechanic's Lien", filed=d(3),
            owner="SUNRISE PROPERTIES LLC", grantee="BAY AREA CONSTRUCTION INC",
            amount="$18,200.00", legal="LOT 3 TRACT 9087",
            prop_address="775 Estudillo Ave", prop_city="San Leandro", prop_zip="94577",
            mail_address="1 Market St Ste 300", mail_city="San Francisco",
            mail_state="CA", mail_zip="94105", clerk_url=CLERK_PORTAL_URL),
        LeadRecord(
            doc_num="2026-198044", doc_type="Lis Pendens", filed=d(6),
            owner="PATEL, ANITA R", grantee="US BANK NATIONAL ASSN",
            amount="$0.00", legal="LOT 18 BLK 4 TRACT 3302",
            prop_address="3120 Birdsall Ave", prop_city="Oakland", prop_zip="94619",
            mail_address="3120 Birdsall Ave", mail_city="Oakland", mail_state="CA",
            mail_zip="94619", clerk_url=CLERK_PORTAL_URL),
        LeadRecord(
            doc_num="2026-198299", doc_type="Notice of Delinquent Assessment (HOA)",
            filed=d(2), owner="CHEN, WEI", grantee="LAKESIDE HOA",
            amount="$9,850.00", legal="UNIT 212 CONDO PLAN 02-118",
            prop_address="280 Caldecott Ln Unit 212", prop_city="Oakland",
            prop_zip="94618", mail_address="450 N Mathilda Ave", mail_city="Sunnyvale",
            mail_state="CA", mail_zip="94086", clerk_url=CLERK_PORTAL_URL),
        LeadRecord(
            doc_num="2026-197660", doc_type="Affidavit of Death - Probate", filed=d(6),
            owner="ESTATE OF HAROLD FREEMAN", grantee="",
            amount="", legal="LOT 5 BLK 1 TRACT 770",
            prop_address="1209 Pacific Ave", prop_city="Alameda", prop_zip="94501",
            mail_address="", mail_city="", mail_state="", mail_zip="",
            clerk_url=CLERK_PORTAL_URL),
    ]
    return samples


# =============================================================================
# MAIN
# =============================================================================

async def run_pipeline(demo: bool = False) -> dict:
    date_to = datetime.now()
    date_from = date_to - timedelta(days=LOOKBACK_DAYS)
    week_ago = date_to - timedelta(days=7)

    log.info("=" * 70)
    log.info("NextKey Home Offer — Alameda County motivated-seller scrape%s",
             "  [DEMO MODE]" if demo else "")
    log.info("Lookback: %d days  (%s -> %s)", LOOKBACK_DAYS,
             date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d"))
    log.info("=" * 70)

    # 1. Scrape the clerk portal (or load demo samples).
    raw_records: list[LeadRecord] = []
    if demo:
        raw_records = _demo_raw_records(date_to)
        log.info("Loaded %d demo sample record(s) — skipping live portal + enrichment.",
                 len(raw_records))
    else:
        try:
            scraper = ClerkScraper(date_from, date_to)
            raw_records = await scraper.run()
        except Exception as exc:  # noqa: BLE001
            log.error("Scrape stage failed entirely: %s", exc)

    if not raw_records:
        log.warning("No raw records returned from the clerk portal. Writing empty outputs.")
        write_empty_outputs(date_from, date_to)
        _log_summary([], date_from, date_to)
        return {"total": 0}

    # 2. Classify + normalize (drops non-target document types).
    classified: list[LeadRecord] = []
    for rec in raw_records:
        try:
            out = classify_and_flag(rec)
            if out is not None:
                classified.append(out)
        except Exception as exc:  # noqa: BLE001
            skip_record(f"classification error: {exc}", rec.doc_type)
    log.info("%d/%d rows matched a target category.", len(classified), len(raw_records))

    # 3. Dedupe early (cheaper enrichment).
    classified = dedupe(classified)
    log.info("%d records after de-duplication.", len(classified))

    # 4. Enrich with parcel / assessor data (skipped in demo — addresses pre-filled).
    if demo:
        log.info("Demo mode: using embedded sample addresses (no live enrichment).")
    else:
        enricher = AssessorEnricher()
        for rec in classified:
            try:
                enricher.enrich(rec)
            except Exception as exc:  # noqa: BLE001
                log.warning("Enrichment failed for owner '%s': %s", rec.owner, exc)

    # 5. Score.
    for rec in classified:
        try:
            new = _is_new(rec.filed, week_ago)
            rec.score = score_record(rec, new_this_week=new)
        except Exception as exc:  # noqa: BLE001
            log.warning("Scoring failed for %s: %s", rec.doc_num, exc)
            rec.score = 30

    # Sort hottest leads first.
    classified.sort(key=lambda r: r.score, reverse=True)

    # 6. Write outputs.
    write_json_outputs(classified, date_from, date_to)
    write_csv_export(classified)
    _log_summary(classified, date_from, date_to)
    return {"total": len(classified)}


def _is_new(filed_iso: str, week_ago: datetime) -> bool:
    try:
        return datetime.strptime(filed_iso[:10], "%Y-%m-%d") >= week_ago
    except Exception:
        return True  # default to "new" within a 7-day lookback window


def _log_summary(records: list[LeadRecord], date_from: datetime,
                 date_to: datetime) -> None:
    log.info("-" * 70)
    log.info("RUN SUMMARY")
    log.info("  Window:        %s -> %s", date_from.strftime("%Y-%m-%d"),
             date_to.strftime("%Y-%m-%d"))
    log.info("  Final leads:   %d", len(records))
    log.info("  With address:  %d", sum(1 for r in records if r.prop_address or r.mail_address))
    log.info("  Skipped rows:  %d", len(SKIPPED))
    if records:
        top = records[0]
        log.info("  Hottest lead:  %s (score %d, %s)", top.owner, top.score, top.cat_label)
    log.info("-" * 70)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="scraper.py",
        description="NextKey Home Offer — Alameda County motivated-seller lead scraper.",
    )
    parser.add_argument(
        "--demo", action="store_true",
        help="Skip the live portal and generate realistic sample leads so you can "
             "preview the JSON + CRM CSV output format.",
    )
    parser.add_argument(
        "--lookback", type=int, default=None,
        help=f"Override LOOKBACK_DAYS (default {LOOKBACK_DAYS}).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    if args.lookback is not None:
        global LOOKBACK_DAYS
        LOOKBACK_DAYS = args.lookback
    try:
        result = asyncio.run(run_pipeline(demo=args.demo))
        return 0 if result is not None else 1
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
        return 130
    except Exception as exc:  # noqa: BLE001
        log.error("Fatal error in main: %s", exc)
        # Still try to leave valid empty outputs behind.
        try:
            now = datetime.now()
            write_empty_outputs(now - timedelta(days=LOOKBACK_DAYS), now)
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    sys.exit(main())
