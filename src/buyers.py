"""Alameda County investor / cash-buyer list builder.

Wholesalers need end buyers to assign contracts to. This script finds them in
the same public records the seller scraper uses:

1. The Clerk-Recorder portal is scanned day by day for recorded deeds
   (DEED, TRUSTEES DEED). Each deed's grantee is the buyer.
2. Deeds where a company (LLC, Inc, LP, ...) took title are kept. Banks,
   government, nonprofits, and owners moving property into their own entity
   are dropped when the buyer list is built.
3. Each deed is matched to the county parcel layer by document number. When a
   parcel's "latest document" is this deed, the parcel's tax-bill mailing
   address belongs to the buyer, so it is used as the buyer's address.

The parcel layer trails the recorder by a few months, so the newest purchases
show the property but leave the buyer address "pending" until the county
catches up. Scanned deeds are stored in data/buyer_deeds.json; every run
re-scans the most recent days and then backfills older days until the
lookback window is covered or the time budget runs out.

Environment variables:
    BUYER_LOOKBACK_DAYS        How far back the buyer list reaches. Default: 365.
    BUYER_RESCAN_DAYS          Recent days re-scanned on every run. Default: 7.
    BUYER_MAX_MINUTES          Stop backfilling older days after this many
                               minutes. Default: 45.
    BUYER_MAX_DETAIL_LOOKUPS   Deed detail pages opened per run to read the APN
                               of purchases the parcel layer has not caught up
                               with. Default: 150.
    HEADFUL / SCRAPER_TIMEOUT_MS
                               Same as src/scraper.py.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as dt
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import scraper as base


BUYER_SOURCE_LABEL = "Alameda County Clerk-Recorder deeds + Assessor parcel roll"
DEED_LABELS = ("DEED", "TRUSTEES DEED", "TRUSTEES DEED NO DA FEE")
AUCTION_DEED_TYPES = {"TRUSTEES DEED", "TRUSTEES DEED NO DA FEE"}
LOOKBACK_DAYS_DEFAULT = 365
RESCAN_DAYS_DEFAULT = 7
MAX_MINUTES_DEFAULT = 45
MAX_DETAIL_LOOKUPS_DEFAULT = 150
PARCEL_PAGE_SIZE = 2000
APN_BATCH_SIZE = 100
MAX_PROPERTIES_PER_BUYER = 25

STORE_PATH = base.DATA_DIR / "buyer_deeds.json"
PARCEL_FIELDS = (
    "APN",
    "LatestDocument_Prefix",
    "LatestDocumentSeries",
    "LatestDocumentDate",
    "SitusStreetNumber",
    "SitusStreetName",
    "SitusUnit",
    "SitusCity",
    "SitusZip",
    "MailingAddressStreet",
    "MailingAddressUnit",
    "MailingAddressCityState",
    "MailingAddressZip",
    "UseCode",
)

BUYER_CSV_COLUMNS = [
    "Company Name",
    "First Name",
    "Last Name",
    "Mailing Address",
    "Mailing City",
    "Mailing State",
    "Mailing Zip",
    "Address Status",
    "Buyer Type",
    "Purchases (Lookback)",
    "Foreclosure Auction Buys",
    "Properties Sold (Lookback)",
    "Last Purchase Date",
    "Cities Bought In",
    "Recent Purchases",
    "Tags",
    "Source",
]

# A grantee has to look like a business to count as an investor buyer.
ENTITY_RE = re.compile(
    r"\b(LLC|L\s?L\s?C|INC|INCORPORATED|CORP|CORPORATION|COMPANY|LP|L\s?P|LLP|LTD|LIMITED|"
    r"HOLDINGS?|PROPERT(?:Y|IES)|INVESTMENTS?|INVESTORS?|CAPITAL|VENTURES?|EQUIT(?:Y|IES)|"
    r"FUND|PARTNERS|PARTNERSHIP|GROUP|DEVELOPMENTS?|DEVELOPERS?|ENTERPRISES?|REALTY|"
    r"HOMES|ACQUISITIONS?|ASSETS?|RESIDENTIAL|REAL ESTATE|BUILDERS?|CONSTRUCTION)\b",
    re.I,
)

# Organizations that take title but will never buy a wholesale contract.
EXCLUDED_ORG_RE = re.compile(
    r"\b(BANK|BANCORP|MORTGAGE|FEDERAL NATIONAL|FEDERAL HOME LOAN|FANNIE MAE|FREDDIE MAC|"
    r"NATIONAL ASSOCIATION|SAVINGS|CREDIT UNION|LOAN SERVICING|SERVICING|SECRETARY OF|"
    r"HOUSING AND URBAN|VETERANS|UNITED STATES|STATE OF CALIFORNIA|CITY OF|COUNTY OF|"
    r"DEPARTMENT OF|SCHOOL|DISTRICT|AUTHORITY|SUCCESSOR AGENCY|REDEVELOPMENT|CHURCH|MINISTR|"
    r"DIOCESE|CATHOLIC|HABITAT FOR HUMANITY|UNIVERSITY|REGENTS|COLLEGE|TITLE|ESCROW|"
    r"TRUSTEE SERVICES?|TRUSTEE CORP|FORECLOSURE|RECONVEYANCE|ASSOCIATION|HOMEOWNERS|"
    r"PACIFIC GAS|UTILITY|MUNICIPAL|RAILROAD|RAILWAY|TRANSIT|HOSPITAL|FOUNDATION|NONPROFIT|"
    r"NON PROFIT|COMMUNITY LAND TRUST|AFFORDABLE|CEMETERY|NATIONSTAR|MR COOPER|LAKEVIEW|"
    r"NEWREZ|CARRINGTON|SELENE|SHELLPOINT|PENNYMAC|LOANDEPOT|OCWEN|BAYVIEW|RUSHMORE|"
    r"CHRISTIANA TRUST|DEUTSCHE|WELLS FARGO|JPMORGAN|CITIBANK|HSBC|BNY|"
    r"HOMEBUYER|EARNED EQUITY|SHARED EQUITY|HOMETAP|UNISON|SPLITERO)\b",
    re.I,
)

# Recorder detail pages print APNs as "099 1315 048", "092A 0616 085", "25 678 1 2".
DETAIL_APN_RE = re.compile(
    r"APN:\s*([0-9]{1,4}[A-Z]?)[\s-]+([0-9]{1,4})[\s-]+([0-9]{1,3})(?:[\s-]+([0-9]{1,3}))?(?![0-9A-Z])"
)

# Real buyers, but they rarely take assignments; listed last and tagged.
INSTITUTIONAL_RE = re.compile(
    r"\b(OPENDOOR|OFFERPAD|INVITATION HOMES|AMERICAN HOMES 4 RENT|PROGRESS RESIDENTIAL|"
    r"TRICON|REDFIN|ZILLOW|MAIN STREET RENEWAL|FIRSTKEY|VINEBROOK|PRETIUM|"
    r"HOME PARTNERS OF AMERICA|ROOFSTOCK|KNOCK|ORCHARD|FLYHOMES|EASYKNOCK|LENNAR|PULTE|"
    r"TOLL BROTHERS|KB HOME|SHEA HOMES|TRI POINTE|TAYLOR MORRISON|D ?R HORTON|MERITAGE|"
    r"WARMINGTON|CITYVENTURES|BROOKFIELD|SUMMERHILL|DAVIDON|TRUMARK|WILLIAM LYON|CENTEX)\b",
    re.I,
)

LEGAL_SUFFIX_RE = re.compile(
    r",?\s+(?:A|AN)\s+[A-Z ]{2,30}?\s+(?:LIMITED LIABILITY COMPANY|CORPORATION|"
    r"LIMITED PARTNERSHIP|GENERAL PARTNERSHIP)\b.*$",
    re.I,
)

# Words too common to prove that the seller and the buyer are the same people.
GENERIC_NAME_TOKENS = {
    "LLC", "INC", "CORP", "CORPORATION", "COMPANY", "LLP", "LTD", "LIMITED", "THE", "AND",
    "TRS", "TRUST", "TRUSTEE", "TRUSTEES", "FAMILY", "LIVING", "REVOCABLE", "IRREVOCABLE",
    "HOLDINGS", "HOLDING", "PROPERTIES", "PROPERTY", "INVESTMENTS", "INVESTMENT", "INVESTORS",
    "CAPITAL", "VENTURES", "VENTURE", "GROUP", "HOMES", "HOME", "REALTY", "REAL", "ESTATE",
    "PARTNERS", "PARTNERSHIP", "EQUITY", "EQUITIES", "FUND", "DEVELOPMENT", "DEVELOPMENTS",
    "ENTERPRISES", "RESIDENTIAL", "ACQUISITIONS", "ASSETS", "MANAGEMENT", "SERIES",
    "CALIFORNIA", "BAY", "AREA", "EAST", "WEST", "NORTH", "SOUTH", "OAKLAND", "BERKELEY",
    "ALAMEDA", "HAYWARD", "FREMONT", "NEW", "FIRST", "AMERICAN", "PACIFIC", "GOLDEN",
    "STATE", "USA", "ONE", "III", "BUILDERS", "CONSTRUCTION",
}


def clean_party(value: str) -> str:
    return base.clean_text(re.sub(r"\(\s*[+-]\s*\)", " ", value or ""))


def display_name(value: str) -> str:
    name = clean_party(value).upper()
    name = LEGAL_SUFFIX_RE.sub("", name)
    name = re.sub(r"\bL\s+L\s+C\b", "LLC", name)
    name = re.sub(r"\bL\s+P\b", "LP", name)
    return base.clean_text(name).strip(" ,.-")


def buyer_key(value: str) -> str:
    name = display_name(value)
    name = re.sub(r"(?:[\s,]+(?:TR|TRS|TRUSTEE|ET AL))+$", "", name)
    return base.normalize_key(name)


def is_entity(name: str) -> bool:
    return bool(ENTITY_RE.search(clean_party(name)))


def distinctive_tokens(name: str) -> set[str]:
    return {
        token
        for token in re.split(r"[^A-Z]+", clean_party(name).upper())
        if len(token) >= 3 and token not in GENERIC_NAME_TOKENS
    }


def is_self_transfer(grantor: str, grantee: str) -> bool:
    """True when the deed likely moves property between the same people."""

    if buyer_key(grantor) and buyer_key(grantor) == buyer_key(grantee):
        return True
    return bool(distinctive_tokens(grantor) & distinctive_tokens(grantee))


def classify_buyer(name: str) -> str:
    """Return 'investor', 'institutional', or '' when the name is not a target buyer."""

    if not is_entity(name) or EXCLUDED_ORG_RE.search(clean_party(name)):
        return ""
    if INSTITUTIONAL_RE.search(clean_party(name)):
        return "institutional"
    return "investor"


def property_use(use_code: str) -> str:
    """Rough label for an Alameda assessor use code (checked against sample parcels)."""

    code = base.clean_text(use_code)
    if not code:
        return ""
    if code.startswith("1"):
        return "Single family"
    if code in {"7300", "7305"}:
        return "Condo / townhome"
    if code.startswith(("2", "7")):
        return "Multi-unit residential"
    if code.startswith("0"):
        return "Vacant land"
    return "Commercial / other"


def parse_detail_apns(text: str) -> list[str]:
    """Return parcel-layer style APNs ("92A-616-85", "25-678-1-2") from detail page text."""

    out = []
    for book, page_no, parcel, sub in DETAIL_APN_RE.findall(base.clean_text(text)):
        book_match = re.fullmatch(r"0*([0-9]+)([A-Z]?)", book)
        if not book_match:
            continue
        parts = [f"{int(book_match.group(1))}{book_match.group(2)}", str(int(page_no)), str(int(parcel))]
        if sub and int(sub):
            parts.append(str(int(sub)))
        out.append("-".join(parts))
    return list(dict.fromkeys(out))


def fuzzy_apn_patterns(apn: str) -> list[str]:
    """LIKE patterns for an APN the parcel layer does not have verbatim.

    Covers parcels later split into sub-parcels ("98-216-6" -> "98-216-6-1") and
    recorder typos in the book suffix ("0480" keyed for "048D" -> "48D-7298-29-3").
    """

    parts = apn.split("-")
    book = re.fullmatch(r"([0-9]+)([A-Z]?)", parts[0]) if len(parts) >= 3 else None
    if not book:
        return []
    page_no, parcel = parts[1], parts[2]
    patterns = [f"{parts[0]}-{page_no}-{parcel}-%"]
    digits = book.group(1)
    if not book.group(2) and len(digits) == 3 and digits.endswith("0"):
        patterns.append(f"{digits[:-1]}_-{page_no}-{parcel}%")
    return patterns


# ---------------------------------------------------------------------------
# Deed store
# ---------------------------------------------------------------------------


def load_store() -> dict[str, Any]:
    if STORE_PATH.exists():
        try:
            store = json.loads(STORE_PATH.read_text(encoding="utf-8"))
            store.setdefault("scanned_days", {})
            store.setdefault("deeds", {})
            return store
        except Exception as exc:  # noqa: BLE001
            logging.warning("Could not read %s; starting a new deed store: %s", STORE_PATH, exc)
    return {"scanned_days": {}, "deeds": {}}


def save_store(store: dict[str, Any], lookback_start: dt.date) -> None:
    cutoff = lookback_start.isoformat()
    store["scanned_days"] = {
        day: info for day, info in sorted(store["scanned_days"].items()) if day >= cutoff
    }
    store["deeds"] = {
        doc: deed
        for doc, deed in sorted(store["deeds"].items())
        if deed.get("filed", "") >= cutoff
    }
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STORE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(store, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp.replace(STORE_PATH)
    logging.info("Wrote %s (%s deeds)", STORE_PATH, len(store["deeds"]))


def worth_storing(grantor: str, grantee: str) -> bool:
    """Keep every deed with a business on either side; classification happens later."""

    return is_entity(grantee) or is_entity(grantor)


# ---------------------------------------------------------------------------
# Parcel layer
# ---------------------------------------------------------------------------


def parcel_doc_num(attributes: dict[str, Any]) -> str:
    prefix = base.clean_text(attributes.get("LatestDocument_Prefix"))
    series = base.clean_text(attributes.get("LatestDocumentSeries"))
    if prefix.isdigit() and series.isdigit():
        return f"{prefix}{series}"
    return ""


def parcel_summary(attributes: dict[str, Any]) -> dict[str, str]:
    street = " ".join(
        part
        for part in (
            base.clean_text(attributes.get("SitusStreetNumber")),
            base.clean_text(attributes.get("SitusStreetName")),
            base.clean_text(attributes.get("SitusUnit")),
        )
        if part
    )
    mail_street = " ".join(
        part
        for part in (
            base.clean_text(attributes.get("MailingAddressStreet")),
            base.clean_text(attributes.get("MailingAddressUnit")),
        )
        if part
    )
    mail_city, mail_state = base.parse_city_state(base.clean_text(attributes.get("MailingAddressCityState")))
    return {
        "apn": base.clean_text(attributes.get("APN")),
        "prop_address": street,
        "prop_city": base.clean_text(attributes.get("SitusCity")),
        "prop_zip": base.clean_text(attributes.get("SitusZip")),
        "use": property_use(attributes.get("UseCode")),
        "mail_address": mail_street,
        "mail_city": mail_city,
        "mail_state": mail_state,
        "mail_zip": base.clean_text(attributes.get("MailingAddressZip")),
        "latest_doc": parcel_doc_num(attributes),
    }


def query_parcels(session: Any, where: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        params = {
            "f": "json",
            "where": where,
            "outFields": ",".join(PARCEL_FIELDS),
            "returnGeometry": "false",
            "resultOffset": str(offset),
            "resultRecordCount": str(PARCEL_PAGE_SIZE),
            "orderByFields": "OBJECTID",
        }

        def do_query() -> dict[str, Any]:
            response = session.get(f"{base.ARCGIS_LAYER_URL}/query", params=params, timeout=base.REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            if "error" in payload:
                raise RuntimeError(payload["error"])
            return payload

        payload = base.retry(do_query, label=f"ArcGIS parcels {where[:40]} offset {offset}")
        features = payload.get("features") or []
        rows.extend(feature.get("attributes") or {} for feature in features)
        offset += len(features)
        if not features or (not payload.get("exceededTransferLimit") and len(features) < PARCEL_PAGE_SIZE):
            return rows


def load_recent_parcels_by_doc(session: Any, since: dt.date) -> tuple[dict[str, list[dict[str, str]]], str]:
    """Map recorder document number -> parcels whose latest deed is that document."""

    attributes = query_parcels(session, f"LatestDocumentDate >= DATE '{since.isoformat()}'")
    by_doc: dict[str, list[dict[str, str]]] = defaultdict(list)
    newest = ""
    for row in attributes:
        summary = parcel_summary(row)
        if summary["latest_doc"]:
            by_doc[summary["latest_doc"]].append(summary)
        stamp = row.get("LatestDocumentDate")
        if isinstance(stamp, (int, float)):
            day = dt.datetime.fromtimestamp(stamp / 1000, dt.timezone.utc).date().isoformat()
            newest = max(newest, day)
    logging.info("Parcel layer: %s parcels with deeds since %s (newest %s)", len(attributes), since, newest or "n/a")
    return by_doc, newest


def load_parcels_by_apn(session: Any, apns: list[str]) -> dict[str, dict[str, str]]:
    """Map recorder APN -> parcel summary, falling back to near matches."""

    out: dict[str, dict[str, str]] = {}
    unique = sorted({apn for apn in apns if re.fullmatch(r"[0-9A-Z-]+", apn or "")})
    for start in range(0, len(unique), APN_BATCH_SIZE):
        batch = unique[start : start + APN_BATCH_SIZE]
        where = "APN IN ({})".format(",".join(f"'{apn}'" for apn in batch))
        try:
            for row in query_parcels(session, where):
                summary = parcel_summary(row)
                out[summary["apn"]] = summary
        except Exception as exc:  # noqa: BLE001
            logging.warning("APN parcel lookup failed for %s APNs: %s", len(batch), exc)

    for apn in unique:
        if apn in out:
            continue
        for pattern in fuzzy_apn_patterns(apn):
            try:
                rows = [parcel_summary(row) for row in query_parcels(session, f"APN LIKE '{pattern}'")]
            except Exception as exc:  # noqa: BLE001
                logging.debug("Fuzzy APN lookup failed for %s: %s", apn, exc)
                continue
            books = {row["apn"].split("-")[0] for row in rows}
            if rows and len(books) == 1:
                out[apn] = rows[0]
                break
    logging.info("APN lookups: %s of %s recorder APNs matched a parcel", len(out), len(unique))
    return out


# ---------------------------------------------------------------------------
# Recorder scan
# ---------------------------------------------------------------------------


def parse_deed_rows(page_html: str) -> list[dict[str, str]]:
    """Read deed rows from an Alameda results grid page."""

    soup = base.import_bs4()(page_html, "lxml")
    rows: list[dict[str, str]] = []
    for raw_row in soup.find_all("tr"):
        values = [base.clean_text(cell.get_text(" ")) for cell in raw_row.find_all(["td", "th"])]
        if not (15 < len(values) <= 60):
            continue
        if not (re.fullmatch(r"\d{1,4}", values[0] or "") and re.fullmatch(r"\d{10,}", values[3] or "")):
            continue
        filed = base.parse_date(values[7])
        if not filed:
            continue
        grantor, grantee = values[13], values[15]
        if not grantor or not grantee:
            combo = re.search(r"\[R\]\s*(.*?)\s*\[E\]\s*(.*)", values[10], re.I)
            if combo:
                grantor = grantor or combo.group(1)
                grantee = grantee or combo.group(2)
        rows.append(
            {
                "doc_num": values[3],
                "filed": filed.isoformat(),
                "doc_type": base.clean_text(values[8]).upper(),
                "grantor": clean_party(grantor),
                "grantee": clean_party(grantee),
                "grantor_more": "(+)" in grantor,
                "grantee_more": "(+)" in grantee,
            }
        )
    return list({row["doc_num"]: row for row in rows}.values())


async def page_doc_nums(page: Any) -> list[str]:
    return [row["doc_num"] for row in parse_deed_rows(await page.content())]


async def lookup_detail_apns(page: Any, doc_num: str, expected_page: list[str]) -> tuple[list[str], bool]:
    """Open one deed's detail page, read its APN(s), and return to the results page.

    Returns (apns, still_on_same_results_page).
    """

    texts = await page.locator("td.fauxDetailLink").evaluate_all(
        "cells => cells.map(cell => (cell.closest('tr') || cell).innerText || '')"
    )
    index = next((i for i, text in enumerate(texts) if doc_num in text), -1)
    if index < 0:
        return [], True

    apns: list[str] = []
    try:
        await page.locator("td.fauxDetailLink").nth(index).click(force=True, timeout=5000)
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            await page.wait_for_timeout(500)
        apns = parse_detail_apns(await page.text_content("body") or "")
    except Exception as exc:  # noqa: BLE001
        logging.debug("Detail lookup failed for %s: %s", doc_num, exc)

    try:
        back = page.get_by_text("Back to Results", exact=False).first
        if await back.count() > 0:
            await back.click(force=True, timeout=5000)
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                await page.wait_for_timeout(500)
    except Exception as exc:  # noqa: BLE001
        logging.debug("Could not return to results after %s: %s", doc_num, exc)

    current = await page_doc_nums(page)
    return apns, bool(current) and current[: len(expected_page)] == expected_page


async def scan_day(
    page: Any,
    day: dt.date,
    store: dict[str, Any],
    parcels_by_doc: dict[str, list[dict[str, str]]],
    detail_budget: list[int],
) -> None:
    await base.open_search_surface(page)
    await base.fill_date_fields(page, day, day)
    if not await base.set_exact_document_labels(page, DEED_LABELS):
        raise RuntimeError("Alameda deed document labels were not found on the search page")
    await base.submit_search(page)

    total = await base.result_total(page)
    seen: dict[str, dict[str, str]] = {}
    details_ok = True
    for _ in range(base.MAX_RESULT_PAGES):
        page_rows = parse_deed_rows(await page.content())
        page_nums = [row["doc_num"] for row in page_rows]
        for row in page_rows:
            if row["doc_type"] not in DEED_LABELS:
                continue
            seen[row["doc_num"]] = row
            if not worth_storing(row["grantor"], row["grantee"]):
                continue
            deed = store["deeds"].setdefault(row["doc_num"], {"apns": [], "detail_checked": False})
            deed.update(row)

            wants_detail = (
                details_ok
                and detail_budget[0] > 0
                and not deed.get("detail_checked")
                and row["doc_num"] not in parcels_by_doc
                and classify_buyer(row["grantee"])
                and not is_self_transfer(row["grantor"], row["grantee"])
            )
            if wants_detail:
                detail_budget[0] -= 1
                apns, details_ok = await lookup_detail_apns(page, row["doc_num"], page_nums)
                deed["apns"] = apns
                deed["detail_checked"] = bool(apns) or details_ok
                if not details_ok:
                    logging.warning("Lost results page after detail lookup on %s; skipping remaining details", day)

        if total is not None and len(seen) >= total:
            break
        if not page_rows or not await base.click_next_results_page(page):
            break

    if total is not None and total >= base.RESULT_CAP:
        logging.warning("Deed search for %s hit the portal cap (%s results)", day, total)
    # An unreadable result count means the search did not run cleanly; retry the day next run.
    complete = total is not None and len(seen) >= min(total, base.RESULT_CAP)
    store["scanned_days"][day.isoformat()] = {
        "total": total if total is not None else len(seen),
        "parsed": len(seen),
        "complete": bool(complete),
        "scanned_at": base.utc_now().isoformat(timespec="seconds"),
    }
    logging.info("Deeds %s: portal total=%s parsed=%s complete=%s", day, total, len(seen), complete)


def days_to_scan(store: dict[str, Any], today: dt.date, lookback_days: int, rescan_days: int, limit: int | None) -> list[dt.date]:
    recent = [today - dt.timedelta(days=offset) for offset in range(max(rescan_days, 0))]
    backfill = []
    for offset in range(max(rescan_days, 0), max(lookback_days, 1)):
        day = today - dt.timedelta(days=offset)
        if not store["scanned_days"].get(day.isoformat(), {}).get("complete"):
            backfill.append(day)
    if limit is not None:
        backfill = backfill[: max(limit, 0)]
    return recent + backfill


async def scan_recorder(
    store: dict[str, Any],
    days: list[dt.date],
    rescan_count: int,
    parcels_by_doc: dict[str, list[dict[str, str]]],
    max_minutes: float,
    max_details: int,
) -> None:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError("Missing dependency 'playwright'. See src/scraper.py for setup.") from exc

    timeout = int(os.getenv("SCRAPER_TIMEOUT_MS", "45000"))
    started = time.monotonic()
    detail_budget = [max_details]

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=os.getenv("HEADFUL", "0") != "1",
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/Los_Angeles",
            viewport={"width": 1500, "height": 1000},
            ignore_https_errors=True,
        )
        await context.add_init_script(base.STEALTH_JS)
        page = await context.new_page()
        page.set_default_timeout(timeout)

        try:
            await page.goto(base.CLERK_PORTAL_URL, wait_until="networkidle", timeout=timeout)
        except Exception:
            await page.goto(base.CLERK_REAL_ESTATE_NEW_SESSION_URL, wait_until="domcontentloaded", timeout=timeout)
        await base.enter_clerk_app(page)
        await base.public_login(page)
        await base.open_search_surface(page)

        for index, day in enumerate(days):
            is_backfill = index >= rescan_count
            elapsed = (time.monotonic() - started) / 60
            if is_backfill and elapsed >= max_minutes:
                logging.info("Time budget reached after %.1f minutes; %s backfill days left for later runs", elapsed, len(days) - index)
                break
            try:
                await scan_day(page, day, store, parcels_by_doc, detail_budget)
            except Exception as exc:  # noqa: BLE001
                logging.warning("Deed scan failed for %s: %s", day, exc)
                try:
                    await page.goto(base.CLERK_REAL_ESTATE_NEW_SESSION_URL, wait_until="domcontentloaded", timeout=timeout)
                    await base.pass_clerk_challenge(page)
                    await base.accept_disclaimer(page)
                except Exception:
                    pass

        await context.close()
        await browser.close()


# ---------------------------------------------------------------------------
# Buyer list
# ---------------------------------------------------------------------------


def build_buyers(
    store: dict[str, Any],
    parcels_by_doc: dict[str, list[dict[str, str]]],
    parcels_by_apn: dict[str, dict[str, str]],
    lookback_start: dt.date,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    buyers: dict[str, dict[str, Any]] = {}
    sales: dict[str, int] = defaultdict(int)
    skipped = {"not_target": 0, "self_transfer": 0}
    cutoff = lookback_start.isoformat()

    for deed in store["deeds"].values():
        if deed.get("filed", "") < cutoff:
            continue
        grantor, grantee = deed.get("grantor", ""), deed.get("grantee", "")
        if is_self_transfer(grantor, grantee):
            skipped["self_transfer"] += 1
            continue
        if classify_buyer(grantor):
            sales[buyer_key(grantor)] += 1
        kind = classify_buyer(grantee)
        if not kind:
            skipped["not_target"] += 1
            continue

        key = buyer_key(grantee)
        buyer = buyers.setdefault(
            key,
            {"key": key, "name": display_name(grantee), "buyer_type": kind, "deeds": [], "_verified": []},
        )

        matched = parcels_by_doc.get(deed["doc_num"], [])
        parcels = matched or [parcels_by_apn[apn] for apn in deed.get("apns", []) if apn in parcels_by_apn]
        if matched:
            buyer["_verified"].append((deed["filed"], matched[0]))
        first = parcels[0] if parcels else {}
        latest = first.get("latest_doc", "")
        resold = bool(not matched and latest.isdigit() and int(latest) > int(deed["doc_num"]))
        buyer["deeds"].append(
            {
                "doc_num": deed["doc_num"],
                "filed": deed["filed"],
                "doc_type": deed.get("doc_type", ""),
                "seller": display_name(grantor),
                "auction": deed.get("doc_type", "") in AUCTION_DEED_TYPES,
                "parcels": len(parcels),
                "apn": first.get("apn", ""),
                "address": first.get("prop_address", ""),
                "city": first.get("prop_city", ""),
                "zip": first.get("prop_zip", ""),
                "use": first.get("use", ""),
                "resold": resold,
            }
        )

    out: list[dict[str, Any]] = []
    for key, buyer in buyers.items():
        deeds = sorted(buyer["deeds"], key=lambda item: item["filed"], reverse=True)
        verified = sorted(buyer.pop("_verified"), key=lambda item: item[0], reverse=True)
        mail = verified[0][1] if verified else {}
        cities = sorted({d["city"] for d in deeds if d["city"]})
        tags = []
        if len(deeds) >= 2:
            tags.append("Repeat buyer")
        if any(d["auction"] for d in deeds):
            tags.append("Foreclosure auction buyer")
        if sales.get(key) or any(d["resold"] for d in deeds):
            tags.append("Flipper (resold a property)")
        if buyer["buyer_type"] == "institutional":
            tags.append("Institutional / builder")
        out.append(
            {
                "name": buyer["name"],
                "buyer_type": "Institutional / builder" if buyer["buyer_type"] == "institutional" else "Investor",
                "mail_address": mail.get("mail_address", ""),
                "mail_city": mail.get("mail_city", ""),
                "mail_state": mail.get("mail_state", "") if mail else "",
                "mail_zip": mail.get("mail_zip", ""),
                "address_status": address_status(mail, deeds),
                "purchases": len(deeds),
                "auction_purchases": sum(1 for d in deeds if d["auction"]),
                "sales": max(sales.get(key, 0), sum(1 for d in deeds if d["resold"])),
                "last_purchase": deeds[0]["filed"] if deeds else "",
                "first_purchase": deeds[-1]["filed"] if deeds else "",
                "cities": cities,
                "tags": tags,
                "properties": deeds[:MAX_PROPERTIES_PER_BUYER],
            }
        )

    out.sort(
        key=lambda b: (
            b["buyer_type"] == "Investor",
            b["purchases"],
            b["address_status"] == "Verified",
            b["last_purchase"],
        ),
        reverse=True,
    )
    return out, skipped


def address_status(mail: dict[str, str], deeds: list[dict[str, Any]]) -> str:
    if mail.get("mail_address"):
        return "Verified"
    if deeds and all(d["resold"] for d in deeds if d["apn"]) and any(d["resold"] for d in deeds):
        return "Not on county roll (resold)"
    return "Pending county update"


def coverage(store: dict[str, Any], today: dt.date, lookback_days: int) -> dict[str, Any]:
    window = [(today - dt.timedelta(days=offset)).isoformat() for offset in range(lookback_days)]
    complete = [day for day in window if store["scanned_days"].get(day, {}).get("complete")]
    return {
        "days_in_window": len(window),
        "days_scanned": len(complete),
        "oldest_scanned": min(complete) if complete else "",
        "newest_scanned": max(complete) if complete else "",
    }


def write_outputs(payload: dict[str, Any]) -> None:
    for directory in (base.DATA_DIR, base.DASHBOARD_DIR):
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / "buyers.json"
        tmp = target.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(target)
        logging.info("Wrote %s", target)

        csv_target = directory / "buyers_ghl.csv"
        tmp_csv = csv_target.with_suffix(".csv.tmp")
        with tmp_csv.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=BUYER_CSV_COLUMNS, lineterminator="\n")
            writer.writeheader()
            for buyer in payload["buyers"]:
                recent = [
                    ", ".join(part for part in (d["address"], d["city"]) if part) or f"Doc {d['doc_num']}"
                    for d in buyer["properties"][:3]
                ]
                writer.writerow(
                    {
                        "Company Name": buyer["name"],
                        "First Name": "",
                        "Last Name": "",
                        "Mailing Address": buyer["mail_address"],
                        "Mailing City": buyer["mail_city"],
                        "Mailing State": buyer["mail_state"],
                        "Mailing Zip": buyer["mail_zip"],
                        "Address Status": buyer["address_status"],
                        "Buyer Type": buyer["buyer_type"],
                        "Purchases (Lookback)": buyer["purchases"],
                        "Foreclosure Auction Buys": buyer["auction_purchases"],
                        "Properties Sold (Lookback)": buyer["sales"],
                        "Last Purchase Date": buyer["last_purchase"],
                        "Cities Bought In": "; ".join(buyer["cities"]),
                        "Recent Purchases": " | ".join(recent),
                        "Tags": "; ".join(["Alameda cash buyer list", *buyer["tags"]]),
                        "Source": BUYER_SOURCE_LABEL,
                    }
                )
        tmp_csv.replace(csv_target)
        logging.info("Wrote %s", csv_target)


def run(lookback_days: int, rescan_days: int, max_minutes: float, max_details: int, backfill_limit: int | None, scan: bool) -> dict[str, Any]:
    today = base.today_pacific()
    lookback_start = today - dt.timedelta(days=lookback_days - 1)
    store = load_store()
    session = base.requests_session()

    try:
        parcels_by_doc, parcel_newest = load_recent_parcels_by_doc(session, lookback_start)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Parcel layer unavailable; buyer addresses will be pending: %s", exc)
        parcels_by_doc, parcel_newest = {}, ""

    if scan:
        days = days_to_scan(store, today, lookback_days, rescan_days, backfill_limit)
        logging.info("Scanning %s day(s) of deeds (%s recent, %s backfill)", len(days), min(rescan_days, len(days)), max(len(days) - rescan_days, 0))
        try:
            asyncio.run(scan_recorder(store, days, rescan_days, parcels_by_doc, max_minutes, max_details))
        except Exception as exc:  # noqa: BLE001
            logging.error("Recorder deed scan failed; building from stored deeds: %s", exc)
        save_store(store, lookback_start)

    apns = [apn for deed in store["deeds"].values() if deed["doc_num"] not in parcels_by_doc for apn in deed.get("apns", [])]
    parcels_by_apn = load_parcels_by_apn(session, apns) if apns else {}
    buyers, skipped = build_buyers(store, parcels_by_doc, parcels_by_apn, lookback_start)

    payload = {
        "fetched_at": base.utc_now().isoformat(),
        "source": BUYER_SOURCE_LABEL,
        "date_range": {"from": lookback_start.isoformat(), "to": today.isoformat(), "lookback_days": lookback_days},
        "coverage": coverage(store, today, lookback_days),
        "parcel_layer_through": parcel_newest,
        "total": len(buyers),
        "with_address": sum(1 for b in buyers if b["address_status"] == "Verified"),
        "repeat_buyers": sum(1 for b in buyers if b["purchases"] >= 2),
        "skipped": skipped,
        "buyers": buyers,
    }
    return payload


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an Alameda County investor buyer list from recorded deeds.")
    parser.add_argument("--lookback-days", type=int, default=int(os.getenv("BUYER_LOOKBACK_DAYS", str(LOOKBACK_DAYS_DEFAULT))))
    parser.add_argument("--rescan-days", type=int, default=int(os.getenv("BUYER_RESCAN_DAYS", str(RESCAN_DAYS_DEFAULT))))
    parser.add_argument("--max-minutes", type=float, default=float(os.getenv("BUYER_MAX_MINUTES", str(MAX_MINUTES_DEFAULT))))
    parser.add_argument(
        "--max-details",
        type=int,
        default=int(os.getenv("BUYER_MAX_DETAIL_LOOKUPS", str(MAX_DETAIL_LOOKUPS_DEFAULT))),
    )
    parser.add_argument("--backfill-limit", type=int, default=None, help="Scan at most this many older days (testing).")
    parser.add_argument("--no-scan", action="store_true", help="Rebuild the buyer list from stored deeds only.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    base.setup_logging(args.verbose)
    payload = run(args.lookback_days, args.rescan_days, args.max_minutes, args.max_details, args.backfill_limit, not args.no_scan)
    write_outputs(payload)
    logging.info(
        "Done. buyers=%s with_address=%s repeat=%s coverage=%s/%s days",
        payload["total"],
        payload["with_address"],
        payload["repeat_buyers"],
        payload["coverage"]["days_scanned"],
        payload["coverage"]["days_in_window"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
