"""Automated Alameda County motivated seller lead scraper.

This scraper collects recent Alameda County recorder documents, enriches them
with parcel mailing/property data, scores motivated seller signals, and writes
JSON plus a GHL-compatible CSV export.

The county sites are public systems that can change without notice. The code
therefore uses layered fallbacks:

1. Playwright drives the Clerk-Recorder portal like a public visitor.
2. Static requests/BeautifulSoup parse any server-rendered tables and detail
   pages returned by the portal.
3. Parcel enrichment prefers a DBF bulk file through dbfread, then falls back
   to the public ArcGIS FeatureServer if no DBF download is exposed.

Environment variables:
    LOOKBACK_DAYS              Number of days to collect. Default: 7.
    HEADFUL                    Set to 1 to show the browser locally.
    PROPERTY_DBF_PATH          Existing local .dbf or .zip path to use first.
    PROPERTY_BULK_DBF_URL      Direct .dbf or .zip URL to use first.
    ENABLE_ARCGIS_FALLBACK     Set to 0 to disable ArcGIS fallback. Default: 1.
    ARCGIS_MAX_RECORDS         Optional cap for fallback parcel records.
    SCRAPER_TIMEOUT_MS         Playwright timeout. Default: 45000.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import dataclasses
import datetime as dt
import html
import json
import logging
import os
import re
import sys
import time
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import unquote, urljoin, urlparse


CLERK_PORTAL_URL = "https://www.acgov.org/auditor/clerk/opr/"
CLERK_APP_URL = "https://rechart1.acgov.org/"
PROPERTY_BULK_DATA_URL = (
    "https://data.acgov.org/datasets/2b026350b5dd40b18ed7a321fdcdba81_0/about"
)
ARCGIS_ITEM_URL = (
    "https://www.arcgis.com/sharing/rest/content/items/"
    "2b026350b5dd40b18ed7a321fdcdba81?f=json"
)
ARCGIS_LAYER_URL = (
    "https://services5.arcgis.com/ROBnTHSNjoZ2Wm1P/arcgis/rest/services/"
    "Parcels/FeatureServer/0"
)
SOURCE_LABEL = "Alameda County Clerk-Recorder Public Records"
LOOKBACK_DAYS_DEFAULT = 7
MAX_RETRIES = 3
REQUEST_TIMEOUT = 60

BASE_DIR = Path(__file__).resolve().parents[1]
SCRAPER_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DASHBOARD_DIR = BASE_DIR / "dashboard"
CACHE_DIR = DATA_DIR / ".cache"

DATE_FORMATS = (
    "%m/%d/%Y",
    "%m/%d/%y",
    "%Y-%m-%d",
    "%b %d, %Y",
    "%B %d, %Y",
    "%m-%d-%Y",
)

GHL_COLUMNS = [
    "First Name",
    "Last Name",
    "Mailing Address",
    "Mailing City",
    "Mailing State",
    "Mailing Zip",
    "Property Address",
    "Property City",
    "Property State",
    "Property Zip",
    "Lead Type",
    "Document Type",
    "Date Filed",
    "Document Number",
    "Amount/Debt Owed",
    "Seller Score",
    "Motivated Seller Flags",
    "Source",
    "Public Records URL",
]

LEAD_TYPES: dict[str, dict[str, Any]] = {
    "LP": {
        "cat": "LP",
        "cat_label": "Lis Pendens",
        "labels": ("LIS PENDENS",),
        "flags": ("Lis pendens",),
    },
    "NOFC": {
        "cat": "NOFC",
        "cat_label": "Notice of Foreclosure",
        "labels": ("NOTICE OF FORECLOSURE", "FORECLOSURE"),
        "flags": ("Pre-foreclosure",),
    },
    "TAXDEED": {
        "cat": "TAXDEED",
        "cat_label": "Tax Deed",
        "labels": ("TAX DEED",),
        "flags": ("Tax lien",),
    },
    "JUD": {
        "cat": "JUD",
        "cat_label": "Judgment",
        "labels": ("JUDGMENT",),
        "flags": ("Judgment lien",),
        "aliases": ("CCJ", "DRJUD"),
    },
    "CCJ": {
        "cat": "JUD",
        "cat_label": "Certified Judgment",
        "labels": ("CERTIFIED JUDGMENT",),
        "flags": ("Judgment lien",),
    },
    "DRJUD": {
        "cat": "JUD",
        "cat_label": "Domestic Judgment",
        "labels": ("DOMESTIC JUDGMENT",),
        "flags": ("Judgment lien",),
    },
    "LNCORPTX": {
        "cat": "TAXLIEN",
        "cat_label": "Corporate Tax Lien",
        "labels": ("CORP TAX LIEN", "CORPORATE TAX LIEN"),
        "flags": ("Tax lien",),
    },
    "LNIRS": {
        "cat": "TAXLIEN",
        "cat_label": "IRS Lien",
        "labels": ("IRS LIEN", "INTERNAL REVENUE SERVICE LIEN"),
        "flags": ("Tax lien",),
    },
    "LNFED": {
        "cat": "TAXLIEN",
        "cat_label": "Federal Lien",
        "labels": ("FEDERAL LIEN",),
        "flags": ("Tax lien",),
    },
    "LN": {
        "cat": "LIEN",
        "cat_label": "Lien",
        "labels": ("LIEN",),
        "flags": ("Mechanic lien",),
    },
    "LNMECH": {
        "cat": "LIEN",
        "cat_label": "Mechanic Lien",
        "labels": ("MECHANIC LIEN", "MECHANICS LIEN"),
        "flags": ("Mechanic lien",),
    },
    "LNHOA": {
        "cat": "LIEN",
        "cat_label": "HOA Lien",
        "labels": ("HOA LIEN", "HOMEOWNERS ASSOCIATION LIEN"),
        "flags": ("Mechanic lien",),
    },
    "MEDLN": {
        "cat": "MEDLN",
        "cat_label": "Medicaid Lien",
        "labels": ("MEDICAID LIEN", "MEDICAL LIEN"),
        "flags": ("Tax lien",),
    },
    "PRO": {
        "cat": "PRO",
        "cat_label": "Probate",
        "labels": ("PROBATE", "PROBATE DOCUMENTS"),
        "flags": ("Probate / estate",),
    },
    "NOC": {
        "cat": "NOC",
        "cat_label": "Notice of Commencement",
        "labels": ("NOTICE OF COMMENCEMENT",),
        "flags": (),
    },
    "RELLP": {
        "cat": "RELLP",
        "cat_label": "Release Lis Pendens",
        "labels": ("RELEASE LIS PENDENS", "RELEASE OF LIS PENDENS"),
        "flags": ("Lis pendens",),
    },
}

REQUESTED_DOC_CODES = [
    "LP",
    "NOFC",
    "TAXDEED",
    "JUD",
    "CCJ",
    "DRJUD",
    "LNCORPTX",
    "LNIRS",
    "LNFED",
    "LN",
    "LNMECH",
    "LNHOA",
    "MEDLN",
    "PRO",
    "NOC",
    "RELLP",
]

OWNER_COLUMNS = ("OWNER", "OWN1", "OWNER_NAME", "OwnerName", "Owner")
SITE_ADDRESS_COLUMNS = ("SITE_ADDR", "SITEADDR", "SitusAddress", "PROPERTY_ADDRESS")
SITE_CITY_COLUMNS = ("SITE_CITY", "SitusCity", "PROPERTY_CITY")
SITE_ZIP_COLUMNS = ("SITE_ZIP", "SitusZip", "PROPERTY_ZIP")
MAIL_ADDRESS_COLUMNS = (
    "ADDR_1",
    "MAILADR1",
    "MAILING_ADDRESS",
    "MailingAddressStreet",
    "MailingAddress",
)
MAIL_CITY_COLUMNS = ("CITY", "MAILCITY", "MailingCity", "MailingAddressCityState")
MAIL_STATE_COLUMNS = ("STATE", "MAILSTATE", "MailingState")
MAIL_ZIP_COLUMNS = ("ZIP", "MAILZIP", "MailingAddressZip", "MailingZip")
APN_COLUMNS = ("APN", "PARCEL", "PARCEL_NUM", "ASSESSOR_PARCEL_NUMBER", "PrintParcel")

CORP_OWNER_RE = re.compile(
    r"\b(LLC|L\.L\.C|INC|INCORPORATED|CORP|CORPORATION|CO\b|COMPANY|LP|LLP|"
    r"LIMITED|HOLDINGS?|ENTERPRISES?|INVESTMENTS?|PARTNERS?|TRUST)\b",
    re.IGNORECASE,
)
AMOUNT_RE = re.compile(r"(?<!\w)\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d{2})?|[0-9]+(?:\.\d{2})?)")
APN_RE = re.compile(r"\b(?:APN|PARCEL|ASSESSOR(?:'S)? PARCEL(?: NUMBER)?)[^\dA-Z]{0,8}([0-9A-Z-]{5,})", re.I)


@dataclasses.dataclass
class ClerkRecord:
    doc_num: str = ""
    doc_type: str = ""
    filed: str = ""
    owner: str = ""
    grantee: str = ""
    legal: str = ""
    amount: str = ""
    clerk_url: str = ""
    raw: dict[str, str] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class ParcelRecord:
    owner: str = ""
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = "CA"
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = "CA"
    mail_zip: str = ""
    apn: str = ""


class ParcelIndex:
    def __init__(self) -> None:
        self.by_owner: dict[str, ParcelRecord] = {}
        self.by_apn: dict[str, ParcelRecord] = {}
        self.total = 0

    def add(self, parcel: ParcelRecord) -> None:
        self.total += 1
        if parcel.apn:
            self.by_apn.setdefault(normalize_apn(parcel.apn), parcel)
        for variant in owner_name_variants(parcel.owner):
            self.by_owner.setdefault(normalize_name(variant), parcel)

    def lookup(self, owner: str = "", legal: str = "") -> ParcelRecord | None:
        for variant in owner_name_variants(owner):
            match = self.by_owner.get(normalize_name(variant))
            if match:
                return match

        for apn in extract_apns(legal):
            match = self.by_apn.get(normalize_apn(apn))
            if match:
                return match
        return None


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def today_pacific() -> dt.date:
    try:
        from zoneinfo import ZoneInfo

        return dt.datetime.now(ZoneInfo("America/Los_Angeles")).date()
    except Exception:
        return dt.date.today()


def parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    text = clean_text(value)
    if not text:
        return None
    text = re.sub(r"\s+\d{1,2}:\d{2}(?:\s*[AP]M)?$", "", text, flags=re.I)
    for fmt in DATE_FORMATS:
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    try:
        return dt.date.fromisoformat(text[:10])
    except ValueError:
        return None


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    value = html.unescape(value)
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_key(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", clean_text(value).upper())


def normalize_name(value: str) -> str:
    value = clean_text(value).upper()
    value = re.sub(r"[^\w\s,&.-]", " ", value)
    value = value.replace("&", " AND ")
    value = re.sub(r"\b(ET AL|TRS|TEE|TRUSTEE|REVOCABLE|LIVING|FAMILY)\b", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" ,.-")


def normalize_apn(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", clean_text(value).upper())


def owner_name_variants(owner: str) -> set[str]:
    owner = normalize_name(owner)
    if not owner:
        return set()

    variants = {owner}

    if "," in owner:
        left, right = [part.strip() for part in owner.split(",", 1)]
        if left and right:
            variants.add(f"{right} {left}")
            variants.add(f"{left} {right}")
            variants.add(f"{left}, {right}")
    else:
        parts = owner.split()
        if len(parts) >= 2:
            first = parts[0]
            last = parts[-1]
            middle = " ".join(parts[1:-1])
            variants.add(f"{first} {last}")
            variants.add(f"{last} {first}")
            variants.add(f"{last}, {first}")
            if middle:
                variants.add(f"{last}, {first} {middle}")
                variants.add(f"{first} {middle} {last}")
                variants.add(f"{last} {first} {middle}")

    return {clean_text(item) for item in variants if clean_text(item)}


def first_present(row: dict[str, Any], columns: Iterable[str]) -> str:
    if not row:
        return ""
    lower_map = {str(key).lower(): key for key in row.keys()}
    for column in columns:
        key = lower_map.get(column.lower())
        if key is not None:
            value = clean_text(row.get(key))
            if value:
                return value
    return ""


def parse_city_state(value: str) -> tuple[str, str]:
    value = clean_text(value)
    if not value:
        return "", ""
    match = re.match(r"^(.*?)(?:,\s*|\s+)([A-Z]{2})$", value)
    if match:
        return clean_text(match.group(1)), match.group(2).upper()
    return value, ""


def extract_apns(text: str) -> list[str]:
    out: list[str] = []
    for match in APN_RE.finditer(clean_text(text)):
        out.append(match.group(1))
    return out


def parse_amount(value: str | None) -> float:
    if not value:
        return 0.0
    text = clean_text(value)
    if not text:
        return 0.0
    candidates = []
    for match in AMOUNT_RE.finditer(text):
        try:
            candidates.append(float(match.group(1).replace(",", "")))
        except ValueError:
            continue
    return max(candidates) if candidates else 0.0


def format_amount(value: str | float | int | None) -> str:
    if isinstance(value, (int, float)):
        amount = float(value)
    else:
        amount = parse_amount(value)
    if amount <= 0:
        return clean_text(value) if isinstance(value, str) else ""
    return f"{amount:.2f}"


def retry(
    action: Callable[[], Any],
    attempts: int = MAX_RETRIES,
    delay: float = 2.0,
    label: str = "operation",
) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as exc:  # noqa: BLE001 - retry should catch all transient failures.
            last_error = exc
            logging.warning("%s failed on attempt %s/%s: %s", label, attempt, attempts, exc)
            if attempt < attempts:
                time.sleep(delay * attempt)
    if last_error:
        raise last_error
    raise RuntimeError(f"{label} failed")


def import_requests():
    try:
        import requests
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'requests'. Install scraper/requirements.txt first."
        ) from exc
    return requests


def import_bs4():
    try:
        from bs4 import BeautifulSoup
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'beautifulsoup4'. Install scraper/requirements.txt first."
        ) from exc
    return BeautifulSoup


def requests_session():
    requests = import_requests()
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (compatible; NextKeyLeadScraper/1.0; "
                "+https://github.com/actions)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


def fetch_url(session: Any, url: str, *, binary: bool = False) -> str | bytes:
    def do_fetch() -> str | bytes:
        response = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        return response.content if binary else response.text

    return retry(do_fetch, label=f"GET {url}")


def post_url(session: Any, url: str, data: dict[str, str], *, binary: bool = False) -> str | bytes:
    def do_post() -> str | bytes:
        response = session.post(url, data=data, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        return response.content if binary else response.text

    return retry(do_post, label=f"POST {url}")


def extract_form_fields(page_html: str) -> dict[str, str]:
    soup = import_bs4()(page_html, "lxml")
    fields: dict[str, str] = {}
    for input_tag in soup.find_all("input"):
        name = input_tag.get("name")
        if not name:
            continue
        input_type = (input_tag.get("type") or "").lower()
        if input_type in {"checkbox", "radio"} and not input_tag.has_attr("checked"):
            continue
        fields[name] = input_tag.get("value") or ""
    return fields


def discover_postback_links(page_html: str, page_url: str) -> list[tuple[str, str, str]]:
    """Return (url, event_target, event_argument) postback candidates."""

    soup = import_bs4()(page_html, "lxml")
    candidates: list[tuple[str, str, str]] = []
    patterns = [
        re.compile(r"__doPostBack\(['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]*)['\"]\)", re.I),
        re.compile(
            r"WebForm_DoPostBackWithOptions\(new WebForm_PostBackOptions\(['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]*)['\"]",
            re.I,
        ),
    ]
    for link in soup.find_all("a", href=True):
        href = html.unescape(link.get("href") or "")
        link_text = clean_text(link.get_text(" "))
        if not href.lower().startswith("javascript:"):
            continue
        for pattern in patterns:
            match = pattern.search(href)
            if match:
                target = match.group(1)
                argument = match.group(2)
                candidates.append((urljoin(page_url, ""), target, argument))
                logging.debug("Found postback %s %s from %s", target, argument, link_text)
                break
    return candidates


def download_postback_target(
    session: Any,
    page_url: str,
    page_html: str,
    target: str,
    argument: str = "",
) -> bytes:
    fields = extract_form_fields(page_html)
    fields["__EVENTTARGET"] = target
    fields["__EVENTARGUMENT"] = argument
    return post_url(session, page_url, fields, binary=True)


def discover_property_download_urls(session: Any) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    def add(url: str) -> None:
        url = clean_text(url)
        if not url:
            return
        absolute = urljoin(PROPERTY_BULK_DATA_URL, html.unescape(url))
        if absolute not in seen:
            seen.add(absolute)
            urls.append(absolute)

    explicit_url = os.getenv("PROPERTY_BULK_DBF_URL", "")
    if explicit_url:
        add(explicit_url)

    try:
        item = session.get(arcgis_item_url(), timeout=REQUEST_TIMEOUT).json()
        service_url = item.get("url")
        if service_url:
            add(service_url)
    except Exception as exc:  # noqa: BLE001
        logging.debug("ArcGIS item metadata lookup failed: %s", exc)

    try:
        page_html = fetch_url(session, PROPERTY_BULK_DATA_URL)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Could not fetch property bulk data page: %s", exc)
        return urls

    soup = import_bs4()(page_html, "lxml")
    for link in soup.find_all(["a", "link"], href=True):
        href = link.get("href") or ""
        text = clean_text(link.get_text(" "))
        combined = f"{href} {text}".lower()
        if any(token in combined for token in (".dbf", ".zip", "download", "export")):
            add(href)

    for script_text in soup.find_all(string=True):
        for match in re.finditer(r"https?://[^\"'\s<>]+", str(script_text)):
            candidate = match.group(0)
            if any(token in candidate.lower() for token in (".dbf", ".zip", "download", "FeatureServer")):
                add(candidate)

    for page_url, target, argument in discover_postback_links(page_html, PROPERTY_BULK_DATA_URL):
        target_text = f"{target} {argument}".lower()
        if any(token in target_text for token in ("download", "dbf", "zip", "export")):
            try:
                blob = download_postback_target(session, page_url, page_html, target, argument)
                path = CACHE_DIR / f"postback-{normalize_key(target) or 'download'}.bin"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(blob)
                add(path.as_uri())
            except Exception as exc:  # noqa: BLE001
                logging.warning("Postback download failed for %s: %s", target, exc)

    return urls


def arcgis_item_url() -> str:
    return ARCGIS_ITEM_URL


def save_blob_to_cache(url: str, blob: bytes) -> Path:
    parsed = urlparse(url)
    name = Path(unquote(parsed.path)).name or "download.bin"
    if "." not in name:
        content_type = ""
        if blob[:2] == b"PK":
            content_type = ".zip"
        elif blob and blob[0] in {0x03, 0x30, 0x83, 0x8B}:
            content_type = ".dbf"
        name = f"{normalize_key(name) or 'download'}{content_type}"
    path = CACHE_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob)
    return path


def resolve_local_or_file_url(value: str) -> Path | None:
    if not value:
        return None
    parsed = urlparse(value)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path))
    path = Path(value)
    return path if path.exists() else None


def extract_dbfs_from_path(path: Path) -> list[Path]:
    if not path.exists():
        return []
    if path.suffix.lower() == ".dbf":
        return [path]
    if path.suffix.lower() == ".zip":
        out_dir = CACHE_DIR / f"unzipped-{path.stem}"
        out_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(path) as zf:
            zf.extractall(out_dir)
        return list(out_dir.rglob("*.dbf"))
    return []


def download_property_dbf_files(session: Any) -> list[Path]:
    local_path = os.getenv("PROPERTY_DBF_PATH", "")
    if local_path:
        dbfs = extract_dbfs_from_path(Path(local_path))
        if dbfs:
            logging.info("Using local property DBF path: %s", local_path)
            return dbfs

    dbf_paths: list[Path] = []
    for url in discover_property_download_urls(session):
        local = resolve_local_or_file_url(url)
        if local:
            dbf_paths.extend(extract_dbfs_from_path(local))
            continue

        parsed = urlparse(url)
        if "FeatureServer" in parsed.path:
            continue

        try:
            blob = fetch_url(session, url, binary=True)
            if not isinstance(blob, bytes) or not blob:
                continue
            path = save_blob_to_cache(url, blob)
            dbf_paths.extend(extract_dbfs_from_path(path))
        except Exception as exc:  # noqa: BLE001
            logging.warning("Could not download property file %s: %s", url, exc)

    return dbf_paths


def parcel_from_row(row: dict[str, Any]) -> ParcelRecord:
    mail_city = first_present(row, MAIL_CITY_COLUMNS)
    mail_state = first_present(row, MAIL_STATE_COLUMNS)
    if not mail_state and mail_city:
        parsed_city, parsed_state = parse_city_state(mail_city)
        mail_city, mail_state = parsed_city, parsed_state

    site_address = first_present(row, SITE_ADDRESS_COLUMNS)
    if not site_address:
        street_number = first_present(row, ("SitusStreetNumber", "SITE_NO"))
        street_name = first_present(row, ("SitusStreetName", "SITE_STREET"))
        unit = first_present(row, ("SitusUnit", "SITE_UNIT"))
        site_address = clean_text(" ".join(part for part in (street_number, street_name, unit) if part))

    mail_address = first_present(row, MAIL_ADDRESS_COLUMNS)
    if mail_address == first_present(row, ("MailingAddress",)):
        mail_address = first_present(row, ("MailingAddressStreet", "MailingAddress"))

    return ParcelRecord(
        owner=first_present(row, OWNER_COLUMNS),
        prop_address=site_address,
        prop_city=first_present(row, SITE_CITY_COLUMNS),
        prop_state="CA",
        prop_zip=first_present(row, SITE_ZIP_COLUMNS),
        mail_address=mail_address,
        mail_city=mail_city,
        mail_state=mail_state or "CA",
        mail_zip=first_present(row, MAIL_ZIP_COLUMNS),
        apn=first_present(row, APN_COLUMNS),
    )


def load_parcels_from_dbf(dbf_paths: list[Path]) -> ParcelIndex:
    index = ParcelIndex()
    if not dbf_paths:
        return index

    try:
        from dbfread import DBF
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'dbfread'. Install scraper/requirements.txt first."
        ) from exc

    for dbf_path in dbf_paths:
        logging.info("Reading parcel DBF: %s", dbf_path)
        try:
            table = DBF(str(dbf_path), ignore_missing_memofile=True, char_decode_errors="ignore")
            for row in table:
                try:
                    parcel = parcel_from_row(dict(row))
                    if parcel.owner or parcel.apn:
                        index.add(parcel)
                except Exception as exc:  # noqa: BLE001
                    logging.debug("Skipping bad DBF row in %s: %s", dbf_path, exc)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Could not read DBF %s: %s", dbf_path, exc)

    logging.info("Parcel DBF rows indexed: %s", index.total)
    return index


def fetch_arcgis_parcel_index(session: Any) -> ParcelIndex:
    index = ParcelIndex()
    if os.getenv("ENABLE_ARCGIS_FALLBACK", "1") == "0":
        return index

    max_records_env = os.getenv("ARCGIS_MAX_RECORDS", "")
    max_records = int(max_records_env) if max_records_env.isdigit() else 0

    layer_info = session.get(f"{ARCGIS_LAYER_URL}?f=json", timeout=REQUEST_TIMEOUT).json()
    page_size = min(int(layer_info.get("maxRecordCount") or 2000), 2000)
    offset = 0
    logging.info("Using ArcGIS parcel fallback with page size %s", page_size)

    while True:
        if max_records and offset >= max_records:
            break

        params = {
            "f": "json",
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "resultOffset": str(offset),
            "resultRecordCount": str(page_size),
            "orderByFields": "OBJECTID",
        }

        def do_query() -> dict[str, Any]:
            response = session.get(
                f"{ARCGIS_LAYER_URL}/query",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            return response.json()

        try:
            payload = retry(do_query, label=f"ArcGIS parcels offset {offset}")
        except Exception as exc:  # noqa: BLE001
            logging.warning("ArcGIS parcel fallback stopped at offset %s: %s", offset, exc)
            break

        features = payload.get("features") or []
        if not features:
            break

        for feature in features:
            try:
                attributes = feature.get("attributes") or {}
                parcel = parcel_from_row(attributes)
                if parcel.owner or parcel.apn or parcel.prop_address:
                    index.add(parcel)
            except Exception as exc:  # noqa: BLE001
                logging.debug("Skipping bad ArcGIS parcel row: %s", exc)

        offset += len(features)
        if not payload.get("exceededTransferLimit") and len(features) < page_size:
            break

    logging.info("ArcGIS parcel rows indexed: %s", index.total)
    return index


def load_parcel_index() -> ParcelIndex:
    session = requests_session()
    dbf_paths = download_property_dbf_files(session)
    index = load_parcels_from_dbf(dbf_paths)
    if index.total:
        return index
    logging.warning("No usable DBF parcel data found; trying ArcGIS fallback.")
    return fetch_arcgis_parcel_index(session)


async def click_if_present(page: Any, selectors: Iterable[str], timeout: int = 2500) -> bool:
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0:
                await locator.click(timeout=timeout, force=True)
                return True
        except Exception:
            continue
    return False


async def accept_disclaimer(page: Any) -> None:
    candidates = [
        "text=/acknowledge the disclaimer/i",
        "text=/enter the site/i",
        "a#cph1_lnkAccept",
        "a[href*='lnkAccept']",
    ]
    if await click_if_present(page, candidates, timeout=5000):
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        return

    for target in ("cph1$lnkAccept", "ctl00$cph1$lnkAccept"):
        try:
            await page.evaluate(
                """target => {
                    if (typeof WebForm_DoPostBackWithOptions === 'function' && typeof WebForm_PostBackOptions === 'function') {
                        WebForm_DoPostBackWithOptions(new WebForm_PostBackOptions(target, '', true, '', '', false, true));
                    } else if (typeof __doPostBack === 'function') {
                        __doPostBack(target, '');
                    }
                }""",
                target,
            )
            await page.wait_for_load_state("networkidle", timeout=10000)
            return
        except Exception:
            continue


async def public_login(page: Any) -> None:
    try:
        pub_radio = page.locator("input[value='rdoPubCpu']").first
        if await pub_radio.count() > 0:
            await pub_radio.check(force=True, timeout=3000)
    except Exception:
        pass

    selectors = [
        "input[name='ctl00$LoginForm1$btnLogon']",
        "input#LoginForm1_btnLogon",
        "text=/public login/i",
        "text=/logon/i",
        "text=/login/i",
    ]
    if await click_if_present(page, selectors, timeout=5000):
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        return

    try:
        await page.evaluate(
            """() => {
                const button = document.querySelector("input[name='ctl00$LoginForm1$btnLogon'], #LoginForm1_btnLogon");
                if (button) button.click();
            }"""
        )
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass


async def open_search_surface(page: Any) -> None:
    link_patterns = [
        "text=/official records/i",
        "text=/search records/i",
        "text=/record search/i",
        "text=/document search/i",
        "text=/name search/i",
        "text=/search/i",
    ]
    if await click_if_present(page, link_patterns, timeout=5000):
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        return

    for path in ("SearchEntry.aspx", "RealEstate/SearchEntry.aspx", "Search.aspx"):
        try:
            await page.goto(urljoin(CLERK_APP_URL, path), wait_until="networkidle", timeout=15000)
            body = await page.text_content("body")
            if body and re.search(r"search|document|record", body, re.I):
                return
        except Exception:
            continue


async def fill_date_fields(page: Any, start_date: dt.date, end_date: dt.date) -> None:
    start = start_date.strftime("%m/%d/%Y")
    end = end_date.strftime("%m/%d/%Y")
    inputs = await page.locator("input:visible").element_handles()
    date_like: list[Any] = []
    for handle in inputs:
        try:
            attrs = " ".join(
                clean_text(await handle.get_attribute(attr) or "")
                for attr in ("id", "name", "title", "placeholder", "aria-label")
            )
            input_type = clean_text(await handle.get_attribute("type") or "").lower()
            if input_type in {"hidden", "submit", "button", "password", "radio", "checkbox"}:
                continue
            if re.search(r"date|record|file|from|to|range", attrs, re.I):
                date_like.append(handle)
        except Exception:
            continue

    if len(date_like) >= 2:
        for handle, value in ((date_like[0], start), (date_like[1], end)):
            try:
                await handle.fill(value)
            except Exception:
                try:
                    await handle.evaluate("(el, value) => { el.value = value; el.dispatchEvent(new Event('change', {bubbles:true})); }", value)
                except Exception:
                    pass
        return

    labels = [
        (re.compile(r"from|start|begin", re.I), start),
        (re.compile(r"to|end", re.I), end),
    ]
    for pattern, value in labels:
        for selector in [
            f"input[title*='{pattern.pattern}' i]",
            f"input[name*='{pattern.pattern}' i]",
            f"input[id*='{pattern.pattern}' i]",
        ]:
            try:
                loc = page.locator(selector).first
                if await loc.count() > 0:
                    await loc.fill(value, timeout=1500)
                    break
            except Exception:
                continue


async def set_document_type(page: Any, doc_code: str) -> bool:
    labels = [doc_code, *LEAD_TYPES.get(doc_code, {}).get("labels", ())]
    labels = [label for label in labels if label]

    selects = page.locator("select")
    select_count = await selects.count()
    for i in range(select_count):
        select = selects.nth(i)
        for label in labels:
            for option in (
                {"label": label},
                {"value": label},
                {"value": doc_code},
            ):
                try:
                    await select.select_option(option, timeout=1500)
                    return True
                except Exception:
                    continue

    editable_inputs = await page.locator(
        "input:visible:not([type='hidden']):not([type='submit']):not([type='button'])"
    ).element_handles()
    for handle in editable_inputs:
        try:
            attrs = " ".join(
                clean_text(await handle.get_attribute(attr) or "")
                for attr in ("id", "name", "title", "placeholder", "aria-label")
            )
            if re.search(r"type|doc|document|instrument|description", attrs, re.I):
                await handle.fill(doc_code)
                return True
        except Exception:
            continue

    for label in labels:
        if await click_if_present(page, [f"text=/{re.escape(label)}/i"], timeout=1000):
            return True
    return False


async def submit_search(page: Any) -> None:
    selectors = [
        "input[type='submit'][value*='Search' i]",
        "input[type='button'][value*='Search' i]",
        "button:has-text('Search')",
        "text=/^search$/i",
    ]
    if await click_if_present(page, selectors, timeout=5000):
        try:
            await page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            await page.wait_for_timeout(3000)
        return
    try:
        await page.keyboard.press("Enter")
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass


async def fetch_clerk_records_with_playwright(start_date: dt.date, end_date: dt.date) -> list[ClerkRecord]:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'playwright'. Install scraper/requirements.txt and run "
            "'python -m playwright install --with-deps chromium'."
        ) from exc

    timeout = int(os.getenv("SCRAPER_TIMEOUT_MS", "45000"))
    headless = os.getenv("HEADFUL", "0") != "1"
    all_records: list[ClerkRecord] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125 Safari/537.36"
            ),
        )
        page = await context.new_page()
        page.set_default_timeout(timeout)

        try:
            await page.goto(CLERK_PORTAL_URL, wait_until="networkidle", timeout=timeout)
        except Exception:
            await page.goto(CLERK_APP_URL, wait_until="networkidle", timeout=timeout)

        await accept_disclaimer(page)
        await public_login(page)
        await open_search_surface(page)

        for doc_code in REQUESTED_DOC_CODES:
            try:
                await open_search_surface(page)
                await fill_date_fields(page, start_date, end_date)
                await set_document_type(page, doc_code)
                await submit_search(page)
                content = await page.content()
                records = parse_clerk_records_from_html(content, str(page.url), doc_code)
                logging.info("Clerk search %s returned %s parsed rows", doc_code, len(records))
                all_records.extend(records)
            except Exception as exc:  # noqa: BLE001
                logging.warning("Clerk search failed for %s: %s", doc_code, exc)

        await context.close()
        await browser.close()

    return dedupe_clerk_records(all_records)


def parse_headers(cells: list[Any]) -> list[str]:
    return [normalize_key(cell.get_text(" ")) for cell in cells]


def map_table_row(headers: list[str], cells: list[Any], base_url: str) -> dict[str, str]:
    values = [clean_text(cell.get_text(" ")) for cell in cells]
    row: dict[str, str] = {}

    for idx, value in enumerate(values):
        header = headers[idx] if idx < len(headers) else f"COL{idx}"
        row[header] = value

    for cell in cells:
        link = cell.find("a", href=True)
        if link:
            row["URL"] = urljoin(base_url, link.get("href"))
            break

    return row


def value_by_header(row: dict[str, str], patterns: Iterable[str]) -> str:
    compiled = [re.compile(pattern, re.I) for pattern in patterns]
    for key, value in row.items():
        pretty_key = key.replace("_", " ")
        if any(pattern.search(pretty_key) for pattern in compiled):
            return clean_text(value)
    return ""


def classify_doc_type(doc_code: str, text: str) -> tuple[str, str, str] | None:
    text_norm = normalize_key(text)
    if not text_norm and doc_code in LEAD_TYPES:
        info = LEAD_TYPES[doc_code]
        return doc_code, info["cat"], info["cat_label"]

    for code in REQUESTED_DOC_CODES:
        info = LEAD_TYPES.get(code, {})
        possible = [code, *info.get("labels", ())]
        for label in possible:
            if normalize_key(label) and normalize_key(label) in text_norm:
                return code, info.get("cat", code), info.get("cat_label", label)

    info = LEAD_TYPES.get(doc_code)
    if info:
        return doc_code, info["cat"], info["cat_label"]
    return None


def parse_clerk_records_from_html(page_html: str, base_url: str, doc_code: str = "") -> list[ClerkRecord]:
    soup = import_bs4()(page_html, "lxml")
    records: list[ClerkRecord] = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        header_cells = rows[0].find_all(["th", "td"])
        headers = parse_headers(header_cells)
        if not headers or all(header.startswith("COL") for header in headers):
            headers = []

        for raw_row in rows[1:]:
            cells = raw_row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            row = map_table_row(headers, cells, base_url)
            if not headers:
                values = [clean_text(cell.get_text(" ")) for cell in cells]
                row.update({f"COL{i}": value for i, value in enumerate(values)})

            record = clerk_record_from_row(row, doc_code)
            if record and is_requested_record(record, doc_code):
                records.append(record)

    if records:
        return dedupe_clerk_records(records)

    text = soup.get_text("\n")
    return parse_records_from_text(text, base_url, doc_code)


def clerk_record_from_row(row: dict[str, str], doc_code: str = "") -> ClerkRecord | None:
    doc_num = value_by_header(
        row,
        (
            r"DOCUMENT.*NUMBER",
            r"INSTRUMENT.*NUMBER",
            r"REFERENCE.*NUMBER",
            r"REF.*",
            r"DOC.*NO",
            r"COL0",
        ),
    )
    doc_type = value_by_header(
        row,
        (
            r"DOCUMENT.*TYPE",
            r"DOC.*DESCRIPTION",
            r"ITEM.*DESCRIPTION",
            r"DESCRIPTION",
            r"ACTION.*TYPE",
            r"TYPE",
            r"COL1",
        ),
    )
    filed = value_by_header(
        row,
        (
            r"FILED",
            r"RECORD.*DATE",
            r"RECORDED",
            r"DATE.*FILED",
            r"DATE.*ADDED",
            r"ACTION.*TIME",
            r"COL2",
        ),
    )
    owner = value_by_header(row, (r"GRANTOR", r"OWNER", r"PARTY.*1", r"NAME", r"COL3"))
    grantee = value_by_header(row, (r"GRANTEE", r"PARTY.*2", r"COL4"))
    legal = value_by_header(row, (r"LEGAL", r"PROPERTY", r"DESCRIPTION", r"COL5"))
    amount = value_by_header(row, (r"AMOUNT", r"DEBT", r"FEE", r"VALUE", r"COL6"))

    if not any((doc_num, doc_type, filed, owner, grantee, legal)):
        return None

    if not doc_type and doc_code:
        doc_type = doc_code

    return ClerkRecord(
        doc_num=doc_num,
        doc_type=doc_type,
        filed=filed,
        owner=owner,
        grantee=grantee,
        legal=legal,
        amount=amount,
        clerk_url=row.get("URL", ""),
        raw=row,
    )


def parse_records_from_text(text: str, base_url: str, doc_code: str) -> list[ClerkRecord]:
    records: list[ClerkRecord] = []
    lines = [clean_text(line) for line in text.splitlines() if clean_text(line)]
    doc_num_re = re.compile(r"\b(?:DOC(?:UMENT)?|INSTRUMENT|REF(?:ERENCE)?)\s*#?\s*:?\s*([0-9A-Z-]+)", re.I)

    current: dict[str, str] = {}
    for line in lines:
        match = doc_num_re.search(line)
        if match and current:
            record = clerk_record_from_row(current, doc_code)
            if record:
                record.clerk_url = base_url
                records.append(record)
            current = {}
        if match:
            current["DOCUMENTNUMBER"] = match.group(1)
        if re.search(r"grantor|owner", line, re.I):
            current["GRANTOR"] = re.sub(r"^.*?(grantor|owner)\s*:?", "", line, flags=re.I).strip()
        if re.search(r"grantee", line, re.I):
            current["GRANTEE"] = re.sub(r"^.*?grantee\s*:?", "", line, flags=re.I).strip()
        if re.search(r"record|filed|date", line, re.I):
            possible = parse_date(line)
            if possible:
                current["RECORDEDDATE"] = possible.strftime("%m/%d/%Y")
        if classify_doc_type(doc_code, line):
            current["DOCUMENTTYPE"] = line
        if "legal" in line.lower():
            current["LEGAL"] = line
        if "$" in line:
            current["AMOUNT"] = line

    if current:
        record = clerk_record_from_row(current, doc_code)
        if record:
            record.clerk_url = base_url
            records.append(record)

    return dedupe_clerk_records([record for record in records if is_requested_record(record, doc_code)])


def is_requested_record(record: ClerkRecord, doc_code: str = "") -> bool:
    text = f"{record.doc_type} {record.raw}".upper()
    if doc_code and doc_code in LEAD_TYPES:
        info = LEAD_TYPES[doc_code]
        terms = [doc_code, *info.get("labels", ())]
        return any(normalize_key(term) in normalize_key(text) for term in terms) or not record.doc_type
    return classify_doc_type(doc_code or record.doc_type, text) is not None


def dedupe_clerk_records(records: Iterable[ClerkRecord]) -> list[ClerkRecord]:
    seen: set[tuple[str, str, str, str]] = set()
    out: list[ClerkRecord] = []
    for record in records:
        key = (
            normalize_key(record.doc_num),
            normalize_key(record.doc_type),
            normalize_key(record.owner),
            clean_text(record.filed),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(record)
    return out


def infer_flags(record: ClerkRecord, doc_code: str, filed_date: dt.date | None, start_date: dt.date) -> list[str]:
    info = LEAD_TYPES.get(doc_code, {})
    flags = list(info.get("flags", ()))
    owner_text = f"{record.owner} {record.grantee}"
    if CORP_OWNER_RE.search(owner_text):
        flags.append("LLC / corp owner")
    if filed_date and filed_date >= start_date:
        flags.append("New this week")
    return list(dict.fromkeys(flags))


def score_record(
    flags: list[str],
    amount: float,
    has_address: bool,
    has_lp_fc_combo: bool,
    filed_date: dt.date | None,
    start_date: dt.date,
) -> int:
    score = 30
    score += 10 * len(flags)
    if has_lp_fc_combo:
        score += 20
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10
    if filed_date and filed_date >= start_date:
        score += 5
    if has_address:
        score += 5
    return max(0, min(100, score))


def classify_record(record: ClerkRecord, fallback_doc_code: str = "") -> tuple[str, str, str]:
    classified = classify_doc_type(fallback_doc_code, f"{record.doc_type} {record.raw}")
    if classified:
        return classified
    code = fallback_doc_code or normalize_key(record.doc_type) or "UNKNOWN"
    return code, code, record.doc_type or code


def enrich_records(
    clerk_records: list[ClerkRecord],
    parcel_index: ParcelIndex,
    start_date: dt.date,
    end_date: dt.date,
) -> list[dict[str, Any]]:
    preliminary: list[dict[str, Any]] = []
    cats_by_owner: dict[str, set[str]] = defaultdict(set)

    for record in clerk_records:
        try:
            doc_code, cat, cat_label = classify_record(record)
            filed_date = parse_date(record.filed)
            parcel = parcel_index.lookup(record.owner, record.legal) if parcel_index else None
            amount_value = parse_amount(record.amount)
            flags = infer_flags(record, doc_code, filed_date, start_date)
            owner_key = normalize_name(record.owner)
            if owner_key:
                cats_by_owner[owner_key].add(cat)

            preliminary.append(
                {
                    "doc_num": clean_text(record.doc_num),
                    "doc_type": clean_text(record.doc_type or doc_code),
                    "filed": filed_date.isoformat() if filed_date else clean_text(record.filed),
                    "cat": cat,
                    "cat_label": cat_label,
                    "owner": clean_text(record.owner),
                    "grantee": clean_text(record.grantee),
                    "amount": format_amount(amount_value),
                    "legal": clean_text(record.legal),
                    "prop_address": parcel.prop_address if parcel else "",
                    "prop_city": parcel.prop_city if parcel else "",
                    "prop_state": parcel.prop_state if parcel else "CA",
                    "prop_zip": parcel.prop_zip if parcel else "",
                    "mail_address": parcel.mail_address if parcel else "",
                    "mail_city": parcel.mail_city if parcel else "",
                    "mail_state": parcel.mail_state if parcel else "CA",
                    "mail_zip": parcel.mail_zip if parcel else "",
                    "clerk_url": clean_text(record.clerk_url) or CLERK_APP_URL,
                    "flags": flags,
                    "_owner_key": owner_key,
                    "_amount_value": amount_value,
                    "_filed_date": filed_date,
                }
            )
        except Exception as exc:  # noqa: BLE001
            logging.warning("Skipping bad clerk record %s: %s", record, exc)

    final: list[dict[str, Any]] = []
    for row in preliminary:
        owner_key = row.pop("_owner_key", "")
        amount_value = float(row.pop("_amount_value", 0.0) or 0.0)
        filed_date = row.pop("_filed_date", None)
        has_address = bool(row.get("prop_address") or row.get("mail_address"))
        has_combo = bool(owner_key and {"LP", "NOFC"}.issubset(cats_by_owner.get(owner_key, set())))
        row["score"] = score_record(
            row["flags"],
            amount_value,
            has_address,
            has_combo,
            filed_date,
            start_date,
        )
        final.append(row)

    final.sort(key=lambda item: (item.get("score", 0), item.get("filed", "")), reverse=True)
    return final


def empty_output(start_date: dt.date, end_date: dt.date) -> dict[str, Any]:
    return {
        "fetched_at": utc_now().isoformat(),
        "source": SOURCE_LABEL,
        "date_range": {
            "from": start_date.isoformat(),
            "to": end_date.isoformat(),
            "lookback_days": (end_date - start_date).days + 1,
        },
        "total": 0,
        "with_address": 0,
        "records": [],
    }


def build_output(records: list[dict[str, Any]], start_date: dt.date, end_date: dt.date) -> dict[str, Any]:
    output = empty_output(start_date, end_date)
    output["total"] = len(records)
    output["with_address"] = sum(1 for row in records if row.get("prop_address") or row.get("mail_address"))
    output["records"] = records
    return output


def write_json_outputs(payload: dict[str, Any]) -> None:
    for target in (DASHBOARD_DIR / "output.json", DATA_DIR / "output.json"):
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(target)
        logging.info("Wrote %s", target)


def split_person_name(owner: str) -> tuple[str, str]:
    owner = clean_text(owner)
    if not owner:
        return "", ""
    if CORP_OWNER_RE.search(owner):
        return "", owner
    if "," in owner:
        last, first = [part.strip() for part in owner.split(",", 1)]
        return first, last
    parts = owner.split()
    if len(parts) == 1:
        return "", parts[0]
    return parts[0], " ".join(parts[1:])


def write_ghl_csv(records: list[dict[str, Any]]) -> None:
    for target in (DASHBOARD_DIR / "ghl_export.csv", DATA_DIR / "ghl_export.csv"):
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".csv.tmp")
        with tmp.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=GHL_COLUMNS)
            writer.writeheader()
            for record in records:
                first, last = split_person_name(record.get("owner", ""))
                writer.writerow(
                    {
                        "First Name": first,
                        "Last Name": last,
                        "Mailing Address": record.get("mail_address", ""),
                        "Mailing City": record.get("mail_city", ""),
                        "Mailing State": record.get("mail_state", ""),
                        "Mailing Zip": record.get("mail_zip", ""),
                        "Property Address": record.get("prop_address", ""),
                        "Property City": record.get("prop_city", ""),
                        "Property State": record.get("prop_state", ""),
                        "Property Zip": record.get("prop_zip", ""),
                        "Lead Type": record.get("cat_label", ""),
                        "Document Type": record.get("doc_type", ""),
                        "Date Filed": record.get("filed", ""),
                        "Document Number": record.get("doc_num", ""),
                        "Amount/Debt Owed": record.get("amount", ""),
                        "Seller Score": record.get("score", ""),
                        "Motivated Seller Flags": "; ".join(record.get("flags", [])),
                        "Source": SOURCE_LABEL,
                        "Public Records URL": record.get("clerk_url", ""),
                    }
                )
        tmp.replace(target)
        logging.info("Wrote %s", target)


async def run_scrape(lookback_days: int) -> dict[str, Any]:
    end_date = today_pacific()
    start_date = end_date - dt.timedelta(days=max(lookback_days, 1) - 1)
    logging.info("Collecting Alameda County records from %s through %s", start_date, end_date)

    try:
        parcel_index = load_parcel_index()
    except Exception as exc:  # noqa: BLE001
        logging.warning("Parcel index unavailable; continuing without address enrichment: %s", exc)
        parcel_index = ParcelIndex()

    try:
        clerk_records = await fetch_clerk_records_with_playwright(start_date, end_date)
    except Exception as exc:  # noqa: BLE001
        logging.error("Clerk portal scrape failed; writing empty result: %s", exc)
        clerk_records = []

    enriched = enrich_records(clerk_records, parcel_index, start_date, end_date)
    return build_output(enriched, start_date, end_date)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Alameda County motivated seller leads.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=int(os.getenv("LOOKBACK_DAYS", str(LOOKBACK_DAYS_DEFAULT))),
        help="Number of days back to scrape. Defaults to LOOKBACK_DAYS or 7.",
    )
    parser.add_argument("--no-csv", action="store_true", help="Do not write GHL CSV exports.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    setup_logging(args.verbose)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    payload = asyncio.run(run_scrape(args.lookback_days))
    write_json_outputs(payload)
    if not args.no_csv:
        write_ghl_csv(payload.get("records", []))

    logging.info(
        "Done. total=%s with_address=%s",
        payload.get("total"),
        payload.get("with_address"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

