"""Marine weather analysis pipeline.

Downloads marine weather sources (a surface-pressure synoptic chart image and
the latest Meteomar textual bulletin), sends both to a multimodal Gemini model,
and writes a Markdown navigation-planning report to the mounted ./output volume.

The pipeline runs once at startup and then every 6 hours, matching the cadence
of new marine forecast emissions.

All comments are in English; the generated report is in Italian.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
# Aliased: write_index() already uses "html" as a local variable name.
from html import escape as html_escape
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, unquote, urlsplit

import requests
import schedule
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from google import genai
from google.genai import types as genai_types
from google.genai import errors as genai_errors

# --------------------------------------------------------------------------- #
# Configuration (all tunables come from environment variables)
# --------------------------------------------------------------------------- #

# Application version. Bump it by hand when the pipeline, the prompt or the web
# page change in a way worth telling apart in production: it is logged at
# startup and shown in the header of index.html, so the running build can be
# identified from the page alone.
APP_VERSION = "0.2.1"

# Gemini API key is mandatory; the app refuses to start without it.
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

# Default to a capable multimodal Gemini model. Overridable via env.
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash").strip()

# Data sources.
PRESSURE_CHART_PAGE = os.getenv(
    "PRESSURE_CHART_URL",
    "https://weather.metoffice.gov.uk/maps-and-charts/surface-pressure",
).strip()
# Optional direct URL to the chart image. If the page scraping fails to locate
# the image (the Met Office page is JavaScript-heavy), set this to a known image
# endpoint to make downloads deterministic.
PRESSURE_CHART_IMAGE_URL = os.getenv("PRESSURE_CHART_IMAGE_URL", "").strip()

METEOMAR_URL = os.getenv(
    "METEOMAR_URL",
    "https://www.meteoam.it/it/messaggio-meteomar",
).strip()

# The Meteomar page is a JavaScript SPA: the bulletin text is not in the static
# HTML but fetched client-side from the Meteo AM Oracle Content Management API.
# We query that API directly (fast, no browser) for the latest
# "Integration-Message" whose name starts with the WMO header of the Italian
# Meteomar bulletin (FXIY61 LIIB, emitted by C.N.M.C.A. Rome). The endpoint,
# public channel token, and WMO prefix are overridable in case the site rotates
# them; if the API fails we fall back to scraping the rendered HTML.
METEOMAR_API_URL = os.getenv(
    "METEOMAR_API_URL",
    "https://cm.meteoam.it/content/published/api/v1.1/items",
).strip()
METEOMAR_CHANNEL_TOKEN = os.getenv(
    "METEOMAR_CHANNEL_TOKEN", "7449487744984981831df3b6b37e73c9"
).strip()
METEOMAR_WMO_PREFIX = os.getenv(
    "METEOMAR_WMO_PREFIX", "MESSAGGI/MSG4/FXIY61"
).strip()

# Navigation areas the report should focus on.
NAV_AREAS = os.getenv(
    "NAV_AREAS",
    "Arcipelago di La Maddalena e Caprera (Sardegna nord-orientale) - zone "
    "Meteomar di riferimento: Mar di Sardegna, Mar di Corsica e Tirreno Settentrionale",
).strip()

# Output directory (mounted as a Docker volume).
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/app/output"))

# Scheduling interval in hours.
RUN_INTERVAL_HOURS = int(os.getenv("RUN_INTERVAL_HOURS", "6"))

# Whether to run the pipeline immediately at startup (before the first tick).
RUN_ON_START = os.getenv("RUN_ON_START", "true").lower() in ("1", "true", "yes")

# Network timeouts (connect, read) in seconds.
HTTP_TIMEOUT = (10, 60)

# Model output ceiling. NOTE: for thinking-capable models (Gemini 2.5/3.x) this
# budget also covers the model's reasoning tokens, so it must be generous or the
# visible report gets truncated mid-text.
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "16000"))

# Thinking budget for reasoning-capable models. -1 = model default (dynamic),
# 0 = disable thinking, N = cap reasoning to N tokens (leaving more of
# MAX_TOKENS for the actual answer). Only applied when >= 0.
GEMINI_THINKING_BUDGET = int(os.getenv("GEMINI_THINKING_BUDGET", "-1"))

# Headless-browser fallback: when static HTML scraping cannot locate the
# JavaScript-rendered chart, render the page with Playwright and screenshot it.
USE_PLAYWRIGHT_FALLBACK = os.getenv(
    "USE_PLAYWRIGHT_FALLBACK", "true"
).lower() in ("1", "true", "yes")
# Max time (ms) to wait for the page to render in the headless browser.
PLAYWRIGHT_TIMEOUT_MS = int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "45000"))

# --- Visitor statistics (the unlisted /statistica page) ---------------------
# Built by parsing the JSON access log nginx writes (see nginx/default.conf),
# so there is no tracking script, no cookie and no third-party service.
STATS_ENABLED = os.getenv("STATS_ENABLED", "true").lower() in ("1", "true", "yes")
# Written by the web container, mounted read-only here.
ACCESS_LOG_PATH = Path(os.getenv("ACCESS_LOG_PATH", "/app/logs/stats.log"))
# Optional MaxMind/DB-IP .mmdb used to resolve city (and country, when
# Cloudflare does not send its location headers). Absent = country only.
GEOIP_DB_PATH = Path(os.getenv("GEOIP_DB_PATH", "/app/geoip/city.mmdb"))
# Only requests newer than this take part in the aggregates.
STATS_WINDOW_DAYS = int(os.getenv("STATS_WINDOW_DAYS", "90"))
# How often the page is regenerated (it is much cheaper than a pipeline run).
STATS_INTERVAL_MINUTES = int(os.getenv("STATS_INTERVAL_MINUTES", "30"))

# Browser-like headers reduce the chance of being served a bot-block page.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    ),
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
}

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("marine-weather")


# --------------------------------------------------------------------------- #
# HTTP session with retries/backoff
# --------------------------------------------------------------------------- #

def build_session() -> requests.Session:
    """Return a requests session with automatic retries for transient errors."""
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,  # 0s, 1.5s, 3s, 6s between attempts
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(BROWSER_HEADERS)
    return session


# --------------------------------------------------------------------------- #
# Source 1: surface-pressure synoptic chart (image)
# --------------------------------------------------------------------------- #

def _guess_media_type(image_bytes: bytes) -> str:
    """Detect the image media type from magic bytes (fallback: image/png)."""
    if image_bytes.startswith(b"\x89PNG"):
        return "image/png"
    if image_bytes.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if image_bytes.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if image_bytes[:4] == b"RIFF" and image_bytes[8:12] == b"WEBP":
        return "image/webp"
    return "image/png"


def _extract_chart_image_url(html: str, base_url: str) -> Optional[str]:
    """Best-effort extraction of the chart image URL from page HTML.

    The Met Office page renders the chart via JavaScript, so this looks for the
    most likely candidates: Open Graph image, then any <img> that looks like a
    pressure/synoptic chart.
    """
    soup = BeautifulSoup(html, "html.parser")

    # URLs that look like site chrome rather than the actual chart. The Met
    # Office page, for instance, advertises a square "social_card.jpg" as its
    # og:image, which is not the chart and 404s when fetched.
    non_chart = ("social_card", "favicon", "/icons/", "logo", "sprite", "placeholder")

    # 1. Open Graph / Twitter preview image (skip obvious non-chart assets).
    for prop in ("og:image", "twitter:image"):
        tag = soup.find("meta", attrs={"property": prop}) or soup.find(
            "meta", attrs={"name": prop}
        )
        if tag and tag.get("content"):
            url = requests.compat.urljoin(base_url, tag["content"])
            if not any(bad in url.lower() for bad in non_chart):
                return url

    # 2. Any <img> whose src/alt hints at a pressure or synoptic chart.
    keywords = ("pressure", "synoptic", "surface", "chart", "isobar")
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        alt = (img.get("alt") or "").lower()
        haystack = f"{src.lower()} {alt}"
        if src and any(k in haystack for k in keywords) and not any(
            bad in src.lower() for bad in non_chart
        ):
            return requests.compat.urljoin(base_url, src)

    return None


def _fetch_image_bytes(
    session: requests.Session, image_url: str
) -> Optional[tuple[bytes, str]]:
    """Download an image URL and return (bytes, media_type), or None on failure."""
    try:
        log.info("Downloading pressure-chart image: %s", image_url)
        img_resp = session.get(image_url, timeout=HTTP_TIMEOUT)
        img_resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("Failed to download chart image: %s", exc)
        return None

    if not img_resp.content:
        log.error("Downloaded chart image is empty.")
        return None

    content_type = img_resp.headers.get("Content-Type", "")
    if not content_type.startswith("image/"):
        log.warning(
            "Chart URL did not return an image (Content-Type: %s). "
            "Falling back to magic-byte detection.",
            content_type or "unknown",
        )

    media_type = _guess_media_type(img_resp.content)
    log.info("Downloaded chart image (%d bytes, %s).", len(img_resp.content), media_type)
    return img_resp.content, media_type


def _download_chart_static(session: requests.Session) -> Optional[tuple[bytes, str]]:
    """Static-HTML strategy: discover the chart URL from the page, then fetch it."""
    image_url = PRESSURE_CHART_IMAGE_URL

    # If no explicit image URL was configured, try to discover it from the page.
    if not image_url:
        try:
            log.info("Fetching pressure-chart page (static): %s", PRESSURE_CHART_PAGE)
            resp = session.get(PRESSURE_CHART_PAGE, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            image_url = _extract_chart_image_url(resp.text, PRESSURE_CHART_PAGE)
        except requests.RequestException as exc:
            log.error("Failed to load pressure-chart page: %s", exc)
            return None

        if not image_url:
            log.info("Chart image not present in static HTML.")
            return None

    return _fetch_image_bytes(session, image_url)


def _download_chart_playwright() -> Optional[tuple[bytes, str]]:
    """Headless-browser fallback: render the JS page and screenshot the chart.

    The Met Office page loads the chart via JavaScript, so it is often absent
    from the static HTML. Playwright renders the page in a real Chromium
    instance, dismisses any cookie banner, locates the chart element, and
    returns a PNG screenshot (falling back to a full-page screenshot).

    Playwright is imported lazily so a missing install doesn't break the rest
    of the app; failures are caught and logged.
    """
    try:
        from playwright.sync_api import sync_playwright  # lazy import
    except ImportError:
        log.error(
            "Playwright is not installed; cannot use headless fallback. "
            "Install it or set USE_PLAYWRIGHT_FALLBACK=false."
        )
        return None

    log.info("Rendering pressure-chart page with headless browser (Playwright).")
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(args=["--no-sandbox", "--disable-dev-shm-usage"])
            try:
                page = browser.new_page(
                    user_agent=BROWSER_HEADERS["User-Agent"],
                    viewport={"width": 1400, "height": 1200},
                )
                page.goto(
                    PRESSURE_CHART_PAGE,
                    wait_until="networkidle",
                    timeout=PLAYWRIGHT_TIMEOUT_MS,
                )

                # Best-effort dismissal of a cookie-consent banner.
                for label in ("Accept all", "Accept All", "Accetta", "I Agree", "Agree"):
                    try:
                        btn = page.get_by_role("button", name=label)
                        if btn.count() > 0:
                            btn.first.click(timeout=3000)
                            page.wait_for_timeout(1000)
                            break
                    except Exception:  # noqa: BLE001 - banner is optional
                        continue

                # Give lazy-loaded chart assets a moment to settle.
                page.wait_for_timeout(2500)

                # Try to screenshot just the chart element; fall back to full page.
                selectors = (
                    "img[alt*='pressure' i]",
                    "img[src*='pressure' i]",
                    "img[src*='surface' i]",
                    "img[alt*='chart' i]",
                    "canvas",
                    "main img",
                )
                for selector in selectors:
                    try:
                        element = page.locator(selector).first
                        if element.count() > 0:
                            element.scroll_into_view_if_needed(timeout=3000)
                            png = element.screenshot(timeout=5000)
                            if png:
                                log.info(
                                    "Captured chart element via selector '%s' (%d bytes).",
                                    selector, len(png),
                                )
                                return png, "image/png"
                    except Exception:  # noqa: BLE001 - try the next selector
                        continue

                # Last resort: full-page screenshot (Gemini can still read it).
                png = page.screenshot(full_page=True)
                log.info("Captured full-page screenshot (%d bytes).", len(png))
                return png, "image/png"
            finally:
                browser.close()
    except Exception:  # noqa: BLE001 - headless rendering is best-effort
        log.exception("Playwright headless fallback failed.")
        return None


def download_pressure_chart(session: requests.Session) -> Optional[tuple[bytes, str]]:
    """Download the latest surface-pressure chart image.

    Tries static HTML scraping first; if that fails and the fallback is enabled,
    renders the page with a headless browser. Returns (bytes, media_type) or None.
    """
    chart = _download_chart_static(session)
    if chart is not None:
        return chart

    if USE_PLAYWRIGHT_FALLBACK and not PRESSURE_CHART_IMAGE_URL:
        chart = _download_chart_playwright()
        if chart is not None:
            return chart

    log.error(
        "Could not obtain the pressure chart. Consider setting "
        "PRESSURE_CHART_IMAGE_URL to a direct image URL."
    )
    return None


# --------------------------------------------------------------------------- #
# Source 2: Meteomar textual bulletin
# --------------------------------------------------------------------------- #

def _fetch_meteomar_api(session: requests.Session) -> Optional[str]:
    """Fetch the latest Meteomar bulletin text from the Meteo AM CMS API.

    Queries the Oracle Content Management endpoint the public page reads
    client-side, returning the raw bulletin body. Returns the cleaned text, or
    None on any failure so the caller can fall back to HTML scraping.
    """
    params = {
        "channelToken": METEOMAR_CHANNEL_TOKEN,
        "fields": "all",
        "limit": "1",
        "orderBy": "fields.date:desc",
        "q": f'type eq "Integration-Message" and name sw "{METEOMAR_WMO_PREFIX}"',
    }
    try:
        log.info("Fetching Meteomar bulletin (API): %s", METEOMAR_API_URL)
        resp = session.get(METEOMAR_API_URL, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        log.warning("Meteomar API request failed: %s", exc)
        return None

    items = data.get("items") or []
    if not items:
        log.warning("Meteomar API returned no items.")
        return None

    body = (items[0].get("fields") or {}).get("body")
    if not isinstance(body, str) or not body.strip():
        log.warning("Meteomar API item has no usable 'body' field.")
        return None

    # The body uses CR/LF line endings (often doubled); normalise to clean
    # single newlines and drop blank lines.
    lines = [ln.strip() for ln in body.replace("\r", "\n").splitlines()]
    cleaned = "\n".join(ln for ln in lines if ln)

    log.info("Fetched Meteomar bulletin via API (%d chars).", len(cleaned))
    return cleaned


def _scrape_meteomar_html(session: requests.Session) -> Optional[str]:
    """Fallback: scrape the Meteomar bulletin from the rendered HTML page.

    The page is a JavaScript SPA, so the static HTML usually contains only
    scaffolding; this path is a best-effort backup for when the API is
    unavailable. Returns the extracted text, or None on failure.
    """
    try:
        log.info("Fetching Meteomar bulletin (HTML): %s", METEOMAR_URL)
        resp = session.get(METEOMAR_URL, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("Failed to load Meteomar page: %s", exc)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Drop non-content elements before extracting text.
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        tag.decompose()

    # Prefer a <main>/<article> container; fall back to the whole body.
    container = soup.find("main") or soup.find("article") or soup.body or soup
    text = container.get_text(separator="\n", strip=True)

    # Collapse blank lines.
    lines = [line for line in (ln.strip() for ln in text.splitlines()) if line]
    cleaned = "\n".join(lines)

    # A genuine bulletin is long and contains the "METEOMAR" marker; anything
    # else is the SPA's placeholder scaffolding, not the actual forecast.
    if len(cleaned) < 200 or "METEOMAR" not in cleaned.upper():
        log.error("Meteomar HTML looks like scaffolding, not a bulletin; scraping failed.")
        return None

    log.info("Extracted Meteomar text via HTML (%d chars).", len(cleaned))
    return cleaned


def scrape_meteomar(session: requests.Session) -> Optional[str]:
    """Return the latest Meteomar bulletin text.

    Prefers the CMS API (clean, structured text); falls back to scraping the
    HTML page. Returns None only if both strategies fail.
    """
    text = _fetch_meteomar_api(session)
    if text is not None:
        return text

    log.info("Falling back to HTML scraping for the Meteomar bulletin.")
    return _scrape_meteomar_html(session)


# --------------------------------------------------------------------------- #
# LLM analysis (Google Gemini, multimodal)
# --------------------------------------------------------------------------- #

def build_system_prompt(now: datetime) -> str:
    """Return the Gemini system prompt for a run emitted at ``now``.

    Section 3 adapts to the day of week: the "weekend" framing is only used from
    Thursday onward (``weekday() >= 3``). Earlier in the week we ask for generic
    forward projections instead, so the report never talks about "il weekend"
    when the weekend is still several days away.
    """
    weekend_focus = now.weekday() >= 3  # Mon=0 ... Thu=3 ... Sun=6

    if weekend_focus:
        section3_title = "## 3. Proiezioni per il Weekend / Navigazione"
        section3_intro = (
            "Usa la sezione delle proiezioni a 12 ore e intervalli successivi del "
            "Meteomar per generare deduzioni pratiche per la navigazione nel "
            "weekend e la vita in barca. Includi almeno:"
        )
    else:
        section3_title = "## 3. Proiezioni / Navigazione"
        section3_intro = (
            "Usa la sezione delle proiezioni a 12 ore e intervalli successivi del "
            "Meteomar per generare deduzioni pratiche per la navigazione nei "
            "prossimi giorni e la vita in barca. NON fare riferimento al "
            '"weekend": limitati all\'orizzonte temporale effettivamente coperto '
            "dal bollettino. Includi almeno:"
        )

    return f"""Sei un meteorologo marino esperto e un istruttore di vela d'altura.
Ricevi due fonti dati:
1. Un'immagine: la carta sinottica di pressione al suolo (isobare, minimi, massimi, fronti).
2. Un testo grezzo: l'ultimo bollettino Meteomar (sezioni SITUAZIONE, PRESSIONE,
   AVVISI, PREVISIONE per zone di mare, e PROIEZIONI a 12h e intervalli successivi).

Produci un report di analisi meteo e pianificazione per la navigazione in
italiano, in formato Markdown rigoroso, seguendo ESATTAMENTE questa struttura:

# Analisi Meteo & Pianificazione — {{DATA E ORARIO DI EMISSIONE}}

## 1. Comparazione
Sezione discorsiva che confronta la situazione sinottica visibile sulla mappa
(distanza tra le isobare, gradiente barico, posizione di minimi/massimi e fronti)
con quanto riportato nelle sezioni "SITUAZIONE" e "PRESSIONE" del Meteomar.
Evidenzia coerenze e discrepanze.

## 2. Il Dettaglio per la nostra area
Analisi mirata sui mari che lambiscono le seguenti aree di navigazione: {NAV_AREAS}.
Sintetizza per le prime 24 ore: vento (direzione e forza in scala Beaufort),
stato del mare, cielo e visibilità, estrapolando i dati dal bollettino.

{section3_title}
{section3_intro}
- **Uso del motore**: necessità di usare il motore in base all'intensità del vento
  previsto (es. venti di Forza 2) e indicazioni sulle brezze.
- **Ancoraggi notturni**: implicazioni per gli ancoraggi in rada dedotte dallo
  stato del mare previsto (es. "MARE 2"), in termini tecnici (protezione,
  esposizione, moto ondoso), senza giudizi soggettivi.

Regole di stile e contenuto:
- Basa OGNI affermazione sui dati forniti (carta e bollettino) e, dove possibile,
  cita il dato di riferimento (es. "MARE 2", "vento SUDOVEST 3", "isobare a 1016 hPa").
  Non inventare valori.
- Esponi i fatti in modo neutro e oggettivo. È VIETATO usare aggettivi enfatici,
  valutativi o promozionali quali "perfetto/a", "ideale", "ottimo", "eccellente",
  "splendido", "magnifico", "straordinario", "fantastico" e simili, così come
  esclamazioni o toni entusiastici. Il divieto vale in QUALSIASI contesto: ad
  esempio non scrivere "perfetta coerenza" ma "piena coerenza" o "totale coerenza".
- Non esprimere giudizi o preferenze personali: limitati a descrivere le condizioni
  e le loro conseguenze pratiche derivandole dai dati (es. "con MARE 2 il moto
  ondoso è contenuto", non "condizioni perfette per l'ancoraggio").
- Se rilevi condizioni favorevoli o sfavorevoli, esprimile in modo fattuale e
  quantificato (forza del vento, stato del mare, visibilità), non con qualificazioni
  soggettive.
- Sono VIETATI anche gli aggettivi soggettivi di comfort/sicurezza quali
  "tranquillo", "comodo", "confortevole", "sicuro", "piacevole", "rilassante",
  "protetto/riparato" usati come giudizio. Al loro posto descrivi il fatto tecnico:
  esposizione ai quadranti, presenza di risacca, moto ondoso, tenuta dell'ancoraggio.
- Per OGNI indicazione di vento riporta SEMPRE sia la forza Beaufort sia il campo
  di velocità corrispondente in nodi (es. "Forza 3 (7-10 nodi)", "SUDOVEST 4
  (11-16 nodi)"). Usa gli intervalli standard della scala Beaufort.
- Se una fonte è mancante o illeggibile, dichiaralo esplicitamente e prosegui con
  l'altra.
- Usa un tono professionale, tecnico e sobrio. Non aggiungere testo fuori dalla
  struttura richiesta.
"""


def build_analysis(
    client: genai.Client,
    chart: Optional[tuple[bytes, str]],
    meteomar_text: Optional[str],
    emission_time: str,
    system_prompt: str,
) -> Optional[str]:
    """Call the multimodal Gemini model and return the Markdown report text."""
    # Assemble the request parts: image (if any) + textual context.
    parts: list = []

    if chart is not None:
        image_bytes, media_type = chart
        parts.append(
            genai_types.Part.from_bytes(data=image_bytes, mime_type=media_type)
        )
    else:
        parts.append(
            genai_types.Part.from_text(
                text="[ATTENZIONE] Carta sinottica NON disponibile per questa emissione."
            )
        )

    meteomar_block = meteomar_text or "[ATTENZIONE] Bollettino Meteomar NON disponibile."
    parts.append(
        genai_types.Part.from_text(
            text=(
                f"Orario di emissione (UTC): {emission_time}\n"
                f"Aree di navigazione richieste: {NAV_AREAS}\n\n"
                "=== TESTO GREZZO METEOMAR ===\n"
                f"{meteomar_block}\n"
                "=== FINE TESTO METEOMAR ===\n\n"
                "Analizza la carta sinottica (immagine) e il bollettino qui sopra e "
                "genera il report seguendo la struttura del system prompt."
            )
        )
    )

    config_kwargs = dict(
        system_instruction=system_prompt,
        max_output_tokens=MAX_TOKENS,
        temperature=0.4,
    )
    # Optionally bound the model's reasoning so the answer always has room.
    if GEMINI_THINKING_BUDGET >= 0:
        config_kwargs["thinking_config"] = genai_types.ThinkingConfig(
            thinking_budget=GEMINI_THINKING_BUDGET
        )

    try:
        log.info("Requesting analysis from Gemini model %s ...", GEMINI_MODEL)
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=parts,
            config=genai_types.GenerateContentConfig(**config_kwargs),
        )
    except genai_errors.APIError as exc:
        log.error("Gemini API error (%s): %s", getattr(exc, "code", "?"), exc)
        return None
    except Exception:  # noqa: BLE001 - defensive: never crash the scheduler here
        log.exception("Unexpected error calling the Gemini API.")
        return None

    # Inspect why generation stopped: thinking models can hit the token ceiling
    # and return a report truncated mid-text, which we must not silently accept.
    finish = None
    try:
        finish = response.candidates[0].finish_reason
    except (AttributeError, IndexError, TypeError):
        pass
    usage = getattr(response, "usage_metadata", None)

    text = (response.text or "").strip()
    if not text:
        log.error("Gemini returned an empty report (check safety filters / quota).")
        return None

    if finish is not None and "MAX_TOKENS" in str(finish):
        log.warning(
            "Report TRUNCATED (finish_reason=MAX_TOKENS): raise MAX_TOKENS "
            "(current=%d) or set GEMINI_THINKING_BUDGET lower. Reasoning tokens: %s.",
            MAX_TOKENS, getattr(usage, "thoughts_token_count", "?"),
        )

    log.info("Report generated (%d chars, finish=%s).", len(text), finish)
    return text


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #

_IMAGE_EXT = {"image/png": "png", "image/jpeg": "jpg",
              "image/gif": "gif", "image/webp": "webp"}


# AI-generated content disclosure appended to every report. It lives in the
# report itself rather than in the page chrome, so it travels with the Markdown
# when a report is downloaded or forwarded, and is shown exactly once when the
# report is rendered in index.html.
AI_DISCLOSURE_MD = """\
\n\n---\n
## Contenuto generato da intelligenza artificiale

Questo report è prodotto automaticamente da un modello di IA generativa ({model})
a partire dalla carta di pressione al suolo del Met Office e dal bollettino
Meteomar del C.N.M.C.A. Il testo non è rivisto da un operatore umano prima della
pubblicazione e può contenere errori o imprecisioni. Non sostituisce i bollettini
meteorologici ufficiali: per la navigazione fare sempre riferimento alle fonti
originali, riportate nella sezione "Fonti".
"""


def _report_subdir(dt: datetime) -> Path:
    """Return the year/month/week subdirectory (relative to OUTPUT_DIR) for a
    report emitted at ``dt``.

    Reports are filed under ``<year>/<month>/W<isoweek>`` so the output volume
    stays tidy as bulletins accumulate. The ISO week number is used so a "week"
    folder maps to the same Mon-Sun span the home page treats as current.
    """
    iso_week = dt.isocalendar()[1]
    return Path(str(dt.year)) / f"{dt.month:02d}" / f"W{iso_week:02d}"


def _build_sources_section(
    stamp: str,
    subdir: Path,
    chart: Optional[tuple[bytes, str]],
    meteomar_text: Optional[str],
) -> str:
    """Save the source chart/bulletin next to the report and return a Markdown
    "Fonti" section that embeds the chart image and the full Meteomar text.

    The sources are saved inside ``OUTPUT_DIR/subdir`` (same folder as the
    report) and the links are made relative to the output root, because the
    Markdown is rendered client-side against the root index.html. The saved
    filenames are timestamped so each report keeps its own sources; they are
    served (and thus viewable/downloadable) by the web container.
    """
    href_prefix = subdir.as_posix()
    parts = ["\n\n---\n\n## Fonti\n"]

    parts.append("### Carta di pressione al suolo (Met Office)\n")
    if chart is not None:
        image_bytes, media_type = chart
        chart_name = f"chart_{stamp}.{_IMAGE_EXT.get(media_type, 'png')}"
        (OUTPUT_DIR / subdir / chart_name).write_bytes(image_bytes)
        parts.append(f"![Carta di pressione al suolo]({href_prefix}/{chart_name})\n")
        parts.append(f"[⬇ Scarica la carta]({href_prefix}/{chart_name})\n")
    else:
        parts.append("_Non disponibile per questa emissione._\n")

    parts.append("### Bollettino Meteomar (testo integrale)\n")
    if meteomar_text:
        mm_name = f"meteomar_{stamp}.txt"
        (OUTPUT_DIR / subdir / mm_name).write_text(meteomar_text, encoding="utf-8")
        parts.append(f"[⬇ Scarica il bollettino]({href_prefix}/{mm_name})\n")
        parts.append(f"```text\n{meteomar_text}\n```\n")
    else:
        parts.append("_Non disponibile per questa emissione._\n")

    return "\n".join(parts)


def write_report(
    markdown: str,
    emission_time: str,
    emission_dt: datetime,
    chart: Optional[tuple[bytes, str]] = None,
    meteomar_text: Optional[str] = None,
) -> Path:
    """Write the report to the output volume and update 'latest.md'.

    The report and its sources are filed under a ``<year>/<month>/W<week>``
    subdirectory (derived from ``emission_dt``) to keep the output volume tidy.
    The pressure-chart image and the raw Meteomar bulletin are saved alongside
    and appended to the report as a "Fonti" section, followed by the AI
    disclosure so it stays with the file when the Markdown is downloaded.
    """
    subdir = _report_subdir(emission_dt)
    (OUTPUT_DIR / subdir).mkdir(parents=True, exist_ok=True)

    stamp = emission_time.replace(":", "").replace("-", "").replace(" ", "_")
    full_markdown = (
        markdown
        + _build_sources_section(stamp, subdir, chart, meteomar_text)
        + AI_DISCLOSURE_MD.format(model=GEMINI_MODEL)
    )

    dated_path = OUTPUT_DIR / subdir / f"analisi_meteo_{stamp}.md"
    latest_path = OUTPUT_DIR / "latest.md"

    dated_path.write_text(full_markdown, encoding="utf-8")
    latest_path.write_text(full_markdown, encoding="utf-8")

    log.info("Report written to %s (and %s).", dated_path, latest_path)

    # Refresh the browsable HTML index served by the companion web container.
    write_index()
    return dated_path


# HTML shell for the report browser. The report list is injected in place of
# {{ITEMS}}; report bodies (Markdown) are rendered client-side with marked.js.
INDEX_TEMPLATE = """<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Analisi Meteo Marina — Caprera</title>
<style>
  :root { color-scheme: light dark; }
  * { box-sizing: border-box; }
  /* Column flexbox so the footer always sits below the content, whether the
     layout is shorter or taller than the viewport. */
  body { margin:0; font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
         background:#0b1622; color:#e6edf3;
         display:flex; flex-direction:column; min-height:100vh; }
  /* Right padding keeps the title clear of the absolutely positioned version
     badge, including when it wraps on narrow screens. */
  header { position:relative; padding:1.1rem 6rem 1.1rem 1.5rem; background:#0d2136;
           border-bottom:1px solid #1e3a5f; }
  header h1 { margin:0; font-size:1.2rem; }
  header p { margin:.3rem 0 0; color:#8aa0b5; font-size:.85rem; }
  header .ver { position:absolute; top:1.1rem; right:1.5rem; color:#8aa0b5;
                font-size:.75rem; border:1px solid #1e3a5f; border-radius:999px;
                padding:.15rem .55rem; white-space:nowrap; }
  /* min-height:0 lets the two panels scroll internally instead of stretching
     the page to the height of the tallest one. */
  .layout { display:flex; flex:1; min-height:0; }
  aside { width:340px; flex:0 0 340px; border-right:1px solid #1e3a5f; overflow-y:auto; }
  /* Sidebar list styles are scoped to <aside> so they never leak into the
     rendered report content in <article>. */
  aside ul { list-style:none; margin:0; padding:0; }
  aside li { display:flex; flex-direction:column; align-items:flex-start; gap:.4rem;
       padding:.7rem .9rem; border-bottom:1px solid #14263c; }
  aside li.empty { color:#8aa0b5; }
  aside section { border-bottom:1px solid #1e3a5f; }
  aside h2 { margin:0; padding:.7rem .9rem; font-size:.78rem; text-transform:uppercase;
       letter-spacing:.06em; color:#8aa0b5; background:#0d2136; }
  /* Archive drill-down: year > month > week, each a collapsible <details>. */
  aside details { border-top:1px solid #14263c; }
  aside details > summary { cursor:pointer; padding:.55rem .9rem; color:#c6d4e2;
       font-size:.85rem; user-select:none; }
  aside details > summary:hover { color:#e6edf3; }
  aside details.yr > summary { font-weight:600; }
  aside details.mo > summary { padding-left:1.6rem; color:#a9bccf; }
  aside details.wk > summary { padding-left:2.3rem; font-size:.8rem; color:#8aa0b5; }
  aside details.wk ul li { padding-left:1.6rem; }
  /* External reference links, always visible at the bottom of the sidebar. */
  aside .utili li { flex-direction:row; align-items:center; gap:.5rem; }
  aside .utili a { color:#7fb4ff; text-decoration:none; font-size:.88rem; }
  aside .utili a:hover { text-decoration:underline; }
  button.view { width:100%; text-align:left; background:none; border:none; color:#7fb4ff;
                cursor:pointer; font-size:.9rem; padding:0; }
  button.view:hover { text-decoration:underline; }
  .links { display:flex; flex-wrap:wrap; gap:.9rem; }
  a.dl { color:#8aa0b5; text-decoration:none; font-size:.78rem; white-space:nowrap; }
  a.dl:hover { color:#e6edf3; }
  main { flex:1; overflow-y:auto; padding:1.5rem 2rem; }
  article { max-width:820px; margin:0 auto; line-height:1.6; }
  article h1 { font-size:1.5rem; } article a { color:#7fb4ff; }
  article ul, article ol { padding-left:1.3rem; }
  article li { margin:.25rem 0; }
  article table { border-collapse:collapse; }
  article th, article td { border:1px solid #2a4256; padding:.4rem .6rem; }
  /* The pressure chart is a fixed-width image and the bulletin is a wide
     preformatted block: both must be kept inside the column instead of
     widening the page. The bulletin scrolls sideways on its own. */
  article img { max-width:100%; height:auto; }
  article pre { overflow-x:auto; }
  .placeholder { color:#8aa0b5; text-align:center; margin-top:3rem; }
  /* Site footer: the GitHub mark is an inline SVG so the page keeps working
     with no icon font, no image request and no third-party asset. */
  body > footer { display:flex; justify-content:center; padding:.75rem 1.5rem;
                  background:#0d2136; border-top:1px solid #1e3a5f; font-size:.8rem; }
  body > footer a { display:inline-flex; align-items:center; gap:.45rem;
                    color:#8aa0b5; text-decoration:none; }
  body > footer a:hover { color:#e6edf3; }
  body > footer svg { width:17px; height:17px; fill:currentColor; }
  /* Phones: the 340px sidebar leaves no room for the report next to it, so
     stack the two. The list is capped and scrolls on its own, keeping the
     report reachable without paging through the whole archive. */
  @media (max-width: 760px) {
    .layout { flex-direction:column; min-height:auto; }
    aside { width:100%; flex:none; max-height:42vh;
            border-right:none; border-bottom:1px solid #1e3a5f; }
    main { padding:1.1rem 1.1rem 2rem; }
    header { padding:.9rem 1.1rem; }
    header h1 { font-size:1.05rem; }
    /* Not enough room beside a wrapping title: let the badge flow below it. */
    header .ver { position:static; display:inline-block; margin-top:.5rem; }
    article h1 { font-size:1.25rem; }
    article table { display:block; overflow-x:auto; }
  }
</style>
</head>
<body>
<header>
  <h1>🌊 Analisi Meteo Marina — Caprera / La Maddalena</h1>
  <p>Report generati automaticamente. Seleziona un report per visualizzarlo o scaricarlo.</p>
  <span class="ver">ver. {{VERSION}}</span>
</header>
<div class="layout">
  <aside>
    <section class="current">
      <h2>Settimana in corso</h2>
      <ul>{{CURRENT}}</ul>
    </section>
    <section class="archive">
      <h2>Archivio</h2>
      {{ARCHIVE}}
    </section>
    <section class="utili">
      <h2>Link utili</h2>
      <ul>
        <li><a href="https://www.sat24.com/it-it/country/it#lightning=on" target="_blank" rel="noopener" data-track="Sat24">🛰 Satellite &amp; fulmini (Sat24)</a></li>
        <li><a href="https://www.meteoam.it/it/meteomar" target="_blank" rel="noopener" data-track="Meteomar (Meteo AM)">📄 Bollettino Meteomar (Meteo AM)</a></li>
      </ul>
    </section>
  </aside>
  <main>
    <article id="content"><p class="placeholder">Seleziona un report dall'elenco.</p></article>
  </main>
</div>
<footer>
  <a href="https://github.com/cvcmeteo/marine-weather-analysis" target="_blank" rel="noopener" data-track="GitHub">
    <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.13 1.92.08 2.12.51.56.82 1.28.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>
    Codice sorgente su GitHub
  </a>
</footer>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<script>
  const content = document.getElementById('content');
  async function show(file){
    content.innerHTML = '<p class="placeholder">Caricamento…</p>';
    try {
      const r = await fetch(file, {cache:'no-store'});
      if(!r.ok) throw new Error(r.status);
      content.innerHTML = marked.parse(await r.text());
    } catch(e){
      content.innerHTML = '<p class="placeholder">Impossibile caricare il report ('+e+').</p>';
    }
  }
  document.querySelectorAll('button.view').forEach(b =>
    b.addEventListener('click', () => show(b.dataset.file)));
  const first = document.querySelector('button.view');   // auto-load newest report
  if (first) show(first.dataset.file);

  // Clicks on outbound links open another site, so they never reach our access
  // log: ping /_e instead, which nginx answers with 204 purely to record the
  // URL. Report views need no beacon — fetching the .md is already a request.
  // An <img> request (rather than fetch/sendBeacon) fires synchronously enough
  // to survive the navigation and works with no CORS or method concerns.
  document.querySelectorAll('a[data-track]').forEach(a =>
    a.addEventListener('click', () => {
      new Image().src = '/_e?ev=out&t=' + encodeURIComponent(a.dataset.track);
    }));
</script>
</body>
</html>
"""


# Italian month names, indexed 1-12 (index 0 unused), for archive labels.
_MONTHS_IT = [
    "", "Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
    "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre",
]


def _report_dt(path: Path) -> Optional[datetime]:
    """Parse the emission datetime encoded in a report filename, or None."""
    stamp = path.stem.replace("analisi_meteo_", "", 1)
    try:
        return datetime.strptime(stamp, "%Y%m%d_%H%M_UTC").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None


def _report_li(path: Path) -> str:
    """Render a single report as an <li> with a view button and source links.

    All hrefs are made relative to OUTPUT_DIR (the site root) so they resolve
    correctly regardless of which year/month/week folder the report lives in.
    """
    try:
        first_line = path.read_text(encoding="utf-8").splitlines()[0]
    except (OSError, IndexError):
        first_line = ""
    title = first_line.lstrip("# ").strip() or path.stem

    # Locate the source files saved next to this report (same timestamp stamp).
    stamp = path.stem.replace("analisi_meteo_", "", 1)
    chart_file = next(iter(path.parent.glob(f"chart_{stamp}.*")), None)
    meteomar_file = path.parent / f"meteomar_{stamp}.txt"

    file_href = path.relative_to(OUTPUT_DIR).as_posix()
    links = [f'<a class="dl" href="{file_href}" download>⬇ Report</a>']
    if chart_file is not None:
        chart_href = chart_file.relative_to(OUTPUT_DIR).as_posix()
        links.append(f'<a class="dl" href="{chart_href}" target="_blank">🗺 Carta</a>')
    if meteomar_file.exists():
        mm_href = meteomar_file.relative_to(OUTPUT_DIR).as_posix()
        links.append(f'<a class="dl" href="{mm_href}" target="_blank">📄 Bollettino</a>')

    return (
        f'<li><button class="view" data-file="{file_href}">{title}</button>'
        f'<span class="links">{"".join(links)}</span></li>'
    )


def write_index() -> None:
    """(Re)generate index.html in the output dir: a browsable list of reports.

    Served by the companion nginx container. The home page shows only the
    reports of the current ISO week; everything older is tucked into a
    collapsible year > month > week archive. Each entry links to the Markdown
    file (downloadable) and can be rendered in-page. Safe to call any time.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Scan recursively (reports now live in year/month/week folders). Filenames
    # are timestamped, so reverse-sorting puts the newest first.
    reports = sorted(OUTPUT_DIR.glob("**/analisi_meteo_*.md"), reverse=True)

    now = datetime.now(timezone.utc)
    current_key = now.isocalendar()[:2]  # (iso_year, iso_week)

    current_items: list[str] = []
    # archive: {year: {month: {iso_week: [paths]}}}, all keys sorted later.
    archive: dict[int, dict[int, dict[int, list[Path]]]] = {}

    for path in reports:
        dt = _report_dt(path)
        if dt is not None and dt.isocalendar()[:2] == current_key:
            current_items.append(_report_li(path))
        elif dt is not None:
            iso_week = dt.isocalendar()[1]
            (archive.setdefault(dt.year, {})
                    .setdefault(dt.month, {})
                    .setdefault(iso_week, [])).append(path)

    if not current_items:
        current_items.append('<li class="empty">Nessun report per la settimana in corso.</li>')

    # Build the nested year > month > week archive (newest first at every level).
    archive_parts: list[str] = []
    for year in sorted(archive, reverse=True):
        month_parts: list[str] = []
        for month in sorted(archive[year], reverse=True):
            week_parts: list[str] = []
            for iso_week in sorted(archive[year][month], reverse=True):
                lis = "\n".join(_report_li(p) for p in archive[year][month][iso_week])
                week_parts.append(
                    f'<details class="wk"><summary>Settimana {iso_week:02d}</summary>'
                    f'<ul>{lis}</ul></details>'
                )
            month_parts.append(
                f'<details class="mo"><summary>{_MONTHS_IT[month]}</summary>'
                f'{"".join(week_parts)}</details>'
            )
        archive_parts.append(
            f'<details class="yr"><summary>{year}</summary>'
            f'{"".join(month_parts)}</details>'
        )

    archive_html = "".join(archive_parts) or '<p class="empty" style="padding:.7rem .9rem">Nessun report archiviato.</p>'

    html = (
        INDEX_TEMPLATE
        .replace("{{CURRENT}}", "\n".join(current_items))
        .replace("{{ARCHIVE}}", archive_html)
        .replace("{{VERSION}}", APP_VERSION)
    )
    (OUTPUT_DIR / "index.html").write_text(html, encoding="utf-8")
    log.info("Wrote index.html (%d report(s)).", len(reports))


# --------------------------------------------------------------------------- #
# Visitor statistics (/statistica)
# --------------------------------------------------------------------------- #

# Requests whose User-Agent matches are crawlers, uptime probes and scripts.
# They dominate the raw counts on a public host, so they are dropped before any
# aggregation: what is left is a rough but honest picture of human traffic.
_BOT_UA_RE = re.compile(
    r"bot|crawl|spider|slurp|scrap|curl|wget|python-requests|go-http-client|"
    r"okhttp|java/|libwww|headless|phantom|monitor|uptime|pingdom|preview|"
    r"facebookexternalhit|whatsapp|telegrambot|feed|lighthouse|apache-httpclient",
    re.IGNORECASE,
)

# Phones and tablets, for the device split. Deliberately crude: the UA string is
# only ever used for these two buckets and for bot filtering.
_MOBILE_UA_RE = re.compile(
    r"android|iphone|ipad|ipod|mobile|windows phone|opera mini",
    re.IGNORECASE,
)

# Report bodies are fetched by the page itself (marked.js) — one request per
# report opened — while the sources are plain downloads.
_REPORT_URI_RE = re.compile(r"/analisi_meteo_(\d{8}_\d{4}_UTC)\.md$")
_CHART_URI_RE = re.compile(r"/chart_(\d{8}_\d{4}_UTC)\.\w+$")
_BULLETIN_URI_RE = re.compile(r"/meteomar_(\d{8}_\d{4}_UTC)\.txt$")

_ITALIAN_WEEKDAYS = ["Lunedì", "Martedì", "Mercoledì", "Giovedì",
                     "Venerdì", "Sabato", "Domenica"]


def _geoip_reader():
    """Open the local GeoIP database, or return None if unavailable.

    Both the MaxMind GeoLite2-City and the DB-IP City Lite files work: they
    share the .mmdb format and the ``country``/``city`` record layout. The
    lookup is optional — without it the page falls back to the country
    Cloudflare reports and simply shows no cities.
    """
    if not GEOIP_DB_PATH.is_file():
        return None
    try:
        import maxminddb  # lazy: the app must run without the package
        return maxminddb.open_database(str(GEOIP_DB_PATH))
    except Exception as exc:  # noqa: BLE001 - never break the page over geo
        log.warning("GeoIP database %s could not be opened: %s", GEOIP_DB_PATH, exc)
        return None


def _geoip_lookup(reader, ip: str) -> tuple[str, str]:
    """Return ``(country_code, city_name)`` for an IP; empty strings if unknown.

    The country is kept as an ISO code so it can be merged with the code
    Cloudflare sends, whatever the source of the lookup.
    """
    if reader is None or not ip:
        return "", ""
    try:
        record = reader.get(ip) or {}
    except Exception:  # noqa: BLE001 - malformed address, unmapped range, ...
        return "", ""
    country = record.get("country") or {}
    city_names = (record.get("city") or {}).get("names", {})
    return (country.get("iso_code") or "",
            city_names.get("it") or city_names.get("en") or "")


# Italian names for the countries this site realistically sees. Anything else
# falls back to the bare ISO code, which is still readable next to the flag.
_COUNTRY_NAMES_IT = {
    "IT": "Italia", "FR": "Francia", "DE": "Germania", "GB": "Regno Unito",
    "ES": "Spagna", "CH": "Svizzera", "AT": "Austria", "NL": "Paesi Bassi",
    "BE": "Belgio", "US": "Stati Uniti", "SE": "Svezia", "NO": "Norvegia",
    "DK": "Danimarca", "FI": "Finlandia", "PL": "Polonia", "PT": "Portogallo",
    "IE": "Irlanda", "GR": "Grecia", "HR": "Croazia", "SI": "Slovenia",
    "MT": "Malta", "MC": "Monaco", "CZ": "Cechia", "RO": "Romania",
    "CA": "Canada", "AU": "Australia", "BR": "Brasile", "AR": "Argentina",
}


def _country_label(code: str) -> str:
    """Render an ISO country code as "🇮🇹 Italia" (flag + name, or + code).

    Regional-indicator letters sit at a fixed offset from A-Z, so the flag can
    be derived from any well-formed code without a lookup table.
    """
    code = (code or "").upper()
    if len(code) != 2 or not code.isalpha():
        return code or "Sconosciuto"
    flag = "".join(chr(0x1F1E6 + ord(letter) - ord("A")) for letter in code)
    return f"{flag} {_COUNTRY_NAMES_IT.get(code, code)}"


def _classify_uri(uri: str, dest: str = "") -> tuple[str, str]:
    """Map a request URI to an ``(event_kind, label)`` pair.

    ``dest`` is the request's ``Sec-Fetch-Dest`` header, needed only to tell a
    chart shown inside a report from one deliberately downloaded (see below).

    Everything that is not one of the tracked interactions (assets, favicons,
    the statistics page itself) is returned as ``("other", "")`` and dropped by
    the caller.
    """
    path = urlsplit(uri).path

    if path in ("/", "/index.html"):
        return "page", "Home"
    if path.startswith("/statistica"):
        return "other", ""          # never count the stats page in its own numbers

    # Outbound-link beacon: the URL carries the event, nothing is served.
    if path.startswith("/_e"):
        params = parse_qs(urlsplit(uri).query)
        target = (params.get("t") or [""])[0][:60]
        kind = (params.get("ev") or [""])[0]
        return ("outlink", unquote(target)) if kind == "out" else ("other", "")

    if path.endswith("/latest.md"):
        return "report", "latest"
    match = _REPORT_URI_RE.search(path)
    if match:
        return "report", match.group(1)
    match = _CHART_URI_RE.search(path)
    if match:
        # The chart is embedded in the report body, so most of its requests are
        # an <img> load that happens by itself when the report is opened
        # (Sec-Fetch-Dest: image). A click on the "Scarica la carta" link is a
        # navigation instead ("document", or "empty" when the browser turns it
        # straight into a download). Browsers that send no Sec-Fetch header at
        # all are counted as views, which is what nearly every chart request is.
        return ("chart" if dest in ("", "image") else "chart_download",
                match.group(1))
    match = _BULLETIN_URI_RE.search(path)
    if match:
        return "bulletin", match.group(1)
    return "other", ""


def _report_title(stamp: str) -> str:
    """Human label for a report timestamp stamp (``YYYYMMDD_HHMM_UTC``).

    Falls back to a formatted date when the report file is gone (or is the
    rolling ``latest.md``, which has no stamp of its own).
    """
    if stamp == "latest":
        return "latest.md (ultimo report)"
    try:
        dt = datetime.strptime(stamp, "%Y%m%d_%H%M_UTC")
    except ValueError:
        return stamp
    label = dt.strftime("%d/%m/%Y %H:%M UTC")
    path = OUTPUT_DIR / _report_subdir(dt) / f"analisi_meteo_{stamp}.md"
    if path.is_file():
        try:
            first_line = path.read_text(encoding="utf-8").splitlines()[0]
            title = first_line.lstrip("# ").strip()
            if title:
                return f"{label} — {title}"
        except (OSError, IndexError):
            pass
    return label


def collect_stats() -> Optional[dict]:
    """Parse the nginx access log and return the aggregates for /statistica.

    Returns None when the log is missing (the web container has not been
    reconfigured yet) so the caller can skip the page instead of publishing an
    empty one. Raw IPs are never kept: they are resolved to a country/city and
    counted into a set of hashes for the unique-visitor figure, and the hashes
    live only for the duration of this call.
    """
    if not ACCESS_LOG_PATH.is_file():
        log.warning("Access log %s not found; skipping statistics.", ACCESS_LOG_PATH)
        return None

    cutoff = datetime.now(timezone.utc) - timedelta(days=STATS_WINDOW_DAYS)
    reader = _geoip_reader()

    countries, cities, hours, weekdays, days = (
        Counter(), Counter(), Counter(), Counter(), Counter())
    reports, charts, bulletins, outlinks, referrers = (
        Counter(), Counter(), Counter(), Counter(), Counter())
    # Charts opened on purpose, as opposed to `charts` (shown inside a report).
    chart_downloads = Counter()
    devices = Counter()
    visitors: set[str] = set()
    page_views = 0
    total = 0
    first_seen: Optional[datetime] = None
    last_seen: Optional[datetime] = None
    malformed = 0

    try:
        lines = ACCESS_LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        log.warning("Could not read %s: %s", ACCESS_LOG_PATH, exc)
        return None

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            malformed += 1
            continue

        # nginx writes $time_iso8601 (e.g. 2026-08-12T09:31:04+00:00); the web
        # container runs with TZ=UTC so the offset is always zero, but parse it
        # properly anyway in case that is ever changed.
        try:
            when = datetime.fromisoformat(entry.get("t", ""))
        except ValueError:
            malformed += 1
            continue
        when = when.astimezone(timezone.utc) if when.tzinfo else when.replace(
            tzinfo=timezone.utc)
        if when < cutoff:
            continue

        if entry.get("status") not in (200, 204, 206, 304):
            continue
        user_agent = entry.get("ua") or ""
        if not user_agent or _BOT_UA_RE.search(user_agent):
            continue

        kind, label = _classify_uri(entry.get("uri") or "", entry.get("dest") or "")
        if kind == "other":
            continue

        total += 1
        first_seen = when if first_seen is None or when < first_seen else first_seen
        last_seen = when if last_seen is None or when > last_seen else last_seen

        hours[when.hour] += 1
        weekdays[when.weekday()] += 1
        days[when.strftime("%Y-%m-%d")] += 1
        devices["Mobile" if _MOBILE_UA_RE.search(user_agent) else "Desktop"] += 1

        ip = entry.get("ip") or ""
        if ip:
            # Hashed with the UA so the raw address is never held beyond this
            # loop; good enough to count returning visitors, useless as an
            # identifier once the function returns.
            visitors.add(str(hash((ip, user_agent))))

        country, city = _geoip_lookup(reader, ip)
        # "XX" is what Cloudflare sends when it cannot place the address.
        country = country or (entry.get("cc") or "")
        city = city or (entry.get("city") or "")
        if country and country not in ("XX", "T1"):
            countries[country] += 1
        if city:
            cities[f"{city} ({country})" if country else city] += 1

        referer = entry.get("ref") or ""
        if referer and referer != "-":
            host = urlsplit(referer).netloc
            if host and "cvcmeteo" not in host:
                referrers[host] += 1

        if kind == "page":
            page_views += 1
        elif kind == "report":
            reports[label] += 1
        elif kind == "chart":
            charts[label] += 1
        elif kind == "chart_download":
            chart_downloads[label] += 1
        elif kind == "bulletin":
            bulletins[label] += 1
        elif kind == "outlink":
            outlinks[label or "(sconosciuto)"] += 1

    if reader is not None:
        reader.close()
    if malformed:
        log.warning("Skipped %d malformed access-log line(s).", malformed)

    return {
        "generated": datetime.now(timezone.utc),
        "first_seen": first_seen,
        "last_seen": last_seen,
        "requests": total,
        "page_views": page_views,
        "visitors": len(visitors),
        "reports": reports,
        "report_total": sum(reports.values()),
        # Deliberate downloads only: a chart shown inside a report is a view.
        "downloads": sum(chart_downloads.values()) + sum(bulletins.values()),
        "chart_views": sum(charts.values()),
        "charts": charts,
        "chart_downloads": chart_downloads,
        "bulletins": bulletins,
        "outlinks": outlinks,
        "referrers": referrers,
        "countries": countries,
        "cities": cities,
        "hours": hours,
        "weekdays": weekdays,
        "days": days,
        "devices": devices,
        "geoip": GEOIP_DB_PATH.is_file(),
    }


def _bar_rows(counter: Counter, limit: int = 12,
              labeller=None, empty: str = "Nessun dato.") -> str:
    """Render a Counter as a list of labelled proportional bars."""
    items = counter.most_common(limit)
    if not items:
        return f'<p class="empty">{empty}</p>'
    top = items[0][1] or 1
    rows = []
    for name, count in items:
        text = labeller(name) if labeller else str(name)
        width = max(1, round(count * 100 / top))
        rows.append(
            f'<div class="row"><span class="lbl" title="{html_escape(text, quote=True)}">'
            f'{html_escape(text)}</span>'
            f'<span class="bar"><i style="width:{width}%"></i></span>'
            f'<span class="num">{count}</span></div>'
        )
    return "".join(rows)


def _hour_columns(hours: Counter) -> str:
    """Render the 24 UTC hour buckets as a column chart (always all 24)."""
    top = max(hours.values(), default=0) or 1
    cols = []
    for hour in range(24):
        count = hours.get(hour, 0)
        height = round(count * 100 / top)
        cols.append(
            f'<div class="col" title="{hour:02d}:00–{hour:02d}:59 UTC — {count}">'
            f'<i style="height:{height}%"></i><span>{hour:02d}</span></div>'
        )
    return "".join(cols)


def _day_columns(days: Counter, limit: int = 30) -> str:
    """Render the last ``limit`` calendar days as a column chart.

    Days with no traffic are shown as gaps rather than omitted, so the shape of
    the series is not distorted by quiet periods.
    """
    if not days:
        return '<p class="empty">Nessun dato.</p>'
    last = max(datetime.strptime(d, "%Y-%m-%d") for d in days)
    span = [(last - timedelta(days=offset)).strftime("%Y-%m-%d")
            for offset in range(limit - 1, -1, -1)]
    top = max(days.values(), default=0) or 1
    cols = []
    for day in span:
        count = days.get(day, 0)
        height = round(count * 100 / top)
        label = day[8:10] if day[8:10] in ("01", "05", "10", "15", "20", "25") else ""
        cols.append(
            f'<div class="col" title="{day} — {count}">'
            f'<i style="height:{height}%"></i><span>{label}</span></div>'
        )
    return "".join(cols)


STATS_TEMPLATE = """<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Statistiche — Analisi Meteo Marina</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing:border-box; }
  body { margin:0; background:#0b1620; color:#e6eef5; line-height:1.5;
         font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif; }
  header { padding:1.2rem 1.6rem; border-bottom:1px solid #1e3a5f; }
  header h1 { margin:0; font-size:1.15rem; }
  header p { margin:.35rem 0 0; color:#8aa0b5; font-size:.82rem; }
  .wrap { padding:1.4rem 1.6rem 3rem; max-width:1100px; margin:0 auto; }
  /* Headline figures: a responsive row of tiles that wraps on narrow screens. */
  .tiles { display:grid; gap:.8rem; margin-bottom:1.6rem;
           grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); }
  .tile { background:#11202e; border:1px solid #1e3a5f; border-radius:8px;
          padding:.85rem 1rem; }
  .tile b { display:block; font-size:1.6rem; font-weight:600; color:#7fd4ff; }
  .tile span { color:#8aa0b5; font-size:.78rem; }
  .grid { display:grid; gap:1rem;
          grid-template-columns:repeat(auto-fit,minmax(330px,1fr)); }
  section { background:#11202e; border:1px solid #1e3a5f; border-radius:8px;
            padding:.9rem 1.1rem 1.1rem; }
  section h2 { margin:0 0 .8rem; font-size:.9rem; color:#c6d4e2;
               font-weight:600; letter-spacing:.02em; }
  section.wide { grid-column:1/-1; }
  .empty { color:#6f8398; font-size:.85rem; margin:0; }
  /* Horizontal bars: fixed label column, elastic bar, right-aligned count. */
  .row { display:grid; grid-template-columns:minmax(0,9.5rem) 1fr 3rem;
         align-items:center; gap:.6rem; margin:.3rem 0; font-size:.82rem; }
  .lbl { overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
         color:#cfdcea; }
  .bar { background:#0b1620; border-radius:3px; height:.62rem; overflow:hidden; }
  .bar i { display:block; height:100%; background:#2d7fb8; }
  .num { text-align:right; color:#8aa0b5; font-variant-numeric:tabular-nums; }
  /* Column charts (hours of the day, last 30 days). */
  .cols { display:flex; align-items:flex-end; gap:2px; height:130px;
          padding-top:.4rem; }
  .col { flex:1; display:flex; flex-direction:column; justify-content:flex-end;
         align-items:center; height:100%; min-width:0; }
  .col i { display:block; width:100%; background:#2d7fb8; border-radius:2px 2px 0 0;
           min-height:1px; }
  .col span { font-size:.6rem; color:#6f8398; margin-top:.25rem;
              white-space:nowrap; }
  footer { margin-top:1.6rem; color:#6f8398; font-size:.75rem; line-height:1.6; }
  @media (max-width:640px) {
    .wrap { padding:1.1rem 1rem 2.5rem; }
    .row { grid-template-columns:minmax(0,7rem) 1fr 2.4rem; }
  }
</style>
</head>
<body>
<header>
  <h1>📊 Statistiche di utilizzo</h1>
  <p>Analisi Meteo Marina — Caprera / La Maddalena · ver. {{VERSION}}<br>
     Periodo: {{PERIOD}} · aggiornato il {{GENERATED}}</p>
</header>
<div class="wrap">
  <div class="tiles">
    <div class="tile"><b>{{VISITORS}}</b><span>visitatori distinti</span></div>
    <div class="tile"><b>{{PAGE_VIEWS}}</b><span>aperture della home</span></div>
    <div class="tile"><b>{{REPORT_VIEWS}}</b><span>report consultati</span></div>
    <div class="tile"><b>{{CHART_VIEWS}}</b><span>carte viste nei report</span></div>
    <div class="tile"><b>{{DOWNLOADS}}</b><span>download di fonti</span></div>
    <div class="tile"><b>{{REQUESTS}}</b><span>interazioni totali</span></div>
  </div>
  <div class="grid">
    <section class="wide">
      <h2>Fascia oraria (UTC)</h2>
      <div class="cols">{{HOURS}}</div>
    </section>
    <section class="wide">
      <h2>Ultimi 30 giorni</h2>
      <div class="cols">{{DAYS}}</div>
    </section>
    <section><h2>Paesi</h2>{{COUNTRIES}}</section>
    <section><h2>Città</h2>{{CITIES}}</section>
    <section class="wide"><h2>Report più consultati</h2>{{REPORTS}}</section>
    <section><h2>Carte viste nei report</h2>{{CHARTS}}</section>
    <section><h2>Carte scaricate</h2>{{CHART_DOWNLOADS}}</section>
    <section><h2>Bollettini scaricati</h2>{{BULLETINS}}</section>
    <section><h2>Click sui link esterni</h2>{{OUTLINKS}}</section>
    <section><h2>Provenienza del traffico</h2>{{REFERRERS}}</section>
    <section><h2>Giorno della settimana</h2>{{WEEKDAYS}}</section>
    <section><h2>Dispositivi</h2>{{DEVICES}}</section>
  </div>
  <footer>
    Dati ricavati dai log del server web: nessun cookie, nessuno script di
    tracciamento, nessun servizio di terze parti. Gli indirizzi IP sono usati
    solo per ricavare paese e città e per contare i visitatori distinti, e non
    vengono conservati in questa pagina. Il traffico riconosciuto come
    automatico (crawler, sonde di monitoraggio) è escluso dai conteggi.
    Le carte «viste nei report» si caricano insieme al report che le contiene;
    i download contano solo i click sui link «Scarica».
    {{GEOIP_NOTE}}
  </footer>
</div>
</body>
</html>
"""


def write_stats() -> None:
    """(Re)generate the unlisted /statistica page. Never raises."""
    if not STATS_ENABLED:
        return
    try:
        data = collect_stats()
        if data is None:
            return

        if data["first_seen"] and data["last_seen"]:
            period = (f'{data["first_seen"]:%d/%m/%Y} – {data["last_seen"]:%d/%m/%Y} '
                      f'(finestra di {STATS_WINDOW_DAYS} giorni)')
        else:
            period = "nessuna visita registrata"

        # Cities can come from the local database or from Cloudflare's visitor
        # location headers, so the note only applies when neither produced any.
        geoip_note = "" if (data["geoip"] or data["cities"]) else (
            "Dettaglio per città non disponibile: nessun database GeoIP "
            "configurato e nessun header di posizione da Cloudflare."
        )

        page = (
            STATS_TEMPLATE
            .replace("{{VERSION}}", APP_VERSION)
            .replace("{{PERIOD}}", period)
            .replace("{{GENERATED}}", f'{data["generated"]:%d/%m/%Y %H:%M UTC}')
            .replace("{{VISITORS}}", str(data["visitors"]))
            .replace("{{PAGE_VIEWS}}", str(data["page_views"]))
            .replace("{{REPORT_VIEWS}}", str(data["report_total"]))
            .replace("{{CHART_VIEWS}}", str(data["chart_views"]))
            .replace("{{DOWNLOADS}}", str(data["downloads"]))
            .replace("{{REQUESTS}}", str(data["requests"]))
            .replace("{{HOURS}}", _hour_columns(data["hours"]))
            .replace("{{DAYS}}", _day_columns(data["days"]))
            .replace("{{COUNTRIES}}", _bar_rows(
                data["countries"], labeller=_country_label))
            .replace("{{CITIES}}", _bar_rows(
                data["cities"], empty="Nessun dato sulle città."))
            .replace("{{REPORTS}}", _bar_rows(
                data["reports"], limit=15, labeller=_report_title))
            .replace("{{CHARTS}}", _bar_rows(
                data["charts"], limit=8, labeller=_report_title))
            .replace("{{CHART_DOWNLOADS}}", _bar_rows(
                data["chart_downloads"], limit=8, labeller=_report_title,
                empty="Nessuna carta scaricata dal link."))
            .replace("{{BULLETINS}}", _bar_rows(
                data["bulletins"], limit=8, labeller=_report_title))
            .replace("{{OUTLINKS}}", _bar_rows(data["outlinks"]))
            .replace("{{REFERRERS}}", _bar_rows(
                data["referrers"], empty="Solo accessi diretti."))
            .replace("{{WEEKDAYS}}", _bar_rows(
                data["weekdays"], limit=7,
                labeller=lambda index: _ITALIAN_WEEKDAYS[int(index)]))
            .replace("{{DEVICES}}", _bar_rows(data["devices"], limit=2))
            .replace("{{GEOIP_NOTE}}", geoip_note)
        )

        stats_dir = OUTPUT_DIR / "statistica"
        stats_dir.mkdir(parents=True, exist_ok=True)
        (stats_dir / "index.html").write_text(page, encoding="utf-8")
        log.info("Wrote statistics page (%d interactions, %d visitors).",
                 data["requests"], data["visitors"])
    except Exception:  # noqa: BLE001 - statistics must never stop the service
        log.exception("Could not generate the statistics page.")


# --------------------------------------------------------------------------- #
# Pipeline orchestration
# --------------------------------------------------------------------------- #

def run_pipeline() -> None:
    """Run one full download → analyze → write cycle. Never raises."""
    emission_dt = datetime.now(timezone.utc)
    emission_time = emission_dt.strftime("%Y-%m-%d %H:%M UTC")
    log.info("=== Pipeline run started (%s) ===", emission_time)

    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        session = build_session()

        chart = download_pressure_chart(session)
        meteomar_text = scrape_meteomar(session)

        # Abort only if BOTH sources are missing; one is enough to be useful.
        if chart is None and meteomar_text is None:
            log.error("Both data sources unavailable; skipping analysis this cycle.")
            return

        # The system prompt is day-aware: it only frames section 3 around "the
        # weekend" from Thursday onward.
        system_prompt = build_system_prompt(emission_dt)
        report = build_analysis(
            client, chart, meteomar_text, emission_time, system_prompt
        )
        if report is None:
            log.error("Analysis failed; no report written this cycle.")
            return

        write_report(report, emission_time, emission_dt, chart, meteomar_text)
        write_stats()
        log.info("=== Pipeline run completed successfully ===")
    except Exception:  # noqa: BLE001 - keep the scheduler alive no matter what
        log.exception("Unexpected error during pipeline run.")


def check_sources() -> int:
    """Self-test: exercise only the scraping/rendering, no LLM call.

    Downloads both sources, saves the chart image for visual inspection, prints
    a preview of the Meteomar text, and returns a process exit code
    (0 = both sources OK, 1 = at least one missing).
    """
    log.info("=== Source self-test (no LLM call) ===")
    session = build_session()

    chart = download_pressure_chart(session)
    if chart is not None:
        image_bytes, media_type = chart
        ext = {"image/png": "png", "image/jpeg": "jpg",
               "image/gif": "gif", "image/webp": "webp"}.get(media_type, "png")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        debug_path = OUTPUT_DIR / f"_debug_chart.{ext}"
        debug_path.write_bytes(image_bytes)
        log.info("CHART OK: %d bytes (%s) saved to %s",
                 len(image_bytes), media_type, debug_path)
    else:
        log.error("CHART FAILED: could not obtain the pressure chart.")

    meteomar_text = scrape_meteomar(session)
    if meteomar_text is not None:
        preview = meteomar_text[:500].replace("\n", " ")
        log.info("METEOMAR OK: %d chars. Preview: %s ...",
                 len(meteomar_text), preview)
    else:
        log.error("METEOMAR FAILED: could not scrape the bulletin.")

    ok = chart is not None and meteomar_text is not None
    log.info("=== Self-test %s ===", "PASSED" if ok else "completed with FAILURES")
    return 0 if ok else 1


def run_service() -> None:
    """Run the long-lived scheduler (default mode)."""
    log.info(
        "Starting marine weather analysis service v%s "
        "(model=%s, interval=%dh, output=%s).",
        APP_VERSION, GEMINI_MODEL, RUN_INTERVAL_HOURS, OUTPUT_DIR,
    )

    # Ensure the browser page exists immediately (lists any existing reports),
    # even before the first pipeline run completes.
    write_index()
    write_stats()

    if RUN_ON_START:
        run_pipeline()

    # Schedule recurring runs every N hours.
    schedule.every(RUN_INTERVAL_HOURS).hours.do(run_pipeline)
    log.info("Scheduler armed: next runs every %d hours.", RUN_INTERVAL_HOURS)

    # Statistics are just a log re-read, so they refresh far more often than the
    # pipeline: without this the page would only move every RUN_INTERVAL_HOURS.
    if STATS_ENABLED:
        schedule.every(STATS_INTERVAL_MINUTES).minutes.do(write_stats)
        log.info("Statistics page refreshed every %d minutes.",
                 STATS_INTERVAL_MINUTES)

    while True:
        schedule.run_pending()
        time.sleep(30)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Marine weather analysis pipeline (scrape → Gemini → Markdown)."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--once",
        action="store_true",
        help="Run a single full pipeline cycle and exit (no scheduler).",
    )
    group.add_argument(
        "--check-sources",
        action="store_true",
        help="Test scraping/rendering only (no LLM call, no API key needed) and exit.",
    )
    group.add_argument(
        "--stats",
        action="store_true",
        help="Rebuild the /statistica page from the access log and exit "
             "(no LLM call, no API key needed).",
    )
    args = parser.parse_args()

    # Source self-test needs neither the API key nor the LLM.
    if args.check_sources:
        sys.exit(check_sources())

    # Same for the statistics page: it only reads the web server's access log.
    if args.stats:
        write_stats()
        sys.exit(0)

    # Both --once and the service mode need a valid API key.
    if not GEMINI_API_KEY:
        log.error("GEMINI_API_KEY is not set. Configure it in the .env file.")
        sys.exit(1)

    if args.once:
        run_pipeline()
        return

    run_service()


if __name__ == "__main__":
    main()
