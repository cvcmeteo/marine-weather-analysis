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
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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

SYSTEM_PROMPT = f"""Sei un meteorologo marino esperto e un istruttore di vela d'altura.
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

## 3. Proiezioni per il Weekend / Navigazione
Usa la sezione delle proiezioni a 12 ore e intervalli successivi del Meteomar per
generare deduzioni pratiche per la vita in barca. Includi almeno:
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
        system_instruction=SYSTEM_PROMPT,
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


def _build_sources_section(
    stamp: str,
    chart: Optional[tuple[bytes, str]],
    meteomar_text: Optional[str],
) -> str:
    """Save the source chart/bulletin next to the report and return a Markdown
    "Fonti" section that embeds the chart image and the full Meteomar text.

    The saved filenames are timestamped so each report keeps its own sources;
    they are served (and thus viewable/downloadable) by the web container.
    """
    parts = ["\n\n---\n\n## Fonti\n"]

    parts.append("### Carta di pressione al suolo (Met Office)\n")
    if chart is not None:
        image_bytes, media_type = chart
        chart_name = f"chart_{stamp}.{_IMAGE_EXT.get(media_type, 'png')}"
        (OUTPUT_DIR / chart_name).write_bytes(image_bytes)
        parts.append(f"![Carta di pressione al suolo]({chart_name})\n")
    else:
        parts.append("_Non disponibile per questa emissione._\n")

    parts.append("### Bollettino Meteomar (testo integrale)\n")
    if meteomar_text:
        mm_name = f"meteomar_{stamp}.txt"
        (OUTPUT_DIR / mm_name).write_text(meteomar_text, encoding="utf-8")
        parts.append(f"[⬇ Scarica il bollettino]({mm_name})\n")
        parts.append(f"```text\n{meteomar_text}\n```\n")
    else:
        parts.append("_Non disponibile per questa emissione._\n")

    return "\n".join(parts)


def write_report(
    markdown: str,
    emission_time: str,
    chart: Optional[tuple[bytes, str]] = None,
    meteomar_text: Optional[str] = None,
) -> Path:
    """Write the report to the output volume and update 'latest.md'.

    The pressure-chart image and the raw Meteomar bulletin are saved alongside
    and appended to the report as a "Fonti" section.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    stamp = emission_time.replace(":", "").replace("-", "").replace(" ", "_")
    full_markdown = markdown + _build_sources_section(stamp, chart, meteomar_text)

    dated_path = OUTPUT_DIR / f"analisi_meteo_{stamp}.md"
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
  body { margin:0; font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
         background:#0b1622; color:#e6edf3; }
  header { padding:1.1rem 1.5rem; background:#0d2136; border-bottom:1px solid #1e3a5f; }
  header h1 { margin:0; font-size:1.2rem; }
  header p { margin:.3rem 0 0; color:#8aa0b5; font-size:.85rem; }
  .layout { display:flex; min-height: calc(100vh - 78px); }
  aside { width:340px; flex:0 0 340px; border-right:1px solid #1e3a5f; overflow-y:auto; }
  /* Sidebar list styles are scoped to <aside> so they never leak into the
     rendered report content in <article>. */
  aside ul { list-style:none; margin:0; padding:0; }
  aside li { display:flex; flex-direction:column; align-items:flex-start; gap:.4rem;
       padding:.7rem .9rem; border-bottom:1px solid #14263c; }
  aside li.empty { color:#8aa0b5; }
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
  .placeholder { color:#8aa0b5; text-align:center; margin-top:3rem; }
</style>
</head>
<body>
<header>
  <h1>🌊 Analisi Meteo Marina — Caprera / La Maddalena</h1>
  <p>Report generati automaticamente. Seleziona un report per visualizzarlo o scaricarlo.</p>
</header>
<div class="layout">
  <aside><ul>{{ITEMS}}</ul></aside>
  <main><article id="content"><p class="placeholder">Seleziona un report dall'elenco.</p></article></main>
</div>
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
</script>
</body>
</html>
"""


def write_index() -> None:
    """(Re)generate index.html in the output dir: a browsable list of reports.

    Served by the companion nginx container. Each entry links to the Markdown
    file (downloadable) and can be rendered in-page. Safe to call any time.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Filenames are timestamped, so reverse-sorting puts the newest first.
    reports = sorted(OUTPUT_DIR.glob("analisi_meteo_*.md"), reverse=True)

    items = []
    for path in reports:
        try:
            first_line = path.read_text(encoding="utf-8").splitlines()[0]
        except (OSError, IndexError):
            first_line = ""
        title = first_line.lstrip("# ").strip() or path.stem

        # Locate the source files saved for this report (same timestamp stamp).
        stamp = path.stem.replace("analisi_meteo_", "", 1)
        chart_file = next(iter(OUTPUT_DIR.glob(f"chart_{stamp}.*")), None)
        meteomar_file = OUTPUT_DIR / f"meteomar_{stamp}.txt"

        links = [f'<a class="dl" href="{path.name}" download>⬇ Report</a>']
        if chart_file is not None:
            links.append(f'<a class="dl" href="{chart_file.name}" target="_blank">🗺 Carta</a>')
        if meteomar_file.exists():
            links.append(f'<a class="dl" href="{meteomar_file.name}" target="_blank">📄 Bollettino</a>')

        items.append(
            f'<li><button class="view" data-file="{path.name}">{title}</button>'
            f'<span class="links">{"".join(links)}</span></li>'
        )

    if not items:
        items.append('<li class="empty">Nessun report ancora disponibile.</li>')

    html = INDEX_TEMPLATE.replace("{{ITEMS}}", "\n".join(items))
    (OUTPUT_DIR / "index.html").write_text(html, encoding="utf-8")
    log.info("Wrote index.html (%d report(s)).", len(reports))


# --------------------------------------------------------------------------- #
# Pipeline orchestration
# --------------------------------------------------------------------------- #

def run_pipeline() -> None:
    """Run one full download → analyze → write cycle. Never raises."""
    emission_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
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

        report = build_analysis(client, chart, meteomar_text, emission_time)
        if report is None:
            log.error("Analysis failed; no report written this cycle.")
            return

        write_report(report, emission_time, chart, meteomar_text)
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
        "Starting marine weather analysis service "
        "(model=%s, interval=%dh, output=%s).",
        GEMINI_MODEL, RUN_INTERVAL_HOURS, OUTPUT_DIR,
    )

    # Ensure the browser page exists immediately (lists any existing reports),
    # even before the first pipeline run completes.
    write_index()

    if RUN_ON_START:
        run_pipeline()

    # Schedule recurring runs every N hours.
    schedule.every(RUN_INTERVAL_HOURS).hours.do(run_pipeline)
    log.info("Scheduler armed: next runs every %d hours.", RUN_INTERVAL_HOURS)

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
    args = parser.parse_args()

    # Source self-test needs neither the API key nor the LLM.
    if args.check_sources:
        sys.exit(check_sources())

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
