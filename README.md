# Marine Weather Analysis & Navigation Planning — Caprera / La Maddalena

A Python application that periodically:

1. Downloads the latest **surface-pressure synoptic chart** (image) from the
   [Met Office](https://weather.metoffice.gov.uk/maps-and-charts/surface-pressure).
2. Fetches the latest **Meteomar bulletin** directly from the
   [meteoam.it](https://www.meteoam.it/it/messaggio-meteomar) API (full text).
3. Sends **image + text** to a multimodal **Google Gemini** model.
4. Generates a Markdown analysis and navigation-planning report, with the sources
   attached, and publishes it as a browsable web page.

The analysis is focused on the **La Maddalena and Caprera archipelago** (Meteomar
zones Mar di Sardegna, Mar di Corsica, Tirreno Settentrionale) and is written in a
technical, factual register (no hype; wind always in Beaufort scale + knots).

> Note: the report content and the web UI are in Italian (the target audience);
> the codebase, comments, and this manual are in English.

The report follows a strict structure:

- **1. Comparison** — synoptic situation (isobars/pressure gradient) vs. the
  *SITUAZIONE* and *PRESSIONE* sections of the Meteomar bulletin.
- **2. Detail for our area** — wind, sea state, sky and visibility over the first
  24 h for the configured navigation areas.
- **3. Weekend / navigation outlook** — practical deductions (engine use, night
  anchorages) from the 12-hour-and-beyond projections.
- **Sources** — pressure chart (image) and full Meteomar bulletin.

---

## Local run (Docker)

### Requirements
- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- A **Gemini API key** (free from [Google AI Studio](https://aistudio.google.com/apikey))

### Configuration
```bash
cp .env.example .env
# then edit .env and set GEMINI_API_KEY=...
```
You can customize the model, navigation areas, interval, data sources and token
budget (see the comments in `.env.example`).

### Start
```bash
docker compose up -d --build
```
Two containers are started:
- **`marine-weather`** — scheduler: runs an initial analysis immediately
  (`RUN_ON_START=true`) and then repeats every `RUN_INTERVAL_HOURS` (default 6).
- **`web`** — nginx server that publishes the reports at **http://localhost:8080**.

### Useful commands
```bash
docker compose logs -f marine-weather   # live logs
docker compose ps                        # container status
docker compose down                      # stop everything
docker compose up -d --build             # rebuild after code changes
```

### Manual testing
```bash
# Test ONLY the sources (no key / no LLM call): saves output/_debug_chart.<ext>
docker compose run --rm marine-weather python main.py --check-sources

# Run ONE full cycle (download → Gemini → Markdown) and exit.
docker compose run --rm --build marine-weather python main.py --once
```

> Note: `docker compose run` on its own reuses the cached image; add `--build`
> after changing the code.

The same commands also work without Docker
(`pip install -r requirements.txt && playwright install chromium`, then
`python main.py --once`).

## Output

Reports are written to `./output`:

- `output/index.html` — browsable page (list, view, download).
- `output/latest.md` — always the most recent report.
- `output/analisi_meteo_<timestamp>.md` — dated history of every emission.
- `output/chart_<timestamp>.gif` and `output/meteomar_<timestamp>.txt` — attached sources.

## Notes and troubleshooting

- **Synoptic chart**: the app looks for the direct image URL in the Met Office
  HTML (skipping social cards/icons); if not found, it uses the **headless
  Playwright/Chromium fallback** (`USE_PLAYWRIGHT_FALLBACK=true`, default). As a
  last resort you can set `PRESSURE_CHART_IMAGE_URL`.
- **Meteomar bulletin**: fetched from the meteoam.it CMS API (clean text); if that
  fails, HTML scraping of the page is attempted as a fallback.
- **Model**: default `gemini-3.5-flash`. *Thinking* models count reasoning tokens
  against `MAX_TOKENS`: if a report comes out truncated, raise `MAX_TOKENS` or
  lower `GEMINI_THINKING_BUDGET`. The logs flag truncations
  (`finish_reason=MAX_TOKENS`).
- **Robustness**: network calls use retry with backoff; exceptions are handled and
  logged without stopping the scheduler.
- **Time zone**: the container uses `Europe/Rome`; the emission times in the report
  are in UTC.

## Project structure

```
.
├── main.py                     # Sources, LLM call, prompt, report, HTML index, scheduling
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Runtime image
├── docker-compose.yml          # Scheduler + nginx web server
├── .env.example                # Configuration template (copy to .env)
└── output/                     # Generated reports (index.html, .md, sources)
```
