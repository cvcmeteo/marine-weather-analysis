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
- **3. Navigation outlook** — practical deductions (engine use, night anchorages)
  from the 12-hour-and-beyond projections. The section is framed around "the
  weekend" only from Thursday onward; earlier in the week it stays on the generic
  horizon covered by the bulletin.
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

- `output/index.html` — browsable page. The home page lists only the current
  ISO week; older reports live in a collapsible year → month → week archive.
- `output/latest.md` — always the most recent report.
- `output/<year>/<month>/W<week>/analisi_meteo_<timestamp>.md` — dated history of
  every emission, filed by year, month and ISO week.
- `output/<year>/<month>/W<week>/chart_<timestamp>.gif` and `meteomar_<timestamp>.txt`
  — attached sources, saved next to their report.

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
- **Version**: `APP_VERSION` in `main.py` is logged at startup and shown as a badge
  in the header of the web page, so the running build can be identified from the
  page alone. Bump it by hand when the pipeline, the prompt or the page change.
- **AI disclosure**: every report `.md` closes with a notice declaring that it was
  produced by a generative AI model (`AI_DISCLOSURE_MD`, appended after "Fonti";
  the model name comes from `GEMINI_MODEL`), as required for AI-generated
  content. It lives in the report rather than in the page, so it travels with the
  file when downloaded and is not shown twice when the report is rendered.
- **Small screens**: below 760 px the sidebar and the report stack vertically; the
  chart image and the bulletin block are constrained so the page never scrolls
  sideways.

## Project structure

```
.
├── main.py                     # Sources, LLM call, prompt, report, HTML index, scheduling
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Runtime image
├── docker-compose.yml          # Scheduler + nginx web server
├── .env.example                # Configuration template (copy to .env)
├── tools/
│   └── migrate_output_layout.py  # One-off: file legacy flat reports into year/month/week
└── output/                     # Generated reports (index.html, .md, sources)
```
