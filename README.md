# Analisi Meteo Marina & Pianificazione Navigazione — Caprera / La Maddalena

Applicazione Python che, periodicamente:

1. Scarica l'ultima **carta sinottica di pressione al suolo** (immagine) dal
   [Met Office](https://weather.metoffice.gov.uk/maps-and-charts/surface-pressure).
2. Recupera l'ultimo **bollettino Meteomar** direttamente dall'API di
   [meteoam.it](https://www.meteoam.it/it/messaggio-meteomar) (testo integrale).
3. Invia **immagine + testo** a un modello multimodale **Google Gemini**.
4. Genera un report Markdown di analisi e pianificazione per la navigazione,
   con le fonti allegate, e lo pubblica come pagina web navigabile.

L'analisi è centrata sull'**Arcipelago di La Maddalena e Caprera** (zone Meteomar
Mar di Sardegna, Mar di Corsica, Tirreno Settentrionale) ed è redatta con un
registro tecnico e fattuale (niente enfasi; venti sempre in scala Beaufort + nodi).

Il report segue una struttura rigorosa:

- **1. Comparazione** — confronto tra situazione sinottica (isobare/gradiente
  barico) e le sezioni *SITUAZIONE* e *PRESSIONE* del Meteomar.
- **2. Il Dettaglio per la nostra area** — vento, stato del mare, cielo e
  visibilità nelle prime 24 h per le aree di navigazione configurate.
- **3. Proiezioni per il Weekend / Navigazione** — deduzioni pratiche (uso del
  motore, ancoraggi notturni) dalle proiezioni a 12 h e successive.
- **Fonti** — carta di pressione (immagine) e bollettino Meteomar integrale.

---

## Esecuzione locale (Docker)

### Requisiti
- [Docker](https://docs.docker.com/get-docker/) e Docker Compose
- Una **chiave API Gemini** (gratuita da [Google AI Studio](https://aistudio.google.com/apikey))

### Configurazione
```bash
cp .env.example .env
# poi apri .env e imposta GEMINI_API_KEY=...
```
Puoi personalizzare modello, aree di navigazione, intervallo, sorgenti dati e
budget token (vedi commenti in `.env.example`).

### Avvio
```bash
docker compose up -d --build
```
Vengono avviati due container:
- **`marine-weather`** — scheduler: esegue subito una prima analisi
  (`RUN_ON_START=true`) e poi ripete ogni `RUN_INTERVAL_HOURS` (default 6).
- **`web`** — server nginx che pubblica i report su **http://localhost:8080**.

### Comandi utili
```bash
docker compose logs -f marine-weather   # log in tempo reale
docker compose ps                        # stato dei container
docker compose down                      # ferma tutto
docker compose up -d --build             # ricostruisce dopo modifiche
```

### Test manuale
```bash
# Testa SOLO le fonti (nessuna chiave/chiamata LLM): salva output/_debug_chart.<ext>
docker compose run --rm marine-weather python main.py --check-sources

# Esegue UN singolo ciclo completo (download → Gemini → Markdown) e termina.
docker compose run --rm --build marine-weather python main.py --once
```

> Nota: `docker compose run` da solo riusa l'immagine in cache; aggiungi `--build`
> dopo aver modificato il codice.

Gli stessi comandi funzionano anche senza Docker
(`pip install -r requirements.txt && playwright install chromium`, poi
`python main.py --once`).

## Output

I report vengono scritti in `./output`:

- `output/index.html` — pagina navigabile (elenco, visualizzazione, download).
- `output/latest.md` — sempre l'ultimo report.
- `output/analisi_meteo_<timestamp>.md` — storico datato di ogni emissione.
- `output/chart_<timestamp>.gif` e `output/meteomar_<timestamp>.txt` — fonti allegate.

## Note e risoluzione problemi

- **Carta sinottica**: l'app cerca l'URL diretto dell'immagine nell'HTML del
  Met Office (scartando social card/icone); se non lo trova, usa il **fallback
  headless con Playwright/Chromium** (`USE_PLAYWRIGHT_FALLBACK=true`, default). In
  ultima istanza puoi impostare `PRESSURE_CHART_IMAGE_URL`.
- **Bollettino Meteomar**: recuperato dall'API CMS di meteoam.it (testo pulito);
  in fallback viene tentato lo scraping HTML della pagina.
- **Modello**: default `gemini-3.5-flash`. Modelli *thinking* contano i token di
  ragionamento in `MAX_TOKENS`: se un report risulta troncato, alza `MAX_TOKENS`
  o riduci `GEMINI_THINKING_BUDGET`. Il log segnala i troncamenti
  (`finish_reason=MAX_TOKENS`).
- **Robustezza**: retry con backoff sulle chiamate di rete; le eccezioni sono
  gestite e loggate senza interrompere lo scheduler.
- **Fuso orario**: il container usa `Europe/Rome`; gli orari di emissione nel
  report sono in UTC.

## Struttura del progetto

```
.
├── main.py                     # Fonti, chiamata LLM, prompt, report, indice HTML, scheduling
├── requirements.txt            # Dipendenze Python
├── Dockerfile                  # Immagine dell'ambiente
├── docker-compose.yml          # Scheduler + web server nginx
├── .env.example                # Modello di configurazione (copia in .env)
└── output/                     # Report generati (index.html, .md, fonti)
```
