# Rental Scraping

A Go-based Airbnb scraping pipeline that:
- discovers homepage location/category spans dynamically from Airbnb,
- scrapes listing cards and detail pages concurrently,
- saves results to a CSV file,
- upserts data into PostgreSQL.

## Features
- Click-driven scraping from homepage spans (`<a>` links in homepage carousels).
- Dynamic span discovery via progressive homepage scrolling (captures lazy-loaded sections).
- Per span workflow: click span -> scrape page 1 + page 2 -> return homepage -> repeat for next span.
- Category-aware scraping from homepage sections (for example: `Popular homes in ...`, `Stay in ...`).
- Concurrent detail-page workers.
- Domain rate limiting and request caps.
- Retry with backoff for scrape requests and PostgreSQL readiness.
- Dual persistence:
  - flat CSV export for easy sharing/analysis,
  - PostgreSQL upsert for durable storage.

## Tech Stack
- Go (`chromedp`, `lib/pq`)
- PostgreSQL 16 (Docker Compose)

## Project Structure
```text
.
├── main.go                     # CLI entrypoint and flag parsing
├── services/
│   ├── config.go               # Scraper service config model
│   ├── scraper.go              # Core scraping orchestration + parsing
│   └── worker.go               # Concurrent detail worker pool
├── storage/
│   └── persistence.go          # CSV + PostgreSQL persistence
├── utils/
│   └── env.go                  # Environment helper utilities
├── models/
│   └── model.go                # Listing model
├── docker-compose.yml          # Local PostgreSQL service
└── .env.postgres.example       # Example DB environment values
```

## Prerequisites
- Go installed (module declares `go 1.25.5` in `go.mod`).
- Docker + Docker Compose (for local PostgreSQL).
- Linux environment with Chrome/Chromium support required by `chromedp`.

## Setup
1. Clone and enter the repository.
```bash
git clone <your-repository-url>
cd Rental-Scraping
```
2. Download Go modules:
```bash
go mod download
```
3. Create env file from example (optional but recommended):
```bash
cp .env.postgres.example .env
```

## Run PostgreSQL (Docker)
Start local PostgreSQL using the provided compose file:
```bash
docker compose --env-file .env.postgres.example up -d postgres
```

Check status:
```bash
docker compose ps
```

Stop database:
```bash
docker compose down
```

## Run the Scraper

### Option A: Use environment variables (recommended)
```bash
set -a
source .env.postgres.example
set +a

go run .
```

### Recommended command (full dynamic homepage spans)
`-max-spans` optional. Unset it (or keep `0`) to scrape all discovered homepage spans.
```bash
go run . -pages-per-span 2 -cards-per-page 5 -workers 1 -timeout-sec 90
```

### Limit run to first N spans (optional)
```bash
go run . -max-spans 3 -pages-per-span 2 -cards-per-page 5 -workers 1 -timeout-sec 90
```

## Insights Screenshot

Terminal insights report example:

![Vacation Rental Market Insights](assets/image.png)


## CLI Flags
| Flag | Default | Description |
|---|---|---|
| `-search-url` | `https://www.airbnb.com/` | Airbnb homepage URL |
| `-max-pages` | `3` | Legacy fallback setting (not used in click-only homepage flow) |
| `-workers` | `5` | Number of concurrent detail workers |
| `-rate` | `1.0` | Requests per second per domain |
| `-burst` | `2` | Rate limiter burst per domain |
| `-max-inflight-domain` | `3` | Max in-flight requests per domain |
| `-max-total-domain` | `300` | Max total requests per domain |
| `-timeout-sec` | `30` | Request timeout in seconds |
| `-retries` | `2` | Retry count per page |
| `-output` | `airbnb_listings.csv` | Output CSV file path |
| `-max-spans` | `0` | Max homepage spans to click (`0` = all discovered) |
| `-pages-per-span` | `2` | Pages to scrape per span |
| `-cards-per-page` | `5` | Listing cards to capture per page |
| `-headless` | `true` | Run browser in headless mode |
| `-db-host` | `127.0.0.1` | PostgreSQL host |
| `-db-port` | `5433` | PostgreSQL port |
| `-db-user` | `postgres` | PostgreSQL user |
| `-db-password` | `postgres` | PostgreSQL password |
| `-db-name` | `rental_scraping` | PostgreSQL database name |
| `-db-sslmode` | `disable` | PostgreSQL SSL mode |

## Environment Variables
`.env.postgres.example` provides defaults for Docker and app DB connection:

```env
POSTGRES_DB=rental_scraping
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_PORT=5433

DB_HOST=127.0.0.1
DB_PORT=5433
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=rental_scraping
DB_SSLMODE=disable
```

## Output: CSV
The scraper writes a single CSV file (default: `airbnb_listings.csv`) with columns:
- `category`
- `title`
- `price`
- `location`
- `rating`
- `url`
- `description`
- `details_json` (JSON object encoded as string)

Example usage with tools:
```bash
head -n 5 airbnb_listings.csv
```

## Output: PostgreSQL
If `DB_HOST` (or `-db-host`) is set, data is also saved to PostgreSQL.

### Table
`listings` is auto-created/updated with this schema:
- `id BIGSERIAL PRIMARY KEY`
- `title TEXT NOT NULL`
- `price TEXT`
- `location TEXT`
- `rating TEXT`
- `category TEXT NOT NULL DEFAULT 'Uncategorized'`
- `url TEXT NOT NULL UNIQUE`
- `description TEXT`
- `details JSONB NOT NULL DEFAULT '{}'::jsonb`
- `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
- `updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`

### Upsert behavior
Rows are upserted on `url` conflict. Existing rows are updated with latest scraped values.

### Verify inserted rows
```bash
docker exec -it rental_scraping_postgres \
  psql -U postgres -d rental_scraping \
  -c "SELECT id, category, title, url, updated_at FROM listings ORDER BY id DESC LIMIT 20;"
```

### View more columns
```bash
docker exec -it rental_scraping_postgres \
  psql -U postgres -d rental_scraping \
  -c "SELECT id, category, title, price, location, rating, url, updated_at FROM listings ORDER BY id DESC LIMIT 100;"
```

### Count rows
```bash
docker exec -it rental_scraping_postgres \
  psql -U postgres -d rental_scraping \
  -c "SELECT COUNT(*) FROM listings;"
```
