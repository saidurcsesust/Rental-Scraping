package main

import (
	"colly-scraper/services"
	"colly-scraper/utils"
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	searchURL := flag.String("search-url", "https://www.airbnb.com/", "Airbnb homepage URL")
	maxPages := flag.Int("max-pages", 3, "Fallback: maximum pages when no spans are found")
	workers := flag.Int("workers", 5, "Number of concurrent detail-page workers")
	rateLimit := flag.Float64("rate", 1.0, "Requests per second per domain")
	burst := flag.Int("burst", 2, "Rate-limit burst per domain")
	maxReqDomain := flag.Int("max-inflight-domain", 3, "Max concurrent in-flight requests per domain")
	maxTotalReqDomain := flag.Int("max-total-domain", 300, "Max total requests per domain")
	timeoutSec := flag.Int("timeout-sec", 30, "Timeout per request (seconds)")
	retries := flag.Int("retries", 2, "Retry count per page")
	output := flag.String("output", "airbnb_listings.csv", "Output CSV file path")
	maxSpans := flag.Int("max-spans", 0, "Maximum homepage spans to click (0 = all discovered)")
	pagesPerSpan := flag.Int("pages-per-span", 2, "Pages to scrape after each span click")
	cardsPerPage := flag.Int("cards-per-page", 5, "Number of listing cards to capture per page")
	headless := flag.Bool("headless", true, "Run browser in headless mode")
	dbHost := flag.String("db-host", utils.EnvOrDefault("DB_HOST", "127.0.0.1"), "PostgreSQL host")
	dbPort := flag.Int("db-port", utils.EnvIntOrDefault("DB_PORT", 5433), "PostgreSQL port")
	dbUser := flag.String("db-user", utils.EnvOrDefault("DB_USER", "postgres"), "PostgreSQL user")
	dbPassword := flag.String("db-password", utils.EnvOrDefault("DB_PASSWORD", "postgres"), "PostgreSQL password")
	dbName := flag.String("db-name", utils.EnvOrDefault("DB_NAME", "rental_scraping"), "PostgreSQL database name")
	dbSSLMode := flag.String("db-sslmode", utils.EnvOrDefault("DB_SSLMODE", "disable"), "PostgreSQL sslmode")
	flag.Parse()

	cfg := services.Config{
		SearchURL:                 *searchURL,
		MaxPages:                  *maxPages,
		Workers:                   *workers,
		RequestTimeout:            time.Duration(*timeoutSec) * time.Second,
		MaxRetries:                *retries,
		RateLimitPerSecond:        *rateLimit,
		RateBurst:                 *burst,
		MaxRequestsPerDomain:      *maxReqDomain,
		MaxTotalRequestsPerDomain: *maxTotalReqDomain,
		OutputFile:                *output,
		MaxSpans:                  *maxSpans,
		PagesPerSpan:              *pagesPerSpan,
		CardsPerPage:              *cardsPerPage,
		Headless:                  *headless,
		DBHost:                    *dbHost,
		DBPort:                    *dbPort,
		DBUser:                    *dbUser,
		DBPassword:                *dbPassword,
		DBName:                    *dbName,
		DBSSLMode:                 *dbSSLMode,
	}

	s := services.NewScraper(cfg)
	if err := s.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "scrape failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Scrape complete. Output: %s\n", cfg.OutputFile)
}
