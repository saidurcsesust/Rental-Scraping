package main

import (
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
	output := flag.String("output", "airbnb_listings.json", "Output JSON file path")
	maxSpans := flag.Int("max-spans", 0, "Maximum homepage spans to click (0 = all discovered)")
	pagesPerSpan := flag.Int("pages-per-span", 2, "Pages to scrape after each span click")
	cardsPerPage := flag.Int("cards-per-page", 5, "Number of listing cards to capture per page")
	headless := flag.Bool("headless", true, "Run browser in headless mode")
	flag.Parse()

	cfg := Config{
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
	}

	scraper := NewScraper(cfg)
	if err := scraper.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "scrape failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Scrape complete. Output: %s\n", cfg.OutputFile)
}
