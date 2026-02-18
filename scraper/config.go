package scraper

import "time"

type Config struct {
	SearchURL                 string
	MaxPages                  int
	Workers                   int
	RequestTimeout            time.Duration
	MaxRetries                int
	RateLimitPerSecond        float64
	RateBurst                 int
	MaxRequestsPerDomain      int
	MaxTotalRequestsPerDomain int
	OutputFile                string
	MaxSpans                  int
	PagesPerSpan              int
	CardsPerPage              int
	Headless                  bool
	DBHost                    string
	DBPort                    int
	DBUser                    string
	DBPassword                string
	DBName                    string
	DBSSLMode                 string
}
