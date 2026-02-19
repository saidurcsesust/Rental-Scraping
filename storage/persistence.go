package storage

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"colly-scraper/models"
)

type Config struct {
	OutputFile string
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string
}

type categoryGroup struct {
	Category string           `json:"category"`
	Listings []models.Listing `json:"listings"`
}

func SaveResults(results []models.Listing, cfg Config) error {
	if len(results) == 0 {
		return errors.New("no listings scraped")
	}

	groups := make([]categoryGroup, 0)
	groupIndex := make(map[string]int)
	for _, listing := range results {
		category := "Uncategorized"
		if listing.Details != nil {
			if v := strings.TrimSpace(listing.Details["home_span"]); v != "" {
				category = v
			}
		}

		idx, ok := groupIndex[category]
		if !ok {
			idx = len(groups)
			groupIndex[category] = idx
			groups = append(groups, categoryGroup{
				Category: category,
				Listings: make([]models.Listing, 0, 8),
			})
		}
		groups[idx].Listings = append(groups[idx].Listings, listing)
	}

	if err := saveResultsToCSV(groups, cfg); err != nil {
		return err
	}

	if strings.TrimSpace(cfg.DBHost) != "" {
		if err := saveResultsToDB(groups, cfg, len(results)); err != nil {
			return err
		}
	}

	printInsightsReport(results)
	return nil
}

func saveResultsToCSV(groups []categoryGroup, cfg Config) error {
	file, err := os.Create(cfg.OutputFile)
	if err != nil {
		return fmt.Errorf("create output csv: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{
		"category",
		"title",
		"price",
		"location",
		"rating",
		"url",
		"description",
		"details_json",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	for _, group := range groups {
		category := strings.TrimSpace(group.Category)
		if category == "" {
			category = "Uncategorized"
		}
		for _, listing := range group.Listings {
			description := ""
			details := listing.Details
			if details == nil {
				details = map[string]string{}
			}
			if listing.Details != nil {
				description = strings.TrimSpace(listing.Details["description"])
			}
			detailsJSON, err := json.Marshal(details)
			if err != nil {
				return fmt.Errorf("marshal listing details for csv %s: %w", listing.URL, err)
			}

			row := []string{
				category,
				listing.Title,
				listing.Price,
				listing.Location,
				listing.Rating,
				listing.URL,
				description,
				string(detailsJSON),
			}
			if err := writer.Write(row); err != nil {
				return fmt.Errorf("write csv row %s: %w", listing.URL, err)
			}
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}
	return nil
}

func saveResultsToDB(groups []categoryGroup, cfg Config, resultCount int) error {
	if err := ensureDatabaseExists(cfg); err != nil {
		return err
	}

	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.DBHost,
		cfg.DBPort,
		cfg.DBUser,
		cfg.DBPassword,
		cfg.DBName,
		cfg.DBSSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	if err := pingPostgresWithRetry(db, 10, time.Second); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	schema := `
CREATE TABLE IF NOT EXISTS listings (
	id BIGSERIAL PRIMARY KEY,
	title TEXT NOT NULL,
	price TEXT,
	location TEXT,
	rating TEXT,
	category TEXT NOT NULL DEFAULT 'Uncategorized',
	url TEXT NOT NULL UNIQUE,
	description TEXT,
	details JSONB NOT NULL DEFAULT '{}'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("create listings table: %w", err)
	}
	if _, err := db.Exec(`ALTER TABLE listings ADD COLUMN IF NOT EXISTS id BIGSERIAL`); err != nil {
		return fmt.Errorf("ensure id column: %w", err)
	}
	if _, err := db.Exec(`ALTER TABLE listings ADD COLUMN IF NOT EXISTS description TEXT`); err != nil {
		return fmt.Errorf("ensure description column: %w", err)
	}
	if _, err := db.Exec(`ALTER TABLE listings ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'Uncategorized'`); err != nil {
		return fmt.Errorf("ensure category column: %w", err)
	}
	if _, err := db.Exec(`ALTER TABLE listings ADD COLUMN IF NOT EXISTS details JSONB NOT NULL DEFAULT '{}'::jsonb`); err != nil {
		return fmt.Errorf("ensure details column: %w", err)
	}
	if _, err := db.Exec(`ALTER TABLE listings ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`); err != nil {
		return fmt.Errorf("ensure created_at column: %w", err)
	}
	if _, err := db.Exec(`ALTER TABLE listings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`); err != nil {
		return fmt.Errorf("ensure updated_at column: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
INSERT INTO listings (title, price, location, rating, category, url, description, details)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
ON CONFLICT (url) DO UPDATE
SET title = EXCLUDED.title,
	price = EXCLUDED.price,
	location = EXCLUDED.location,
	rating = EXCLUDED.rating,
	category = EXCLUDED.category,
	description = EXCLUDED.description,
	details = EXCLUDED.details,
	updated_at = NOW();
`)
	if err != nil {
		return fmt.Errorf("prepare upsert: %w", err)
	}
	defer stmt.Close()

	for _, group := range groups {
		for _, listing := range group.Listings {
			category := strings.TrimSpace(group.Category)
			if category == "" {
				category = "Uncategorized"
			}
			description := ""
			details := listing.Details
			if details == nil {
				details = map[string]string{}
			}
			if listing.Details != nil {
				description = strings.TrimSpace(listing.Details["description"])
			}
			detailsJSON, err := json.Marshal(details)
			if err != nil {
				return fmt.Errorf("marshal listing details %s: %w", listing.URL, err)
			}

			if _, err := stmt.Exec(
				listing.Title,
				listing.Price,
				listing.Location,
				listing.Rating,
				category,
				listing.URL,
				description,
				string(detailsJSON),
			); err != nil {
				return fmt.Errorf("upsert listing %s: %w", listing.URL, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	fmt.Printf("saved listings to postgres host=%s port=%d db=%s table=listings count=%d\n", cfg.DBHost, cfg.DBPort, cfg.DBName, resultCount)
	return nil
}

func pingPostgresWithRetry(db *sql.DB, attempts int, delay time.Duration) error {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		lastErr = db.Ping()
		if lastErr == nil {
			return nil
		}
		time.Sleep(delay)
	}
	return lastErr
}

func ensureDatabaseExists(cfg Config) error {
	adminDSN := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=postgres sslmode=%s",
		cfg.DBHost,
		cfg.DBPort,
		cfg.DBUser,
		cfg.DBPassword,
		cfg.DBSSLMode,
	)

	adminDB, err := sql.Open("postgres", adminDSN)
	if err != nil {
		return fmt.Errorf("open postgres admin db: %w", err)
	}
	defer adminDB.Close()

	if err := adminDB.Ping(); err != nil {
		return fmt.Errorf("ping postgres admin db: %w", err)
	}

	dbName := strings.TrimSpace(cfg.DBName)
	if dbName == "" {
		return errors.New("database name is empty")
	}

	var exists int
	if err := adminDB.QueryRow(`SELECT 1 FROM pg_database WHERE datname = $1`, dbName).Scan(&exists); err == nil && exists == 1 {
		return nil
	}

	escaped := strings.ReplaceAll(dbName, `"`, `""`)
	if _, err := adminDB.Exec(fmt.Sprintf(`CREATE DATABASE "%s"`, escaped)); err != nil {
		return fmt.Errorf("create database %q: %w", dbName, err)
	}
	fmt.Printf("created postgres database db=%s\n", dbName)
	return nil
}

var numberRe = regexp.MustCompile(`([0-9]+(?:\.[0-9]+)?)`)

func parsePriceValue(raw string) (float64, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, false
	}
	m := numberRe.FindStringSubmatch(strings.ReplaceAll(s, ",", ""))
	if len(m) < 2 {
		return 0, false
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func parseRatingValue(raw string) (float64, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, false
	}
	m := numberRe.FindStringSubmatch(s)
	if len(m) < 2 {
		return 0, false
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func printInsightsReport(results []models.Listing) {
	type ratedListing struct {
		Title    string
		Rating   float64
		Location string
	}

	total := len(results)
	airbnb := len(results)
	locationCount := make(map[string]int)
	priceCount := 0
	priceSum := 0.0
	minPrice := 0.0
	maxPrice := 0.0
	mostExpensive := models.Listing{}
	topRated := make([]ratedListing, 0, len(results))

	for _, l := range results {
		location := strings.TrimSpace(l.Location)
		if location == "" {
			location = "Unknown"
		}
		locationCount[location]++

		if p, ok := parsePriceValue(l.Price); ok {
			if priceCount == 0 || p < minPrice {
				minPrice = p
			}
			if priceCount == 0 || p > maxPrice {
				maxPrice = p
				mostExpensive = l
			}
			priceSum += p
			priceCount++
		}

		if r, ok := parseRatingValue(l.Rating); ok {
			title := strings.TrimSpace(l.Title)
			if title == "" {
				title = "Untitled listing"
			}
			topRated = append(topRated, ratedListing{
				Title:    title,
				Rating:   r,
				Location: strings.TrimSpace(l.Location),
			})
		}
	}

	avgPrice := 0.0
	if priceCount > 0 {
		avgPrice = priceSum / float64(priceCount)
	}

	type locationItem struct {
		Name  string
		Count int
	}
	locs := make([]locationItem, 0, len(locationCount))
	for name, count := range locationCount {
		locs = append(locs, locationItem{Name: name, Count: count})
	}
	sort.Slice(locs, func(i, j int) bool {
		if locs[i].Count == locs[j].Count {
			return locs[i].Name < locs[j].Name
		}
		return locs[i].Count > locs[j].Count
	})

	sort.Slice(topRated, func(i, j int) bool {
		if topRated[i].Rating == topRated[j].Rating {
			return topRated[i].Title < topRated[j].Title
		}
		return topRated[i].Rating > topRated[j].Rating
	})
	if len(topRated) > 5 {
		topRated = topRated[:5]
	}

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("      Vacation Rental Market Insights")
	fmt.Println("========================================")
	fmt.Printf("%-28s %d\n", "Total Listings Scraped:", total)
	fmt.Printf("%-28s %d\n", "Airbnb Listings:", airbnb)
	if priceCount > 0 {
		fmt.Printf("%-28s $%.2f\n", "Average Price:", avgPrice)
		fmt.Printf("%-28s $%.2f\n", "Minimum Price:", minPrice)
		fmt.Printf("%-28s $%.2f\n", "Maximum Price:", maxPrice)
		fmt.Println()
		fmt.Println("Most Expensive Property")
		fmt.Println("----------------------------------------")
		fmt.Printf("%-12s %s\n", "Title:", strings.TrimSpace(mostExpensive.Title))
		fmt.Printf("%-12s $%.2f\n", "Price:", maxPrice)
		fmt.Printf("%-12s %s\n", "Location:", strings.TrimSpace(mostExpensive.Location))
	} else {
		fmt.Printf("%-28s N/A\n", "Average Price:")
		fmt.Printf("%-28s N/A\n", "Minimum Price:")
		fmt.Printf("%-28s N/A\n", "Maximum Price:")
		fmt.Println()
		fmt.Println("Most Expensive Property")
		fmt.Println("----------------------------------------")
		fmt.Printf("%-12s N/A\n", "Title:")
		fmt.Printf("%-12s N/A\n", "Price:")
		fmt.Printf("%-12s N/A\n", "Location:")
	}

	fmt.Println()
	fmt.Println("Listings per Location")
	fmt.Println("----------------------------------------")
	for _, item := range locs {
		fmt.Printf("%-30s %d\n", item.Name+":", item.Count)
	}

	fmt.Println()
	fmt.Println("Top 5 Highest Rated Properties")
	fmt.Println("----------------------------------------")
	if len(topRated) == 0 {
		fmt.Println("N/A")
		return
	}
	for i, item := range topRated {
		fmt.Printf("%d. %s  (%.2f)\n", i+1, item.Title, item.Rating)
	}
	fmt.Println("========================================")
}
