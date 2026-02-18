package scraper

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"colly-scraper/models"
)

func (s *Scraper) saveResults() error {
	if len(s.results) == 0 {
		return errors.New("no listings scraped")
	}

	groups := make([]categoryGroup, 0)
	groupIndex := make(map[string]int)
	for _, listing := range s.results {
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

	if err := s.saveResultsToCSV(groups); err != nil {
		return err
	}

	if strings.TrimSpace(s.cfg.DBHost) != "" {
		if err := s.saveResultsToDB(groups); err != nil {
			return err
		}
	}

	return nil
}

func (s *Scraper) saveResultsToCSV(groups []categoryGroup) error {
	file, err := os.Create(s.cfg.OutputFile)
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

func (s *Scraper) saveResultsToDB(groups []categoryGroup) error {
	if err := s.ensureDatabaseExists(); err != nil {
		return err
	}

	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		s.cfg.DBHost,
		s.cfg.DBPort,
		s.cfg.DBUser,
		s.cfg.DBPassword,
		s.cfg.DBName,
		s.cfg.DBSSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	if err := s.pingPostgresWithRetry(db, 10, time.Second); err != nil {
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

	fmt.Printf("saved listings to postgres host=%s port=%d db=%s table=listings count=%d\n", s.cfg.DBHost, s.cfg.DBPort, s.cfg.DBName, len(s.results))
	return nil
}

func (s *Scraper) pingPostgresWithRetry(db *sql.DB, attempts int, delay time.Duration) error {
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

func (s *Scraper) ensureDatabaseExists() error {
	adminDSN := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=postgres sslmode=%s",
		s.cfg.DBHost,
		s.cfg.DBPort,
		s.cfg.DBUser,
		s.cfg.DBPassword,
		s.cfg.DBSSLMode,
	)

	adminDB, err := sql.Open("postgres", adminDSN)
	if err != nil {
		return fmt.Errorf("open postgres admin db: %w", err)
	}
	defer adminDB.Close()

	if err := adminDB.Ping(); err != nil {
		return fmt.Errorf("ping postgres admin db: %w", err)
	}

	dbName := strings.TrimSpace(s.cfg.DBName)
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
