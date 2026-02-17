package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"colly-scraper/models"

	"github.com/chromedp/chromedp"
	"golang.org/x/time/rate"
)

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
}

type homeSpan struct {
	ID    string
	Label string
	Link  string
}

var homeCategoryLabelRe = regexp.MustCompile(`(?i)^(popular homes in|available next month in|stay in)\s+.+$`)

type categoryListingSeed struct {
	Label string
	URL   string
	Title string
}

type categoryGroup struct {
	Category string           `json:"category"`
	Listings []models.Listing `json:"listings"`
}

type Scraper struct {
	cfg Config

	allocatorCtx context.Context
	browserCtx   context.Context
	cancelAlloc  context.CancelFunc
	cancelBrowse context.CancelFunc

	visited sync.Map

	results   []models.Listing
	resultsMu sync.Mutex

	limitersMu       sync.Mutex
	domainLimiters   map[string]*rate.Limiter
	domainSemaphores map[string]chan struct{}
	domainReqCount   map[string]int
}

func NewScraper(cfg Config) *Scraper {
	return &Scraper{
		cfg:              cfg,
		domainLimiters:   make(map[string]*rate.Limiter),
		domainSemaphores: make(map[string]chan struct{}),
		domainReqCount:   make(map[string]int),
	}
}

func (s *Scraper) Start(ctx context.Context) error {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", s.cfg.Headless),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.UserAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
	)

	s.allocatorCtx, s.cancelAlloc = chromedp.NewExecAllocator(ctx, opts...)
	s.browserCtx, s.cancelBrowse = chromedp.NewContext(s.allocatorCtx)

	defer s.cancelBrowse()
	defer s.cancelAlloc()

	jobs := make(chan detailJob, s.cfg.Workers*4)
	var wg sync.WaitGroup
	s.startWorkers(ctx, s.cfg.Workers, jobs, &wg)

	spans, err := s.collectHomeSpans(ctx)
	if err != nil {
		fmt.Printf("span discovery failed err=%v\n", err)
	}
	fmt.Printf("discovered home spans: %d\n", len(spans))
	for _, sp := range spans {
		fmt.Printf("span id=%q label=%q link=%q\n", sp.ID, sp.Label, sp.Link)
	}

	if len(spans) == 0 {
		fmt.Println("no home spans found, trying direct homepage category extraction")
		homeListings, homeErr := s.scrapeHomepageCategoriesDirect(ctx)
		if homeErr != nil {
			fmt.Printf("direct homepage category extraction failed err=%v\n", homeErr)
		}
		if len(homeListings) > 0 {
			fmt.Printf("direct homepage categories extracted listings=%d\n", len(homeListings))
			s.enqueueListings(homeListings, jobs)
		} else {
			fmt.Println("direct homepage extraction returned no listings, falling back to URL pagination mode")
			s.scrapeByURLPagination(ctx, jobs)
		}
	} else {
		s.scrapeByHomeSpans(ctx, spans, jobs)
	}

	close(jobs)
	wg.Wait()

	return s.saveResults()
}

func (s *Scraper) scrapeByHomeSpans(ctx context.Context, spans []homeSpan, jobs chan<- detailJob) {
	for _, span := range spans {
		if strings.TrimSpace(span.Link) == "" {
			continue
		}
		spanProduced := 0
		remaining := s.cfg.CardsPerPage
		for page := 0; page < s.cfg.PagesPerSpan; page++ {
			if remaining <= 0 {
				break
			}
			pageURL := buildPaginatedAirbnbURL(span.Link, page)
			listings, pageErr := s.scrapeSearchPage(ctx, pageURL, remaining, span.Label)
			if pageErr != nil {
				fmt.Printf("span scrape failed id=%q label=%q page=%d err=%v\n", span.ID, span.Label, page+1, pageErr)
				continue
			}
			spanProduced += len(listings)
			remaining -= len(listings)
			s.enqueueListings(listings, jobs)
		}
		if spanProduced == 0 {
			fallbackBase := buildLabelSearchURL(span.Label)
			for page := 0; page < s.cfg.PagesPerSpan; page++ {
				if remaining <= 0 {
					break
				}
				pageURL := buildPaginatedAirbnbURL(fallbackBase, page)
				listings, pageErr := s.scrapeSearchPage(ctx, pageURL, remaining, span.Label)
				if pageErr != nil {
					fmt.Printf("fallback scrape failed label=%q page=%d err=%v\n", span.Label, page+1, pageErr)
					continue
				}
				spanProduced += len(listings)
				remaining -= len(listings)
				s.enqueueListings(listings, jobs)
			}
		}
		fmt.Printf("span done label=%q listings=%d\n", span.Label, spanProduced)
	}
}

func (s *Scraper) scrapeByURLPagination(ctx context.Context, jobs chan<- detailJob) {
	baseURL := s.cfg.SearchURL
	if isAirbnbHomeRoot(baseURL) {
		baseURL = "https://www.airbnb.com/s/homes"
	}
	for page := 0; page < s.cfg.MaxPages; page++ {
		searchURL := buildPaginatedAirbnbURL(baseURL, page)
		listings, err := s.scrapeSearchPage(ctx, searchURL, s.cfg.CardsPerPage, "")
		if err != nil {
			fmt.Printf("search page failed page=%d err=%v\n", page+1, err)
			continue
		}
		if len(listings) == 0 {
			break
		}
		s.enqueueListings(listings, jobs)
	}
}

func (s *Scraper) scrapeHomepageCategoriesDirect(ctx context.Context) ([]models.Listing, error) {
	pageURL := s.cfg.SearchURL
	if strings.TrimSpace(pageURL) == "" {
		pageURL = "https://www.airbnb.com/"
	}

	var raw []map[string]string
	err := s.withRetry(ctx, pageURL, func(runCtx context.Context) error {
		tab, cancelTab := chromedp.NewContext(s.browserCtx)
		defer cancelTab()
		tabCtx, cancel := context.WithTimeout(tab, s.cfg.RequestTimeout)
		defer cancel()

		if err := s.acquire(runCtx, pageURL); err != nil {
			return err
		}
		defer s.release(pageURL)

		script := fmt.Sprintf(`((maxPerCategory) => {
			const normalize = (s) => (s || '').replace(/\s+/g, ' ').trim();
			const categoryRe = /^(popular homes in|available next month in|stay in)\s+.+/i;
			const blocked = new Set(['log in', 'sign up', 'search', 'filters', 'map', 'next', 'previous', 'help', 'host', 'become a host', 'airbnb your home']);
			const out = [];
			const seen = new Set();

			for (const btn of document.querySelectorAll('button, [role="button"]')) {
				const t = normalize(btn.textContent).toLowerCase();
				if (t === 'accept all' || t === 'accept' || t === 'agree') {
					btn.click();
					break;
				}
			}

			window.scrollTo(0, 0);
			const maxScroll = Math.min(document.body ? document.body.scrollHeight : 0, window.innerHeight * 4);
			for (let y = 0; y <= maxScroll; y += Math.max(600, window.innerHeight)) {
				window.scrollTo(0, y);
			}

			const headings = Array.from(document.querySelectorAll('h1, h2, h3, [role="heading"], span, div'));
			const chosen = [];
			for (const h of headings) {
				const label = normalize(h.textContent);
				if (!label) continue;
				const lower = label.toLowerCase();
				if (blocked.has(lower)) continue;
				if (!categoryRe.test(label)) continue;
				chosen.push({ node: h, label });
			}

			for (const item of chosen) {
				const label = item.label;
				let container = item.node.closest('section, article, main, div');
				if (!container) container = document.body;
				let links = Array.from(container.querySelectorAll('a[href*="/rooms/"]'));
				if (links.length === 0 && container.parentElement) {
					links = Array.from(container.parentElement.querySelectorAll('a[href*="/rooms/"]'));
				}

				let count = 0;
				for (const a of links) {
					if (count >= maxPerCategory) break;
					const href = normalize(a.href);
					if (!href) continue;
					const key = label + '|' + href;
					if (seen.has(key)) continue;
					seen.add(key);
					const title = normalize(a.getAttribute('aria-label') || a.textContent);
					out.push({ label, url: href, title });
					count++;
				}
			}
			return out;
		})(%d);`, s.cfg.CardsPerPage)

		return chromedp.Run(tabCtx,
			chromedp.Navigate(pageURL),
			chromedp.Sleep(7*time.Second),
			chromedp.Evaluate(script, &raw),
		)
	})
	if err != nil {
		return nil, err
	}

	seeds := parseCategorySeeds(raw)
	if len(seeds) == 0 {
		return nil, nil
	}

	byCategory := make(map[string]int)
	listings := make([]models.Listing, 0, len(seeds))
	for _, seed := range seeds {
		label := strings.TrimSpace(seed.Label)
		if label == "" {
			continue
		}
		if byCategory[label] >= s.cfg.CardsPerPage {
			continue
		}
		u := normalizeListingURL(seed.URL)
		if u == "" {
			continue
		}
		title := strings.TrimSpace(seed.Title)
		if title == "" {
			title = "Untitled listing"
		}
		listing := models.Listing{
			Title:  title,
			URL:    u,
			Price:  "",
			Rating: "",
		}
		listing.Details = map[string]string{
			"home_span": label,
		}
		listings = append(listings, listing)
		byCategory[label]++
	}
	return listings, nil
}

func (s *Scraper) enqueueListings(listings []models.Listing, jobs chan<- detailJob) {
	for _, l := range listings {
		if _, exists := s.visited.LoadOrStore(l.URL, struct{}{}); exists {
			continue
		}
		s.addResult(l)
		jobs <- detailJob{ListingURL: l.URL}
	}
}

func (s *Scraper) collectHomeSpans(ctx context.Context) ([]homeSpan, error) {
	pageURL := s.cfg.SearchURL
	if strings.TrimSpace(pageURL) == "" {
		pageURL = "https://www.airbnb.com/"
	}

	var raw []map[string]any
	err := s.withRetry(ctx, pageURL, func(runCtx context.Context) error {
		tab, cancelTab := chromedp.NewContext(s.browserCtx)
		defer cancelTab()
		tabCtx, cancel := context.WithTimeout(tab, s.cfg.RequestTimeout)
		defer cancel()

		if err := s.acquire(runCtx, pageURL); err != nil {
			return err
		}
		defer s.release(pageURL)

		script := `(() => {
			const blocked = new Set(['log in', 'sign up', 'search', 'filters', 'map', 'next', 'previous', 'help', 'host', 'become a host', 'airbnb your home']);
			const categoryRe = /^(popular homes in|available next month in|stay in)\s+.+/i;

			// Best-effort consent dismiss to expose homepage content.
			for (const btn of document.querySelectorAll('button, [role="button"]')) {
				const t = (btn.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
				if (t === 'accept all' || t === 'accept' || t === 'agree') {
					btn.click();
					break;
				}
			}

			const anchors = Array.from(document.querySelectorAll('a[href]'));
			const out = [];
			const seen = new Set();
			for (const a of anchors) {
				const href = (a.href || '').trim();
				if (!href) continue;
				if (!/airbnb\./i.test(href)) continue;
				if (href.includes('/login') || href.includes('/signup')) continue;

				const spanTexts = Array.from(a.querySelectorAll('span'))
					.map(sp => (sp.textContent || '').replace(/\s+/g, ' ').trim())
					.filter(Boolean);
				const candidates = [
					(a.getAttribute('aria-label') || '').replace(/\s+/g, ' ').trim(),
					(a.textContent || '').replace(/\s+/g, ' ').trim(),
					...spanTexts
				].filter(Boolean);

				let label = '';
				for (const c of candidates) {
					if (categoryRe.test(c)) {
						label = c;
						break;
					}
				}
				if (!label) {
					label = candidates.find(c => c.length > 3 && c.length <= 120) || '';
				}
				if (!label) continue;

				const lower = label.toLowerCase();
				if (blocked.has(lower)) continue;
				if (!categoryRe.test(label) && !href.includes('/s/')) continue;

				const id = (a.id || a.getAttribute('data-testid') || a.querySelector('span[id]')?.id || 'no-id').trim();
				const key = id + '|' + href;
				if (seen.has(key)) continue;
				seen.add(key);
				out.push({ id, label, link: href });
			}
			return out;
		})();`

		return chromedp.Run(tabCtx,
			chromedp.Navigate(pageURL),
			chromedp.Sleep(6*time.Second),
			chromedp.Evaluate(script, &raw),
		)
	})
	if err != nil {
		return nil, err
	}

	spans := make([]homeSpan, 0, len(raw))
	seen := make(map[string]struct{})
	for _, item := range raw {
		id, _ := item["id"].(string)
		label, _ := item["label"].(string)
		link, _ := item["link"].(string)
		id = strings.TrimSpace(id)
		label = strings.TrimSpace(label)
		link = strings.TrimSpace(link)
		if id == "" || label == "" || link == "" {
			continue
		}
		key := id + "|" + link
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		spans = append(spans, homeSpan{ID: id, Label: label, Link: link})
	}

	if s.cfg.MaxSpans > 0 && len(spans) > s.cfg.MaxSpans {
		spans = spans[:s.cfg.MaxSpans]
	}
	if len(spans) == 0 {
		extra, err := s.collectHomeSpansFromHTMLAnchors(ctx, pageURL)
		if err == nil && len(extra) > 0 {
			spans = extra
		}
	}
	listingLike := make([]homeSpan, 0, len(spans))
	categoryLike := make([]homeSpan, 0, len(spans))
	for _, sp := range spans {
		if isHomeCategoryLabel(sp.Label) {
			categoryLike = append(categoryLike, sp)
		}
		l := strings.ToLower(sp.Link)
		if strings.Contains(l, "/s/") || strings.Contains(l, "/homes") {
			listingLike = append(listingLike, sp)
		}
	}
	if len(categoryLike) > 0 {
		return categoryLike, nil
	}
	if len(listingLike) > 0 {
		return listingLike, nil
	}
	return spans, nil
}

func isHomeCategoryLabel(label string) bool {
	return homeCategoryLabelRe.MatchString(strings.TrimSpace(label))
}

func (s *Scraper) collectHomeSpansFromHTMLAnchors(ctx context.Context, pageURL string) ([]homeSpan, error) {
	var raw []map[string]string
	err := s.withRetry(ctx, pageURL, func(runCtx context.Context) error {
		tab, cancelTab := chromedp.NewContext(s.browserCtx)
		defer cancelTab()
		tabCtx, cancel := context.WithTimeout(tab, s.cfg.RequestTimeout)
		defer cancel()

		if err := s.acquire(runCtx, pageURL); err != nil {
			return err
		}
		defer s.release(pageURL)

		script := `(() => {
			const normalize = (s) => (s || '').replace(/\s+/g, ' ').trim();
			const categoryRe = /^(popular homes in|available next month in|stay in)\s+.+/i;
			const out = [];
			const seen = new Set();
			for (const a of document.querySelectorAll('a[href]')) {
				const href = normalize(a.href);
				if (!href) continue;
				if (!/airbnb\./i.test(href) && !href.startsWith('/')) continue;
				const candidates = [
					normalize(a.getAttribute('aria-label')),
					normalize(a.textContent)
				].filter(Boolean);
				let label = '';
				for (const c of candidates) {
					if (categoryRe.test(c)) {
						label = c;
						break;
					}
				}
				if (!label) continue;
				const abs = href.startsWith('/') ? ('https://www.airbnb.com' + href) : href;
				const key = label + '|' + abs;
				if (seen.has(key)) continue;
				seen.add(key);
				out.push({
					id: normalize(a.id || a.getAttribute('data-testid') || 'no-id'),
					label,
					link: abs
				});
			}
			return out;
		})();`

		return chromedp.Run(tabCtx,
			chromedp.Navigate(pageURL),
			chromedp.Sleep(7*time.Second),
			chromedp.Evaluate(script, &raw),
		)
	})
	if err != nil {
		return nil, err
	}

	out := make([]homeSpan, 0, len(raw))
	for _, item := range raw {
		id := strings.TrimSpace(item["id"])
		label := strings.TrimSpace(item["label"])
		link := strings.TrimSpace(item["link"])
		if id == "" {
			id = "no-id"
		}
		if label == "" || link == "" {
			continue
		}
		out = append(out, homeSpan{ID: id, Label: label, Link: link})
	}
	return out, nil
}

func (s *Scraper) processDetailJob(ctx context.Context, job detailJob) {
	listingURL := job.ListingURL
	details, err := s.scrapeDetailPageWithRetry(ctx, listingURL)
	if err != nil {
		fmt.Printf("detail failed url=%s err=%v\n", listingURL, err)
		return
	}
	s.attachDetails(listingURL, details)
}

func (s *Scraper) scrapeSearchPage(ctx context.Context, pageURL string, limit int, spanLabel string) ([]models.Listing, error) {
	var raw []map[string]string
	err := s.withRetry(ctx, pageURL, func(runCtx context.Context) error {
		tab, cancelTab := chromedp.NewContext(s.browserCtx)
		defer cancelTab()
		tabCtx, cancel := context.WithTimeout(tab, s.cfg.RequestTimeout)
		defer cancel()

		if err := s.acquire(runCtx, pageURL); err != nil {
			return err
		}
		defer s.release(pageURL)

		script := fmt.Sprintf(`((maxItems) => {
			const out = [];
			const cards = Array.from(document.querySelectorAll('[data-testid="card-container"], [itemprop="itemListElement"], article'));
			for (const card of cards) {
				if (out.length >= maxItems) break;
				const a = card.querySelector('a[href*="/rooms/"]');
				const href = a ? (a.href || '') : '';
				if (!href) continue;
				const text = (card.innerText || '').replace(/\s+/g, ' ').trim();
				const titleNode = card.querySelector('[data-testid="listing-card-title"], [itemprop="name"]');
				const locationNode = card.querySelector('[data-testid="listing-card-subtitle"], [data-testid="listing-card-name"]');
				const priceNode = card.querySelector('[data-testid="price-availability-row"]');
				const detailNode = card.querySelector('[data-testid="listing-card-subtitle"]');
				const ratingNode = card.querySelector('[aria-label*="rated"], [aria-label*="Rating"], [data-testid="review-score"]');
				const priceMatch = text.match(/\$\s?\d[\d,]*/);
				const ratingMatch = text.match(/([0-5]\.\d{1,2})/);
				out.push({
					url: href,
					title: (titleNode?.textContent || '').replace(/\s+/g, ' ').trim(),
					price: (priceNode?.textContent || (priceMatch ? priceMatch[0] : '')).replace(/\s+/g, ' ').trim(),
					location: (locationNode?.textContent || '').replace(/\s+/g, ' ').trim(),
					rating: (ratingNode?.textContent || (ratingMatch ? ratingMatch[1] : '')).replace(/\s+/g, ' ').trim(),
					card_details: (detailNode?.textContent || '').replace(/\s+/g, ' ').trim()
				});
			}
			// Fallback when card container selector misses due variant markup.
			if (out.length === 0) {
				const links = Array.from(document.querySelectorAll('a[href*="/rooms/"]'));
				const seen = new Set();
				for (const a of links) {
					if (out.length >= maxItems) break;
					const href = (a.href || '').trim();
					if (!href || seen.has(href)) continue;
					seen.add(href);
					const host = a.closest('[data-testid="card-container"], article, [itemprop="itemListElement"]') || a;
					const text = (host.innerText || a.innerText || '').replace(/\s+/g, ' ').trim();
					const priceMatch = text.match(/\$\s?\d[\d,]*/);
					const ratingMatch = text.match(/([0-5]\.\d{1,2})/);
					out.push({
						url: href,
						title: (a.getAttribute('aria-label') || '').replace(/\s+/g, ' ').trim(),
						price: (priceMatch ? priceMatch[0] : '').replace(/\s+/g, ' ').trim(),
						location: '',
						rating: (ratingMatch ? ratingMatch[1] : '').replace(/\s+/g, ' ').trim(),
						card_details: text
					});
				}
			}
			// Final fallback: parse room URLs from embedded HTML/JSON payloads.
			if (out.length === 0) {
				const html = (document.documentElement && document.documentElement.innerHTML) ? document.documentElement.innerHTML : '';
				const roomPattern = new RegExp('(?:https?://www\\\\.airbnb\\\\.com)?/rooms/(\\\\d+)', 'g');
				const matches = html.match(roomPattern) || [];
				const seen = new Set();
				for (const m of matches) {
					if (out.length >= maxItems) break;
					const idMatch = m.match(new RegExp('/rooms/(\\\\d+)'));
					if (!idMatch || !idMatch[1]) continue;
					const href = 'https://www.airbnb.com/rooms/' + idMatch[1];
					if (seen.has(href)) continue;
					seen.add(href);
					out.push({
						url: href,
						title: '',
						price: '',
						location: '',
						rating: '',
						card_details: ''
					});
				}
			}
			return out;
		})(%d);`, limit)

		return chromedp.Run(tabCtx,
			chromedp.Navigate(pageURL),
			chromedp.Sleep(5*time.Second),
			chromedp.Evaluate(script, &raw),
		)
	})
	if err != nil {
		return nil, err
	}

	return parseRawListings(raw, limit, spanLabel), nil
}

func parseRawListings(raw []map[string]string, limit int, spanLabel string) []models.Listing {
	if limit <= 0 {
		limit = len(raw)
	}

	listings := make([]models.Listing, 0, minInt(len(raw), limit))
	for _, item := range raw {
		if len(listings) >= limit {
			break
		}
		u := normalizeListingURL(item["url"])
		if u == "" {
			continue
		}
		title := strings.TrimSpace(item["title"])
		if title == "" {
			title = "Untitled listing"
		}
		listing := models.Listing{
			Title:    title,
			Price:    cleanPrice(item["price"]),
			Location: strings.TrimSpace(item["location"]),
			Rating:   strings.TrimSpace(item["rating"]),
			URL:      u,
		}
		cardDetails := strings.TrimSpace(item["card_details"])
		if spanLabel != "" || cardDetails != "" {
			listing.Details = map[string]string{}
			if spanLabel != "" {
				listing.Details["home_span"] = spanLabel
			}
			if cardDetails != "" {
				listing.Details["card_details"] = cardDetails
			}
		}
		listings = append(listings, listing)
	}
	return listings
}

func parseCategorySeeds(raw []map[string]string) []categoryListingSeed {
	out := make([]categoryListingSeed, 0, len(raw))
	seen := make(map[string]struct{})
	for _, item := range raw {
		label := strings.TrimSpace(item["label"])
		u := strings.TrimSpace(item["url"])
		title := strings.TrimSpace(item["title"])
		if label == "" || u == "" {
			continue
		}
		key := label + "|" + u
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, categoryListingSeed{
			Label: label,
			URL:   u,
			Title: title,
		})
	}
	return out
}

func (s *Scraper) scrapeDetailPageWithRetry(ctx context.Context, listingURL string) (map[string]string, error) {
	var details map[string]string
	err := s.withRetry(ctx, listingURL, func(runCtx context.Context) error {
		tab, cancelTab := chromedp.NewContext(s.browserCtx)
		defer cancelTab()
		tabCtx, cancel := context.WithTimeout(tab, s.cfg.RequestTimeout)
		defer cancel()

		if err := s.acquire(runCtx, listingURL); err != nil {
			return err
		}
		defer s.release(listingURL)

		var pageText string
		script := `(() => {
			const text = (document.body && document.body.innerText) ? document.body.innerText : '';
			const descNode = document.querySelector('[data-section-id="DESCRIPTION_DEFAULT"] span, [itemprop="description"]');
			const amenityNodes = Array.from(document.querySelectorAll('[data-testid="amenity-row"], [data-section-id="AMENITIES_DEFAULT"] li'));
			const amenities = amenityNodes.slice(0, 20).map(n => (n.textContent || '').replace(/\s+/g, ' ').trim()).filter(Boolean);
			return {
				description: descNode ? descNode.textContent.replace(/\s+/g, ' ').trim() : '',
				amenities: amenities.join(', '),
				text
			};
		})();`

		var payload map[string]string
		if err := chromedp.Run(tabCtx,
			chromedp.Navigate(listingURL),
			chromedp.Sleep(2*time.Second),
			chromedp.Evaluate(script, &payload),
		); err != nil {
			return err
		}

		pageText = payload["text"]
		details = map[string]string{
			"description": strings.TrimSpace(payload["description"]),
			"amenities":   strings.TrimSpace(payload["amenities"]),
		}
		for k, v := range inferStructuredDetails(pageText) {
			if v != "" {
				details[k] = v
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return details, nil
}

func (s *Scraper) withRetry(ctx context.Context, pageURL string, fn func(context.Context) error) error {
	var lastErr error
	for attempt := 1; attempt <= s.cfg.MaxRetries+1; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}
		backoff := time.Duration(attempt*attempt) * 400 * time.Millisecond
		time.Sleep(backoff)
	}
	return fmt.Errorf("retries exhausted for %s: %w", pageURL, lastErr)
}

func (s *Scraper) acquire(ctx context.Context, pageURL string) error {
	domain, err := getDomain(pageURL)
	if err != nil {
		return err
	}

	s.limitersMu.Lock()
	limiter, ok := s.domainLimiters[domain]
	if !ok {
		limiter = rate.NewLimiter(rate.Limit(s.cfg.RateLimitPerSecond), s.cfg.RateBurst)
		s.domainLimiters[domain] = limiter
	}
	sem, ok := s.domainSemaphores[domain]
	if !ok {
		sem = make(chan struct{}, s.cfg.MaxRequestsPerDomain)
		s.domainSemaphores[domain] = sem
	}
	if s.domainReqCount[domain] >= s.cfg.MaxTotalRequestsPerDomain {
		s.limitersMu.Unlock()
		return fmt.Errorf("domain request cap reached for %s", domain)
	}
	s.domainReqCount[domain]++
	s.limitersMu.Unlock()

	if err := limiter.Wait(ctx); err != nil {
		return err
	}

	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Scraper) release(pageURL string) {
	domain, err := getDomain(pageURL)
	if err != nil {
		return
	}
	s.limitersMu.Lock()
	sem := s.domainSemaphores[domain]
	s.limitersMu.Unlock()
	select {
	case <-sem:
	default:
	}
}

func (s *Scraper) addResult(l models.Listing) {
	s.resultsMu.Lock()
	s.results = append(s.results, l)
	s.resultsMu.Unlock()
}

func (s *Scraper) attachDetails(listingURL string, details map[string]string) {
	s.resultsMu.Lock()
	defer s.resultsMu.Unlock()
	for i := range s.results {
		if s.results[i].URL == listingURL {
			if s.results[i].Details == nil {
				s.results[i].Details = make(map[string]string)
			}
			for k, v := range details {
				s.results[i].Details[k] = v
			}
			return
		}
	}
}

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

	data, err := json.MarshalIndent(groups, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.cfg.OutputFile, data, 0o644)
}

func buildPaginatedAirbnbURL(base string, page int) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	q := u.Query()
	q.Set("items_offset", fmt.Sprintf("%d", page*20))
	q.Set("section_offset", fmt.Sprintf("%d", page))
	u.RawQuery = q.Encode()
	return u.String()
}

func normalizeListingURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if !strings.Contains(u.Path, "/rooms/") {
		return ""
	}
	u.RawQuery = ""
	u.Fragment = ""
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	if u.Host == "" {
		u.Host = "www.airbnb.com"
	}
	return u.String()
}

func getDomain(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", fmt.Errorf("missing host in %q", raw)
	}
	return strings.ToLower(u.Host), nil
}

func cleanPrice(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func inferStructuredDetails(text string) map[string]string {
	compact := strings.Join(strings.Fields(text), " ")
	out := map[string]string{}

	patterns := map[string]*regexp.Regexp{
		"guests":   regexp.MustCompile(`(?i)(\d+)\s+guest`),
		"bedrooms": regexp.MustCompile(`(?i)(\d+)\s+bedroom`),
		"beds":     regexp.MustCompile(`(?i)(\d+)\s+bed`),
		"baths":    regexp.MustCompile(`(?i)(\d+(?:\.\d+)?)\s+bath`),
	}

	for key, re := range patterns {
		m := re.FindStringSubmatch(compact)
		if len(m) > 1 {
			out[key] = m[1]
		}
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func buildLabelSearchURL(label string) string {
	clean := strings.TrimSpace(label)
	clean = strings.ReplaceAll(clean, "&", " and ")
	clean = strings.Join(strings.Fields(clean), "-")
	if clean == "" {
		return "https://www.airbnb.com/s/homes"
	}
	return "https://www.airbnb.com/s/" + clean + "/homes"
}

func isAirbnbHomeRoot(rawURL string) bool {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if !strings.Contains(host, "airbnb.") {
		return false
	}
	path := strings.TrimSpace(u.EscapedPath())
	return path == "" || path == "/"
}
