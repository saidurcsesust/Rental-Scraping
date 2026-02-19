package scraper

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"colly-scraper/models"

	"github.com/chromedp/chromedp"
	_ "github.com/lib/pq"
	"golang.org/x/time/rate"
)

type homeSpan struct {
	ID    string
	Label string
	Link  string
}

var homeCategoryLabelRe = regexp.MustCompile(`(?i)^(popular homes in|available next month in|stay in)\s+.+$`)
var homeCategoryLocationRe = regexp.MustCompile(`(?i)^(?:popular homes in|available next month in|stay in)\s+(.+)$`)
var moneyAmountRe = regexp.MustCompile(`\$\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)`)

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
		fmt.Println("no clickable home category spans found; click-only mode skipped scraping")
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
		clickListings, clickErr := s.scrapeByHomeSpanClickFlow(ctx, span)
		if clickErr != nil {
			fmt.Printf("span click-flow failed id=%q label=%q err=%v\n", span.ID, span.Label, clickErr)
		}
		if len(clickListings) > 0 {
			s.enqueueListings(clickListings, jobs)
			fmt.Printf("span done label=%q listings=%d via=click-flow\n", span.Label, len(clickListings))
		} else {
			fmt.Printf("span done label=%q listings=0 via=click-flow\n", span.Label)
		}
	}
}

func (s *Scraper) scrapeByHomeSpanClickFlow(ctx context.Context, span homeSpan) ([]models.Listing, error) {
	homeURL := s.cfg.SearchURL
	if strings.TrimSpace(homeURL) == "" {
		homeURL = "https://www.airbnb.com/"
	}
	pageCount := s.cfg.PagesPerSpan
	if pageCount <= 0 {
		pageCount = 1
	}

	var listings []models.Listing
	err := s.withRetry(ctx, homeURL, func(runCtx context.Context) error {
		tab, cancelTab := chromedp.NewContext(s.browserCtx)
		defer cancelTab()
		tabCtx, cancel := context.WithTimeout(tab, s.cfg.RequestTimeout*time.Duration(pageCount+1))
		defer cancel()

		if err := s.acquire(runCtx, homeURL); err != nil {
			return err
		}
		defer s.release(homeURL)

		if err := chromedp.Run(tabCtx,
			chromedp.Navigate(homeURL),
			chromedp.Sleep(6*time.Second),
		); err != nil {
			return err
		}

		var clicked bool
		clickScript := fmt.Sprintf(`((targetHref, targetLabel, targetID) => {
			const normalize = (s) => (s || '').replace(/\s+/g, ' ').trim();
			const lower = (s) => normalize(s).toLowerCase();
			const hrefNorm = normalize(targetHref);
			const labelNorm = lower(targetLabel);
			const idNorm = normalize(targetID);

			for (const btn of document.querySelectorAll('button, [role="button"]')) {
				const t = lower(btn.textContent);
				if (t === 'accept all' || t === 'accept' || t === 'agree') {
					btn.click();
					break;
				}
			}

			const anchors = Array.from(document.querySelectorAll('a[href]'));
			const isRoomLink = (href) => href.includes('/rooms/');
			let best = null;
			let bestScore = -1;

			for (const a of anchors) {
				const href = normalize(a.href);
				if (!href || isRoomLink(href)) continue;

				let score = 0;
				const spanIDs = Array.from(a.querySelectorAll('span[id]')).map(n => normalize(n.id));
				const texts = [
					normalize(a.getAttribute('aria-label')),
					normalize(a.textContent),
					...Array.from(a.querySelectorAll('span')).map(n => normalize(n.textContent))
				].filter(Boolean);
				const textJoined = lower(texts.join(' '));

				if (idNorm && (normalize(a.id) === idNorm || spanIDs.includes(idNorm))) score += 100;
				if (hrefNorm && href === hrefNorm) score += 90;
				if (hrefNorm && href.includes(hrefNorm)) score += 70;
				if (labelNorm && textJoined.includes(labelNorm)) score += 60;
				if (href.includes('/s/') || href.includes('/homes')) score += 10;

				if (score > bestScore) {
					best = a;
					bestScore = score;
				}
			}

			if (!best || bestScore < 20) {
				return false;
			}
			best.click();
			return true;
		})(%q, %q, %q);`, span.Link, span.Label, span.ID)

		if err := chromedp.Run(tabCtx, chromedp.Evaluate(clickScript, &clicked)); err != nil {
			return err
		}
		if !clicked {
			return fmt.Errorf("category link not clickable")
		}

		if err := chromedp.Run(tabCtx, chromedp.Sleep(5*time.Second)); err != nil {
			return err
		}

		targetUnique := pageCount * s.cfg.CardsPerPage
		collected := make([]models.Listing, 0, targetUnique)
		seenURL := make(map[string]struct{}, targetUnique)
		for page := 0; page < pageCount; page++ {
			pageListings, err := s.extractListingsFromCurrentPage(tabCtx, s.cfg.CardsPerPage, span.Label)
			if err != nil {
				return err
			}
			addedThisPage := 0
			for _, l := range pageListings {
				if addedThisPage >= s.cfg.CardsPerPage {
					break
				}
				if _, ok := seenURL[l.URL]; ok {
					continue
				}
				seenURL[l.URL] = struct{}{}
				collected = append(collected, l)
				addedThisPage++
			}

			if page == pageCount-1 {
				break
			}

			var beforeURL string
			if err := chromedp.Run(tabCtx, chromedp.Location(&beforeURL)); err != nil {
				return err
			}

			var moved bool
			targetPageNo := page + 2
			nextScript := fmt.Sprintf(`(() => {
				const normalize = (s) => (s || '').replace(/\s+/g, ' ').trim().toLowerCase();
				const nav = (node) => {
					if (!node) return false;
					const href = (node.getAttribute && node.getAttribute('href')) || node.href || '';
					if (href) {
						window.location.assign(href);
						return true;
					}
					node.click();
					return true;
				};
				const isDisabled = (n) => {
					if (!n) return true;
					if (n.disabled) return true;
					const ariaDisabled = (n.getAttribute && n.getAttribute('aria-disabled')) || '';
					return ariaDisabled.toLowerCase() === 'true';
				};

				const roots = Array.from(document.querySelectorAll('div[class*="p1j2gy66"], nav')).concat([document]);
				for (const root of roots) {
					const pageLinks = Array.from(root.querySelectorAll('a[href], button[role="link"], a[role="link"]'));
					for (const n of pageLinks) {
						if (isDisabled(n)) continue;
						const text = normalize(n.textContent || '');
						const aria = normalize(n.getAttribute ? n.getAttribute('aria-label') : '');
						if (text === '%d' || aria === '%d' || aria.includes('page %d')) {
							if (nav(n)) return true;
						}
					}
					for (const n of pageLinks) {
						if (isDisabled(n)) continue;
						const aria = normalize(n.getAttribute ? n.getAttribute('aria-label') : '');
						if (aria === 'next' || aria.includes('next')) {
							if (nav(n)) return true;
						}
					}
				}
				return false;
			})();`, targetPageNo, targetPageNo, targetPageNo)

			if err := chromedp.Run(tabCtx, chromedp.Evaluate(nextScript, &moved)); err != nil {
				return err
			}
			if !moved {
				break
			}

			urlChanged := false
			for i := 0; i < 10; i++ {
				var afterURL string
				if err := chromedp.Run(tabCtx,
					chromedp.Sleep(700*time.Millisecond),
					chromedp.Location(&afterURL),
				); err != nil {
					return err
				}
				if strings.TrimSpace(afterURL) != "" && afterURL != beforeURL {
					urlChanged = true
					break
				}
			}
			if !urlChanged {
				fmt.Printf("span pagination click had no URL change label=%q from=%q\n", span.Label, beforeURL)
			}
			if err := chromedp.Run(tabCtx, chromedp.Sleep(2*time.Second)); err != nil {
				return err
			}
		}

		if len(collected) < targetUnique {
			fmt.Printf("span click-flow unique shortfall label=%q wanted=%d got=%d\n", span.Label, targetUnique, len(collected))
		}
		listings = collected
		_ = chromedp.Run(tabCtx, chromedp.Navigate(homeURL), chromedp.Sleep(2*time.Second))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return listings, nil
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
			const uniqueLinks = (nodes) => {
				const byHref = new Map();
				for (const n of nodes || []) {
					const href = normalize(n && n.href ? n.href : '');
					if (!href || byHref.has(href)) continue;
					byHref.set(href, n);
				}
				return Array.from(byHref.values());
			};

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
				let links = [];

				// Airbnb category rows can be rendered in sibling carousels; walk up until we find enough cards.
				let container = item.node;
				for (let depth = 0; depth < 7 && container; depth++) {
					const candidate = uniqueLinks(container.querySelectorAll('a[href*="/rooms/"]'));
					if (candidate.length > links.length) {
						links = candidate;
					}
					if (links.length >= maxPerCategory) break;
					container = container.parentElement;
				}

				// Fallback: use sibling areas near the heading when cards are outside the immediate ancestor tree.
				if (links.length < maxPerCategory && item.node.parentElement) {
					const parent = item.node.parentElement;
					for (const sib of Array.from(parent.children)) {
						if (sib === item.node) continue;
						const candidate = uniqueLinks(sib.querySelectorAll('a[href*="/rooms/"]'));
						if (candidate.length > links.length) {
							links = candidate;
						}
						if (links.length >= maxPerCategory) break;
					}
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
			Title:    title,
			URL:      u,
			Price:    "",
			Location: extractLocationFromHomeSpan(label),
			Rating:   "",
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
			const headingLinks = Array.from(document.querySelectorAll('h1 a[href], h2 a[href], h3 a[href], [role="heading"] a[href]'));
			const out = [];
			const seen = new Set();
			const consider = (a) => {
				const href = (a.href || '').trim();
				if (!href) return;
				if (!/airbnb\./i.test(href)) return;
				if (href.includes('/login') || href.includes('/signup')) return;
				if (!href.includes('/s/') || !href.includes('/homes')) return;

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
				if (!label) return;

				const lower = label.toLowerCase();
				if (blocked.has(lower)) return;

				const id = (a.id || a.getAttribute('data-testid') || a.querySelector('span[id]')?.id || 'no-id').trim();
				const key = href;
				if (seen.has(key)) return;
				seen.add(key);
				out.push({ id, label, link: href });
			};

			for (const a of headingLinks) {
				consider(a);
			}
			for (const a of anchors) {
				consider(a);
			}
			return out;
		})();`

		return chromedp.Run(tabCtx,
			chromedp.Navigate(pageURL),
			chromedp.Sleep(5*time.Second),
			chromedp.Evaluate(`window.scrollTo(0, 0);`, nil),
			chromedp.Sleep(1200*time.Millisecond),
			chromedp.Evaluate(`window.scrollTo(0, Math.max(900, window.innerHeight * 1));`, nil),
			chromedp.Sleep(1200*time.Millisecond),
			chromedp.Evaluate(`window.scrollTo(0, Math.max(1800, window.innerHeight * 2));`, nil),
			chromedp.Sleep(1200*time.Millisecond),
			chromedp.Evaluate(`window.scrollTo(0, Math.max(2700, window.innerHeight * 3));`, nil),
			chromedp.Sleep(1200*time.Millisecond),
			chromedp.Evaluate(`window.scrollTo(0, Math.max(3600, window.innerHeight * 4));`, nil),
			chromedp.Sleep(1200*time.Millisecond),
			chromedp.Evaluate(`window.scrollTo(0, Math.max(4500, window.innerHeight * 5));`, nil),
			chromedp.Sleep(1200*time.Millisecond),
			chromedp.Evaluate(`window.scrollTo(0, Math.max(5400, window.innerHeight * 6));`, nil),
			chromedp.Sleep(1200*time.Millisecond),
			chromedp.Evaluate(`window.scrollTo(0, 0);`, nil),
			chromedp.Sleep(1800*time.Millisecond),
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
	for _, sp := range spans {
		l := strings.ToLower(sp.Link)
		if strings.Contains(l, "/s/") || strings.Contains(l, "/homes") {
			listingLike = append(listingLike, sp)
		}
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

func (s *Scraper) extractListingsFromCurrentPage(tabCtx context.Context, limit int, spanLabel string) ([]models.Listing, error) {
	seen := make(map[string]models.Listing, limit)
	candidateLimit := limit * 8
	if candidateLimit < limit {
		candidateLimit = limit
	}
	for attempt := 0; attempt < 5; attempt++ {
		var raw []map[string]string
		script := fmt.Sprintf(`((maxItems) => {
			const out = [];
			const cards = Array.from(document.querySelectorAll('[data-testid="card-container"], [itemprop="itemListElement"], article'));
			const seenHref = new Set();
			const pushCard = (card, href, fallbackTitle) => {
				if (!href || seenHref.has(href)) return;
				seenHref.add(href);
				const text = (card?.innerText || '').replace(/\s+/g, ' ').trim();
				const titleNode = card ? card.querySelector('[data-testid="listing-card-title"], [itemprop="name"]') : null;
				const locationNode = card ? card.querySelector('[data-testid="listing-card-subtitle"], [data-testid="listing-card-name"]') : null;
				const priceNode = card ? card.querySelector('[data-testid="price-availability-row"]') : null;
				const detailNode = card ? card.querySelector('[data-testid="listing-card-subtitle"]') : null;
				const ratingNode = card ? card.querySelector('[aria-label*="rated"], [aria-label*="Rating"], [data-testid="review-score"]') : null;
				const priceMatch = text.match(/\$\s?\d[\d,]*/);
				const ratingMatch = text.match(/([0-5]\.\d{1,2})/);
				out.push({
					url: href,
					title: ((titleNode?.textContent || fallbackTitle || '')).replace(/\s+/g, ' ').trim(),
					price: (priceNode?.textContent || (priceMatch ? priceMatch[0] : '')).replace(/\s+/g, ' ').trim(),
					location: (locationNode?.textContent || '').replace(/\s+/g, ' ').trim(),
					rating: (ratingNode?.textContent || (ratingMatch ? ratingMatch[1] : '')).replace(/\s+/g, ' ').trim(),
					card_details: (detailNode?.textContent || text || '').replace(/\s+/g, ' ').trim()
				});
			};

			// First pass: card containers
			for (const card of cards) {
				const a = card.querySelector('a[href*="/rooms/"]');
				const href = a ? (a.href || '').trim() : '';
				pushCard(card, href, '');
			}

			// Second pass: any room links present in DOM
			const links = Array.from(document.querySelectorAll('a[href*="/rooms/"]'));
			for (const a of links) {
				const href = (a.href || '').trim();
				const host = a.closest('[data-testid="card-container"], article, [itemprop="itemListElement"]');
				pushCard(host, href, (a.getAttribute('aria-label') || '').trim());
			}

			// Third pass: parse embedded HTML payload URLs if still short
			if (out.length < maxItems) {
				const html = (document.documentElement && document.documentElement.innerHTML) ? document.documentElement.innerHTML : '';
				const roomPattern = new RegExp('(?:https?://www\\\\.airbnb\\\\.com)?/rooms/(\\\\d+)', 'g');
				const matches = html.match(roomPattern) || [];
				for (const m of matches) {
					const idMatch = m.match(new RegExp('/rooms/(\\\\d+)'));
					if (!idMatch || !idMatch[1]) continue;
					const href = 'https://www.airbnb.com/rooms/' + idMatch[1];
					pushCard(null, href, '');
					if (out.length >= maxItems) break;
				}
			}

			if (out.length > maxItems) return out.slice(0, maxItems);
			return out;
		})(%d);`, candidateLimit)

		if err := chromedp.Run(tabCtx, chromedp.Evaluate(script, &raw)); err != nil {
			return nil, err
		}
		for _, l := range parseRawListings(raw, candidateLimit, spanLabel) {
			if _, ok := seen[l.URL]; ok {
				continue
			}
			seen[l.URL] = l
		}
		if len(seen) >= candidateLimit {
			break
		}
		if err := chromedp.Run(tabCtx,
			chromedp.Evaluate(`window.scrollBy(0, Math.max(900, window.innerHeight));`, nil),
			chromedp.Sleep(1200*time.Millisecond),
		); err != nil {
			return nil, err
		}
	}

	out := make([]models.Listing, 0, len(seen))
	for _, l := range seen {
		out = append(out, l)
	}
	return out, nil
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
		if listing.Location == "" && spanLabel != "" {
			listing.Location = extractLocationFromHomeSpan(spanLabel)
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
			const normalize = (s) => (s || '').replace(/\s+/g, ' ').trim();
			const titleNode = document.querySelector('h1');
			const ogTitle = document.querySelector('meta[property="og:title"]');
			const descNode = document.querySelector('[data-section-id="DESCRIPTION_DEFAULT"] span, [itemprop="description"]');
			const amenityNodes = Array.from(document.querySelectorAll('[data-testid="amenity-row"], [data-section-id="AMENITIES_DEFAULT"] li'));
			const amenities = amenityNodes.slice(0, 20).map(n => (n.textContent || '').replace(/\s+/g, ' ').trim()).filter(Boolean);
			return {
				title: normalize((titleNode && titleNode.textContent) || (ogTitle && ogTitle.getAttribute('content')) || ''),
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
			"title":       strings.TrimSpace(payload["title"]),
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
			if t := strings.TrimSpace(details["title"]); t != "" {
				if strings.TrimSpace(s.results[i].Title) == "" || strings.EqualFold(strings.TrimSpace(s.results[i].Title), "Untitled listing") {
					s.results[i].Title = t
				}
			}
			if s.results[i].Details == nil {
				s.results[i].Details = make(map[string]string)
			}
			for k, v := range details {
				if k == "title" {
					continue
				}
				s.results[i].Details[k] = v
			}
			return
		}
	}
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
	raw := strings.TrimSpace(s)
	if raw == "" {
		return ""
	}

	normalized := strings.Join(strings.Fields(raw), " ")
	lower := strings.ToLower(normalized)
	matches := moneyAmountRe.FindAllStringSubmatchIndex(normalized, -1)
	if len(matches) == 0 {
		return ""
	}

	// Preferred source: explicit total shown as "for 2 nights".
	if idx := strings.Index(lower, "for 2 nights"); idx >= 0 {
		best := -1
		for i, m := range matches {
			if len(m) < 4 {
				continue
			}
			fullEnd := m[1]
			if fullEnd <= idx {
				best = i
			}
		}
		if best == -1 {
			best = len(matches) - 1
		}
		amount := normalized[matches[best][2]:matches[best][3]]
		return fmt.Sprintf("$%s for 2 nights", amount)
	}

	firstAmount := normalized[matches[0][2]:matches[0][3]]
	perNight := strings.Contains(lower, "/ night") || strings.Contains(lower, " per night") || strings.Contains(lower, " nightly")
	if perNight {
		total, ok := multiplyAmount(firstAmount, 2)
		if ok {
			return fmt.Sprintf("$%s for 2 nights", total)
		}
	}

	// Fallback: keep first parsed amount but normalize to the requested 2-night format.
	return fmt.Sprintf("$%s for 2 nights", firstAmount)
}

func multiplyAmount(raw string, nights int) (string, bool) {
	clean := strings.ReplaceAll(strings.TrimSpace(raw), ",", "")
	if clean == "" || nights <= 0 {
		return "", false
	}
	value, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return "", false
	}
	total := value * float64(nights)
	if math.Abs(total-math.Round(total)) < 1e-9 {
		return strconv.FormatInt(int64(math.Round(total)), 10), true
	}
	return strconv.FormatFloat(total, 'f', 2, 64), true
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

func extractLocationFromHomeSpan(label string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return ""
	}
	m := homeCategoryLocationRe.FindStringSubmatch(label)
	if len(m) < 2 {
		return ""
	}
	return strings.TrimSpace(m[1])
}
