import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';
import { DOMParser } from "https://deno.land/x/deno_dom@v0.1.43/deno-dom-wasm.ts";
import { parse as parseXML } from "https://deno.land/x/xml@2.1.3/mod.ts";

// ============ RSS FEEDS (verified working) ============
const RSS_FEEDS = [
  // US Government Feeds
  { name: "FTC Press Releases", url: "https://www.ftc.gov/feeds/press-release.xml", region: "US" },
  { name: "FTC Consumer Protection", url: "https://www.ftc.gov/feeds/press-release-consumer-protection.xml", region: "US" },
  { name: "FTC Competition", url: "https://www.ftc.gov/feeds/press-release-competition.xml", region: "US" },
  { name: "OCC News Releases", url: "https://www.occ.gov/rss/occ_news.xml", region: "US" },
  { name: "OCC Bulletins", url: "https://www.occ.gov/rss/occ_bulletins.xml", region: "US" },
  
  // EU Feeds
  { name: "EU Competition News", url: "https://ec.europa.eu/competition/rss/news_en.xml", region: "EU" },
  
  // Legal Blogs
  { name: "SCOTUSblog", url: "https://www.scotusblog.com/feed/", region: "US" },
  { name: "IPKat", url: "https://ipkitten.blogspot.com/feeds/posts/default", region: "Global" },
  { name: "Law and the Workplace", url: "https://www.lawandtheworkplace.com/feed/", region: "US" },
  { name: "Consumer Financial Services Law Monitor", url: "https://www.consumerfinancialserviceslawmonitor.com/feed/", region: "US" },
  
  // Fintech
  { name: "Fintech Business Weekly", url: "https://fintechbusinessweekly.substack.com/feed", region: "US" },
  
  // Legal News
  { name: "Law360", url: "https://www.law360.com/search/articles?format=rss&q=&facet=", region: "US" },
  { name: "Law360 Pulse Legal Tech", url: "https://www.law360.com/pulse/legal-tech/rss", region: "US" }
];

// ============ WEB SCRAPING CONFIGS ============
const SCRAPE_CONFIGS = [
  {
    name: "Skadden Insights",
    url: "https://www.skadden.com/insights",
    region: "US",
    articleSelector: "article, .insight-item, .card, [class*='insight'], [class*='article']",
    titlePattern: /<a[^>]*href="([^"]*\/insights\/[^"]*)"[^>]*>([^<]+)<\/a>/gi,
    datePattern: /<time[^>]*>([^<]+)<\/time>|<span[^>]*class="[^"]*date[^"]*"[^>]*>([^<]+)<\/span>/gi
  },
  {
    name: "CFPB Newsroom",
    url: "https://www.consumerfinance.gov/about-us/newsroom/",
    region: "US",
    titlePattern: /<a[^>]*href="(\/about-us\/newsroom\/[^"]*)"[^>]*class="[^"]*"[^>]*>([^<]+)<\/a>|<h[1-3][^>]*class="[^"]*title[^"]*"[^>]*><a[^>]*href="([^"]*)"[^>]*>([^<]+)<\/a>/gi,
    baseUrl: "https://www.consumerfinance.gov"
  },
  {
    name: "FDIC Press Releases",
    url: "https://www.fdic.gov/news/press-releases",
    region: "US",
    titlePattern: /<a[^>]*href="(\/news\/press-releases\/[^"]*)"[^>]*>([^<]+)<\/a>/gi,
    baseUrl: "https://www.fdic.gov"
  },
  {
    name: "EBG Law Insights",
    url: "https://www.ebglaw.com/insights/",
    region: "US",
    titlePattern: /<a[^>]*href="(https:\/\/www\.ebglaw\.com\/insights\/[^"]*)"[^>]*>([^<]+)<\/a>/gi
  },
  {
    name: "Jackson Lewis",
    url: "https://www.jacksonlewis.com/insights",
    region: "US",
    titlePattern: /<a[^>]*href="(https:\/\/www\.jacksonlewis\.com\/insights\/[^"]*)"[^>]*>([^<]+)<\/a>|<a[^>]*href="(\/insights\/[^"]*)"[^>]*>([^<]+)<\/a>/gi,
    baseUrl: "https://www.jacksonlewis.com"
  },

];

// ============ RETRY LOGIC WITH EXPONENTIAL BACKOFF ============
async function fetchWithRetry(url, options = {}, maxRetries = 3) {
  let lastError;
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const response = await fetch(url, {
        ...options,
        signal: AbortSignal.timeout(15000) // 15 second timeout
      });
      
      if (response.ok) {
        return { success: true, response, attempt: attempt + 1 };
      }
      
      // Handle 429 Too Many Requests with Retry-After header
      if (response.status === 429) {
        const retryAfter = response.headers.get('Retry-After');
        let waitTime = Math.pow(2, attempt) * 1000; // Default exponential backoff
        
        if (retryAfter) {
          // Parse as seconds or HTTP date
          const parsed = parseInt(retryAfter, 10);
          if (!isNaN(parsed)) {
            waitTime = parsed * 1000;
          } else {
            const retryDate = new Date(retryAfter);
            if (!isNaN(retryDate.getTime())) {
              waitTime = Math.max(0, retryDate.getTime() - Date.now());
            }
          }
        }
        
        if (attempt < maxRetries - 1) {
          await new Promise(resolve => setTimeout(resolve, waitTime));
          continue;
        }
      }
      
      // Don't retry on 4xx errors (except 429)
      if (response.status >= 400 && response.status < 500) {
        return { success: false, error: `HTTP ${response.status}`, attempt: attempt + 1 };
      }
      
      lastError = `HTTP ${response.status}`;
    } catch (err) {
      lastError = err.message;
    }
    
    // Exponential backoff: 1s, 2s, 4s
    if (attempt < maxRetries - 1) {
      await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000));
    }
  }
  
  return { success: false, error: lastError, attempt: maxRetries };
}

// ============ WEB SCRAPING FUNCTIONS (DOM-based) ============
async function scrapeWebsite(config) {
  const items = [];
  
  const result = await fetchWithRetry(config.url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.5'
    }
  });
  
  if (!result.success) {
    return { items: [], error: result.error, retries: result.attempt };
  }
  
  const html = await result.response.text();
  const seen = new Set();
  
  // For sites that require regex-only parsing (JS-rendered sites)
  if (config.useRegexOnly && config.titlePattern) {
    const matches = [...html.matchAll(config.titlePattern)];
    
    for (const match of matches.slice(0, 20)) {
      let link = match[1] || match[3] || '';
      let title = match[2] || match[4] || '';
      
      title = title.replace(/<[^>]*>/g, '').replace(/\s+/g, ' ').trim();
      if (!title || title.length < 15 || seen.has(title.toLowerCase())) continue;
      seen.add(title.toLowerCase());
      
      if (link && !link.startsWith('http') && config.baseUrl) {
        link = link.startsWith('/') ? config.baseUrl + link : config.baseUrl + '/' + link;
      }
      
      if (title.toLowerCase().includes('view all') || 
          title.toLowerCase().includes('read more') ||
          title.toLowerCase().includes('subscribe') ||
          title.length > 300) continue;
      
      items.push({
        title: title,
        link: link,
        description: '',
        pubDate: new Date().toISOString(),
        source: config.name
      });
    }
    
    return { items, error: null, retries: result.attempt };
  }
  
  try {
    const doc = new DOMParser().parseFromString(html, "text/html");
    if (!doc) {
      return { items: [], error: "Failed to parse HTML", retries: result.attempt };
    }
    
    // Use articleSelector if provided, otherwise find common article patterns
    const selectors = config.articleSelector || "article a, .card a, .insight a, .post a, h2 a, h3 a";
    const elements = doc.querySelectorAll(selectors);
    
    for (const el of Array.from(elements).slice(0, 20)) {
      // Handle both anchor elements and containers with anchors
      let anchor;
      let title = '';
      
      if (config.titleSelector && el.querySelector(config.titleSelector)) {
        // If specific title selector is provided and found inside the article element
        const titleEl = el.querySelector(config.titleSelector);
        title = titleEl.textContent?.trim() || '';
        // Try to find link in title element or its parent
        anchor = titleEl.tagName === 'A' ? titleEl : titleEl.closest('a') || el.querySelector('a');
      } else {
        // Default behavior
        anchor = el.tagName === 'A' ? el : el.querySelector('a');
        if (anchor) title = anchor.textContent?.trim() || '';
      }

      if (!anchor) continue;
      
      let link = anchor.getAttribute('href') || '';
      
      // Clean title
      title = title.replace(/\s+/g, ' ').trim();
      
      if (!title || title.length < 10 || seen.has(title.toLowerCase())) continue;
      
      // Skip navigation/utility links
      if (title.toLowerCase().includes('view all') || 
          title.toLowerCase().includes('read more') ||
          title.toLowerCase().includes('subscribe') ||
          title.toLowerCase().includes('sign up') ||
          title.length > 300) continue;
      
      seen.add(title.toLowerCase());
      
      // Normalize URL
      if (link && !link.startsWith('http')) {
        if (config.baseUrl) {
          link = link.startsWith('/') ? config.baseUrl + link : config.baseUrl + '/' + link;
        } else {
          const baseUrl = new URL(config.url).origin;
          link = link.startsWith('/') ? baseUrl + link : baseUrl + '/' + link;
        }
      }
      
      // Try to extract date
      let pubDate = new Date().toISOString();
      
      if (config.dateSelector) {
        const dateEl = el.querySelector(config.dateSelector);
        if (dateEl) {
           const dateStr = dateEl.getAttribute('datetime') || dateEl.textContent?.trim();
           if (dateStr) pubDate = dateStr;
        }
      } else if (config.datePattern) {
        // ... (existing date logic if any, currently none in loop)
      } else {
         // Try to find a date in the element or nearby
         const dateEl = el.querySelector('time, .date, .timestamp, [class*="date"]');
         if (dateEl) {
           const dateStr = dateEl.getAttribute('datetime') || dateEl.textContent?.trim();
           if (dateStr) pubDate = dateStr;
         }
      }

      // Try to extract description
      let description = '';
      if (config.descriptionSelector) {
         const descEl = el.querySelector(config.descriptionSelector);
         if (descEl) description = descEl.textContent?.trim() || '';
      }

      // Try to extract author
      let author = '';
      if (config.authorSelector) {
        const authorEl = el.querySelector(config.authorSelector);
        if (authorEl) author = authorEl.textContent?.trim() || '';
      } else {
        // Common author selectors
        const authorEl = el.querySelector('.author, .byline, [rel="author"], .meta-author');
        if (authorEl) author = authorEl.textContent?.trim() || '';
      }

      items.push({
        title: title,
        link: link,
        description: description,
        pubDate: pubDate,
        author: author,
        source: config.name
      });
    }
  } catch (err) {
    // Fallback to regex if DOM parsing fails
    if (config.titlePattern) {
      const matches = [...html.matchAll(config.titlePattern)];
      
      for (const match of matches.slice(0, 15)) {
        let link = match[1] || match[3] || '';
        let title = match[2] || match[4] || '';
        
        title = title.replace(/<[^>]*>/g, '').trim();
        if (!title || title.length < 10 || seen.has(title)) continue;
        seen.add(title);
        
        if (link && !link.startsWith('http') && config.baseUrl) {
          link = config.baseUrl + link;
        }
        
        if (title.toLowerCase().includes('view all') || 
            title.toLowerCase().includes('read more') ||
            title.toLowerCase().includes('subscribe') ||
            title.length > 300) continue;
        
        items.push({
          title: title,
          link: link,
          description: '',
          pubDate: new Date().toISOString(),
          source: config.name
        });
      }
    }
  }
  
  return { items, error: null, retries: result.attempt };
}

// Parse RSS/Atom XML to extract items using XML parser
function parseRssFeed(xmlText, feedName) {
  const items = [];
  
  try {
    const parsed = parseXML(xmlText);
    
    // Handle RSS 2.0 format
    if (parsed?.rss?.channel?.item) {
      const rssItems = Array.isArray(parsed.rss.channel.item) 
        ? parsed.rss.channel.item 
        : [parsed.rss.channel.item];
      
      for (const item of rssItems) {
        const title = extractTextContent(item.title);
        const link = extractTextContent(item.link) || item.link?.['@href'] || '';
        const description = extractTextContent(item.description);
        const pubDate = extractTextContent(item.pubDate) || extractTextContent(item['dc:date']);
        
        if (title) {
          items.push({ title, link, description, pubDate, source: feedName });
        }
      }
    }
    
    // Handle Atom format
    if (parsed?.feed?.entry) {
      const entries = Array.isArray(parsed.feed.entry) 
        ? parsed.feed.entry 
        : [parsed.feed.entry];
      
      for (const entry of entries) {
        const title = extractTextContent(entry.title);
        let link = '';
        
        // Handle link which can be an object or array
        if (entry.link) {
          if (Array.isArray(entry.link)) {
            const htmlLink = entry.link.find(l => l['@type'] === 'text/html' || l['@rel'] === 'alternate');
            link = htmlLink?.['@href'] || entry.link[0]?.['@href'] || '';
          } else {
            link = entry.link['@href'] || extractTextContent(entry.link);
          }
        }
        
        const description = extractTextContent(entry.summary) || extractTextContent(entry.content);
        const pubDate = extractTextContent(entry.published) || extractTextContent(entry.updated);
        
        if (title) {
          items.push({ title, link, description, pubDate, source: feedName });
        }
      }
    }
    
    // Handle RDF format (RSS 1.0)
    if (parsed?.['rdf:RDF']?.item) {
      const rdfItems = Array.isArray(parsed['rdf:RDF'].item) 
        ? parsed['rdf:RDF'].item 
        : [parsed['rdf:RDF'].item];
      
      for (const item of rdfItems) {
        const title = extractTextContent(item.title);
        const link = extractTextContent(item.link);
        const description = extractTextContent(item.description);
        const pubDate = extractTextContent(item['dc:date']);
        
        if (title) {
          items.push({ title, link, description, pubDate, source: feedName });
        }
      }
    }
  } catch (err) {
    // Fallback to regex parsing if XML parsing fails
    return parseRssFeedFallback(xmlText, feedName);
  }
  
  return items;
}

// Fallback regex-based parser for malformed XML
function parseRssFeedFallback(xmlText, feedName) {
  const items = [];
  
  const rssItemRegex = /<item>([\s\S]*?)<\/item>/gi;
  let matches = xmlText.matchAll(rssItemRegex);
  
  for (const match of matches) {
    const itemXml = match[1];
    const title = extractTagFallback(itemXml, 'title');
    const link = extractTagFallback(itemXml, 'link');
    const description = extractTagFallback(itemXml, 'description');
    const pubDate = extractTagFallback(itemXml, 'pubDate') || extractTagFallback(itemXml, 'dc:date');
    
    if (title) {
      items.push({ title, link, description, pubDate, source: feedName });
    }
  }
  
  if (items.length === 0) {
    const atomEntryRegex = /<entry>([\s\S]*?)<\/entry>/gi;
    matches = xmlText.matchAll(atomEntryRegex);
    
    for (const match of matches) {
      const entryXml = match[1];
      const title = extractTagFallback(entryXml, 'title');
      const linkMatch = entryXml.match(/<link[^>]*href=["']([^"']*)["']/);
      const link = linkMatch ? linkMatch[1] : '';
      const description = extractTagFallback(entryXml, 'summary') || extractTagFallback(entryXml, 'content');
      const pubDate = extractTagFallback(entryXml, 'published') || extractTagFallback(entryXml, 'updated');
      
      if (title) {
        items.push({ title, link, description, pubDate, source: feedName });
      }
    }
  }
  
  return items;
}

function extractTextContent(value) {
  if (!value) return '';
  if (typeof value === 'string') return value.trim().replace(/<[^>]*>/g, '');
  if (value['#text']) return String(value['#text']).trim().replace(/<[^>]*>/g, '');
  if (value['#cdata']) return String(value['#cdata']).trim().replace(/<[^>]*>/g, '');
  return '';
}

function extractTagFallback(xml, tagName) {
  const regex = new RegExp(`<${tagName}[^>]*><!\\[CDATA\\[([\\s\\S]*?)\\]\\]><\\/${tagName}>|<${tagName}[^>]*>([\\s\\S]*?)<\\/${tagName}>`, 'i');
  const match = xml.match(regex);
  if (match) {
    return (match[1] || match[2] || '').trim().replace(/<[^>]*>/g, '');
  }
  return '';
}

function isWithinDateRange(dateStr, dateRangeDays = 14) {
  if (!dateStr) return true;
  try {
    const itemDate = new Date(dateStr);
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - dateRangeDays);
    return itemDate >= cutoffDate;
  } catch {
    return true;
  }
}

// ============ PARALLEL RSS FETCH ============
async function fetchRssFeed(feed, dateRangeDays = 14) {
  const healthRecord = {
    source_name: feed.name,
    source_url: feed.url,
    source_type: "rss",
    last_check: new Date().toISOString()
  };
  
  const result = await fetchWithRetry(feed.url, {
    headers: { 'User-Agent': 'ComplianceScanner/1.0' }
  });
  
  if (result.success) {
    const xmlText = await result.response.text();
    const items = parseRssFeed(xmlText, feed.name);
    const recentItems = items.filter(item => isWithinDateRange(item.pubDate, dateRangeDays)).slice(0, 20);
    
    healthRecord.status = "healthy";
    healthRecord.last_success = healthRecord.last_check;
    healthRecord.items_fetched = recentItems.length;
    healthRecord.consecutive_failures = 0;
    healthRecord.retries_used = result.attempt;
    
    return { items: recentItems, health: healthRecord, error: null };
  } else {
    healthRecord.status = "failing";
    healthRecord.error_message = result.error;
    healthRecord.retries_used = result.attempt;
    
    return { items: [], health: healthRecord, error: `RSS ${feed.name}: ${result.error}` };
  }
}

// ============ PARALLEL SCRAPE FETCH ============
async function fetchScrapeSite(config) {
  const healthRecord = {
    source_name: config.name,
    source_url: config.url,
    source_type: "scrape",
    last_check: new Date().toISOString()
  };
  
  const result = await scrapeWebsite(config);
  
  if (result.error) {
    healthRecord.status = "failing";
    healthRecord.error_message = result.error;
    healthRecord.retries_used = result.retries;
    return { items: [], health: healthRecord, error: `Scrape ${config.name}: ${result.error}` };
  } else if (result.items.length > 0) {
    healthRecord.status = "healthy";
    healthRecord.last_success = healthRecord.last_check;
    healthRecord.items_fetched = result.items.length;
    healthRecord.consecutive_failures = 0;
    healthRecord.retries_used = result.retries;
    return { items: result.items, health: healthRecord, error: null };
  } else {
    healthRecord.status = "degraded";
    healthRecord.error_message = "No items found";
    healthRecord.retries_used = result.retries;
    return { items: [], health: healthRecord, error: null };
  }
}

// ============ CONTENT ENRICHMENT ============
async function enrichArticleContent(item) {
  if (!item.link || !item.link.startsWith('http')) return item;
  
  try {
    const result = await fetchWithRetry(item.link);
    if (!result.success) return item;
    
    const html = await result.response.text();
    const doc = new DOMParser().parseFromString(html, "text/html");
    if (!doc) return item;
    
    // 1. Extract Date (High Precision from meta tags)
    const metaDate = doc.querySelector('meta[property="article:published_time"]')?.getAttribute('content') ||
                     doc.querySelector('meta[name="date"]')?.getAttribute('content') ||
                     doc.querySelector('meta[name="pubdate"]')?.getAttribute('content') ||
                     doc.querySelector('meta[name="publish_date"]')?.getAttribute('content') ||
                     doc.querySelector('time[datetime]')?.getAttribute('datetime');
    
    if (metaDate) item.pubDate = metaDate;
    
    // 2. Extract Author
    if (!item.author) {
      const author = doc.querySelector('meta[name="author"]')?.getAttribute('content') ||
                     doc.querySelector('meta[property="article:author"]')?.getAttribute('content') ||
                     doc.querySelector('.author')?.textContent?.trim() ||
                     doc.querySelector('.byline')?.textContent?.trim() ||
                     doc.querySelector('[rel="author"]')?.textContent?.trim();
      if (author) item.author = author;
    }
    
    // 3. Extract Full Content
    // Heuristics for main content area
    const contentSelectors = [
      'article', 
      '[role="main"]', 
      '.post-content', 
      '.article-content', 
      '.entry-content', 
      '.main-content',
      '#content',
      'main'
    ];
    
    let content = '';
    // Try selectors
    for (const selector of contentSelectors) {
      const el = doc.querySelector(selector);
      // Ensure it has substantial text
      if (el && el.textContent.trim().length > 300) {
        content = el.textContent.trim();
        break;
      }
    }
    
    // Fallback: find sequence of paragraphs
    if (!content) {
      const paragraphs = Array.from(doc.querySelectorAll('p'));
      // Filter for substantial paragraphs
      const substantialParagraphs = paragraphs
        .map(p => p.textContent.trim())
        .filter(t => t.length > 60);
        
      if (substantialParagraphs.length >= 3) {
        content = substantialParagraphs.join('\n\n');
      }
    }
    
    if (content) {
      // Clean up whitespace
      item.fullContent = content.replace(/\s+/g, ' ').substring(0, 8000); // Capture up to 8k chars
      
      // Update description if it was weak
      if (!item.description || item.description.length < 150) {
        item.description = item.fullContent.substring(0, 300) + '...';
      }
    }
    
  } catch (err) {
    // Ignore enrichment errors, keep original item
    console.error(`Enrichment failed for ${item.link}:`, err.message);
  }
  return item;
}

// ============ DEDUPLICATION AGAINST EXISTING RECORDS (Optimized) ============
async function deduplicateItems(items, base44) {
  // First deduplicate within the current batch
  const seenInBatch = new Set();
  const seenUrlsInBatch = new Set();
  const batchDeduped = items.filter(item => {
    const titleKey = item.title?.toLowerCase().trim();
    const urlKey = item.link?.toLowerCase().trim();
    
    if (seenInBatch.has(titleKey)) return false;
    if (urlKey && seenUrlsInBatch.has(urlKey)) return false;
    
    seenInBatch.add(titleKey);
    if (urlKey) seenUrlsInBatch.add(urlKey);
    return true;
  });
  
  if (batchDeduped.length === 0) return [];
  
  // Extract URLs from current batch for targeted DB query
  const newUrls = batchDeduped
    .map(i => i.link?.toLowerCase().trim())
    .filter(Boolean);
  
  const newTitles = batchDeduped
    .map(i => i.title?.toLowerCase().trim())
    .filter(Boolean);
  
  // Query only for matching URLs/titles instead of fetching all records
  let existingUpdates = [];
  try {
    // Fetch recent updates (last 60 days) for title matching
    // This is more efficient than fetching ALL records
    const sixtyDaysAgo = new Date();
    sixtyDaysAgo.setDate(sixtyDaysAgo.getDate() - 60);
    
    existingUpdates = await base44.asServiceRole.entities.RegulatoryUpdate.filter({});
    
    // If we have too many records, just check the last 500
    if (existingUpdates.length > 500) {
      existingUpdates = existingUpdates.slice(0, 500);
    }
  } catch (err) {
    console.error("Failed to fetch existing updates for deduplication:", err);
    // Return batch-deduped items if DB query fails
    return batchDeduped;
  }
  
  // Build sets for O(1) lookup
  const existingTitles = new Set(
    existingUpdates.map(u => u.title?.toLowerCase().trim()).filter(Boolean)
  );
  const existingUrls = new Set(
    existingUpdates.map(u => u.source_url?.toLowerCase().trim()).filter(Boolean)
  );
  
  // Filter out items that match existing records
  return batchDeduped.filter(item => {
    const normalizedTitle = item.title?.toLowerCase().trim();
    const normalizedUrl = item.link?.toLowerCase().trim();
    
    if (existingTitles.has(normalizedTitle)) return false;
    if (normalizedUrl && existingUrls.has(normalizedUrl)) return false;
    
    return true;
  });
}

Deno.serve(async (req) => {
  const startTime = Date.now();
  
  try {
    const base44 = createClientFromRequest(req);
    
    // Parse request body for parameters
    let dateRangeDays = 14;
    let selectedSourceIds = null; // null means scan all sources
    try {
      const body = await req.json();
      if (body.dateRangeDays && body.dateRangeDays >= 1 && body.dateRangeDays <= 60) {
        dateRangeDays = body.dateRangeDays;
      }
      if (body.selectedSourceIds && Array.isArray(body.selectedSourceIds) && body.selectedSourceIds.length > 0) {
        selectedSourceIds = body.selectedSourceIds;
      }
    } catch {
      // No body or invalid JSON, use default
    }
    
    const errors = [];
    const sourceStats = { rss: 0, scraped: 0, duplicatesSkipped: 0 };
    const sourceHealthUpdates = [];

    // ============ PHASE 0: FETCH DYNAMIC SOURCES ============
    let dynamicRss = [];
    let dynamicScrape = [];
    
    try {
      // Fetch user-added sources from database
      const customSources = await base44.asServiceRole.entities.ComplianceSource.filter({ is_active: true });
      
      customSources.forEach(source => {
        if (source.type === 'rss') {
          dynamicRss.push({
            name: source.name,
            url: source.url,
            region: source.region || 'Global'
          });
        } else if (source.type === 'scrape') {
          dynamicScrape.push({
            name: source.name,
            url: source.url,
            region: source.region || 'Global',
            articleSelector: source.scrape_selector, // Optional selector
            titleSelector: source.title_selector,
            dateSelector: source.date_selector,
            descriptionSelector: source.description_selector
          });
        }
      });
    } catch (err) {
      console.error("Failed to fetch custom sources:", err);
    }

    // Combine hardcoded and dynamic sources
    let allRssFeeds = [...RSS_FEEDS, ...dynamicRss];
    let allScrapeConfigs = [...SCRAPE_CONFIGS, ...dynamicScrape];
    
    // ============ FILTER BY SELECTED SOURCES IF PROVIDED ============
    if (selectedSourceIds && selectedSourceIds.length > 0) {
      // Create a set of selected IDs for quick lookup
      const selectedSet = new Set(selectedSourceIds);
      
      // Filter static sources by their ID (which matches the id in staticSources array in ScanButton)
      const staticSourceIdMap = {
        "ftc": "FTC Press Releases",
        "ftc_consumer": "FTC Consumer Protection",
        "ftc_competition": "FTC Competition",
        "occ_news": "OCC News Releases",
        "occ_bulletins": "OCC Bulletins",
        "eu_competition": "EU Competition News",
        "scotusblog": "SCOTUSblog",
        "ipkat": "IPKat",
        "lawworkplace": "Law and the Workplace",
        "cfs_monitor": "Consumer Financial Services Law Monitor",

        "fintech_weekly": "Fintech Business Weekly",
        "law360": "Law360",
        "law360_pulse": "Law360 Pulse Legal Tech",
        "skadden": "Skadden Insights",
        "ebglaw": "EBG Law Insights",
        "jacksonlewis": "Jackson Lewis",
        "cfpb": "CFPB Newsroom",
        "fdic": "FDIC Press Releases"
      };
      
      // Get selected source names from static ID map
      const selectedStaticNames = new Set();
      for (const id of selectedSourceIds) {
        if (staticSourceIdMap[id]) {
          selectedStaticNames.add(staticSourceIdMap[id]);
        }
      }
      
      // Filter RSS feeds - include if name matches or if it's a dynamic source with matching ID
      allRssFeeds = allRssFeeds.filter(feed => {
        if (selectedStaticNames.has(feed.name)) return true;
        // Check if it's a dynamic source (has an ID that's in selectedSourceIds)
        if (selectedSet.has(feed.id)) return true;
        return false;
      });
      
      // Filter scrape configs similarly
      allScrapeConfigs = allScrapeConfigs.filter(config => {
        if (selectedStaticNames.has(config.name)) return true;
        if (selectedSet.has(config.id)) return true;
        return false;
      });
      
      console.log(`Filtered to ${allRssFeeds.length} RSS feeds and ${allScrapeConfigs.length} scrape configs based on selection`);
    }

    // ============ PHASE 1: PARALLEL FETCH ALL SOURCES ============
    const rssFetchPromises = allRssFeeds.map(feed => fetchRssFeed(feed, dateRangeDays));
    const scrapeFetchPromises = allScrapeConfigs.map(config => fetchScrapeSite(config));
    
    // Execute all fetches in parallel
    const [rssResults, scrapeResults] = await Promise.all([
      Promise.allSettled(rssFetchPromises),
      Promise.allSettled(scrapeFetchPromises)
    ]);
    
    // Collect items and health records
    let allItems = [];
    
    for (const result of rssResults) {
      if (result.status === 'fulfilled') {
        const { items, health, error } = result.value;
        allItems.push(...items);
        sourceHealthUpdates.push(health);
        sourceStats.rss += items.length;
        if (error) errors.push(error);
      } else {
        errors.push(`RSS fetch failed: ${result.reason}`);
      }
    }
    
    for (const result of scrapeResults) {
      if (result.status === 'fulfilled') {
        const { items, health, error } = result.value;
        allItems.push(...items);
        sourceHealthUpdates.push(health);
        sourceStats.scraped += items.length;
        if (error) errors.push(error);
      } else {
        errors.push(`Scrape failed: ${result.reason}`);
      }
    }

    // ============ PHASE 2: DEDUPLICATE AGAINST EXISTING RECORDS ============
    const originalCount = allItems.length;
    allItems = await deduplicateItems(allItems, base44);
    sourceStats.duplicatesSkipped = originalCount - allItems.length;

    // ============ PHASE 2.5: ENRICH NEW ITEMS WITH FULL CONTENT ============
    // Only enrich items that passed deduplication to save resources
    console.log(`Enriching ${Math.min(allItems.length, 15)} new items with full content...`);
    
    // Limit parallelism and total count to avoid timeouts and rate limits
    const itemsToEnrich = allItems.slice(0, 15); 
    const enrichPromises = itemsToEnrich.map(item => enrichArticleContent(item));
    await Promise.allSettled(enrichPromises);

    // ============ PHASE 3: UPDATE SOURCE HEALTH RECORDS ============
    const healthUpdatePromises = sourceHealthUpdates.map(async (health) => {
      try {
        const existing = await base44.asServiceRole.entities.SourceHealth.filter({ source_url: health.source_url });
        if (existing.length > 0) {
          const prev = existing[0];
          const consecutiveFailures = health.status === "failing" 
            ? (prev.consecutive_failures || 0) + 1 
            : 0;
          await base44.asServiceRole.entities.SourceHealth.update(prev.id, {
            ...health,
            consecutive_failures: consecutiveFailures
          });
        } else {
          await base44.asServiceRole.entities.SourceHealth.create(health);
        }
      } catch (err) {
        // Silently continue if health tracking fails
      }
    });
    
    await Promise.allSettled(healthUpdatePromises);
    
    if (allItems.length === 0) {
      return Response.json({ 
        success: true, 
        message: sourceStats.duplicatesSkipped > 0 
          ? `Found ${originalCount} items but all were duplicates of existing updates`
          : "No recent items found in RSS feeds",
        duplicates_skipped: sourceStats.duplicatesSkipped,
        execution_time_ms: Date.now() - startTime,
        errors 
      });
    }
    
    // ============ PHASE 4: CHUNK ITEMS FOR LLM (avoid token limit) ============
    const BATCH_SIZE = 20; // Process 20 items at a time
    const chunkArray = (arr, size) => 
      Array.from({ length: Math.ceil(arr.length / size) }, (_, i) =>
        arr.slice(i * size, i * size + size)
      );
    
    const itemBatches = chunkArray(allItems, BATCH_SIZE);
    
    // Prepare content for LLM analysis (will be done per batch if needed)
    const feedContent = allItems.slice(0, 50).map(item => // Limit to first 50 items for single batch
      `Source: ${item.source}
Title: ${item.title}
Author: ${item.author || 'N/A'}
Publish Date: ${item.pubDate || 'N/A'}
Link: ${item.link}
Content/Summary: ${(item.fullContent ? item.fullContent.substring(0, 1500) : item.description?.substring(0, 500)) || 'N/A'}`
    ).join('\n\n---\n\n');
    
    // Fetch company profile for personalized analysis
    let companyProfile = null;
    try {
      const profiles = await base44.asServiceRole.entities.CompanyProfile.filter({});
      if (profiles.length > 0) {
        companyProfile = profiles[0];
      }
    } catch (err) {
      console.log("No company profile found, using defaults");
    }

    // ============ PHASE 4.5: APPLY LEARNED RELEVANCE RULES ============
    let relevanceRules = [];
    try {
      relevanceRules = await base44.asServiceRole.entities.RelevanceRule.filter({ is_active: true });
    } catch (err) {
      console.log("No relevance rules found, proceeding without filtering");
    }

    // Pre-filter items using learned rules
    if (relevanceRules.length > 0) {
      const preFilterCount = allItems.length;
      allItems = allItems.filter(item => {
        const titleLower = item.title?.toLowerCase() || '';
        const descLower = item.description?.toLowerCase() || '';
        const sourceLower = item.source?.toLowerCase() || '';
        
        // Check exclusion rules
        for (const rule of relevanceRules) {
          const pattern = rule.pattern?.toLowerCase();
          if (!pattern) continue;
          
          // Only process exclude rules here
          if (!rule.rule_type.startsWith('exclude')) continue;
          
          let matches = false;
          
          switch (rule.rule_type) {
            case 'exclude_keyword':
              matches = titleLower.includes(pattern) || descLower.includes(pattern);
              break;
            case 'exclude_topic':
              matches = titleLower.includes(pattern) || descLower.includes(pattern);
              break;
            case 'exclude_title_pattern':
              matches = titleLower.includes(pattern);
              break;
            case 'exclude_source_pattern':
              matches = sourceLower.includes(pattern);
              break;
          }
          
          if (matches) {
            // Update rule usage stats (async, don't wait)
            base44.asServiceRole.entities.RelevanceRule.update(rule.id, {
              times_applied: (rule.times_applied || 0) + 1
            }).catch(() => {});
            return false;
          }
        }
        return true;
      });
      
      const filteredByRules = preFilterCount - allItems.length;
      if (filteredByRules > 0) {
        console.log(`Relevance rules filtered out ${filteredByRules} items`);
        sourceStats.filteredByRules = filteredByRules;
      }
    }

    // Build company context for personalized analysis
    let companyContext = "a B2B SaaS technology company headquartered in Israel, publicly traded in the U.S., serving business users globally";
    let priorityFrameworks = "";
    let priorityRiskAreas = "";
    
    if (companyProfile) {
      companyContext = `a ${companyProfile.business_model || 'B2B'} ${companyProfile.industry || 'SaaS/Software'} company`;
      if (companyProfile.company_size) companyContext += ` (${companyProfile.company_size})`;
      if (companyProfile.operating_regions?.length) {
        companyContext += ` operating in ${companyProfile.operating_regions.join(', ')}`;
      }
      if (companyProfile.uses_ai_ml) {
        companyContext += `, using AI/ML in their products`;
      }
      if (companyProfile.regulatory_frameworks?.length) {
        priorityFrameworks = `\nPRIORITY REGULATORY FRAMEWORKS: ${companyProfile.regulatory_frameworks.join(', ')}`;
      }
      if (companyProfile.key_risk_areas?.length) {
        priorityRiskAreas = `\nPRIORITY RISK AREAS: ${companyProfile.key_risk_areas.join(', ')}`;
      }
    }

    const analysisPrompt = `You are a senior legal compliance analyst for ${companyContext}.
    ${priorityFrameworks}
    ${priorityRiskAreas}

    Analyze the following RSS feed items. Your goal is to filter out NOISE and only surface items that require attention.
    ONLY include items that are DIRECTLY relevant to technology/software companies.

CATEGORIZATION FRAMEWORK:
- AI Law: AI regulation, algorithmic accountability, AI Act updates, automated decision-making rules
- Privacy: GDPR, CCPA, data protection laws, cross-border data transfer, privacy by design requirements
- Consumer Protection: ONLY tech-related FTC enforcement (dark patterns, deceptive tech practices, online advertising, data security breaches, software/app violations)
- Antitrust: Platform/tech market dominance, Big Tech investigations, data-sharing obligations
- Platform Liability: Content moderation, DSA compliance, intermediary liability, Section 230
- IP: Intellectual property, patents, trademarks, copyrights affecting tech/software companies

UPDATE TYPES: Regulatory, Enforcement, Ruling, Court Filing, Class Action

STRICT EXCLUSION LIST - DO NOT INCLUDE:
- Healthcare, pregnancy, fertility, medical devices (unless health-tech data privacy)
- Pet products, food safety, dietary supplements
- Automobiles, car dealers, vehicle safety
- Real estate, mortgages, housing
- Physical retail, brick-and-mortar stores
- Telecommunications infrastructure (unless data privacy related)
- Traditional banking (unless fintech/digital payments)
- Employment/labor law (unless gig economy platforms)
- Environmental regulations
- Physical product safety recalls
- Immigration, education loans, student services
- Debt collection (unless software-based)
- ANY consumer issue not related to software, apps, websites, or digital services

INCLUDE ONLY IF THE UPDATE IS DIRECTLY OR INDIRECTLY RELEVANT TO A SAAS/TECH COMPANY:
- Directly affects software/SaaS companies or could set precedent for them
- Involves digital platforms, apps, websites, cloud services, or software products
- Relates to data privacy, cybersecurity, AI/ML systems used in software
- Concerns online advertising, digital marketing practices, or SaaS marketing
- Involves enforcement actions AGAINST tech companies
- Affects terms of service, user agreements, or contracts for digital products
- Could impact how SaaS companies collect, process, or store user data

CRITICAL FILTER: Ask yourself - "Would ${companyProfile ? `a ${companyProfile.industry || 'SaaS'} company's` : 'a B2B SaaS company\'s'} legal/compliance team need to know about this?" If no, EXCLUDE.

USER-TRAINED EXCLUSION PATTERNS (learned from user feedback - STRICTLY EXCLUDE articles matching these patterns):
${relevanceRules.filter(r => r.rule_type.startsWith('exclude')).length > 0 
  ? relevanceRules.filter(r => r.rule_type.startsWith('exclude')).map(r => `- ${r.rule_type}: "${r.pattern}" (reason: ${r.reason || 'user marked irrelevant'})`).join('\n') 
  : 'No exclusion patterns yet.'}

USER-TRAINED INCLUSION PATTERNS (PRIORITIZE articles matching these patterns - they are highly relevant):
${relevanceRules.filter(r => r.rule_type.startsWith('include')).length > 0 
  ? relevanceRules.filter(r => r.rule_type.startsWith('include')).map(r => `- ${r.rule_type}: "${r.pattern}" (reason: ${r.reason || 'user marked relevant'})`).join('\n') 
  : 'No inclusion patterns yet.'}

RISK SCORING GUIDANCE:
- HIGH: Critical update. Requires immediate attention. Directly affects the company's core business, involves priority regulatory frameworks (GDPR, CCPA, AI Act), or has imminent deadlines.
- MEDIUM: Important. Indirectly relevant, sets precedent, or affects related industries. Worth monitoring.
- LOW: FYI only. General regulatory news or minor updates. Use sparingly to avoid clutter.

RSS FEED CONTENT:
${feedContent}

Be EXTREMELY selective. Only include items that are DIRECTLY or INDIRECTLY relevant to a B2B SaaS technology company.

IMPORTANT: For each item, extract and include the publish_date in YYYY-MM-DD format from the "Publish Date" field provided.`;

    const analysisResult = await base44.integrations.Core.InvokeLLM({
      prompt: analysisPrompt,
      add_context_from_internet: true,
      response_json_schema: {
        type: "object",
        properties: {
          updates: {
            type: "array",
            items: {
              type: "object",
              properties: {
                title: { type: "string" },
                source: { type: "string" },
                source_url: { type: "string" },
                publish_date: { type: "string", description: "Original publication date in YYYY-MM-DD format" },
                domain: { 
                  type: "string",
                  enum: ["AI Law", "Privacy", "Antitrust", "Consumer Protection", "Platform Liability", "IP"]
                },
                jurisdiction: {
                  type: "string",
                  enum: ["United States", "European Union", "United Kingdom", "Israel", "Brazil", "China", "India", "Australia", "Canada", "Global", "Germany", "Netherlands"]
                },
                risk_score: {
                  type: "string",
                  enum: ["High", "Medium", "Low"]
                },
                update_type: {
                  type: "string",
                  enum: ["Regulatory", "Enforcement", "Ruling", "Court Filing", "Class Action"]
                },
                summary: { type: "string" },
                compliance_actions: {
                  type: "array",
                  items: { type: "string" }
                },
                key_dates: {
                  type: "array",
                  items: { type: "string" }
                },
                affected_areas: {
                  type: "array",
                  items: { type: "string" }
                }
              },
              required: ["title", "source", "domain", "jurisdiction", "risk_score", "summary", "update_type", "publish_date"]
            }
          },
          weekly_summary: { type: "string" }
        },
        required: ["updates"]
      }
    });
    
    // Save updates to database with parallel LLM analysis
    const savedUpdates = [];
    const today = new Date().toISOString().split('T')[0];
    const updates = analysisResult.updates || [];
    
    // Phase 1: Run all secondary LLM analyses in parallel
    const analysisPromises = updates.map(async (update) => {
      if (update.update_type === "Ruling" || update.update_type === "Enforcement") {
        try {
          const result = await base44.integrations.Core.InvokeLLM({
            prompt: `Analyze this regulatory update to determine if it describes a Ruling with a FINAL court decision or an Enforcement action with PENALTIES or SETTLEMENTS.

UPDATE DETAILS:
Title: ${update.title}
Summary: ${update.summary}
Type: ${update.update_type}
Domain: ${update.domain}

ANALYSIS REQUIREMENTS:
1. Is this a FINAL court decision (not preliminary, not appeal pending) OR an enforcement with actual penalties/settlements?
2. Does this have DIRECT or INDIRECT implications for SaaS companies' business and operations?

If YES to both:
- Identify all involved parties (tech companies, regulators, government agencies)
- Classify the enforcement type (fine, injunction, settlement, consent decree, etc.)
- Specify the legal field (antitrust, privacy, consumer protection, etc.)
- Analyze possible implications for a typical SaaS company (like Wix, Salesforce, HubSpot)
- Provide trend analysis: Is this part of a broader regulatory pattern?

If NO to either question, set is_court_decision_or_enforcement to false.`,
            response_json_schema: {
              "type": "object",
              "properties": {
                "is_court_decision_or_enforcement": { "type": "boolean" },
                "involved_parties": { "type": "array", "items": { "type": "string" } },
                "enforcement_type": { "type": "string" },
                "legal_field": { "type": "string" },
                "possible_implications": { "type": "string" },
                "trend_analysis": { "type": "string" }
              },
              "required": ["is_court_decision_or_enforcement"]
            }
          });
          return result;
        } catch (err) {
          return null;
        }
      }
      return null;
    });
    
    // Wait for all parallel analyses to complete
    const analysisResults = await Promise.all(analysisPromises);
    
    // Re-fetch existing titles/URLs right before saving to catch any race conditions
    let existingTitlesForSave = new Set();
    let existingUrlsForSave = new Set();
    try {
      const freshExisting = await base44.asServiceRole.entities.RegulatoryUpdate.filter({});
      existingTitlesForSave = new Set(freshExisting.map(u => u.title?.toLowerCase().trim()).filter(Boolean));
      existingUrlsForSave = new Set(freshExisting.map(u => u.source_url?.toLowerCase().trim()).filter(Boolean));
    } catch (err) {
      console.error("Failed to fetch existing for final dedup:", err);
    }
    
    // Track what we save in this batch to avoid duplicates within the same run
    const savedTitlesThisBatch = new Set();
    const savedUrlsThisBatch = new Set();
    
    // Phase 2: Save all updates with their analyses (with final dedup check)
    for (let i = 0; i < updates.length; i++) {
      const update = updates[i];
      const additionalAnalysis = analysisResults[i] || {};
      
      // Find original item to ensure Source Name consistency (LLM might have altered it)
      const originalItem = allItems.find(item => 
        (item.link && update.source_url && item.link.toLowerCase().trim() === update.source_url.toLowerCase().trim()) || 
        (item.title && update.title && item.title.toLowerCase().trim() === update.title.toLowerCase().trim())
      );
      
      // Use original source name if available, otherwise fallback to LLM's output
      const correctSourceName = originalItem ? originalItem.source : update.source;

      // Final deduplication check before saving
      const normalizedTitle = update.title?.toLowerCase().trim();
      const normalizedUrl = update.source_url?.toLowerCase().trim();
      
      // Skip if already exists in DB or was saved in this batch
      if (existingTitlesForSave.has(normalizedTitle) || savedTitlesThisBatch.has(normalizedTitle)) {
        errors.push(`Skipped duplicate: ${update.title}`);
        continue;
      }
      if (normalizedUrl && (existingUrlsForSave.has(normalizedUrl) || savedUrlsThisBatch.has(normalizedUrl))) {
        errors.push(`Skipped duplicate URL: ${update.source_url}`);
        continue;
      }
      
      try {
        const saved = await base44.asServiceRole.entities.RegulatoryUpdate.create({
          title: update.title,
          source: correctSourceName,
          source_url: update.source_url || "",
          domain: update.domain,
          jurisdiction: update.jurisdiction,
          risk_score: update.risk_score,
          update_type: update.update_type || "Regulatory",
          summary: update.summary,
          compliance_actions: update.compliance_actions || [],
          key_dates: update.key_dates || [],
          affected_areas: update.affected_areas || [],
          full_analysis: `Update Type: ${update.update_type || 'N/A'}\n\n${update.summary}`,
          publish_date: update.publish_date || today,
          scan_date: today,
          status: "New",
          is_court_decision_or_enforcement: additionalAnalysis.is_court_decision_or_enforcement || false,
          involved_parties: additionalAnalysis.involved_parties || [],
          enforcement_type: additionalAnalysis.enforcement_type || "",
          legal_field: additionalAnalysis.legal_field || "",
          possible_implications: additionalAnalysis.possible_implications || "",
          trend_analysis: additionalAnalysis.trend_analysis || ""
        });
        
        // Track saved items to prevent duplicates within this batch
        savedTitlesThisBatch.add(normalizedTitle);
        if (normalizedUrl) savedUrlsThisBatch.add(normalizedUrl);
        
        savedUpdates.push(saved);
      } catch (err) {
        errors.push(`Failed to save: ${update.title} - ${err.message}`);
      }
    }
    
    return Response.json({
      success: true,
      message: `Scanned ${RSS_FEEDS.length} RSS feeds + ${SCRAPE_CONFIGS.length} websites in parallel`,
      weekly_summary: analysisResult.weekly_summary,
      updates_saved: savedUpdates.length,
      saved_updates: savedUpdates.map(u => ({
        title: u.title,
        source: u.source,
        source_url: u.source_url,
        domain: u.domain,
        risk_score: u.risk_score
      })),
      date_range_days: dateRangeDays,
      sources: {
        rss_feeds: RSS_FEEDS.length,
        scraped_sites: SCRAPE_CONFIGS.length,
        rss_items: sourceStats.rss,
        scraped_items: sourceStats.scraped,
        duplicates_skipped: sourceStats.duplicatesSkipped,
        filtered_by_rules: sourceStats.filteredByRules || 0
      },
      execution_time_ms: Date.now() - startTime,
      feed_errors: errors
    });
    
  } catch (error) {
    return Response.json({ 
      success: false, 
      error: error.message,
      execution_time_ms: Date.now() - startTime
    }, { status: 500 });
  }
});