use crate::form4::parse_form4;
use chrono::Utc;
use common::{EventSender, SystemEvent};
use serde::Deserialize;
use std::collections::HashSet;
use std::time::Duration;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub struct PollerConfig {
    /// SEC requires a descriptive UA: "AppName/Version (contact@email)".
    /// Set this to something real before running against the live feed.
    pub user_agent: String,
    pub poll_interval: Duration,
    pub max_concurrent_fetches: usize,
}

impl Default for PollerConfig {
    fn default() -> Self {
        Self {
            user_agent: "insider-trade-tracker/0.1 (set-your-contact@example.com)".to_string(),
            poll_interval: Duration::from_secs(3),
            max_concurrent_fetches: 8,
        }
    }
}

const CURRENT_FEED_URL: &str =
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=include&count=100&output=atom";

#[derive(Debug, Deserialize)]
struct Feed {
    #[serde(default, rename = "entry")]
    entries: Vec<Entry>,
}

#[derive(Debug, Deserialize)]
struct Entry {
    link: Link,
}

#[derive(Debug, Deserialize)]
struct Link {
    #[serde(rename = "@href")]
    href: String,
}

#[derive(Debug, Deserialize)]
struct IndexJson {
    directory: IndexDirectory,
}

#[derive(Debug, Deserialize)]
struct IndexDirectory {
    item: Vec<IndexItem>,
}

#[derive(Debug, Deserialize)]
struct IndexItem {
    name: String,
}

/// Runs forever: poll the current-events feed, dedup, fetch + parse new
/// Form 4 filings, and push `InsiderFiling` events onto `tx`.
pub async fn poll_loop(cfg: PollerConfig, tx: EventSender) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .user_agent(cfg.user_agent.clone())
        .build()?;

    let mut seen: HashSet<String> = HashSet::new();
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(cfg.max_concurrent_fetches));

    loop {
        let started = std::time::Instant::now();
        match fetch_feed(&client).await {
            Ok(entries) => {
                let mut joins = Vec::new();
                for href in entries {
                    let Some(accession) = extract_accession(&href) else {
                        continue;
                    };
                    if !seen.insert(accession.clone()) {
                        continue;
                    }

                    let client = client.clone();
                    let tx = tx.clone();
                    let sem = semaphore.clone();
                    joins.push(tokio::spawn(async move {
                        let _permit = sem.acquire_owned().await.ok();
                        if let Err(e) = fetch_and_emit(&client, &href, &accession, &tx).await {
                            warn!(accession, error = %e, "failed to process filing");
                        }
                    }));
                }
                for j in joins {
                    let _ = j.await;
                }
            }
            Err(e) => error!(error = %e, "failed to fetch current-events feed"),
        }

        debug!(elapsed_ms = %started.elapsed().as_millis(), seen_count = seen.len(), "poll cycle complete");

        // Cap the seen-set so long-running processes don't grow unbounded.
        if seen.len() > 50_000 {
            seen.clear();
            info!("cleared dedup cache after reaching cap");
        }

        tokio::time::sleep(cfg.poll_interval).await;
    }
}

async fn fetch_feed(client: &reqwest::Client) -> anyhow::Result<Vec<String>> {
    let body = client.get(CURRENT_FEED_URL).send().await?.text().await?;
    let feed: Feed = quick_xml::de::from_str(&body)?;
    Ok(feed.entries.into_iter().map(|e| e.link.href).collect())
}

/// The current-events feed link points at an `-index.htm` page. Accession
/// numbers embed as the trailing path segment, e.g.
/// `.../000032019324000007-index.htm` -> `0000320193-24-000007`.
fn extract_accession(href: &str) -> Option<String> {
    let file = href.rsplit('/').next()?;
    let raw = file.strip_suffix("-index.htm")?;
    if raw.len() < 18 {
        return None;
    }
    Some(format!("{}-{}-{}", &raw[0..10], &raw[10..12], &raw[12..]))
}

fn filing_dir_url(href: &str) -> Option<String> {
    href.rsplit_once('/').map(|(dir, _)| dir.to_string())
}

async fn fetch_and_emit(
    client: &reqwest::Client,
    entry_href: &str,
    accession: &str,
    tx: &EventSender,
) -> anyhow::Result<()> {
    let dir = filing_dir_url(entry_href).ok_or_else(|| anyhow::anyhow!("bad entry href"))?;
    let index: IndexJson = client
        .get(format!("{dir}/index.json"))
        .send()
        .await?
        .json()
        .await?;

    // Prefer the canonical primary_doc.xml; fall back to any other .xml
    // that isn't the filing summary doc.
    let Some(xml_name) = index
        .directory
        .item
        .iter()
        .find(|i| i.name.eq_ignore_ascii_case("primary_doc.xml"))
        .or_else(|| {
            index
                .directory
                .item
                .iter()
                .find(|i| i.name.ends_with(".xml") && !i.name.eq_ignore_ascii_case("FilingSummary.xml"))
        })
        .map(|i| i.name.clone())
    else {
        debug!(accession, "no xml doc found in filing directory, skipping");
        return Ok(());
    };

    let xml = client.get(format!("{dir}/{xml_name}")).send().await?.text().await?;
    let observed_at = Utc::now();
    let txs = parse_form4(&xml, accession, observed_at)?;

    for insider_tx in txs {
        if tx.send(SystemEvent::InsiderFiling(insider_tx)).await.is_err() {
            warn!("event channel closed, stopping emit");
            break;
        }
    }
    Ok(())
}
