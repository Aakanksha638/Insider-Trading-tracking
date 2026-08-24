use crate::state::{InsiderEvent, SymbolState};
use chrono::Duration as ChronoDuration;
use common::{
    Direction, EventReceiver, EventSender, InsiderTx, Signal, SignalStrength, SystemEvent,
    TransactionCode,
};
use std::collections::HashMap;
use tracing::{debug, info};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Window over which we count distinct insiders trading the same
    /// direction to detect "cluster buying/selling".
    pub cluster_window: ChronoDuration,
    /// Minimum notional (shares * price) for an open-market transaction to
    /// be considered at all — filters out payroll-scale noise.
    pub min_notional: f64,
    /// Notional above which a single filing alone is enough for a Strong signal.
    pub strong_notional: f64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            cluster_window: ChronoDuration::days(14),
            min_notional: 25_000.0,
            strong_notional: 1_000_000.0,
        }
    }
}

/// Consumes InsiderFiling + Tick events from `rx`, maintains per-symbol
/// state, and emits `SystemEvent::Signal` onto `out` whenever a filing
/// clears the scoring bar.
pub async fn run(cfg: EngineConfig, mut rx: EventReceiver, out: EventSender) -> anyhow::Result<()> {
    let mut symbols: HashMap<String, SymbolState> = HashMap::new();

    while let Some(event) = rx.recv().await {
        match event {
            SystemEvent::Tick(tick) => {
                symbols.entry(tick.symbol.clone()).or_default().push_tick(tick);
            }
            SystemEvent::InsiderFiling(tx) => {
                if let Some(signal) = score_filing(&cfg, &mut symbols, &tx) {
                    info!(%signal, "emitting signal");
                    if out.send(SystemEvent::Signal(signal)).await.is_err() {
                        break;
                    }
                } else {
                    debug!(
                        symbol = %tx.issuer_symbol,
                        code = ?tx.code,
                        "filing did not clear scoring bar"
                    );
                }
            }
            SystemEvent::Signal(_) => { /* not consumed here */ }
        }
    }
    Ok(())
}

fn score_filing(
    cfg: &EngineConfig,
    symbols: &mut HashMap<String, SymbolState>,
    tx: &InsiderTx,
) -> Option<Signal> {
    // Only open-market purchases/sales carry directional conviction; grants,
    // option exercises, and tax-withholding dispositions are comp mechanics.
    if !tx.code.is_open_market_signal() {
        return None;
    }
    let notional = tx.notional()?;
    if notional < cfg.min_notional {
        return None;
    }

    let direction = match tx.code {
        TransactionCode::Purchase => Direction::Buy,
        TransactionCode::Sale => Direction::Sell,
        _ => unreachable!("filtered by is_open_market_signal"),
    };

    let state = symbols.entry(tx.issuer_symbol.clone()).or_default();
    state.push_insider_event(
        InsiderEvent {
            filer_cik: tx.filer_cik.clone(),
            direction,
            at: tx.filed_at,
        },
        cfg.cluster_window,
    );
    let cluster = state.cluster_count(direction);

    // --- scoring ---
    // Base weight from insider role (officers/directors/10%-owners are
    // presumed to have the most information content).
    let mut score = 0u32;
    if tx.is_officer {
        score += 2;
    }
    if tx.is_director {
        score += 1;
    }
    if tx.is_ten_pct_owner {
        score += 1;
    }
    if notional >= cfg.strong_notional {
        score += 3;
    } else {
        score += 1;
    }
    // Cluster buying/selling (multiple distinct insiders, same direction,
    // same window) is the strongest known signal in the public literature
    // on Form 4-based strategies — weight it heavily.
    if cluster >= 3 {
        score += 4;
    } else if cluster == 2 {
        score += 2;
    }

    let strength = match score {
        0..=2 => SignalStrength::Weak,
        3..=5 => SignalStrength::Moderate,
        _ => SignalStrength::Strong,
    };

    let reason = format!(
        "{:?} by {} ({}{}{}), {:.0} shares @ {:?} = ${:.0} notional, {} distinct insider(s) {:?} in window",
        tx.code,
        tx.filer_name,
        if tx.is_officer { "officer " } else { "" },
        if tx.is_director { "director " } else { "" },
        if tx.is_ten_pct_owner { "10%-owner " } else { "" },
        tx.shares,
        tx.price_per_share,
        notional,
        cluster,
        direction,
    );

    Some(Signal {
        symbol: tx.issuer_symbol.clone(),
        direction,
        strength,
        reason,
        generated_at: chrono::Utc::now(),
        source_accession: tx.accession_number.clone(),
    })
}
