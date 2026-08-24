use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// SEC Form 4 transaction codes we care about (subset — see Table I/II of Form 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionCode {
    /// Open market or private purchase
    Purchase,
    /// Open market or private sale
    Sale,
    /// Grant, award, or other acquisition from the company
    Grant,
    /// Option exercise
    Exercise,
    /// Disposition to the issuer (e.g. tax withholding)
    DispositionToIssuer,
    Other(char),
}

impl TransactionCode {
    pub fn from_form4_code(c: char) -> Self {
        match c {
            'P' => Self::Purchase,
            'S' => Self::Sale,
            'A' => Self::Grant,
            'M' => Self::Exercise,
            'F' => Self::DispositionToIssuer,
            other => Self::Other(other),
        }
    }

    /// Whether this code represents genuine open-market conviction
    /// (as opposed to comp-related grants/withholding, which are noisy).
    pub fn is_open_market_signal(&self) -> bool {
        matches!(self, Self::Purchase | Self::Sale)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Buy,
    Sell,
}

/// A single reportable insider transaction, normalized from a Form 4 filing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsiderTx {
    pub accession_number: String,
    pub issuer_symbol: String,
    pub issuer_cik: String,
    pub filer_name: String,
    pub filer_cik: String,
    pub is_officer: bool,
    pub is_director: bool,
    pub is_ten_pct_owner: bool,
    pub code: TransactionCode,
    pub shares: f64,
    pub price_per_share: Option<f64>,
    pub shares_owned_after: f64,
    pub transaction_date: DateTime<Utc>,
    /// When we observed the filing on EDGAR — this is the latency-critical timestamp.
    pub filed_at: DateTime<Utc>,
}

impl InsiderTx {
    pub fn notional(&self) -> Option<f64> {
        self.price_per_share.map(|p| p * self.shares)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TickSide {
    Bid,
    Ask,
    Trade,
}

/// A single market data event. Kept minimal/Copy-friendly for the hot path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketTick {
    pub symbol: String,
    pub price: f64,
    pub size: f64,
    pub side: TickSide,
    /// Exchange/venue timestamp if available, else receipt time.
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SignalStrength {
    Weak,
    Moderate,
    Strong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub symbol: String,
    pub direction: Direction,
    pub strength: SignalStrength,
    pub reason: String,
    pub generated_at: DateTime<Utc>,
    /// Accession number of the triggering filing, for auditability.
    pub source_accession: String,
}

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{:?}/{:?}] {} — {}",
            self.direction, self.strength, self.symbol, self.reason
        )
    }
}
