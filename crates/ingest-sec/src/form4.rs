use chrono::{NaiveDate, TimeZone, Utc};
use common::{InsiderTx, TransactionCode};
use serde::Deserialize;

// --- Raw XML shape (subset of the SEC ownershipDocument schema) ---------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OwnershipDocument {
    issuer: Issuer,
    #[serde(rename = "reportingOwner")]
    reporting_owner: ReportingOwner,
    #[serde(default, rename = "nonDerivativeTable")]
    non_derivative_table: Option<NonDerivativeTable>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Issuer {
    issuer_cik: String,
    #[serde(default)]
    issuer_trading_symbol: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReportingOwner {
    reporting_owner_id: ReportingOwnerId,
    #[serde(default)]
    reporting_owner_relationship: Option<ReportingOwnerRelationship>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReportingOwnerId {
    rpt_owner_cik: String,
    rpt_owner_name: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ReportingOwnerRelationship {
    #[serde(default)]
    is_director: FlagVal,
    #[serde(default)]
    is_officer: FlagVal,
    #[serde(default)]
    is_ten_percent_owner: FlagVal,
}

/// SEC represents booleans inconsistently as "0"/"1" text nodes; parse leniently.
#[derive(Debug, Deserialize, Default)]
struct FlagVal(#[serde(rename = "$text", default)] String);
impl FlagVal {
    fn is_true(&self) -> bool {
        matches!(self.0.trim(), "1" | "true")
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NonDerivativeTable {
    #[serde(default, rename = "nonDerivativeTransaction")]
    transactions: Vec<NonDerivativeTransaction>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NonDerivativeTransaction {
    transaction_date: ValueWrap<String>,
    transaction_coding: TransactionCoding,
    transaction_amounts: TransactionAmounts,
    #[serde(default)]
    post_transaction_amounts: Option<PostTransactionAmounts>,
}

#[derive(Debug, Deserialize)]
struct ValueWrap<T> {
    value: T,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransactionCoding {
    transaction_code: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransactionAmounts {
    transaction_shares: ValueWrap<f64>,
    #[serde(default)]
    transaction_price_per_share: Option<ValueWrap<f64>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PostTransactionAmounts {
    shares_owned_following_transaction: ValueWrap<f64>,
}

// --- Public parse entry point --------------------------------------------

/// Parse a raw Form 4 XML body into zero or more normalized `InsiderTx`.
/// `accession_number` and `observed_at` come from the feed layer, since
/// they aren't part of the filing body itself.
pub fn parse_form4(
    xml: &str,
    accession_number: &str,
    observed_at: chrono::DateTime<Utc>,
) -> anyhow::Result<Vec<InsiderTx>> {
    let doc: OwnershipDocument = quick_xml::de::from_str(xml)?;

    let rel = doc
        .reporting_owner
        .reporting_owner_relationship
        .unwrap_or_default();

    let Some(table) = doc.non_derivative_table else {
        return Ok(vec![]);
    };

    let mut out = Vec::with_capacity(table.transactions.len());
    for tx in table.transactions {
        let transaction_date = NaiveDate::parse_from_str(&tx.transaction_date.value, "%Y-%m-%d")
            .map(|d| Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).unwrap()))
            .unwrap_or(observed_at);

        let code = tx
            .transaction_coding
            .transaction_code
            .chars()
            .next()
            .map(TransactionCode::from_form4_code)
            .unwrap_or(TransactionCode::Other('?'));

        out.push(InsiderTx {
            accession_number: accession_number.to_string(),
            issuer_symbol: doc.issuer.issuer_trading_symbol.clone(),
            issuer_cik: doc.issuer.issuer_cik.clone(),
            filer_name: doc.reporting_owner.reporting_owner_id.rpt_owner_name.clone(),
            filer_cik: doc.reporting_owner.reporting_owner_id.rpt_owner_cik.clone(),
            is_officer: rel.is_officer.is_true(),
            is_director: rel.is_director.is_true(),
            is_ten_pct_owner: rel.is_ten_percent_owner.is_true(),
            code,
            shares: tx.transaction_amounts.transaction_shares.value,
            price_per_share: tx
                .transaction_amounts
                .transaction_price_per_share
                .map(|v| v.value),
            shares_owned_after: tx
                .post_transaction_amounts
                .map(|p| p.shares_owned_following_transaction.value)
                .unwrap_or(0.0),
            transaction_date,
            filed_at: observed_at,
        });
    }

    Ok(out)
}
