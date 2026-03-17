//! AIP-132 compatible pagination.
//!
//! Implements page_size + opaque page_token pagination per Google AIP-132/158.
//! The page token encodes the offset as a base64 string, making it opaque
//! to clients while remaining stateless on the server.
//!
//! # Custom Token Encoding
//!
//! By default, page tokens use URL-safe base64 encoding. For security-sensitive
//! applications, implement [`PageTokenCodec`] to add encryption, HMAC signing,
//! or other tamper-proofing to page tokens.

use base64::prelude::*;
use quiver_driver_core::{Connection, Row, Statement};
use quiver_error::QuiverError;

/// A paginated list response, compatible with AIP-132.
#[derive(Debug, Clone)]
pub struct PageResponse {
    /// The items in this page.
    pub items: Vec<Row>,
    /// Token for the next page. Empty if this is the last page.
    pub next_page_token: String,
    /// Total number of items matching the query (optional).
    /// Computed only if `include_total_size` was set.
    pub total_size: Option<i64>,
}

/// A pagination request.
#[derive(Debug, Clone)]
pub struct PageRequest {
    /// Maximum items to return. Clamped to `max_page_size`.
    pub page_size: i32,
    /// Opaque token from a previous response's `next_page_token`.
    pub page_token: String,
}

impl PageRequest {
    pub fn first_page(page_size: i32) -> Self {
        Self {
            page_size,
            page_token: String::new(),
        }
    }

    pub fn next(page_size: i32, token: &str) -> Self {
        Self {
            page_size,
            page_token: token.to_string(),
        }
    }
}

/// Configuration for paginated queries.
pub struct PaginateConfig {
    /// Default page size if client sends 0.
    pub default_page_size: i32,
    /// Maximum allowed page size. Values above this are clamped.
    pub max_page_size: i32,
    /// Whether to compute total_size (requires an extra COUNT query).
    pub include_total_size: bool,
}

impl Default for PaginateConfig {
    fn default() -> Self {
        Self {
            default_page_size: 50,
            max_page_size: 1000,
            include_total_size: false,
        }
    }
}

/// Codec for encoding/decoding page tokens.
///
/// Implement this trait to customize how page tokens are serialized. Use cases:
/// - HMAC-signed tokens to prevent offset tampering
/// - AES-encrypted tokens to hide pagination state from clients
/// - Custom serialization formats
///
/// The default implementation ([`Base64PageTokenCodec`]) uses URL-safe base64.
pub trait PageTokenCodec {
    /// Encode an offset into an opaque page token string.
    fn encode(&self, offset: u64) -> String;
    /// Decode an opaque page token string back to an offset.
    fn decode(&self, token: &str) -> Result<u64, QuiverError>;
}

/// Default page token codec using URL-safe base64 (no padding).
///
/// Token format: `base64url("quiver:v1:{offset}")`
pub struct Base64PageTokenCodec;

impl PageTokenCodec for Base64PageTokenCodec {
    fn encode(&self, offset: u64) -> String {
        let payload = format!("quiver:v1:{}", offset);
        BASE64_URL_SAFE_NO_PAD.encode(payload.as_bytes())
    }

    fn decode(&self, token: &str) -> Result<u64, QuiverError> {
        if token.is_empty() {
            return Ok(0);
        }

        let decoded = BASE64_URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| QuiverError::Validation("invalid page token".into()))?;

        let s = String::from_utf8(decoded)
            .map_err(|_| QuiverError::Validation("invalid page token encoding".into()))?;

        let offset_str = s
            .strip_prefix("quiver:v1:")
            .ok_or_else(|| QuiverError::Validation("invalid page token version".into()))?;

        offset_str
            .parse::<u64>()
            .map_err(|_| QuiverError::Validation("invalid page token offset".into()))
    }
}

/// Execute a paginated query following AIP-132/158.
///
/// Uses the default [`Base64PageTokenCodec`] for token encoding.
/// For custom token encoding, use [`paginate_with_codec`].
///
/// The `base_query` should be a SELECT without LIMIT/OFFSET -- pagination
/// adds those automatically. The `count_query` is optional and used only
/// when `config.include_total_size` is true.
pub async fn paginate(
    conn: &dyn Connection,
    base_query: &Statement,
    count_query: Option<&Statement>,
    request: &PageRequest,
    config: &PaginateConfig,
) -> Result<PageResponse, QuiverError> {
    paginate_with_codec(
        conn,
        base_query,
        count_query,
        request,
        config,
        &Base64PageTokenCodec,
    )
    .await
}

/// Execute a paginated query with a custom [`PageTokenCodec`].
///
/// This allows plugging in HMAC-signed, encrypted, or otherwise customized
/// page token encoding for security-sensitive applications.
pub async fn paginate_with_codec(
    conn: &dyn Connection,
    base_query: &Statement,
    count_query: Option<&Statement>,
    request: &PageRequest,
    config: &PaginateConfig,
    codec: &dyn PageTokenCodec,
) -> Result<PageResponse, QuiverError> {
    // Resolve page size per AIP-132 rules
    let page_size = if request.page_size <= 0 {
        config.default_page_size
    } else if request.page_size > config.max_page_size {
        config.max_page_size
    } else {
        request.page_size
    };

    // Decode offset from page token
    let offset = codec.decode(&request.page_token)?;

    // Fetch one extra row to detect if there's a next page
    let limit = page_size as u64 + 1;
    let paginated_sql = format!("{} LIMIT {} OFFSET {}", base_query.sql, limit, offset);
    let paginated_stmt = Statement::new(paginated_sql, base_query.params.clone());
    let mut rows = conn.query(&paginated_stmt).await?;

    // Determine if there are more results
    let has_next = rows.len() > page_size as usize;
    if has_next {
        rows.truncate(page_size as usize);
    }

    let next_page_token = if has_next {
        codec.encode(offset + page_size as u64)
    } else {
        String::new()
    };

    // Optionally compute total size
    let total_size = if config.include_total_size {
        if let Some(cq) = count_query {
            conn.query(cq).await?.first().and_then(|r| r.get_i64(0))
        } else {
            // Auto-generate count query by wrapping the base query
            let count_stmt = Statement::new(
                format!("SELECT COUNT(*) FROM ({})", base_query.sql),
                base_query.params.clone(),
            );
            conn.query(&count_stmt)
                .await?
                .first()
                .and_then(|r| r.get_i64(0))
        }
    } else {
        None
    };

    Ok(PageResponse {
        items: rows,
        next_page_token,
        total_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_token_roundtrip() {
        let codec = Base64PageTokenCodec;
        let token = codec.encode(50);
        let offset = codec.decode(&token).unwrap();
        assert_eq!(offset, 50);
    }

    #[test]
    fn empty_token_returns_zero() {
        let codec = Base64PageTokenCodec;
        assert_eq!(codec.decode("").unwrap(), 0);
    }

    #[test]
    fn invalid_token_returns_error() {
        let codec = Base64PageTokenCodec;
        assert!(codec.decode("garbage").is_err());
    }

    #[test]
    fn page_token_various_offsets() {
        let codec = Base64PageTokenCodec;
        for offset in [0, 1, 10, 100, 999, 10000] {
            let token = codec.encode(offset);
            assert_eq!(codec.decode(&token).unwrap(), offset);
        }
    }

    #[test]
    fn page_token_is_url_safe_base64() {
        let codec = Base64PageTokenCodec;
        let token = codec.encode(12345);
        // URL-safe base64: only alphanumeric, '-', '_' (no '+', '/', '=')
        assert!(
            token
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "token contains non-URL-safe characters: {}",
            token
        );
        assert!(!token.contains('+'));
        assert!(!token.contains('/'));
        assert!(!token.contains('='));
    }

    #[test]
    fn custom_codec_roundtrip() {
        // A trivial custom codec that reverses the base64 string
        struct ReversedCodec;
        impl PageTokenCodec for ReversedCodec {
            fn encode(&self, offset: u64) -> String {
                let base = Base64PageTokenCodec.encode(offset);
                base.chars().rev().collect()
            }
            fn decode(&self, token: &str) -> Result<u64, QuiverError> {
                let reversed: String = token.chars().rev().collect();
                Base64PageTokenCodec.decode(&reversed)
            }
        }

        let codec = ReversedCodec;
        for offset in [0, 1, 50, 999, 100000] {
            let token = codec.encode(offset);
            assert_eq!(codec.decode(&token).unwrap(), offset);
            // Default codec should NOT be able to decode reversed tokens
            assert!(Base64PageTokenCodec.decode(&token).is_err());
        }
    }
}
