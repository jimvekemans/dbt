//! SQL for splitting a model's output into deterministic hash chunks, used by
//! the `full_refresh_strategy: swap` rebuild to load a large model in pieces.

use dbt_adapter_core::AdapterType;

/// A non-negative bucket number in `0..chunks` for `column`, or `None` when the
/// backend has no default hash expression (projects then override the
/// `get_chunk_predicate` macro).
///
/// The hash must be deterministic across statements: every chunk re-runs the
/// model SQL, and each row has to land in exactly one chunk.
pub fn chunk_bucket_expr(adapter_type: AdapterType, column: &str, chunks: u64) -> Option<String> {
    use AdapterType::*;
    let expr = match adapter_type {
        // Signed 64-bit hashes: take `abs` after `mod`, which cannot overflow.
        Snowflake => format!("abs(mod(hash({column}), {chunks}))"),
        Bigquery => format!("abs(mod(farm_fingerprint(cast({column} as string)), {chunks}))"),
        Postgres => format!("abs(mod(hashtext(cast({column} as text)), {chunks}))"),
        Redshift => format!("abs(mod(fnv_hash({column}), {chunks}))"),
        Databricks | Spark => format!("pmod(xxhash64({column}), {chunks})"),
        Trino | Starburst | Athena => format!(
            "abs(mod(from_big_endian_64(xxhash64(to_utf8(cast({column} as varchar)))), {chunks}))"
        ),
        Fabric => format!("abs(checksum({column}) % {chunks})"),
        // Unsigned hashes.
        DuckDB => format!("hash({column}) % {chunks}"),
        ClickHouse => format!("cityHash64({column}) % {chunks}"),
        SingleStore => format!("crc32({column}) % {chunks}"),
        Salesforce | Exasol | Datafusion | Dremio | Oracle | LakeCompute => return None,
    };
    Some(expr)
}

/// Range of [`row_hash_expr`]: hashes are in `0..2^32`, so the sum over a
/// relation fits a `numeric(28, 6)` for up to about 10^12 rows.
pub const ROW_HASH_RANGE: u64 = 1 << 32;

/// A non-negative integer hash of `expr` in `0..ROW_HASH_RANGE`, for the row
/// fingerprints that verify chunked rebuilds. The same per-backend hash as the
/// chunk buckets, or `None` where that has no default.
pub fn row_hash_expr(adapter_type: AdapterType, expr: &str) -> Option<String> {
    chunk_bucket_expr(adapter_type, expr, ROW_HASH_RANGE)
}

/// Boolean SQL selecting the rows of chunk `chunk` (0-based) out of `chunks`.
/// Rows whose `column` is NULL belong to chunk 0 and are excluded from every
/// other chunk explicitly: some backends hash NULL to NULL (the row would be
/// lost), others to a number (DuckDB; the row would load twice).
pub fn chunk_predicate(
    adapter_type: AdapterType,
    column: &str,
    chunks: u64,
    chunk: u64,
) -> Option<String> {
    let bucket = chunk_bucket_expr(adapter_type, column, chunks)?;
    Some(if chunk == 0 {
        format!("({column} is null or {bucket} = 0)")
    } else {
        format!("({column} is not null and {bucket} = {chunk})")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_keys_go_to_chunk_zero() {
        assert_eq!(
            chunk_predicate(AdapterType::DuckDB, "id", 4, 0).unwrap(),
            "(id is null or hash(id) % 4 = 0)"
        );
        assert_eq!(
            chunk_predicate(AdapterType::DuckDB, "id", 4, 3).unwrap(),
            "(id is not null and hash(id) % 4 = 3)"
        );
    }

    #[test]
    fn singlestore_uses_crc32() {
        assert_eq!(
            chunk_predicate(AdapterType::SingleStore, "`hkey`", 16, 5).unwrap(),
            "(`hkey` is not null and crc32(`hkey`) % 16 = 5)"
        );
    }

    #[test]
    fn unsupported_backends_have_no_default() {
        assert!(chunk_predicate(AdapterType::Exasol, "id", 2, 0).is_none());
        assert!(row_hash_expr(AdapterType::Exasol, "id").is_none());
    }

    #[test]
    fn row_hashes_are_non_negative_32_bit() {
        assert_eq!(
            row_hash_expr(AdapterType::SingleStore, "r").unwrap(),
            "crc32(r) % 4294967296"
        );
        assert_eq!(
            row_hash_expr(AdapterType::Snowflake, "r").unwrap(),
            "abs(mod(hash(r), 4294967296))"
        );
    }
}
