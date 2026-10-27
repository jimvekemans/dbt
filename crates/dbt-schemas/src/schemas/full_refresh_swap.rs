//! Validation of `full_refresh_strategy: swap` and its `chunks`, shared by
//! parsing (to fail early) and execution (`dbt-tasks-sa`'s
//! `full_refresh_swap`). See `docs/full-refresh-swap.md`.

use dbt_common::{ErrorCode, FsResult, fs_err};

use crate::schemas::common::{DbtIncrementalStrategy, DbtMaterialization, DbtUniqueKey};
use crate::schemas::project::FullRefreshStrategy;
use crate::schemas::{DbtModel, InternalDbtNodeAttributes};

/// How a `swap` rebuild splits the model's output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwapPlan {
    /// Number of hash chunks; 1 loads the model in a single statement.
    pub chunks: u64,
    /// Output column the chunks are hashed on, when `chunks > 1`.
    pub chunk_by: Option<String>,
}

/// The validated `swap` configuration of `model`, or `None` when the model does
/// not use `full_refresh_strategy: swap`.
pub fn swap_plan(model: &DbtModel) -> FsResult<Option<SwapPlan>> {
    let config = &model.deprecated_config;
    let unique_id = &model.__common_attr__.unique_id;
    if config.full_refresh_strategy != Some(FullRefreshStrategy::Swap) {
        if config.chunks.is_some_and(|chunks| chunks != 1) || config.chunk_by.is_some() {
            return Err(fs_err!(
                ErrorCode::InvalidConfig,
                "`chunks` and `chunk_by` require `full_refresh_strategy: swap` on {unique_id}",
            ));
        }
        return Ok(None);
    }
    let materialized = model.materialized();
    if !matches!(
        materialized,
        DbtMaterialization::Table | DbtMaterialization::Incremental
    ) {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "`full_refresh_strategy: swap` applies to table and incremental models; {unique_id} is materialized as {materialized}",
        ));
    }
    if model.__common_attr__.language.as_deref() == Some("python") {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "`full_refresh_strategy: swap` is not supported for Python model {unique_id}",
        ));
    }

    let chunks = config.chunks.unwrap_or(1);
    if chunks == 0 {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "`chunks` must be at least 1 for {unique_id}",
        ));
    }
    let is_microbatch =
        model.__model_attr__.incremental_strategy == Some(DbtIncrementalStrategy::Microbatch);
    if is_microbatch && (chunks > 1 || config.chunk_by.is_some()) {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "Microbatch model {unique_id} is rebuilt batch by batch; remove `chunks` and `chunk_by`",
        ));
    }
    let chunk_by = if chunks > 1 {
        let column = config
            .chunk_by
            .clone()
            .or_else(|| match &config.unique_key {
                Some(DbtUniqueKey::Single(key)) => Some(key.clone()),
                Some(DbtUniqueKey::Multiple(keys)) if keys.len() == 1 => Some(keys[0].clone()),
                _ => None,
            });
        Some(column.ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "Set `chunk_by` for {unique_id}: `chunks: {chunks}` needs a column to hash, and `unique_key` is not a single column",
            )
        })?)
    } else {
        None
    };
    Ok(Some(SwapPlan { chunks, chunk_by }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::project::ModelConfig;

    fn model(config: ModelConfig, materialized: DbtMaterialization) -> DbtModel {
        let mut model = DbtModel {
            deprecated_config: config,
            ..Default::default()
        };
        model.__common_attr__.unique_id = "model.p.m".to_string();
        model.__base_attr__.materialized = materialized;
        model
    }

    fn swap(chunks: Option<u64>) -> ModelConfig {
        ModelConfig {
            full_refresh_strategy: Some(FullRefreshStrategy::Swap),
            chunks,
            ..Default::default()
        }
    }

    #[test]
    fn in_place_is_not_planned() {
        let m = model(ModelConfig::default(), DbtMaterialization::Table);
        assert_eq!(swap_plan(&m).unwrap(), None);
    }

    #[test]
    fn chunk_by_defaults_to_single_unique_key() {
        let mut config = swap(Some(8));
        config.unique_key = Some(DbtUniqueKey::Single("id".to_string()));
        let m = model(config, DbtMaterialization::Incremental);
        assert_eq!(
            swap_plan(&m).unwrap(),
            Some(SwapPlan {
                chunks: 8,
                chunk_by: Some("id".to_string())
            })
        );
    }

    #[test]
    fn chunk_by_defaults_to_one_element_unique_key_list() {
        let mut config = swap(Some(8));
        config.unique_key = Some(DbtUniqueKey::Multiple(vec!["id".to_string()]));
        let m = model(config, DbtMaterialization::Incremental);
        assert_eq!(
            swap_plan(&m).unwrap(),
            Some(SwapPlan {
                chunks: 8,
                chunk_by: Some("id".to_string())
            })
        );
    }

    #[test]
    fn chunks_without_a_column_is_an_error() {
        let mut config = swap(Some(8));
        config.unique_key = Some(DbtUniqueKey::Multiple(vec!["a".into(), "b".into()]));
        let m = model(config, DbtMaterialization::Incremental);
        assert!(swap_plan(&m).is_err());
    }

    #[test]
    fn views_are_rejected() {
        let m = model(swap(None), DbtMaterialization::View);
        assert!(swap_plan(&m).is_err());
    }

    #[test]
    fn chunks_without_swap_is_an_error() {
        let config = ModelConfig {
            chunks: Some(16),
            ..Default::default()
        };
        let m = model(config, DbtMaterialization::Table);
        assert!(swap_plan(&m).is_err());
    }
}
