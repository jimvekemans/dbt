//! Validation of `full_refresh_strategy` and `chunks`, shared by parsing (to
//! fail early) and execution (`dbt-tasks-sa`'s `full_refresh_swap`). See
//! `docs/full-refresh-swap.md`.

use dbt_common::{ErrorCode, FsResult, fs_err};

use crate::schemas::common::{DbtIncrementalStrategy, DbtMaterialization, DbtUniqueKey};
use crate::schemas::project::FullRefreshStrategy;
use crate::schemas::{DbtModel, InternalDbtNodeAttributes};

/// How a rebuild splits the model into chunks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwapPlan {
    /// Number of hash chunks; 1 loads the model in a single statement.
    pub chunks: u64,
    /// Output column the chunks are hashed on, for model SQL without
    /// `get_chunk_filter` predicates in its imports. `None` when `chunks > 1`
    /// relies on those predicates alone.
    pub chunk_output_by: Option<String>,
}

/// The validated `swap` configuration of `model`, or `None` when the model does
/// not use `full_refresh_strategy: swap`.
pub fn swap_plan(model: &DbtModel) -> FsResult<Option<SwapPlan>> {
    if model.deprecated_config.full_refresh_strategy != Some(FullRefreshStrategy::Swap) {
        return Ok(None);
    }
    chunk_plan(model, "`full_refresh_strategy: swap`").map(Some)
}

/// The chunks of an `in_place` rebuild of `model`, or `None` when it uses
/// `swap` or is not chunked. The first chunk replaces the relation through the
/// model's materialization; the others are inserted into it.
pub fn in_place_chunk_plan(model: &DbtModel) -> FsResult<Option<SwapPlan>> {
    let config = &model.deprecated_config;
    if config.full_refresh_strategy == Some(FullRefreshStrategy::Swap)
        || (config.chunks.unwrap_or(1) == 1 && config.chunk_output_by.is_none())
    {
        return Ok(None);
    }
    let plan = chunk_plan(model, "`chunks`")?;
    Ok((plan.chunks > 1).then_some(plan))
}

/// Validate both strategies, for parsing.
pub fn validate_rebuild_config(model: &DbtModel) -> FsResult<()> {
    swap_plan(model)?;
    in_place_chunk_plan(model)?;
    Ok(())
}

fn chunk_plan(model: &DbtModel, feature: &str) -> FsResult<SwapPlan> {
    let config = &model.deprecated_config;
    let unique_id = &model.__common_attr__.unique_id;
    let materialized = model.materialized();
    if !matches!(
        materialized,
        DbtMaterialization::Table | DbtMaterialization::Incremental
    ) {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "{feature} applies to table and incremental models; {unique_id} is materialized as {materialized}",
        ));
    }
    if model.__common_attr__.language.as_deref() == Some("python") {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "{feature} is not supported for Python model {unique_id}",
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
    if is_microbatch && (chunks > 1 || config.chunk_output_by.is_some()) {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "Microbatch model {unique_id} is rebuilt batch by batch; remove `chunks` and `chunk_output_by`",
        ));
    }
    // Without `chunk_output_by` or a single-column `unique_key`, the model SQL must
    // carry `get_chunk_filter` predicates; that is checked when it runs.
    let chunk_output_by = (chunks > 1)
        .then(|| {
            config
                .chunk_output_by
                .clone()
                .or_else(|| match &config.unique_key {
                    Some(DbtUniqueKey::Single(key)) => Some(key.clone()),
                    Some(DbtUniqueKey::Multiple(keys)) if keys.len() == 1 => Some(keys[0].clone()),
                    _ => None,
                })
        })
        .flatten();
    Ok(SwapPlan {
        chunks,
        chunk_output_by,
    })
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
    fn chunk_output_by_defaults_to_single_unique_key() {
        let mut config = swap(Some(8));
        config.unique_key = Some(DbtUniqueKey::Single("id".to_string()));
        let m = model(config, DbtMaterialization::Incremental);
        assert_eq!(
            swap_plan(&m).unwrap(),
            Some(SwapPlan {
                chunks: 8,
                chunk_output_by: Some("id".to_string())
            })
        );
    }

    #[test]
    fn chunk_output_by_defaults_to_one_element_unique_key_list() {
        let mut config = swap(Some(8));
        config.unique_key = Some(DbtUniqueKey::Multiple(vec!["id".to_string()]));
        let m = model(config, DbtMaterialization::Incremental);
        assert_eq!(
            swap_plan(&m).unwrap(),
            Some(SwapPlan {
                chunks: 8,
                chunk_output_by: Some("id".to_string())
            })
        );
    }

    #[test]
    fn chunks_without_a_column_rely_on_chunk_filters() {
        let mut config = swap(Some(8));
        config.unique_key = Some(DbtUniqueKey::Multiple(vec!["a".into(), "b".into()]));
        let m = model(config, DbtMaterialization::Incremental);
        assert_eq!(
            swap_plan(&m).unwrap(),
            Some(SwapPlan {
                chunks: 8,
                chunk_output_by: None
            })
        );
    }

    #[test]
    fn views_are_rejected() {
        let m = model(swap(None), DbtMaterialization::View);
        assert!(swap_plan(&m).is_err());
    }

    #[test]
    fn chunks_without_swap_rebuild_in_place() {
        let config = ModelConfig {
            chunks: Some(16),
            ..Default::default()
        };
        let m = model(config, DbtMaterialization::Table);
        assert_eq!(swap_plan(&m).unwrap(), None);
        assert_eq!(
            in_place_chunk_plan(&m).unwrap(),
            Some(SwapPlan {
                chunks: 16,
                chunk_output_by: None
            })
        );
    }

    #[test]
    fn swap_models_have_no_in_place_plan() {
        let m = model(swap(Some(4)), DbtMaterialization::Incremental);
        assert_eq!(in_place_chunk_plan(&m).unwrap(), None);
    }

    #[test]
    fn unchunked_in_place_models_have_no_plan() {
        let m = model(ModelConfig::default(), DbtMaterialization::View);
        assert_eq!(in_place_chunk_plan(&m).unwrap(), None);
    }

    #[test]
    fn chunked_in_place_views_are_rejected() {
        let config = ModelConfig {
            chunks: Some(4),
            ..Default::default()
        };
        let m = model(config, DbtMaterialization::View);
        assert!(in_place_chunk_plan(&m).is_err());
    }
}
