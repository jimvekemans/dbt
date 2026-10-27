//! `full_refresh_strategy: swap`: rebuild a model into a shadow relation,
//! optionally in hash chunks, and swap it into place only once every chunk has
//! loaded. Readers keep seeing the old relation until the swap; a failed
//! rebuild leaves the target untouched.
//!
//! dbt drives the rebuild; the SQL that differs per warehouse or per project
//! goes through dispatched macros (`make_intermediate_relation`,
//! `make_backup_relation`, `get_chunk_predicate`, `swap_relations`), so
//! projects override it the usual way. See `docs/full-refresh-swap.md`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dbt_adapter::cast_util::downcast_value_to_dyn_base_relation;
use dbt_adapter::relation::create_relation_from_node;
use dbt_adapter::response::AdapterResponse;
use dbt_adapter_core::AdapterType;
use dbt_common::io_args::IoArgs;
use dbt_common::{ErrorCode, FsError, FsResult, fs_err};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::phases::run::build_run_node_context;
use dbt_schemas::materialization_resolver::MaterializationResolver;
use dbt_schemas::schemas::common::DbtMaterialization;
pub use dbt_schemas::schemas::full_refresh_swap::{SwapPlan, swap_plan};
#[cfg(test)]
use dbt_schemas::schemas::project::FullRefreshStrategy;
use dbt_schemas::schemas::{DbtModel, InternalDbtNode, InternalDbtNodeAttributes, NodePathKind};
use dbt_schemas::state::DbtRuntimeConfig;
use dbt_telemetry::ExecutionPhase;
use dbt_yaml::Verbatim;
use minijinja::Value;
use tracing::{debug, warn};

use crate::materialize::{NodeHookPhase, execute_node_hooks, materialize_model, model_hook_style};

/// Whether a `swap` model is rebuilt on this run. Table models always are;
/// incremental models when a full refresh is in effect or the target does not
/// exist yet. Runs on a worker thread: it may query the warehouse.
pub fn rebuilds_from_scratch(model: &DbtModel, full_refresh: bool, jinja_env: &JinjaEnv) -> bool {
    if model.materialized() != DbtMaterialization::Incremental || full_refresh {
        return true;
    }
    !target_exists(model, jinja_env)
}

fn target_exists(model: &DbtModel, jinja_env: &JinjaEnv) -> bool {
    let Some(adapter) = jinja_env.get_base_adapter() else {
        return false;
    };
    let relation = match create_relation_from_node(model.node_adapter(), model, None) {
        Ok(relation) => relation,
        Err(e) => {
            warn!("Failed to create relation for {}: {e}", model.unique_id());
            return false;
        }
    };
    let state = jinja_env.new_state_with_context(BTreeMap::new());
    adapter
        .get_relation(
            &state,
            relation.database().unwrap_or_default(),
            relation.schema().unwrap_or_default(),
            relation.identifier().unwrap_or_default(),
            false,
        )
        .is_ok_and(|relation| !relation.is_none())
}

/// What a swap rebuild needs from the run: the same inputs `materialize_model`
/// takes.
pub struct SwapEnv<'a> {
    pub adapter_type: AdapterType,
    pub runtime_config: &'a DbtRuntimeConfig,
    pub materialization_resolver: &'a Arc<MaterializationResolver>,
    pub jinja_env: Arc<JinjaEnv>,
    pub base_context: &'a BTreeMap<String, Value>,
    pub io_args: &'a IoArgs,
}

/// A rebuild in progress: the target, its shadow and the backup name, plus a
/// run context of the real target to evaluate macros in.
pub struct ShadowBuild {
    context: BTreeMap<String, Value>,
    run_path: PathBuf,
    /// The model's own alias, for messages.
    alias: String,
    /// The model as it builds into the shadow relation, without hooks.
    pub shadow_model: Arc<DbtModel>,
    /// Whether grants are applied to the target after the swap instead of to
    /// the shadow; see [`grants_follow_rename`].
    grants_after_swap: bool,
}

const TARGET: &str = "__dbt_swap_target";
const SHADOW: &str = "__dbt_swap_shadow";
const BACKUP: &str = "__dbt_swap_backup";

impl ShadowBuild {
    /// Name the shadow and backup relations, drop a shadow left behind by a
    /// failed run, and run the model's pre-hooks against the real target.
    pub fn begin(model: &DbtModel, sql: Option<&str>, env: &SwapEnv<'_>) -> FsResult<Self> {
        let (mut context, _result_store) = build_run_node_context(
            model,
            &model.deprecated_config,
            env.adapter_type,
            None,
            env.base_context,
            env.io_args,
            ExecutionPhase::Run,
            None,
            env.runtime_config.dependencies.keys().cloned().collect(),
        );
        let run_path = model
            .get_node_path(
                NodePathKind::Executable,
                &env.io_args.in_dir,
                &env.io_args.out_dir,
            )
            .into_owned();
        // Typed like the materializations type it: dropping and renaming need it.
        let target = eval(
            &env.jinja_env,
            &context,
            &run_path,
            "this.incorporate(type='table')",
        )?;
        context.insert(TARGET.to_string(), target);
        let shadow = eval(
            &env.jinja_env,
            &context,
            &run_path,
            &format!("make_intermediate_relation({TARGET})"),
        )?;
        let backup = eval(
            &env.jinja_env,
            &context,
            &run_path,
            &format!("make_backup_relation({TARGET}, 'table')"),
        )?;
        let shadow_identifier = downcast_value_to_dyn_base_relation(&shadow)
            .map_err(|e| fs_err!(ErrorCode::Unexpected, "Invalid shadow relation: {e}"))?
            .identifier()
            .unwrap_or_default()
            .to_string();
        context.insert(SHADOW.to_string(), shadow);
        context.insert(BACKUP.to_string(), backup);

        let grants_after_swap = !grants_follow_rename(env.adapter_type);
        let build = Self {
            shadow_model: Arc::new(shadow_model(model, &shadow_identifier, grants_after_swap)),
            grants_after_swap,
            alias: model.__base_attr__.alias.clone(),
            context,
            run_path,
        };
        build.drop_shadow(&env.jinja_env)?;
        debug!(
            "Rebuilding {} into {shadow_identifier} before swapping it in",
            model.unique_id()
        );
        run_hooks(model, sql, env, NodeHookPhase::Pre)?;
        Ok(build)
    }

    /// Load the model into the shadow relation, chunk by chunk. The first chunk
    /// goes through the model's own materialization, so every adapter-specific
    /// DDL option, contract and `persist_docs` (and grants, where they survive
    /// the rename) applies to the shadow; the remaining chunks are inserted
    /// into it.
    pub fn load(
        &self,
        sql: &str,
        plan: &SwapPlan,
        sql_header: Option<Value>,
        env: &SwapEnv<'_>,
    ) -> FsResult<Option<AdapterResponse>> {
        let sql = sql.trim().trim_end_matches(';');
        // Chunk inserts run outside the materialization, so they prepend the
        // model's `sql_header` themselves.
        let header = sql_header
            .as_ref()
            .filter(|header| !header.is_none())
            .map(|header| format!("{header}\n"))
            .unwrap_or_default();
        let (_, response) = materialize_model(
            &self.chunk_sql(sql, plan, 0, &env.jinja_env)?,
            self.shadow_model.clone(),
            env.adapter_type,
            env.runtime_config,
            env.materialization_resolver,
            env.jinja_env.clone(),
            env.base_context,
            env.io_args,
            sql_header,
        )
        .map_err(|e| self.relabel(e, env))?;
        if plan.chunks == 1 {
            return Ok(response);
        }

        let columns = eval(
            &env.jinja_env,
            &self.context,
            &self.run_path,
            &format!("adapter.get_columns_in_relation({SHADOW}) | map(attribute='quoted') | list"),
        )?
        .try_iter()
        .map_err(|e| fs_err!(ErrorCode::Unexpected, "Cannot list shadow columns: {e}"))?
        .map(|column| column.to_string())
        .collect::<Vec<_>>()
        .join(", ");
        let shadow = self.context[SHADOW].to_string();
        for chunk in 1..plan.chunks {
            let insert = format!(
                "{header}insert into {shadow} ({columns})\nselect {columns} from (\n{}\n) as dbt_chunk",
                self.chunk_sql(sql, plan, chunk, &env.jinja_env)?
            );
            debug!("Loading chunk {}/{} of {shadow}", chunk + 1, plan.chunks);
            self.execute(&env.jinja_env, &insert)?;
        }
        Ok(response)
    }

    /// Swap the shadow into place, apply grants and run the post-hooks against
    /// the real target. Returns the materialization result for the target.
    pub fn finish(
        &self,
        model: &DbtModel,
        sql: Option<&str>,
        env: &SwapEnv<'_>,
    ) -> FsResult<Value> {
        eval(
            &env.jinja_env,
            &self.context,
            &self.run_path,
            &format!("swap_relations({TARGET}, {SHADOW}, {BACKUP})"),
        )?;
        if self.grants_after_swap {
            // The target name keeps the grants of whatever carried it before,
            // so always diff against them.
            eval(
                &env.jinja_env,
                &self.context,
                &self.run_path,
                &format!("apply_grants({TARGET}, config.get('grants'), should_revoke=True)"),
            )?;
        }
        run_hooks(model, sql, env, NodeHookPhase::Post)?;
        eval(
            &env.jinja_env,
            &self.context,
            &self.run_path,
            &format!("{{'relations': [{TARGET}]}}"),
        )
    }

    /// Errors from the shadow's materialization name the shadow (its alias and
    /// run file); point them at the model the user knows instead.
    fn relabel(&self, mut error: Box<FsError>, env: &SwapEnv<'_>) -> Box<FsError> {
        let shadow_path = self.shadow_model.get_node_path(
            NodePathKind::Executable,
            &env.io_args.in_dir,
            &env.io_args.out_dir,
        );
        let shadow_label = format!(
            "in model {} ({})",
            self.shadow_model.__base_attr__.alias,
            shadow_path.display()
        );
        let model_label = format!("in model {} ({})", self.alias, self.run_path.display());
        error.context = error.context.replacen(&shadow_label, &model_label, 1);
        // The rendered Jinja stack ends in "(in <run dir>/<shadow>.sql)".
        let shadow_alias = &self.shadow_model.__base_attr__.alias;
        error.context = error.context.replace(
            &format!("/{shadow_alias}.sql"),
            &format!("/{}.sql", self.alias),
        );
        error
    }

    /// Drop the shadow after a failed rebuild. The target was never touched.
    pub fn abort(&self, jinja_env: &JinjaEnv) {
        if let Err(e) = self.drop_shadow(jinja_env) {
            warn!(
                "Failed to drop {} after a failed rebuild: {e}",
                self.context[SHADOW]
            );
        }
    }

    fn drop_shadow(&self, jinja_env: &JinjaEnv) -> FsResult<()> {
        eval(
            jinja_env,
            &self.context,
            &self.run_path,
            &format!("adapter.drop_relation({SHADOW})"),
        )
        .map(|_| ())
    }

    /// The model SQL restricted to one chunk.
    fn chunk_sql(
        &self,
        sql: &str,
        plan: &SwapPlan,
        chunk: u64,
        jinja_env: &JinjaEnv,
    ) -> FsResult<String> {
        let Some(column) = &plan.chunk_by else {
            return Ok(sql.to_string());
        };
        let mut context = self.context.clone();
        context.insert(
            "__dbt_swap_column".to_string(),
            Value::from(column.as_str()),
        );
        let predicate = eval(
            jinja_env,
            &context,
            &self.run_path,
            &format!(
                "get_chunk_predicate(__dbt_swap_column, {}, {chunk})",
                plan.chunks
            ),
        )?;
        Ok(format!(
            "select * from (\n{sql}\n) as dbt_chunk_source\nwhere {predicate}"
        ))
    }

    fn execute(&self, jinja_env: &JinjaEnv, sql: &str) -> FsResult<()> {
        let mut context = self.context.clone();
        context.insert("__dbt_swap_sql".to_string(), Value::from(sql));
        eval(
            jinja_env,
            &context,
            &self.run_path,
            "adapter.execute(__dbt_swap_sql, auto_begin=True)",
        )?;
        eval(jinja_env, &context, &self.run_path, "adapter.commit()").map(|_| ())
    }
}

/// Whether a renamed relation keeps its grants. Where it does, grants go on the
/// shadow and readers keep access across the swap. SingleStore keys privileges
/// by table name: grants on the shadow would stay behind on its old name, and
/// the target name keeps the grants of the relation swapped out.
fn grants_follow_rename(adapter_type: AdapterType) -> bool {
    !matches!(adapter_type, AdapterType::SingleStore)
}

/// A copy of `model` that materializes into `shadow_identifier`. Hooks are
/// removed, and grants when `strip_grants`: they run once, against the real
/// target, around the swap.
pub fn shadow_model(model: &DbtModel, shadow_identifier: &str, strip_grants: bool) -> DbtModel {
    let mut shadow = model.clone();
    shadow.__base_attr__.alias = shadow_identifier.to_string();
    shadow.deprecated_config.pre_hook = Verbatim::from(None);
    shadow.deprecated_config.post_hook = Verbatim::from(None);
    if strip_grants {
        shadow.deprecated_config.grants = Default::default();
    }
    shadow
}

fn run_hooks(
    model: &DbtModel,
    sql: Option<&str>,
    env: &SwapEnv<'_>,
    phase: NodeHookPhase,
) -> FsResult<()> {
    execute_node_hooks(
        model,
        &model.deprecated_config,
        env.adapter_type,
        env.runtime_config,
        env.jinja_env.clone(),
        env.base_context,
        env.io_args,
        sql,
        model_hook_style(env.adapter_type, &model.__base_attr__.materialized),
        NodePathKind::Executable,
        phase,
    )
}

fn eval(
    jinja_env: &JinjaEnv,
    context: &BTreeMap<String, Value>,
    run_path: &Path,
    expr: &str,
) -> FsResult<Value> {
    jinja_env
        .compile_expression(expr)?
        .eval_with_file_only_stack_frame(context, &[], run_path)
}

/// Rebuild a non-microbatch `swap` model: shadow, chunks, swap.
#[allow(clippy::too_many_arguments)]
pub fn materialize_model_swap(
    sql: &str,
    model: &DbtModel,
    plan: &SwapPlan,
    sql_header: Option<Value>,
    env: &SwapEnv<'_>,
) -> FsResult<(Value, Option<AdapterResponse>)> {
    let build = ShadowBuild::begin(model, Some(sql), env)?;
    let response = build
        .load(sql, plan, sql_header, env)
        .inspect_err(|_| build.abort(&env.jinja_env))?;
    let relations = build
        .finish(model, Some(sql), env)
        .inspect_err(|_| build.abort(&env.jinja_env))?;
    Ok((relations, response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::schemas::project::ModelConfig;

    #[test]
    fn shadow_model_drops_hooks_and_renames() {
        let mut model = DbtModel {
            deprecated_config: ModelConfig {
                full_refresh_strategy: Some(FullRefreshStrategy::Swap),
                ..Default::default()
            },
            ..Default::default()
        };
        model.__common_attr__.unique_id = "model.p.m".to_string();
        let shadow = shadow_model(&model, "m__dbt_tmp", true);
        assert_eq!(shadow.__base_attr__.alias, "m__dbt_tmp");
        assert_eq!(shadow.__common_attr__.unique_id, "model.p.m");
    }

    #[test]
    fn grants_stay_on_the_shadow_unless_keyed_by_name() {
        assert!(grants_follow_rename(AdapterType::Postgres));
        assert!(grants_follow_rename(AdapterType::Snowflake));
        assert!(!grants_follow_rename(AdapterType::SingleStore));
    }
}
