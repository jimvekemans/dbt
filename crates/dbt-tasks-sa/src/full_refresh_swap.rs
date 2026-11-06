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

use dbt_adapter::Adapter;
use dbt_adapter::cast_util::downcast_value_to_dyn_base_relation;
use dbt_adapter::relation::create_relation_from_node;
use dbt_adapter::response::AdapterResponse;
use dbt_adapter_core::{AdapterType, grants_follow_rename};
use dbt_common::io_args::IoArgs;
use dbt_common::tracing::dbt_emit::{
    emit_debug_log_message, emit_info_log_message, emit_warn_log_message,
};
use dbt_common::{ErrorCode, FsError, FsResult, fs_err};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::phases::run::build_run_node_context;
use dbt_schemas::materialization_resolver::MaterializationResolver;
use dbt_schemas::schemas::common::DbtMaterialization;
pub use dbt_schemas::schemas::full_refresh_swap::{SwapPlan, in_place_chunk_plan, swap_plan};
#[cfg(test)]
use dbt_schemas::schemas::project::FullRefreshStrategy;
use dbt_schemas::schemas::{DbtModel, InternalDbtNode, InternalDbtNodeAttributes, NodePathKind};
use dbt_schemas::state::DbtRuntimeConfig;
use dbt_telemetry::ExecutionPhase;
use dbt_yaml::Verbatim;
use minijinja::Value;

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
            emit_warn_log_message(
                ErrorCode::SwapTargetRelationCreateFailed,
                format!("Failed to create relation for {}: {e}", model.unique_id()),
            );
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
    pub adapter: &'a Adapter,
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
    shadow_run_path: PathBuf,
    /// The model's own alias, for messages.
    alias: String,
    /// The model as it builds into the shadow relation, without hooks.
    pub shadow_model: Arc<DbtModel>,
    /// Whether grants are applied to the target after the swap instead of to
    /// the shadow; see [`grants_follow_rename`].
    grants_after_swap: bool,
    /// Whether the target relation existed in the database before the rebuild started.
    target_existed_at_start: bool,
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
            .map_err(|e| {
                fs_err!(
                    ErrorCode::MacroUnsupportedValueType,
                    "Invalid shadow relation: {e}"
                )
            })?
            .identifier()
            .unwrap_or_default()
            .to_string();
        context.insert(SHADOW.to_string(), shadow);
        context.insert(BACKUP.to_string(), backup);

        let grants_after_swap = !grants_follow_rename(env.adapter_type);
        let target_eval_result = eval(
            &env.jinja_env,
            &context,
            &run_path,
            &format!("load_cached_relation({TARGET}) is not none"),
        );
        let confirmed_target_exists = match &target_eval_result {
            Ok(v) => v.is_true(),
            Err(e) => {
                emit_warn_log_message(
                    ErrorCode::SwapTargetCheckFailed,
                    format!(
                        "Failed to check if target exists for {}: {e}",
                        model.unique_id()
                    ),
                );
                false
            }
        };
        let target_existed_at_start = target_eval_result.map(|v| v.is_true()).unwrap_or(true);
        let shadow_model = Arc::new(shadow_model(model, &shadow_identifier, grants_after_swap));
        let shadow_run_path = shadow_model.get_node_path_abs(
            NodePathKind::Executable,
            &env.io_args.in_dir,
            &env.io_args.out_dir,
        );
        let build = Self {
            shadow_model,
            grants_after_swap,
            target_existed_at_start,
            alias: model.__base_attr__.alias.clone(),
            context,
            run_path,
            shadow_run_path,
        };
        build.drop_shadow(&env.jinja_env)?;
        if confirmed_target_exists {
            build.drop_backup(&env.jinja_env)?;
        }
        emit_debug_log_message(format!(
            "Rebuilding {} into {shadow_identifier} before swapping it in",
            model.unique_id()
        ));
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
        report_chunk(0, plan.chunks, &self.context, SHADOW);
        let (_, response) = materialize_model(
            &chunk_sql(&self.context, &self.run_path, sql, plan, 0, &env.jinja_env)?,
            self.shadow_model.clone(),
            env.adapter,
            env.runtime_config,
            env.materialization_resolver,
            env.jinja_env.clone(),
            env.base_context,
            env.io_args,
            sql_header,
        )
        .map_err(|e| self.relabel(e, env))?;
        insert_remaining_chunks(
            &self.context,
            &self.run_path,
            SHADOW,
            sql,
            plan,
            &header,
            &env.jinja_env,
        )?;
        Ok(response)
    }

    /// The live and shadow relations, rendered as they appear in SQL.
    pub fn relation_names(&self) -> (String, String) {
        (
            self.context[TARGET].to_string(),
            self.context[SHADOW].to_string(),
        )
    }

    /// Compare the loaded shadow with the unchunked model SQL when
    /// `verify_chunked_rebuilds` is on.
    pub fn verify(
        &self,
        sql: &str,
        plan: &SwapPlan,
        model: &DbtModel,
        env: &SwapEnv<'_>,
    ) -> FsResult<()> {
        verify_chunks(
            &self.context,
            &self.run_path,
            SHADOW,
            sql.trim().trim_end_matches(';'),
            plan,
            &env.jinja_env,
            &model.__common_attr__.unique_id,
        )
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
        let model_run_path = model.get_node_path_abs(
            NodePathKind::Executable,
            &env.io_args.in_dir,
            &env.io_args.out_dir,
        );
        let _ = std::fs::rename(&self.shadow_run_path, &model_run_path);
        eval(
            &env.jinja_env,
            &self.context,
            &self.run_path,
            &format!("{{'relations': [{TARGET}]}}"),
        )
    }

    /// Errors from the shadow's materialization name the shadow (its alias and
    /// run file); point them at the model the user knows instead.
    pub fn relabel(&self, mut error: Box<FsError>, env: &SwapEnv<'_>) -> Box<FsError> {
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

    /// Drop the shadow after a failed rebuild. The target was restored or never
    /// touched; preserves shadow if restore failed.
    /// If the target existed before the rebuild and the backup was not restored
    /// (or the target was dropped and not restored), preserve the shadow.
    pub fn abort(&self, jinja_env: &JinjaEnv) {
        let _ = std::fs::remove_file(&self.shadow_run_path);
        let should_drop = if self.target_existed_at_start {
            eval(
                jinja_env,
                &self.context,
                &self.run_path,
                &format!(
                    "load_cached_relation({BACKUP}) is none and load_cached_relation({TARGET}) is not none"
                ),
            )
            .map(|v| v.is_true())
            .unwrap_or(false)
        } else {
            true
        };

        if should_drop {
            if let Err(e) = self.drop_shadow(jinja_env) {
                emit_warn_log_message(
                    ErrorCode::SwapShadowDropFailed,
                    format!(
                        "Failed to drop {} after a failed rebuild: {e}",
                        self.context[SHADOW]
                    ),
                );
            }
        } else {
            emit_warn_log_message(
                ErrorCode::SwapShadowPreserved,
                format!(
                    "Preserving shadow {} after failed swap because target {} was not restored or backup {} remains",
                    self.context[SHADOW], self.context[TARGET], self.context[BACKUP]
                ),
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

    fn drop_backup(&self, jinja_env: &JinjaEnv) -> FsResult<()> {
        eval(
            jinja_env,
            &self.context,
            &self.run_path,
            &format!("adapter.drop_relation({BACKUP})"),
        )
        .map(|_| ())
    }
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

/// Opens and closes the tag `get_chunk_filter` puts after each chunk predicate:
/// `<predicate> /* dbt_chunk: <column> */`.
const CHUNK_FILTER_TAG_OPEN: &str = "/* dbt_chunk: ";
const CHUNK_FILTER_TAG_CLOSE: &str = " */";

/// The columns of the `get_chunk_filter` predicates in `sql`, in order of first
/// appearance.
fn chunk_filter_columns(sql: &str) -> Vec<&str> {
    let mut columns: Vec<&str> = Vec::new();
    let mut rest = sql;
    while let Some(start) = rest.find(CHUNK_FILTER_TAG_OPEN) {
        rest = &rest[start + CHUNK_FILTER_TAG_OPEN.len()..];
        let Some(end) = rest.find(CHUNK_FILTER_TAG_CLOSE) else {
            break;
        };
        let column = &rest[..end];
        if !columns.contains(&column) {
            columns.push(column);
        }
        rest = &rest[end + CHUNK_FILTER_TAG_CLOSE.len()..];
    }
    columns
}

fn chunk_filter(predicate: &str, column: &str) -> String {
    format!("{predicate}{CHUNK_FILTER_TAG_OPEN}{column}{CHUNK_FILTER_TAG_CLOSE}")
}

/// What `rewrite_chunk_filters` turns the chunk-0 predicates into.
#[derive(Debug, Clone, Copy)]
enum ChunkTarget {
    /// The predicates of this chunk.
    Chunk(u64),
    /// No filtering: the whole model, to verify a chunked rebuild against.
    All,
}

/// `sql` with every chunk-0 `get_chunk_filter` predicate replaced as `target`
/// says. `predicate(column, chunk)` renders `get_chunk_predicate`; every tag must
/// carry its chunk-0 predicate, so hand-written filters are rejected.
fn rewrite_chunk_filters(
    sql: &str,
    target: ChunkTarget,
    mut predicate: impl FnMut(&str, u64) -> FsResult<String>,
) -> FsResult<String> {
    let mut rewritten = sql.to_string();
    for column in chunk_filter_columns(sql) {
        let tag = format!("{CHUNK_FILTER_TAG_OPEN}{column}{CHUNK_FILTER_TAG_CLOSE}");
        let first = chunk_filter(&format!("{} ", predicate(column, 0)?), column);
        let tags = sql.matches(&tag).count();
        let filters = sql.matches(&first).count();
        if filters != tags {
            return Err(fs_err!(
                ErrorCode::InvalidConfig,
                "{} of the {tags} chunk filters on `{column}` do not read `{}`: render them with `get_chunk_filter`",
                tags - filters,
                first.trim_end_matches(&tag).trim_end(),
            ));
        }
        let replacement = match target {
            ChunkTarget::Chunk(0) => continue,
            ChunkTarget::Chunk(chunk) => predicate(column, chunk)?,
            ChunkTarget::All => "(1 = 1)".to_string(),
        };
        rewritten = rewritten.replace(&first, &chunk_filter(&format!("{replacement} "), column));
    }
    Ok(rewritten)
}

/// `get_chunk_predicate(column, chunks, chunk)`, rendered in `context`.
fn chunk_predicate(
    context: &BTreeMap<String, Value>,
    run_path: &Path,
    jinja_env: &JinjaEnv,
    chunks: u64,
    column: &str,
    chunk: u64,
) -> FsResult<String> {
    let mut context = context.clone();
    context.insert("__dbt_swap_column".to_string(), Value::from(column));
    Ok(eval(
        jinja_env,
        &context,
        run_path,
        &format!("get_chunk_predicate(__dbt_swap_column, {chunks}, {chunk})"),
    )?
    .to_string())
}

/// The model SQL restricted to one chunk: through its `get_chunk_filter`
/// predicates when it has them, so every chunk only reads its share of the
/// inputs; otherwise by splitting the model's output on `chunk_output_by`.
fn chunk_sql(
    context: &BTreeMap<String, Value>,
    run_path: &Path,
    sql: &str,
    plan: &SwapPlan,
    chunk: u64,
    jinja_env: &JinjaEnv,
) -> FsResult<String> {
    if plan.chunks == 1 {
        return Ok(sql.to_string());
    }
    let predicate = |column: &str, chunk: u64| {
        chunk_predicate(context, run_path, jinja_env, plan.chunks, column, chunk)
    };
    if !chunk_filter_columns(sql).is_empty() {
        return rewrite_chunk_filters(sql, ChunkTarget::Chunk(chunk), predicate);
    }
    let Some(column) = &plan.chunk_output_by else {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "`chunks: {}` needs `get_chunk_filter` predicates in the model SQL, `chunk_output_by`, or a single-column `unique_key`",
            plan.chunks
        ));
    };
    let predicate = predicate(column, chunk)?;
    Ok(format!(
        "select * from (\n{sql}\n) as dbt_chunk_source\nwhere {predicate}"
    ))
}

/// Warn when a chunked model falls back to splitting its output on its
/// `unique_key` because its SQL has no `get_chunk_filter` predicates: every
/// chunk then reads all of its inputs. An explicit `chunk_output_by` is a
/// choice and is not warned about.
fn warn_if_output_filtered(model: &DbtModel, sql: &str, plan: &SwapPlan) {
    if plan.chunks == 1
        || model.deprecated_config.chunk_output_by.is_some()
        || !chunk_filter_columns(sql).is_empty()
    {
        return;
    }
    if let Some(column) = &plan.chunk_output_by {
        emit_warn_log_message(
            ErrorCode::ChunkedRebuildFiltersOutput,
            format!(
                "{} is rebuilt in {} chunks by splitting its output on `{column}`, so every chunk reads all of its inputs. Filter the inputs instead with `get_chunk_filter` (`chunk_by` in `import_dependencies`), or set `chunk_output_by: {column}` to keep this.",
                &model.__common_attr__.unique_id, plan.chunks
            ),
        );
    }
}

fn report_chunk(chunk: u64, chunks: u64, context: &BTreeMap<String, Value>, relation_var: &str) {
    if chunks > 1 {
        let target = &context[TARGET];
        if relation_var == TARGET {
            emit_info_log_message(format!("Loading chunk {}/{chunks} of {target}", chunk + 1));
        } else {
            let shadow = &context[relation_var];
            emit_info_log_message(format!(
                "Loading chunk {}/{chunks} of {target} (into {shadow})",
                chunk + 1
            ));
        }
    }
}

/// With `verify_chunked_rebuilds` on, compare the relation bound to
/// `relation_var` with the model SQL run without its chunk filters, and fail
/// when they differ. Only models with `get_chunk_filter` predicates are
/// checked: splitting the output is correct by construction.
fn verify_chunks(
    context: &BTreeMap<String, Value>,
    run_path: &Path,
    relation_var: &str,
    sql: &str,
    plan: &SwapPlan,
    jinja_env: &JinjaEnv,
    unique_id: &str,
) -> FsResult<()> {
    if plan.chunks == 1
        || chunk_filter_columns(sql).is_empty()
        || !dbt_flags::VERIFY_CHUNKED_REBUILDS.enabled()
    {
        return Ok(());
    }
    let unchunked = rewrite_chunk_filters(sql, ChunkTarget::All, |column, chunk| {
        chunk_predicate(context, run_path, jinja_env, plan.chunks, column, chunk)
    })?;
    let relation = context[relation_var].to_string();
    let query = format!(
        "select chunked.n as chunked_rows, unchunked.n as unchunked_rows, only_chunked.n as only_chunked_rows, only_unchunked.n as only_unchunked_rows from
(select count(*) as n from {relation}) as chunked,
(select count(*) as n from (\n{unchunked}\n) as dbt_unchunked) as unchunked,
(select count(*) as n from (select * from {relation} except select * from (\n{unchunked}\n) as dbt_unchunked) as d) as only_chunked,
(select count(*) as n from (select * from (\n{unchunked}\n) as dbt_unchunked except select * from {relation}) as d) as only_unchunked"
    );
    let mut context = context.clone();
    context.insert("__dbt_swap_sql".to_string(), Value::from(query));
    let row = eval(
        jinja_env,
        &context,
        run_path,
        "run_query(__dbt_swap_sql).rows[0]",
    )?;
    let counts = row
        .try_iter()
        .map_err(|e| {
            fs_err!(
                ErrorCode::MacroUnsupportedValueType,
                "Cannot read the chunk verification result: {e}"
            )
        })?
        .map(|value| value.to_string().parse::<u64>().unwrap_or(u64::MAX))
        .collect::<Vec<_>>();
    let [chunked, unchunked, only_chunked, only_unchunked] = counts[..] else {
        return Err(fs_err!(
            ErrorCode::MacroUnsupportedValueType,
            "Unexpected chunk verification result: {counts:?}"
        ));
    };
    if chunked != unchunked || only_chunked != 0 || only_unchunked != 0 {
        return Err(fs_err!(
            ErrorCode::ChunkVerificationFailed,
            "The chunked rebuild of {unique_id} differs from its SQL without chunk filters: {chunked} rows chunked, {unchunked} unchunked; {only_chunked} distinct rows only in the chunked result, {only_unchunked} only in the unchunked one. Check that every `chunk_by` / `get_chunk_filter` names the column that holds the same key in its input (the column in the join), that grouping and window keys match it, and that the output is deterministic (no current_timestamp() or random())."
        ));
    }
    if relation_var == TARGET {
        emit_info_log_message(format!(
            "Verified {relation}: {chunked} rows, the same as without chunk filters"
        ));
    } else {
        let target = &context[TARGET];
        emit_info_log_message(format!(
            "Verified {target} (in {relation}): {chunked} rows, the same as without chunk filters"
        ));
    }
    Ok(())
}

/// `sql` with every whole occurrence of the relation `from` replaced by `to`,
/// and how many there were. An occurrence that continues an identifier on
/// either side (`schema.orders` in `schema.orders_v2`) is not the relation.
pub(crate) fn replace_relation(sql: &str, from: &str, to: &str) -> (String, usize) {
    let continues = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
    let mut out = String::with_capacity(sql.len());
    let mut count = 0;
    let mut rest = sql;
    let mut before: Option<char> = None;
    while let Some(pos) = rest.find(from) {
        let preceding = rest[..pos].chars().next_back().or(before);
        let following = rest[pos + from.len()..].chars().next();
        out.push_str(&rest[..pos]);
        if preceding.is_some_and(|c| continues(c) || c == '.') || following.is_some_and(continues) {
            out.push_str(from);
        } else {
            out.push_str(to);
            count += 1;
        }
        before = from.chars().next_back();
        rest = &rest[pos + from.len()..];
    }
    out.push_str(rest);
    (out, count)
}

/// Insert chunks `1..plan.chunks` of `sql` into the relation bound to
/// `relation_var` in `context`, which chunk 0 created. Each chunk commits on
/// its own.
fn insert_remaining_chunks(
    context: &BTreeMap<String, Value>,
    run_path: &Path,
    relation_var: &str,
    sql: &str,
    plan: &SwapPlan,
    header: &str,
    jinja_env: &JinjaEnv,
) -> FsResult<()> {
    if plan.chunks == 1 {
        return Ok(());
    }
    let columns = eval(
        jinja_env,
        context,
        run_path,
        &format!(
            "adapter.get_columns_in_relation({relation_var}) | map(attribute='quoted') | list"
        ),
    )?
    .try_iter()
    .map_err(|e| {
        fs_err!(
            ErrorCode::MacroUnsupportedValueType,
            "Cannot list the columns of {}: {e}",
            context[relation_var]
        )
    })?
    .map(|column| column.to_string())
    .collect::<Vec<_>>()
    .join(", ");
    let relation = context[relation_var].to_string();
    for chunk in 1..plan.chunks {
        let insert = format!(
            "{header}insert into {relation} ({columns})\nselect {columns} from (\n{}\n) as dbt_chunk",
            chunk_sql(context, run_path, sql, plan, chunk, jinja_env)?
        );
        report_chunk(chunk, plan.chunks, context, relation_var);
        execute(jinja_env, context, run_path, &insert)?;
    }
    Ok(())
}

fn execute(
    jinja_env: &JinjaEnv,
    context: &BTreeMap<String, Value>,
    run_path: &Path,
    sql: &str,
) -> FsResult<()> {
    let mut context = context.clone();
    context.insert("__dbt_swap_sql".to_string(), Value::from(sql));
    eval(
        jinja_env,
        &context,
        run_path,
        "adapter.execute(__dbt_swap_sql, auto_begin=True)",
    )?;
    eval(jinja_env, &context, run_path, "adapter.commit()").map(|_| ())
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

/// Rebuild a non-microbatch `swap` model: shadow, chunks, swap. `before_swap`
/// runs once the shadow is complete (and verified, with
/// `verify_chunked_rebuilds`); an error from it keeps the live relation.
pub fn materialize_model_swap(
    sql: &str,
    model: &DbtModel,
    plan: &SwapPlan,
    sql_header: Option<Value>,
    env: &SwapEnv<'_>,
    before_swap: &dyn Fn(&ShadowBuild) -> FsResult<()>,
) -> FsResult<(Value, Option<AdapterResponse>)> {
    warn_if_output_filtered(model, sql, plan);
    let build = ShadowBuild::begin(model, Some(sql), env)?;
    let response = build
        .load(sql, plan, sql_header, env)
        .and_then(|response| {
            build.verify(sql, plan, model, env)?;
            before_swap(&build)?;
            Ok(response)
        })
        .inspect_err(|_| build.abort(&env.jinja_env))?;
    let relations = build
        .finish(model, Some(sql), env)
        .inspect_err(|_| build.abort(&env.jinja_env))?;
    Ok((relations, response))
}

/// Rebuild a chunked `in_place` model: chunk 0 replaces the relation through
/// the model's own materialization (pre-hooks, DDL configs, contracts, grants),
/// the other chunks are inserted into it, then the post-hooks run. Readers see
/// a partial relation until the last chunk is in. When a later chunk (or the
/// verification) fails, the partial relation is dropped, so the next run
/// rebuilds it instead of updating it incrementally.
pub fn materialize_model_chunked_in_place(
    sql: &str,
    model: &DbtModel,
    plan: &SwapPlan,
    sql_header: Option<Value>,
    env: &SwapEnv<'_>,
) -> FsResult<(Value, Option<AdapterResponse>)> {
    let sql = sql.trim().trim_end_matches(';');
    let header = sql_header
        .as_ref()
        .filter(|header| !header.is_none())
        .map(|header| format!("{header}\n"))
        .unwrap_or_default();
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
    let target = eval(
        &env.jinja_env,
        &context,
        &run_path,
        "this.incorporate(type='table')",
    )?;
    context.insert(TARGET.to_string(), target);

    warn_if_output_filtered(model, sql, plan);
    report_chunk(0, plan.chunks, &context, TARGET);
    // Post-hooks run once, after the last chunk.
    let mut first_chunk_model = model.clone();
    first_chunk_model.deprecated_config.post_hook = Verbatim::from(None);
    let (relations, response) = materialize_model(
        &chunk_sql(&context, &run_path, sql, plan, 0, &env.jinja_env)?,
        Arc::new(first_chunk_model),
        env.adapter,
        env.runtime_config,
        env.materialization_resolver,
        env.jinja_env.clone(),
        env.base_context,
        env.io_args,
        sql_header,
    )?;
    let loaded = insert_remaining_chunks(
        &context,
        &run_path,
        TARGET,
        sql,
        plan,
        &header,
        &env.jinja_env,
    )
    .and_then(|_| {
        verify_chunks(
            &context,
            &run_path,
            TARGET,
            sql,
            plan,
            &env.jinja_env,
            &model.__common_attr__.unique_id,
        )
    });
    if let Err(error) = loaded {
        let target = &context[TARGET];
        match eval(
            &env.jinja_env,
            &context,
            &run_path,
            &format!("adapter.drop_relation({TARGET})"),
        ) {
            Ok(_) => emit_warn_log_message(
                ErrorCode::ChunkedRebuildPartialDropped,
                format!(
                    "Dropped {target}: its chunked rebuild failed after the first chunk, and a partial table must not be updated incrementally. The next run rebuilds it from scratch."
                ),
            ),
            Err(drop_error) => emit_warn_log_message(
                ErrorCode::ChunkedRebuildPartialDropFailed,
                format!(
                    "{target} holds a partial chunked rebuild and could not be dropped ({drop_error}). Rebuild it with --full-refresh before the next incremental run."
                ),
            ),
        }
        return Err(error);
    }
    run_hooks(model, Some(sql), env, NodeHookPhase::Post)?;
    Ok((relations, response))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crc32_predicate(column: &str, chunk: u64) -> FsResult<String> {
        Ok(if chunk == 0 {
            format!("({column} is null or crc32({column}) % 4 = 0)")
        } else {
            format!("({column} is not null and crc32({column}) % 4 = {chunk})")
        })
    }

    const CHUNKED_SQL: &str = "with o as (select * from orders where (customer_id is null or crc32(customer_id) % 4 = 0) /* dbt_chunk: customer_id */),
c as (select * from customers where (customer_id is null or crc32(customer_id) % 4 = 0) /* dbt_chunk: customer_id */),
s as (select * from stores)
select * from o join c using (customer_id) join s using (store_id)";

    #[test]
    fn chunk_filter_columns_are_found_once_each() {
        assert_eq!(chunk_filter_columns(CHUNKED_SQL), vec!["customer_id"]);
        assert!(chunk_filter_columns("select 1").is_empty());
    }

    #[test]
    fn chunk_filters_are_rewritten_in_place() {
        let rewritten =
            rewrite_chunk_filters(CHUNKED_SQL, ChunkTarget::Chunk(3), crc32_predicate).unwrap();
        assert_eq!(
            rewritten.matches("(customer_id is not null and crc32(customer_id) % 4 = 3) /* dbt_chunk: customer_id */").count(),
            2
        );
        assert!(!rewritten.contains("is null or"));
        assert!(rewritten.contains("s as (select * from stores)"));
        assert!(!rewritten.contains("dbt_chunk_source"));
    }

    #[test]
    fn chunk_zero_is_left_as_compiled() {
        assert_eq!(
            rewrite_chunk_filters(CHUNKED_SQL, ChunkTarget::Chunk(0), crc32_predicate).unwrap(),
            CHUNKED_SQL
        );
    }

    #[test]
    fn hand_written_chunk_filters_are_rejected() {
        let sql = "select * from t where crc32(k) % 8 = 0 /* dbt_chunk: k */";
        assert!(rewrite_chunk_filters(sql, ChunkTarget::Chunk(1), crc32_predicate).is_err());
        assert!(rewrite_chunk_filters(sql, ChunkTarget::All, crc32_predicate).is_err());
    }

    #[test]
    fn unchunked_sql_has_no_filters_left() {
        let unchunked =
            rewrite_chunk_filters(CHUNKED_SQL, ChunkTarget::All, crc32_predicate).unwrap();
        assert_eq!(
            unchunked
                .matches("(1 = 1) /* dbt_chunk: customer_id */")
                .count(),
            2
        );
        assert!(!unchunked.contains("crc32"));
    }

    #[test]
    fn relations_are_replaced_whole() {
        let sql = "select * from `db`.`orders` o join `db`.`orders_v2` v using (id)";
        assert_eq!(
            replace_relation(sql, "`db`.`orders`", "`db`.`orders__dbt_tmp_1`"),
            (
                "select * from `db`.`orders__dbt_tmp_1` o join `db`.`orders_v2` v using (id)"
                    .to_string(),
                1
            )
        );
        let unquoted = "select * from db.orders, db.orders_v2, x.db.orders where db.orders.id = 1";
        assert_eq!(
            replace_relation(unquoted, "db.orders", "db.shadow"),
            (
                "select * from db.shadow, db.orders_v2, x.db.orders where db.shadow.id = 1"
                    .to_string(),
                2
            )
        );
        assert_eq!(
            replace_relation("select 1", "db.orders", "x"),
            ("select 1".to_string(), 0)
        );
    }
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

    #[test]
    fn relabel_rewrites_shadow_name_and_path() {
        let mut model = DbtModel {
            deprecated_config: ModelConfig {
                full_refresh_strategy: Some(FullRefreshStrategy::Swap),
                ..Default::default()
            },
            ..Default::default()
        };
        model.__common_attr__.unique_id = "model.p.my_model".to_string();
        model.__base_attr__.alias = "my_model".to_string();
        let shadow_build = ShadowBuild {
            shadow_model: Arc::new(shadow_model(&model, "my_model__dbt_tmp", false)),
            grants_after_swap: true,
            target_existed_at_start: true,
            alias: "my_model".to_string(),
            context: BTreeMap::new(),
            run_path: PathBuf::from("target/run/p/models/my_model.sql"),
            shadow_run_path: PathBuf::from("target/run/p/models/my_model__dbt_tmp.sql"),
        };
        let io_args = IoArgs {
            in_dir: PathBuf::from("."),
            out_dir: PathBuf::from("target"),
            ..Default::default()
        };
        let runtime_config = DbtRuntimeConfig::default();
        let resolver = Arc::new(MaterializationResolver::new(&BTreeMap::new(), "p"));
        let jinja = Arc::new(JinjaEnv::new(minijinja::Environment::new()));
        let base_ctx = BTreeMap::new();
        let adapter = Adapter::new_parse_phase_adapter(
            AdapterType::SingleStore,
            dbt_yaml::Mapping::new(),
            dbt_schemas::schemas::relations::DEFAULT_DBT_QUOTING,
            Arc::new(dbt_adapter::sql_types::DefaultTypeOps::new(
                AdapterType::SingleStore,
            )),
            None,
        );
        let env = SwapEnv {
            adapter: &adapter,
            adapter_type: AdapterType::SingleStore,
            runtime_config: &runtime_config,
            materialization_resolver: &resolver,
            jinja_env: jinja,
            base_context: &base_ctx,
            io_args: &io_args,
        };
        let shadow_path = shadow_build.shadow_model.get_node_path(
            NodePathKind::Executable,
            &env.io_args.in_dir,
            &env.io_args.out_dir,
        );
        let shadow_label = format!(
            "in model {} ({})",
            shadow_build.shadow_model.__base_attr__.alias,
            shadow_path.display()
        );
        let mut err = dbt_common::fs_err!(ErrorCode::ExecutionError, "error {}", shadow_label);
        err = shadow_build.relabel(err, &env);
        assert!(!err.context.contains("my_model__dbt_tmp"));
        assert!(
            err.context
                .contains("in model my_model (target/run/p/models/my_model.sql)")
        );
    }

    #[test]
    fn abort_removes_shadow_run_file() {
        let dir = tempfile::tempdir().unwrap();
        let shadow_run_path = dir.path().join("my_model__dbt_tmp_1234567890.sql");
        std::fs::write(&shadow_run_path, "select 1").unwrap();
        let model = DbtModel::default();
        let shadow_build = ShadowBuild {
            shadow_model: Arc::new(shadow_model(&model, "my_model__dbt_tmp_1234567890", false)),
            grants_after_swap: false,
            target_existed_at_start: false,
            alias: "my_model".to_string(),
            context: BTreeMap::from([(
                SHADOW.to_string(),
                Value::from("my_model__dbt_tmp_1234567890"),
            )]),
            run_path: dir.path().join("my_model.sql"),
            shadow_run_path: shadow_run_path.clone(),
        };
        let jinja = JinjaEnv::new(minijinja::Environment::new());
        shadow_build.abort(&jinja);
        assert!(!shadow_run_path.exists());
    }
}
