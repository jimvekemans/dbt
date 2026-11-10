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
use dbt_schemas::schemas::project::ChunkVerification;
#[cfg(test)]
use dbt_schemas::schemas::project::FullRefreshStrategy;
use dbt_schemas::schemas::{DbtModel, InternalDbtNode, InternalDbtNodeAttributes, NodePathKind};
use dbt_schemas::state::DbtRuntimeConfig;
use dbt_telemetry::ExecutionPhase;
use dbt_yaml::Verbatim;
use minijinja::Value;
use minijinja::value::OverlayMap;

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
    pub base_context: &'a Value,
    pub io_args: &'a IoArgs,
}

/// A rebuild in progress: the target, its shadow and the backup name, plus a
/// run context of the real target to evaluate macros in.
pub struct ShadowBuild {
    context: OverlayMap,
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
    /// into it. `chunk_verification` checks the first chunk (`sampled`) or the
    /// whole shadow (`full`).
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
        let header = header_prefix(sql_header.as_ref());
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
        let mut verifier = Verifier::new(
            &self.context,
            &self.run_path,
            SHADOW,
            sql,
            plan,
            &header,
            &env.jinja_env,
            &self.shadow_model.__common_attr__.unique_id,
        );
        verifier.after_first_chunk()?;
        insert_remaining_chunks(
            &self.context,
            &self.run_path,
            SHADOW,
            sql,
            plan,
            &header,
            &env.jinja_env,
        )?;
        verifier.after_last_chunk()?;
        Ok(response)
    }

    /// The live and shadow relations, rendered as they appear in SQL.
    pub fn relation_names(&self) -> (String, String) {
        (
            var(&self.context, TARGET).to_string(),
            var(&self.context, SHADOW).to_string(),
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
                        var(&self.context, SHADOW)
                    ),
                );
            }
        } else {
            emit_warn_log_message(
                ErrorCode::SwapShadowPreserved,
                format!(
                    "Preserving shadow {} after failed swap because target {} was not restored or backup {} remains",
                    var(&self.context, SHADOW),
                    var(&self.context, TARGET),
                    var(&self.context, BACKUP)
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

/// One hash bucket: `chunk` (0-based) of `chunks`. The load uses the plan's
/// chunks; verification rebuilds rows under other bucketings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Bucket {
    chunks: u64,
    chunk: u64,
}

/// `sql` with every chunk-0 `get_chunk_filter` predicate of a model chunked in
/// `chunks` replaced by the predicate of `target`. `predicate(column, chunks,
/// chunk)` renders `get_chunk_predicate`; every tag must carry its chunk-0
/// predicate, so hand-written filters are rejected.
fn rewrite_chunk_filters(
    sql: &str,
    chunks: u64,
    target: Bucket,
    mut predicate: impl FnMut(&str, u64, u64) -> FsResult<String>,
) -> FsResult<String> {
    let mut rewritten = sql.to_string();
    for column in chunk_filter_columns(sql) {
        let tag = format!("{CHUNK_FILTER_TAG_OPEN}{column}{CHUNK_FILTER_TAG_CLOSE}");
        let first = chunk_filter(&format!("{} ", predicate(column, chunks, 0)?), column);
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
        if target == (Bucket { chunks, chunk: 0 }) {
            continue;
        }
        let replacement = predicate(column, target.chunks, target.chunk)?;
        rewritten = rewritten.replace(&first, &chunk_filter(&format!("{replacement} "), column));
    }
    Ok(rewritten)
}

/// `get_chunk_predicate(column, chunks, chunk)`, rendered in `context`.
fn chunk_predicate(
    context: &OverlayMap,
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
    context: &OverlayMap,
    run_path: &Path,
    sql: &str,
    plan: &SwapPlan,
    chunk: u64,
    jinja_env: &JinjaEnv,
) -> FsResult<String> {
    if plan.chunks == 1 {
        return Ok(sql.to_string());
    }
    let target = Bucket {
        chunks: plan.chunks,
        chunk,
    };
    if !chunk_filter_columns(sql).is_empty() {
        return bucket_sql(context, run_path, sql, plan, target, jinja_env);
    }
    let Some(column) = &plan.chunk_output_by else {
        return Err(fs_err!(
            ErrorCode::InvalidConfig,
            "`chunks: {}` needs `get_chunk_filter` predicates in the model SQL, `chunk_output_by`, or a single-column `unique_key`",
            plan.chunks
        ));
    };
    let predicate = chunk_predicate(context, run_path, jinja_env, plan.chunks, column, chunk)?;
    Ok(format!(
        "select * from (\n{sql}\n) as dbt_chunk_source\nwhere {predicate}"
    ))
}

/// The model SQL, whose chunk filters were rendered for `plan.chunks`, with
/// those filters selecting `target` instead.
fn bucket_sql(
    context: &OverlayMap,
    run_path: &Path,
    sql: &str,
    plan: &SwapPlan,
    target: Bucket,
    jinja_env: &JinjaEnv,
) -> FsResult<String> {
    rewrite_chunk_filters(sql, plan.chunks, target, |column, chunks, chunk| {
        chunk_predicate(context, run_path, jinja_env, chunks, column, chunk)
    })
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
                "{} is rebuilt in {} chunks by splitting its output on `{column}`, so every chunk reads all of its inputs. Filter the inputs instead with `chunk_where`, `chunk_and` or `get_chunk_filter`, or set `chunk_output_by: {column}` to keep this.",
                &model.__common_attr__.unique_id, plan.chunks
            ),
        );
    }
}

fn report_chunk(chunk: u64, chunks: u64, context: &OverlayMap, relation_var: &str) {
    if chunks > 1 {
        let target = var(context, TARGET);
        if relation_var == TARGET {
            emit_info_log_message(format!("Loading chunk {}/{chunks} of {target}", chunk + 1));
        } else {
            let shadow = var(context, relation_var);
            emit_info_log_message(format!(
                "Loading chunk {}/{chunks} of {target} (into {shadow})",
                chunk + 1
            ));
        }
    }
}

/// How this rebuild checks its chunk filters: the model's `chunk_verification`,
/// or `sampled` with the `verify_chunked_rebuilds` flag when it is unset.
/// Unchunked models and models without chunk filters are never checked:
/// splitting the output is correct by construction.
fn verification_mode(plan: &SwapPlan, sql: &str, unique_id: &str) -> ChunkVerification {
    let configured = plan.verification.unwrap_or_else(|| {
        if dbt_flags::VERIFY_CHUNKED_REBUILDS.enabled() {
            ChunkVerification::Sampled
        } else {
            ChunkVerification::Off
        }
    });
    if configured != ChunkVerification::Off
        && (plan.chunks == 1 || chunk_filter_columns(sql).is_empty())
    {
        emit_debug_log_message(format!(
            "Not verifying {unique_id}: it is not rebuilt in chunks with chunk filters"
        ));
        return ChunkVerification::Off;
    }
    if plan.verification == Some(ChunkVerification::Off)
        && dbt_flags::VERIFY_CHUNKED_REBUILDS.enabled()
    {
        emit_debug_log_message(format!(
            "Not verifying {unique_id}: its `chunk_verification` is `off`"
        ));
    }
    configured
}

/// The rows of a query, described independently of their order: how many
/// there are and the sum of their hashes (`get_rows_fingerprint`). The
/// fingerprints of disjoint sets of rows add up.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Fingerprint {
    rows: i128,
    hash_sum: i128,
}

impl std::ops::Add for Fingerprint {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            rows: self.rows + other.rows,
            hash_sum: self.hash_sum + other.hash_sum,
        }
    }
}

impl std::iter::Sum for Fingerprint {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |total, part| total + part)
    }
}

impl std::fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} rows, hash sum {}", self.rows, self.hash_sum)
    }
}

/// An integer from a query result. Decimal types may render a zero fraction
/// (`12.000000`); anything else, NULL included, is `None`.
fn parse_integer(text: &str) -> Option<i128> {
    let text = text.trim();
    let (integer, fraction) = text.split_once('.').unwrap_or((text, ""));
    if !fraction.chars().all(|c| c == '0') {
        return None;
    }
    integer.parse().ok()
}

/// The checks of a chunked rebuild, against the relation it loads into.
struct Verifier<'a> {
    context: OverlayMap,
    run_path: &'a Path,
    relation_var: &'a str,
    sql: &'a str,
    plan: &'a SwapPlan,
    header: &'a str,
    jinja_env: &'a JinjaEnv,
    unique_id: &'a str,
    /// Whether `header` (the model's `sql_header`) ran on this connection.
    header_applied: bool,
}

impl<'a> Verifier<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        context: &OverlayMap,
        run_path: &'a Path,
        relation_var: &'a str,
        sql: &'a str,
        plan: &'a SwapPlan,
        header: &'a str,
        jinja_env: &'a JinjaEnv,
        unique_id: &'a str,
    ) -> Self {
        Self {
            context: context.clone(),
            run_path,
            relation_var,
            sql,
            plan,
            header,
            jinja_env,
            unique_id,
            header_applied: false,
        }
    }

    /// After chunk 0 is loaded, with `sampled`: the relation holds exactly
    /// chunk 0 of N, which is chunks 0 and N of 2N. Rebuild those two
    /// (half a chunk each) and compare.
    fn after_first_chunk(&mut self) -> FsResult<()> {
        if verification_mode(self.plan, self.sql, self.unique_id) != ChunkVerification::Sampled {
            return Ok(());
        }
        let chunks = self.plan.chunks;
        emit_info_log_message(format!(
            "Verifying chunk 1/{chunks} of {}: rebuilding it as chunks 1 and {} of {}",
            self.place(),
            chunks + 1,
            2 * chunks
        ));
        let rebuilt = [0, chunks]
            .into_iter()
            .map(|chunk| {
                self.bucket(Bucket {
                    chunks: 2 * chunks,
                    chunk,
                })
            })
            .sum::<FsResult<Fingerprint>>()?;
        self.compare(
            &format!("chunk 1/{chunks}"),
            2 * chunks,
            ChunkVerification::Sampled,
            rebuilt,
        )
    }

    /// After the last chunk, with `full`: rebuild the whole model in N+1
    /// chunks, each smaller than a chunk of the load, and compare.
    fn after_last_chunk(&mut self) -> FsResult<()> {
        if verification_mode(self.plan, self.sql, self.unique_id) != ChunkVerification::Full {
            return Ok(());
        }
        let chunks = self.plan.chunks + 1;
        emit_info_log_message(format!(
            "Verifying {}: rebuilding it in {chunks} chunks",
            self.place()
        ));
        let mut rebuilt = Fingerprint::default();
        for chunk in 0..chunks {
            emit_debug_log_message(format!(
                "Verifying chunk {}/{chunks} of {}",
                chunk + 1,
                var(&self.context, TARGET)
            ));
            rebuilt = rebuilt + self.bucket(Bucket { chunks, chunk })?;
        }
        self.compare("all chunks", chunks, ChunkVerification::Full, rebuilt)
    }

    /// Fail unless the relation's rows match `rebuilt`, the same rows built in
    /// `chunks` chunks.
    fn compare(
        &mut self,
        what: &str,
        chunks: u64,
        mode: ChunkVerification,
        rebuilt: Fingerprint,
    ) -> FsResult<()> {
        let relation = var(&self.context, self.relation_var).to_string();
        let loaded = self.fingerprint(&format!("select * from {relation}"))?;
        if loaded != rebuilt {
            let nesting = if mode == ChunkVerification::Sampled {
                " A custom `get_chunk_predicate` must also nest (chunk k of N = chunks k and k+N of 2N) for `chunk_verification: sampled`; use `full` otherwise."
            } else {
                ""
            };
            return Err(fs_err!(
                ErrorCode::ChunkVerificationFailed,
                "The chunked rebuild of {} depends on how it is chunked: {what} of {} loaded in {} chunks has {loaded}, the same rows rebuilt in {chunks} chunks have {rebuilt}. Check that every chunk filter (`chunk_where`, `chunk_and`, `get_chunk_filter`) names the column that holds the same key in its input (the column in the join), that grouping and window keys match it, that the output is deterministic (no current_timestamp(), random() or ties in a window's order by), and that no input changed during the rebuild.{nesting}",
                self.unique_id,
                var(&self.context, TARGET),
                self.plan.chunks,
            ));
        }
        emit_info_log_message(format!(
            "Verified {what} of {}: {loaded}, the same in {chunks} chunks",
            self.place()
        ));
        Ok(())
    }

    /// The fingerprint of the model SQL restricted to `bucket`.
    fn bucket(&mut self, bucket: Bucket) -> FsResult<Fingerprint> {
        let sql = bucket_sql(
            &self.context,
            self.run_path,
            self.sql,
            self.plan,
            bucket,
            self.jinja_env,
        )?;
        let other_modes = if bucket.chunks == 2 * self.plan.chunks {
            "`off`"
        } else {
            "`sampled` or `off`"
        };
        self.fingerprint(&sql).map_err(|mut error| {
            error.context = format!(
                "{} (while verifying {} with `chunk_verification`: chunk {} of {} did not run; with heavy key skew a verification chunk can need more than a chunk of the load, so consider `chunk_verification: {other_modes}` for this model)",
                error.context,
                self.unique_id,
                bucket.chunk + 1,
                bucket.chunks
            );
            error
        })
    }

    /// Run `get_rows_fingerprint` over `sql`, with the relation's columns.
    fn fingerprint(&mut self, sql: &str) -> FsResult<Fingerprint> {
        // Session settings in `sql_header` (optimizer switches, ...) apply to the
        // verification queries as to the load. They run as their own statement:
        // the fingerprint query's result must be the only one fetched.
        if !self.header_applied && !self.header.trim().is_empty() {
            execute(self.jinja_env, &self.context, self.run_path, self.header)?;
            self.header_applied = true;
        }
        if !self.context.contains_key(COLUMNS) {
            let columns = eval(
                self.jinja_env,
                &self.context,
                self.run_path,
                &format!("adapter.get_columns_in_relation({})", self.relation_var),
            )?;
            self.context.insert(COLUMNS.to_string(), columns);
        }
        let mut context = self.context.clone();
        context.insert("__dbt_swap_sql".to_string(), Value::from(sql));
        let query = eval(
            self.jinja_env,
            &context,
            self.run_path,
            &format!("get_rows_fingerprint(__dbt_swap_sql, {COLUMNS})"),
        )?;
        context.insert("__dbt_swap_sql".to_string(), query);
        let row = eval(
            self.jinja_env,
            &context,
            self.run_path,
            "run_query(__dbt_swap_sql).rows[0]",
        )?;
        let values = row
            .try_iter()
            .map_err(|e| {
                fs_err!(
                    ErrorCode::MacroUnsupportedValueType,
                    "Cannot read the fingerprint of {}: {e}",
                    var(&self.context, TARGET)
                )
            })?
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        if let [rows, hash_sum] = values.as_slice()
            && let (Some(rows), Some(hash_sum)) = (parse_integer(rows), parse_integer(hash_sum))
        {
            return Ok(Fingerprint { rows, hash_sum });
        }
        Err(fs_err!(
            ErrorCode::MacroUnsupportedValueType,
            "`get_rows_fingerprint` must return two integers (row count, hash sum), got {values:?}"
        ))
    }

    /// The relation being verified, for messages: the target, and the shadow
    /// when it is not loaded in place.
    fn place(&self) -> String {
        if self.relation_var == TARGET {
            var(&self.context, TARGET).to_string()
        } else {
            format!(
                "{} (in {})",
                var(&self.context, TARGET),
                var(&self.context, self.relation_var)
            )
        }
    }
}

/// Context key of the loaded relation's columns, read once per verification.
const COLUMNS: &str = "__dbt_swap_columns";

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
    context: &OverlayMap,
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
            var(context, relation_var)
        )
    })?
    .map(|column| column.to_string())
    .collect::<Vec<_>>()
    .join(", ");
    let relation = var(context, relation_var).to_string();
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

/// The model's `sql_header`, to prepend to statements that run outside its
/// materialization (chunk inserts, verification queries).
fn header_prefix(sql_header: Option<&Value>) -> String {
    sql_header
        .filter(|header| !header.is_none())
        .map(|header| format!("{header}\n"))
        .unwrap_or_default()
}

fn execute(jinja_env: &JinjaEnv, context: &OverlayMap, run_path: &Path, sql: &str) -> FsResult<()> {
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

/// A variable this module put into a run context, `Undefined` when absent.
fn var(context: &OverlayMap, name: &str) -> Value {
    context.get(name).unwrap_or_default()
}

fn eval(
    jinja_env: &JinjaEnv,
    context: &OverlayMap,
    run_path: &Path,
    expr: &str,
) -> FsResult<Value> {
    jinja_env
        .compile_expression(expr)?
        .eval_with_file_only_stack_frame(context, &[], run_path)
}

/// Rebuild a non-microbatch `swap` model: shadow, chunks, swap. `before_swap`
/// runs once the shadow is complete (and verified, with `chunk_verification`);
/// an error from it keeps the live relation.
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
    let header = header_prefix(sql_header.as_ref());
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
    let mut verifier = Verifier::new(
        &context,
        &run_path,
        TARGET,
        sql,
        plan,
        &header,
        &env.jinja_env,
        &model.__common_attr__.unique_id,
    );
    let loaded = verifier
        .after_first_chunk()
        .and_then(|_| {
            insert_remaining_chunks(
                &context,
                &run_path,
                TARGET,
                sql,
                plan,
                &header,
                &env.jinja_env,
            )
        })
        .and_then(|_| verifier.after_last_chunk());
    if let Err(error) = loaded {
        let target = var(&context, TARGET);
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

    fn crc32_predicate(column: &str, chunks: u64, chunk: u64) -> FsResult<String> {
        Ok(if chunk == 0 {
            format!("({column} is null or crc32({column}) % {chunks} = 0)")
        } else {
            format!("({column} is not null and crc32({column}) % {chunks} = {chunk})")
        })
    }

    fn bucket(chunks: u64, chunk: u64) -> Bucket {
        Bucket { chunks, chunk }
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
            rewrite_chunk_filters(CHUNKED_SQL, 4, bucket(4, 3), crc32_predicate).unwrap();
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
            rewrite_chunk_filters(CHUNKED_SQL, 4, bucket(4, 0), crc32_predicate).unwrap(),
            CHUNKED_SQL
        );
    }

    #[test]
    fn hand_written_chunk_filters_are_rejected() {
        let sql = "select * from t where crc32(k) % 8 = 0 /* dbt_chunk: k */";
        assert!(rewrite_chunk_filters(sql, 8, bucket(8, 1), crc32_predicate).is_err());
        assert!(rewrite_chunk_filters(sql, 8, bucket(16, 0), crc32_predicate).is_err());
    }

    #[test]
    fn chunk_filters_are_rewritten_for_other_bucketings() {
        // `sampled`: chunk 0 of 4 is chunks 0 and 4 of 8.
        let sub = rewrite_chunk_filters(CHUNKED_SQL, 4, bucket(8, 4), crc32_predicate).unwrap();
        assert_eq!(
            sub.matches("(customer_id is not null and crc32(customer_id) % 8 = 4) /* dbt_chunk: customer_id */").count(),
            2
        );
        // `full`: chunk 0 of 5 differs from chunk 0 of 4, so it is rewritten too.
        let first = rewrite_chunk_filters(CHUNKED_SQL, 4, bucket(5, 0), crc32_predicate).unwrap();
        assert_eq!(
            first
                .matches("(customer_id is null or crc32(customer_id) % 5 = 0) /* dbt_chunk: customer_id */")
                .count(),
            2
        );
        assert!(!first.contains("% 4"));
    }

    #[test]
    fn fingerprints_add_up() {
        let a = Fingerprint {
            rows: 2,
            hash_sum: 10,
        };
        let b = Fingerprint {
            rows: 3,
            hash_sum: 4_294_967_295,
        };
        assert_eq!(
            a + b,
            Fingerprint {
                rows: 5,
                hash_sum: 4_294_967_305
            }
        );
        assert_eq!(
            [a, b]
                .into_iter()
                .map(Ok)
                .sum::<FsResult<Fingerprint>>()
                .unwrap(),
            a + b
        );
        assert_eq!(Fingerprint::default() + a, a);
    }

    #[test]
    fn query_integers_are_parsed_strictly() {
        assert_eq!(parse_integer("42"), Some(42));
        assert_eq!(parse_integer(" 42.000000 "), Some(42));
        assert_eq!(
            parse_integer("92233720368547758070000"),
            Some(92_233_720_368_547_758_070_000)
        );
        assert_eq!(parse_integer("42.5"), None);
        assert_eq!(parse_integer("none"), None);
        assert_eq!(parse_integer(""), None);
    }

    #[test]
    fn verification_needs_chunks_and_chunk_filters() {
        let plan = |chunks, verification| SwapPlan {
            chunks,
            chunk_output_by: None,
            verification,
        };
        let full = Some(ChunkVerification::Full);
        assert_eq!(
            verification_mode(&plan(4, full), CHUNKED_SQL, "model.p.m"),
            ChunkVerification::Full
        );
        assert_eq!(
            verification_mode(
                &plan(4, Some(ChunkVerification::Off)),
                CHUNKED_SQL,
                "model.p.m"
            ),
            ChunkVerification::Off
        );
        // Without the flag (the test default), unset means off.
        assert_eq!(
            verification_mode(&plan(4, None), CHUNKED_SQL, "model.p.m"),
            ChunkVerification::Off
        );
        // A no-op where there is nothing to verify, so it can be set per folder.
        assert_eq!(
            verification_mode(&plan(1, full), CHUNKED_SQL, "model.p.m"),
            ChunkVerification::Off
        );
        assert_eq!(
            verification_mode(&plan(4, full), "select 1", "model.p.m"),
            ChunkVerification::Off
        );
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
            context: OverlayMap::default(),
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
        let base_ctx = Value::from(BTreeMap::<String, Value>::new());
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
            )])
            .into(),
            run_path: dir.path().join("my_model.sql"),
            shadow_run_path: shadow_run_path.clone(),
        };
        let jinja = JinjaEnv::new(minijinja::Environment::new());
        shadow_build.abort(&jinja);
        assert!(!shadow_run_path.exists());
    }
}
