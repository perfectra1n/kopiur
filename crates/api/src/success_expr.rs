//! `successExpr`: a sandboxed CEL pass/fail predicate over a verification result
//! (ADR-0005 §4/§15).
//!
//! A verification (ADR-0005 §4) can run blob-level `kopia snapshot verify` or a
//! deep scratch-restore. `successExpr` lets a user assert the result is *good* —
//! killing the silent "0 files" success (a verify that technically succeeds but
//! restored nothing). Example: `"stats.files > 0 && stats.errors == 0"`.
//!
//! This reuses the `cel`/`*Expr` foundation from [`crate::identity`] (ADR-0004 §5):
//! compile → execute → require a typed result, length-capped as the cost-budget
//! surrogate, out-of-scope variables rejected at admission. The difference is the
//! environment and the required result type: `successExpr` returns a **bool** over a
//! verify-result environment, where identity expressions return a **string** over an
//! identity environment.
//!
//! ## CEL environment
//!
//! - `stats` — a map `{files, bytes, errors}` (integers). Always present.
//! - `snapshot` — a map of snapshot metadata (the snapshot id under `id`). Always
//!   present (possibly empty).
//! - `restored` — a map `{files, checksumMatches}` for the *deep* (scratch-restore)
//!   tier. Present (possibly empty) so an expression that only references it under a
//!   guard validates; a quick verify leaves it empty.
//!
//! Each value is supplied by the mover from the real verify result and evaluated
//! there; admission validation ([`validate_success_expr`]) trial-evaluates against a
//! representative environment so a typo / out-of-scope variable / non-bool result is
//! rejected on `kubectl apply` rather than at first verify run.

use std::collections::BTreeMap;

use cel::{Context, Program, Value};

use crate::error::{ValidationError, ValidationResult};
use crate::identity::MAX_EXPR_LEN;

/// The integer stats a verification reports, exposed to `successExpr` as `stats`.
/// Used both by the mover (filled from the real kopia result) and by
/// [`validate_success_expr`] (a representative trial value).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VerifyStats {
    /// Number of files verified/restored.
    pub files: i64,
    /// Number of bytes verified/restored.
    pub bytes: i64,
    /// Number of errors encountered (0 = clean).
    pub errors: i64,
}

/// The optional deep-restore stats, exposed as `restored`. `None` for the quick
/// (blob-level) tier; the environment then exposes an empty `restored` map.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoredStats {
    /// Number of files written by the scratch-restore.
    pub files: i64,
    /// Whether the restored content's checksums matched the snapshot.
    pub checksum_matches: bool,
}

/// The full environment a `successExpr` evaluates against.
#[derive(Debug, Clone, Default)]
pub struct SuccessExprInputs<'a> {
    /// `stats.{files,bytes,errors}`.
    pub stats: VerifyStats,
    /// `snapshot.*` metadata (e.g. `snapshot.id`). Empty when unknown.
    pub snapshot: BTreeMap<String, String>,
    /// `restored.{files,checksumMatches}` for the deep tier; `None` for quick.
    pub restored: Option<RestoredStats>,
    /// Lifetime tie so the struct can borrow string slices in future without churn.
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Compile a `successExpr`, enforcing the [`MAX_EXPR_LEN`] budget first (shared with
/// identity expressions). Maps a parse failure to
/// [`ValidationError::SuccessExprCompile`].
fn compile(expr: &str) -> ValidationResult<Program> {
    if expr.len() > MAX_EXPR_LEN {
        return Err(ValidationError::SuccessExprCompile {
            expr: expr.to_string(),
            reason: format!(
                "expression is {} bytes; the maximum is {MAX_EXPR_LEN}",
                expr.len()
            ),
        });
    }
    Program::compile(expr).map_err(|e| ValidationError::SuccessExprCompile {
        expr: expr.to_string(),
        reason: e.to_string(),
    })
}

/// Build the CEL context for a `successExpr`: `stats`, `snapshot`, `restored`.
fn context<'a>(inputs: &SuccessExprInputs<'_>) -> Context<'a> {
    let mut ctx = Context::default();
    // Maps with string keys; the values that may be ints are added as a serde map.
    let stats: BTreeMap<String, i64> = BTreeMap::from([
        ("files".to_string(), inputs.stats.files),
        ("bytes".to_string(), inputs.stats.bytes),
        ("errors".to_string(), inputs.stats.errors),
    ]);
    let _ = ctx.add_variable("stats", &stats);
    let _ = ctx.add_variable("snapshot", &inputs.snapshot);
    // `restored` is always present (possibly empty) so a guarded reference validates;
    // a quick verify leaves it empty rather than absent.
    match inputs.restored {
        Some(r) => {
            // files as int; checksumMatches as bool — two separate typed maps would
            // not share a value type, so serialize a JSON object instead.
            let restored = serde_json::json!({
                "files": r.files,
                "checksumMatches": r.checksum_matches,
            });
            let _ = ctx.add_variable("restored", &restored);
        }
        None => {
            let empty: BTreeMap<String, i64> = BTreeMap::new();
            let _ = ctx.add_variable("restored", &empty);
        }
    }
    ctx
}

/// Evaluate a compiled `successExpr` [`Program`] against `inputs`, requiring a bool
/// result. Maps an evaluation failure to [`ValidationError::SuccessExprEval`] and a
/// non-bool result to [`ValidationError::SuccessExprType`].
pub fn eval_success_expr(expr: &str, inputs: &SuccessExprInputs<'_>) -> ValidationResult<bool> {
    let program = compile(expr)?;
    let ctx = context(inputs);
    match program.execute(&ctx) {
        Ok(Value::Bool(b)) => Ok(b),
        Ok(other) => Err(ValidationError::SuccessExprType {
            expr: expr.to_string(),
            got: other.type_of().to_string(),
        }),
        Err(e) => Err(ValidationError::SuccessExprEval {
            expr: expr.to_string(),
            reason: e.to_string(),
        }),
    }
}

/// Validate a `successExpr` at admission (ADR-0005 §4/§15): it must compile, and —
/// because CEL reports an out-of-scope variable only at *evaluation* time — it must
/// trial-evaluate against a representative environment without referencing an
/// undeclared variable, returning a **bool**. Missing *map keys* (e.g.
/// `snapshot.tags['x']` when the trial data lacks `x`) are tolerated as
/// data-dependent, mirroring [`crate::identity::validate_identity_expr`].
pub fn validate_success_expr(expr: &str) -> ValidationResult {
    let program = compile(expr)?;
    // A representative non-empty environment so guards behave: include the deep
    // `restored` map so a deep-only expression validates, and a snapshot id.
    let inputs = SuccessExprInputs {
        stats: VerifyStats {
            files: 1,
            bytes: 1,
            errors: 0,
        },
        snapshot: BTreeMap::from([("id".to_string(), "trial".to_string())]),
        restored: Some(RestoredStats {
            files: 1,
            checksum_matches: true,
        }),
        _marker: std::marker::PhantomData,
    };
    let ctx = context(&inputs);
    match program.execute(&ctx) {
        Ok(Value::Bool(_)) => Ok(()),
        Ok(other) => Err(ValidationError::SuccessExprType {
            expr: expr.to_string(),
            got: other.type_of().to_string(),
        }),
        // An undeclared-variable reference (typo / out-of-scope) is a hard reject.
        // Other runtime errors (NoSuchKey on a data-dependent map index) are tolerated.
        Err(cel::ExecutionError::UndeclaredReference(name)) => {
            Err(ValidationError::SuccessExprEval {
                expr: expr.to_string(),
                reason: format!("undeclared reference to '{name}'"),
            })
        }
        Err(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs(files: i64, errors: i64) -> SuccessExprInputs<'static> {
        SuccessExprInputs {
            stats: VerifyStats {
                files,
                bytes: files * 100,
                errors,
            },
            snapshot: BTreeMap::from([("id".to_string(), "k1".to_string())]),
            restored: None,
            _marker: std::marker::PhantomData,
        }
    }

    #[test]
    fn evaluates_true_and_false() {
        let expr = "stats.files > 0 && stats.errors == 0";
        assert!(eval_success_expr(expr, &inputs(5, 0)).unwrap());
        // Zero files → the silent-empty-success guard fails.
        assert!(!eval_success_expr(expr, &inputs(0, 0)).unwrap());
        // Errors present → fails.
        assert!(!eval_success_expr(expr, &inputs(5, 2)).unwrap());
    }

    #[test]
    fn deep_restored_environment_is_available() {
        let mut i = inputs(3, 0);
        i.restored = Some(RestoredStats {
            files: 3,
            checksum_matches: true,
        });
        assert!(eval_success_expr("restored.files > 0 && restored.checksumMatches", &i).unwrap());
        i.restored = Some(RestoredStats {
            files: 3,
            checksum_matches: false,
        });
        assert!(!eval_success_expr("restored.checksumMatches", &i).unwrap());
    }

    #[test]
    fn snapshot_metadata_is_available() {
        let i = inputs(1, 0);
        assert!(eval_success_expr("snapshot.id == 'k1'", &i).unwrap());
    }

    #[test]
    fn non_bool_result_is_an_error() {
        let err = eval_success_expr("stats.files", &inputs(1, 0)).unwrap_err();
        assert!(matches!(err, ValidationError::SuccessExprType { .. }));
    }

    #[test]
    fn typo_is_an_error() {
        let err = eval_success_expr("statss.files > 0", &inputs(1, 0)).unwrap_err();
        assert!(matches!(err, ValidationError::SuccessExprEval { .. }));
    }

    // --- validate_success_expr (admission) ---

    #[test]
    fn validate_accepts_valid_bool_exprs() {
        assert!(validate_success_expr("stats.files > 0 && stats.errors == 0").is_ok());
        assert!(validate_success_expr("restored.checksumMatches").is_ok());
        assert!(validate_success_expr("snapshot.id != ''").is_ok());
        // Data-dependent map index on snapshot is tolerated.
        assert!(validate_success_expr("snapshot.id != '' && stats.errors == 0").is_ok());
    }

    #[test]
    fn validate_rejects_syntax_error() {
        let err = validate_success_expr("stats.files >").unwrap_err();
        assert!(matches!(err, ValidationError::SuccessExprCompile { .. }));
    }

    #[test]
    fn validate_rejects_out_of_scope_variable() {
        let err = validate_success_expr("bogus > 0").unwrap_err();
        assert!(matches!(err, ValidationError::SuccessExprEval { .. }));
    }

    #[test]
    fn validate_rejects_non_bool_result() {
        let err = validate_success_expr("stats.files + 1").unwrap_err();
        assert!(matches!(err, ValidationError::SuccessExprType { .. }));
    }

    #[test]
    fn validate_rejects_over_length_expr() {
        let long = format!("stats.files == {} ", "1".repeat(MAX_EXPR_LEN));
        let err = validate_success_expr(&long).unwrap_err();
        assert!(matches!(err, ValidationError::SuccessExprCompile { .. }));
    }
}
