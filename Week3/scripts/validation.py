from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, expr, lit

class SchemaContractError(ValueError):
    """Raised when a dataset is missing a REQUIRED column. Aborts the run."""


@dataclass
class RuleResult:
    rule_id: str
    rule_type: str
    passed: DataFrame
    failed: DataFrame
    metrics: dict = field(default_factory=dict)


@dataclass
class ValidationOutcome:

    dataset: str
    passed: DataFrame
    rejected: DataFrame          
    per_rule_metrics: list[dict]
    rows_in: int
    rows_out: int
    rows_rejected: int

class ValidationRule:

    rule_id: str = "abstract"
    rule_type: str = "abstract"

    def _apply(self, df: DataFrame) -> tuple[DataFrame, DataFrame, str]:
        raise NotImplementedError

    def apply(self, df: DataFrame) -> RuleResult:
        start = time.time()
        rows_in = df.count()
        passed, failed, reason = self._apply(df)

        failed = (
            failed
            .withColumn("_rule_id", lit(self.rule_id))
            .withColumn("_reject_reason", lit(reason))
        )
        rows_failed = failed.count()

        return RuleResult(
            rule_id=self.rule_id,
            rule_type=self.rule_type,
            passed=passed,
            failed=failed,
            metrics={
                "rule_id": self.rule_id,
                "rule_type": self.rule_type,
                "rows_in": rows_in,
                "rows_failed": rows_failed,
                "rows_passed": rows_in - rows_failed,
                "elapsed_s": round(time.time() - start, 3),
            },
        )

class NotNullRule(ValidationRule):

    rule_type = "not_null"

    def __init__(self, columns: Sequence[str], rule_id: str | None = None):
        self.columns = list(columns)
        self.rule_id = rule_id or f"not_null[{','.join(self.columns)}]"

    def _apply(self, df: DataFrame) -> tuple[DataFrame, DataFrame, str]:
        present = [c for c in self.columns if c in df.columns]
        if not present:
            return df, df.limit(0), f"columns not present: {self.columns}"

        cond = None
        for c in present:
            null_c = col(c).isNull()
            cond = null_c if cond is None else (cond | null_c)

        failed = df.filter(cond)
        passed = df.filter(~cond)
        return passed, failed, f"NULL in required column(s): {present}"


class NumericRangeRule(ValidationRule):
    rule_type = "numeric_range"

    def __init__(
        self,
        field: str,
        lo: float | None = None,
        hi: float | None = None,
        rule_id: str | None = None,
    ):
        if lo is None and hi is None:
            raise ValueError("NumericRangeRule requires at least one bound")
        self.field = field
        self.lo = lo
        self.hi = hi
        self.rule_id = rule_id or f"range[{field}:{lo}..{hi}]"

    def _apply(self, df: DataFrame) -> tuple[DataFrame, DataFrame, str]:
        if self.field not in df.columns:
            return df, df.limit(0), f"column {self.field!r} not present"

        passes = col(self.field).isNull()
        if self.lo is not None:
            passes = passes | (col(self.field) >= self.lo)
        if self.hi is not None:
            if self.lo is not None:
                passes = passes & ((col(self.field) <= self.hi) | col(self.field).isNull())
            else:
                passes = passes | (col(self.field) <= self.hi)

        passed = df.filter(passes)
        failed = df.filter(~passes)
        return passed, failed, f"{self.field} outside [{self.lo}, {self.hi}]"


class AllowedValuesRule(ValidationRule):
    rule_type = "allowed_values"

    def __init__(self, field: str, values: Iterable, rule_id: str | None = None):
        self.field = field
        self.values = list(values)
        self.rule_id = rule_id or f"allowed[{field}]"

    def _apply(self, df: DataFrame) -> tuple[DataFrame, DataFrame, str]:
        if self.field not in df.columns:
            return df, df.limit(0), f"column {self.field!r} not present"
        passes = col(self.field).isNull() | col(self.field).isin(self.values)
        return df.filter(passes), df.filter(~passes), (
            f"{self.field} not in {self.values}"
        )


class PredicateRule(ValidationRule):
    rule_type = "predicate"

    def __init__(self, rule_id: str, sql_expr: str):
        self.rule_id = rule_id
        self.sql_expr = sql_expr

    def _apply(self, df: DataFrame) -> tuple[DataFrame, DataFrame, str]:
        try:
            passed = df.filter(expr(self.sql_expr))
            failed = df.filter(~expr(self.sql_expr) | expr(self.sql_expr).isNull())
        except Exception as e:
            return df.limit(0), df, f"predicate failed to evaluate: {e}"
        return passed, failed, f"predicate `{self.sql_expr}` is false or null"


class ReferenceIntegrityRule(ValidationRule):
    rule_type = "reference_integrity"
    _cache: dict[tuple[str, str], set] = {}

    def __init__(
        self,
        field: str,
        ref_delta_path: str,
        ref_column: str,
        rule_id: str | None = None,
    ):
        self.field = field
        self.ref_delta_path = ref_delta_path
        self.ref_column = ref_column
        self.rule_id = rule_id or f"ref[{field}->{ref_column}]"

    def _load_reference(self, spark: SparkSession) -> set | None:
        key = (self.ref_delta_path, self.ref_column)
        if key in self._cache:
            return self._cache[key]
        try:
            ref_df = spark.read.format("delta").load(self.ref_delta_path)
        except Exception:
            self._cache[key] = None
            return None
        values = {r[self.ref_column] for r in ref_df.select(self.ref_column).collect()}
        self._cache[key] = values
        return values

    def _apply(self, df: DataFrame) -> tuple[DataFrame, DataFrame, str]:
        if self.field not in df.columns:
            return df, df.limit(0), f"column {self.field!r} not present"

        spark = df.sparkSession
        ref_values = self._load_reference(spark)
        if ref_values is None:
            return df, df.limit(0), (
                f"reference table {self.ref_delta_path} not readable"
            )

        passes = col(self.field).isNull() | col(self.field).isin(list(ref_values))
        passed = df.filter(passes)
        failed = df.filter(~passes)
        return passed, failed, (
            f"{self.field} has no match in {self.ref_delta_path}::{self.ref_column}"
        )


class DuplicateRule(ValidationRule):

    rule_type = "duplicate"

    def __init__(self, key_cols: Sequence[str], rule_id: str | None = None):
        self.key_cols = list(key_cols)
        self.rule_id = rule_id or f"duplicate[{','.join(self.key_cols)}]"

    def _apply(self, df: DataFrame) -> tuple[DataFrame, DataFrame, str]:
        present = [c for c in self.key_cols if c in df.columns]
        if not present:
            return df, df.limit(0), f"key columns not present: {self.key_cols}"

        from pyspark.sql import Window
        from pyspark.sql.functions import row_number

        w = Window.partitionBy(*[col(c) for c in present]).orderBy(lit(1))
        with_rn = df.withColumn("_dup_rn", row_number().over(w))
        passed = with_rn.filter(col("_dup_rn") == 1).drop("_dup_rn")
        failed = with_rn.filter(col("_dup_rn") > 1).drop("_dup_rn")
        return passed, failed, f"duplicate on key {present}"


class SchemaContractRule(ValidationRule):
    rule_type = "schema_contract"

    def __init__(
        self,
        required: Iterable[str],
        known_optional: Iterable[str] = (),
        strict_extras: bool = False,
        rule_id: str = "schema_contract",
    ):
        self.required = set(required)
        self.known_optional = set(known_optional)
        self.strict_extras = strict_extras
        self.rule_id = rule_id

    def _apply(self, df: DataFrame) -> tuple[DataFrame, DataFrame, str]:
        actual = {c.lower() for c in df.columns}
        missing = self.required - actual
        if missing:
            raise SchemaContractError(
                f"missing required columns: {sorted(missing)}"
            )

        allowed = self.required | self.known_optional
        unknown = actual - allowed

        empty = df.limit(0)
        if not unknown:
            result_reason = "all required columns present"
        elif self.strict_extras:
            result_reason = f"unexpected extra columns: {sorted(unknown)}"
        else:
            result_reason = (
                f"all required columns present; extras (not enforced): "
                f"{sorted(unknown)}"
            )
        return df, empty, result_reason

    def apply(self, df: DataFrame) -> RuleResult:
        start = time.time()
        rows_in = df.count()
        try:
            passed, failed, reason = self._apply(df)
        except SchemaContractError as e:
            failed = (
                df.limit(0)
                .withColumn("_rule_id", lit(self.rule_id))
                .withColumn("_reject_reason", lit(str(e)))
            )
            return RuleResult(
                rule_id=self.rule_id,
                rule_type=self.rule_type,
                passed=df.limit(0),
                failed=failed,
                metrics={
                    "rule_id": self.rule_id,
                    "rule_type": self.rule_type,
                    "rows_in": rows_in,
                    "rows_failed": rows_in,   # whole batch is untrusted
                    "rows_passed": 0,
                    "hard_failure": True,
                    "reason": str(e),
                    "elapsed_s": round(time.time() - start, 3),
                },
            )

        actual = {c.lower() for c in df.columns}
        unexpected = sorted(actual - (self.required | self.known_optional))
        if unexpected and not self.strict_extras:
            unexpected_note = unexpected
            unexpected = []
        else:
            unexpected_note = unexpected
        failed = failed.withColumn("_rule_id", lit(self.rule_id)) \
                       .withColumn("_reject_reason", lit(reason))
        return RuleResult(
            rule_id=self.rule_id,
            rule_type=self.rule_type,
            passed=passed,
            failed=failed,
            metrics={
                "rule_id": self.rule_id,
                "rule_type": self.rule_type,
                "rows_in": rows_in,
                "rows_failed": 0,
                "rows_passed": rows_in,
                "hard_failure": False,
                "unexpected_columns_present": unexpected_note,
                "unexpected_columns_flagged": unexpected,
                "strict_extras": self.strict_extras,
                "reason": reason,
                "elapsed_s": round(time.time() - start, 3),
            },
        )


class Validator:

    def __init__(self, rules: Sequence[ValidationRule]):
        self.rules = list(rules)

    def run(self, df: DataFrame, dataset: str) -> ValidationOutcome:
        rows_in = df.count()
        current = df
        rejected_frames: list[DataFrame] = []
        per_rule: list[dict] = []
        hard_failed = False

        for rule in self.rules:
            result = rule.apply(current)
            per_rule.append(result.metrics)

            if result.metrics.get("hard_failure"):
                hard_failed = True
                current = current.limit(0)
                rejected_frames.append(result.failed)
                break

            current = result.passed
            if result.failed.take(1):  # non-empty
                rejected_frames.append(result.failed)

        if rejected_frames:
            rejected = rejected_frames[0]
            for extra in rejected_frames[1:]:
                rejected = rejected.unionByName(extra, allowMissingColumns=True)
        else:
            rejected = (
                df.limit(0)
                  .withColumn("_rule_id", lit(None).cast("string"))
                  .withColumn("_reject_reason", lit(None).cast("string"))
            )

        return ValidationOutcome(
            dataset=dataset,
            passed=current,
            rejected=rejected,
            per_rule_metrics=per_rule,
            rows_in=rows_in,
            rows_out=current.count() if not hard_failed else 0,
            rows_rejected=rows_in - (current.count() if not hard_failed else 0),
        )

def rules_from_legacy_config(config: dict) -> list[ValidationRule]:
    rules: list[ValidationRule] = []

    if config.get("expected_cols"):
        rules.append(
            SchemaContractRule(
                required=config["expected_cols"],
                known_optional=config.get("optional_cols", set()),
            )
        )

    if config.get("key_cols"):
        rules.append(NotNullRule(config["key_cols"], rule_id="not_null[key_cols]"))

    for fld, (lo, hi) in (config.get("numeric_checks") or {}).items():
        if lo is None and hi is None:
            continue
        rules.append(NumericRangeRule(fld, lo, hi))

    if config.get("key_cols"):
        rules.append(DuplicateRule(config["key_cols"]))

    return rules


def rules_for_dataset(config: dict) -> list[ValidationRule]:
    if "rules" in config and config["rules"] is not None:
        rules = list(config["rules"])
    else:
        rules = rules_from_legacy_config(config)
    if "extra_rules" in config and config["extra_rules"]:
        rules.extend(config["extra_rules"])
    return rules


__all__ = [
    "AllowedValuesRule",
    "DuplicateRule",
    "NotNullRule",
    "NumericRangeRule",
    "PredicateRule",
    "ReferenceIntegrityRule",
    "SchemaContractError",
    "SchemaContractRule",
    "RuleResult",
    "ValidationOutcome",
    "ValidationRule",
    "Validator",
    "rules_for_dataset",
    "rules_from_legacy_config",
]
