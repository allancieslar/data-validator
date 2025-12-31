import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


@dataclass
class Issue:
    row_index: Optional[int]  # None = file-level issue
    column: Optional[str]
    rule: str
    message: str


def load_rulepack(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_csv(path: Path) -> pd.DataFrame:
    # Try common encodings to avoid annoying decode errors
    for enc in ("utf-8", "utf-8-sig", "latin1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    # Last attempt without specifying encoding (lets pandas decide)
    return pd.read_csv(path)


def validate_required_columns(df: pd.DataFrame, required: List[str]) -> List[Issue]:
    issues: List[Issue] = []
    missing = [c for c in required if c not in df.columns]
    for col in missing:
        issues.append(Issue(None, col, "required_columns", f"Missing required column: {col}"))
    return issues


def validate_numeric_ranges(df: pd.DataFrame, ranges: Dict[str, Dict[str, Any]]) -> List[Issue]:
    issues: List[Issue] = []

    for col, spec in ranges.items():
        if col not in df.columns:
            continue  # handled by required_columns if needed

        series = pd.to_numeric(df[col], errors="coerce")
        min_v = spec.get("min", None)
        max_v = spec.get("max", None)

        # Flag non-numeric values
        non_numeric_mask = series.isna() & df[col].notna()
        for idx in df.index[non_numeric_mask]:
            issues.append(Issue(int(idx), col, "numeric_type", f"Non-numeric value in '{col}': {df.loc[idx, col]}"))

        if min_v is not None:
            mask = series.notna() & (series < min_v)
            for idx in df.index[mask]:
                issues.append(Issue(int(idx), col, "min", f"Value {series.loc[idx]} < min {min_v}"))

        if max_v is not None:
            mask = series.notna() & (series > max_v)
            for idx in df.index[mask]:
                issues.append(Issue(int(idx), col, "max", f"Value {series.loc[idx]} > max {max_v}"))

    return issues


def validate_logical_rules(df: pd.DataFrame, logical_rules: List[Dict[str, Any]]) -> List[Issue]:
    issues: List[Issue] = []

    for rule in logical_rules:
        rtype = rule.get("type")

        if rtype == "greater_or_equal":
            left = rule["left"]
            right = rule["right"]
            desc = rule.get("description", f"{left} must be >= {right}")

            if left not in df.columns or right not in df.columns:
                continue

            left_s = pd.to_numeric(df[left], errors="coerce")
            right_s = pd.to_numeric(df[right], errors="coerce")

            mask = left_s.notna() & right_s.notna() & (left_s < right_s)
            for idx in df.index[mask]:
                issues.append(Issue(int(idx), f"{left},{right}", "greater_or_equal",
                                    f"{desc}. {left}={left_s.loc[idx]} < {right}={right_s.loc[idx]}"))

        elif rtype == "range_consistency":
            min_field = rule["min_field"]
            value_field = rule["value_field"]
            max_field = rule["max_field"]
            desc = rule.get("description", f"{value_field} must be between {min_field} and {max_field}")

            if min_field not in df.columns or value_field not in df.columns or max_field not in df.columns:
                continue

            min_s = pd.to_numeric(df[min_field], errors="coerce")
            val_s = pd.to_numeric(df[value_field], errors="coerce")
            max_s = pd.to_numeric(df[max_field], errors="coerce")

            mask = min_s.notna() & val_s.notna() & max_s.notna() & ((val_s < min_s) | (val_s > max_s))
            for idx in df.index[mask]:
                issues.append(Issue(int(idx), f"{min_field},{value_field},{max_field}", "range_consistency",
                                    f"{desc}. min={min_s.loc[idx]}, value={val_s.loc[idx]}, max={max_s.loc[idx]}"))

        else:
            issues.append(Issue(None, None, "unknown_rule", f"Unknown logical rule type: {rtype}"))

    return issues


def build_report(df: pd.DataFrame, issues: List[Issue]) -> Dict[str, Any]:
    file_issues = [i for i in issues if i.row_index is None]
    row_issues = [i for i in issues if i.row_index is not None]

    return {
        "rows_checked": int(df.shape[0]),
        "columns": list(df.columns),
        "file_issues": [
            {"column": i.column, "rule": i.rule, "message": i.message} for i in file_issues
        ],
        "row_issue_count": len(row_issues),
        "issues_sample": [
            {"row": i.row_index, "column": i.column, "rule": i.rule, "message": i.message}
            for i in row_issues[:25]
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate a CSV using a JSON rulepack.")
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--rules", required=True, help="Path to rulepack JSON")
    parser.add_argument("--out", default="out", help="Output directory (default: out)")
    args = parser.parse_args()

    input_path = Path(args.input)
    rules_path = Path(args.rules)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    rulepack = load_rulepack(rules_path)
    df = load_csv(input_path)

    issues: List[Issue] = []
    issues += validate_required_columns(df, rulepack.get("required_columns", []))
    issues += validate_numeric_ranges(df, rulepack.get("numeric_ranges", {}))
    issues += validate_logical_rules(df, rulepack.get("logical_rules", []))

    report = build_report(df, issues)

    # Write report
    report_path = out_dir / "data_quality_report.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # Print summary
    stitle = rulepack.get("dataset_name", "Dataset")
    print(f"Validated: {stitle}")
    print(f"Rows checked: {report['rows_checked']}")
    print(f"File issues: {len(report['file_issues'])}")
    print(f"Row issues: {report['row_issue_count']}")
    print(f"Report written to: {report_path.resolve()}")

    if report["file_issues"]:
        print("\nMissing/invalid columns:")
        for i in report["file_issues"]:
            print(f"- {i['message']}")

    if report["row_issue_count"] > 0:
        print("\nSample row issues (first 25):")
        for i in report["issues_sample"]:
            print(f"- row {i['row']}: {i['message']}")


if __name__ == "__main__":
    main()