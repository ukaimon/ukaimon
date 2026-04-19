from __future__ import annotations

import random
from collections import defaultdict


def _normalize_replicates(value: object) -> int:
    if value in (None, "", 0):
        return 1
    return max(1, int(value))


def _sort_conditions_by_concentration(
    conditions: list[dict[str, object]],
    baseline_value: float | None,
) -> list[dict[str, object]]:
    baseline_items = [
        condition
        for condition in conditions
        if baseline_value is not None
        and float(condition.get("concentration_value", 0.0)) == float(baseline_value)
    ]
    regular_items = [condition for condition in conditions if condition not in baseline_items]
    baseline_items = sorted(baseline_items, key=lambda item: float(item.get("concentration_value", 0.0)))
    regular_items = sorted(regular_items, key=lambda item: float(item.get("concentration_value", 0.0)))
    return baseline_items + regular_items


def generate_batch_plan_items(
    conditions: list[dict[str, object]],
    baseline_value: float | None = None,
    execution_mode: str = "randomized_blocks",
    seed: int | None = None,
) -> list[dict[str, object]]:
    if not conditions:
        return []

    rng = random.Random(seed)
    normalized_conditions = []
    for condition in conditions:
        item = dict(condition)
        item["planned_replicates"] = _normalize_replicates(condition.get("planned_replicates"))
        normalized_conditions.append(item)

    if execution_mode == "fully_randomized":
        expanded: list[dict[str, object]] = []
        for condition in normalized_conditions:
            for rep_no in range(1, int(condition["planned_replicates"]) + 1):
                expanded.append({"condition": condition, "rep_no": rep_no})
        rng.shuffle(expanded)
        return [
            {
                "condition_id": item["condition"]["condition_id"],
                "planned_order": index,
                "rep_no": item["rep_no"],
                "planned_status": "waiting",
            }
            for index, item in enumerate(expanded, start=1)
        ]

    if execution_mode == "concentration_grouped":
        ordered_conditions = _sort_conditions_by_concentration(normalized_conditions, baseline_value)
        plan_rows: list[dict[str, object]] = []
        for condition in ordered_conditions:
            for rep_no in range(1, int(condition["planned_replicates"]) + 1):
                plan_rows.append(
                    {
                        "condition_id": condition["condition_id"],
                        "planned_order": len(plan_rows) + 1,
                        "rep_no": rep_no,
                        "planned_status": "waiting",
                    }
                )
        return plan_rows

    max_rep = max(int(condition["planned_replicates"]) for condition in normalized_conditions)
    plan_rows: list[dict[str, object]] = []
    rep_counts: defaultdict[str, int] = defaultdict(int)
    ordered_conditions = _sort_conditions_by_concentration(normalized_conditions, baseline_value)

    for block_no in range(1, max_rep + 1):
        block_conditions = [
            condition
            for condition in ordered_conditions
            if int(condition["planned_replicates"]) >= block_no
        ]
        baseline_items = [
            condition
            for condition in block_conditions
            if baseline_value is not None
            and float(condition.get("concentration_value", 0.0)) == float(baseline_value)
        ]
        regular_items = [condition for condition in block_conditions if condition not in baseline_items]

        if execution_mode == "fixed":
            regular_items.sort(key=lambda item: float(item.get("concentration_value", 0.0)))
        else:
            rng.shuffle(regular_items)

        for condition in baseline_items + regular_items:
            rep_counts[str(condition["condition_id"])] += 1
            plan_rows.append(
                {
                    "condition_id": condition["condition_id"],
                    "planned_order": len(plan_rows) + 1,
                    "rep_no": rep_counts[str(condition["condition_id"])],
                    "planned_status": "waiting",
                }
            )
    return plan_rows
