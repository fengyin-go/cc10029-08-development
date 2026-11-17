"""采集流程 DAG 图服务：生成节点、边示例结构与流转规则。

流程状态按「草稿 → 待校验 → 已启用 → 已停用」顺序推进，已停用是终态，
不能回退到旧环节；每次执行都会生成流程执行数据快照，并把快照编号记回流程。
"""
from __future__ import annotations

from typing import Any

from app.services.flowrun import FlowRunError, FlowRunService, flow_run_service
from app.store import store

MODULE = "flowline"
REQUIRED_FIELDS = ["流程编号", "流程名称", "步骤序列"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用配置": "已启用", "停用配置": "已停用"}
NEGATIVE_ACTIONS = ["停用配置"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class FlowlineService:
    def __init__(self, runs: FlowRunService | None = None) -> None:
        self.runs = runs or flow_run_service

    def get_graph(self) -> dict[str, Any]:
        rows = store.rows(MODULE)
        nodes: list[dict] = []
        edges: list[dict] = []
        for i, row in enumerate(rows):
            nodes.append({
                "id": int(row.get("id", i+1)),
                "name": str(row.get("流程名称") or row.get("流程编号") or f"流程{i+1}"),
                "status": str(row.get("status", STATUS_ORDER[0])),
                **{k: row[k] for k in row if k not in ("id", "status", "pending", "abnormal")},
            })
        for i in range(len(nodes)-1):
            edges.append({"from": nodes[i]["id"], "to": nodes[i+1]["id"], "label": "依赖"})
        return {"nodes": nodes, "edges": edges}

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id",0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["执行状态"] = "未执行"
        entry["最近快照"] = None
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流程编排 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集流程可执行范围"
        target = ACTION_RULES[action]
        current = str(entry.get("status") or STATUS_ORDER[0])
        if current not in STATUS_ORDER or target not in STATUS_ORDER:
            return None, f"状态「{current}」不在允许的状态序列里"
        if STATUS_ORDER.index(target) <= STATUS_ORDER.index(current):
            return None, f"采集流程状态不能从「{current}」回到「{target}」"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"流程编排已{action}"

    # ---------- 流程执行 ----------

    def execute(
        self,
        entry_id: int,
        *,
        resume_from: int | None = None,
        fail_at_step: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流程编排 {entry_id} 不存在或已归档"
        if entry.get("status") == STATUS_ORDER[-1]:
            return None, "采集流程已停用，不能再次执行"
        try:
            snapshot = self.runs.execute(
                entry_id, resume_from=resume_from, fail_at_step=fail_at_step, payload=payload)
        except FlowRunError as exc:
            return None, str(exc)
        entry["执行状态"] = "已完成"
        entry["最近快照"] = snapshot.get("快照编号")
        entry["步骤回执数"] = len(snapshot.get("步骤回执", []))
        entry.pop("失败原因", None)
        return snapshot, "流程执行数据快照已生成"

    def list_snapshots(self, flow_id: int | None = None) -> list[dict[str, Any]]:
        return self.runs.list_snapshots(flow_id)

    def list_runs(self, flow_id: int | None = None) -> list[dict[str, Any]]:
        return self.runs.list_runs(flow_id)

    def get_snapshot(self, run_id: str) -> dict[str, Any] | None:
        return self.runs.find_snapshot(run_id)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self.runs.find_run(run_id)

    def list_receipts(self, run_id: str) -> list[dict[str, Any]]:
        return self.runs.list_receipts(run_id)

    def validate(self, run_id: str) -> dict[str, Any] | None:
        snapshot = self.runs.find_snapshot(run_id)
        if snapshot is None:
            return None
        return FlowRunService.validate_snapshot(snapshot)

    def land(self, run_id: str) -> tuple[dict[str, Any] | None, str]:
        try:
            return self.runs.land(run_id), "落地批次回执已生成"
        except FlowRunError as exc:
            return None, str(exc)

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]
