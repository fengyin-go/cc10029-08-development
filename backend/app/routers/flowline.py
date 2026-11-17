"""采集流程 DAG 拓扑接口：返回节点、连线图结构，并驱动流程执行数据快照。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, FlowRunPayload
from app.services.flowline import FlowlineService, csv_cell
from app.services.flowrun import flow_run_service

router = APIRouter(prefix="/api/flowline", tags=["采集流程"])
service = FlowlineService()
LIST_FIELDS = ["流程编号", "流程名称", "步骤序列", "输入站点", "输出目标", "失败处理", "触发条件", "版本号"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/export")
def export_entries() -> StreamingResponse:
    nodes, edges = service.raw_data()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status"])
    for node in nodes:
        writer.writerow(["node", node.get("id",""), csv_cell(node.get("name","")), "", "", "", node.get("status","")])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from",""), edge.get("to",""), edge.get("label",""), ""])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=flowline.csv"})


@router.get("/snapshots")
def list_snapshots(flow_id: int | None = Query(default=None)) -> dict[str, Any]:
    """流程执行数据快照清单，可按采集流程过滤。"""
    return {"items": service.list_snapshots(flow_id)}


@router.get("/runs")
def list_runs(flow_id: int | None = Query(default=None)) -> dict[str, Any]:
    """流程运行清单：可选定某次运行查看步骤回执与失败处理结果。"""
    return {"items": service.list_runs(flow_id)}


@router.get("/runs/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    run = service.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"流程运行 {run_id} 不存在")
    return run


@router.get("/snapshots/{run_id}")
def get_snapshot(run_id: str) -> dict[str, Any]:
    """读取单份流程执行数据快照，包含按步骤顺序排列的回执。"""
    snapshot = service.get_snapshot(run_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"流程执行数据快照 {run_id} 不存在")
    return snapshot


@router.get("/snapshots/{run_id}/receipts")
def list_receipts(run_id: str) -> dict[str, Any]:
    """查看指定流程运行的步骤回执、输出目标与失败处理结果。"""
    if service.get_snapshot(run_id) is None:
        receipts = service.list_receipts(run_id)
        if not receipts:
            raise HTTPException(status_code=404, detail=f"流程运行 {run_id} 不存在")
        return {"run_id": run_id, "items": receipts, "complete": False}
    return {"run_id": run_id, "items": service.list_receipts(run_id), "complete": True}


@router.get("/snapshots/{run_id}/validation")
def validate_snapshot(run_id: str) -> dict[str, Any]:
    """校验流程执行数据快照并返回结论，供存储落地台账核对。"""
    snapshot = service.get_snapshot(run_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"流程执行数据快照 {run_id} 不存在")
    return flow_run_service.validate_snapshot(snapshot)


@router.get("/landings")
def list_landings(run_id: str | None = Query(default=None)) -> dict[str, Any]:
    """落地批次回执清单：每条都引用步骤输出与流程版本。"""
    return {"items": flow_run_service.list_landing(run_id=run_id)}


@router.get("/snapshots/{run_id}/landing")
def get_landing(run_id: str) -> dict[str, Any]:
    landings = flow_run_service.list_landing(run_id=run_id)
    if not landings:
        raise HTTPException(status_code=404, detail=f"流程执行数据快照 {run_id} 尚未生成落地批次回执")
    return landings[0]


@router.post("/snapshots/{run_id}/land")
def land_snapshot(run_id: str) -> ActionResult:
    """消费流程执行数据快照，生成落地批次回执并同步校验结论到存储台账。"""
    landing_or_none, message = service.land(run_id)
    if landing_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=landing_or_none)


@router.post("/snapshots/{run_id}/validation")
def sync_validation(run_id: str) -> ActionResult:
    """把快照校验结论重新同步到存储落地的落地批次回执清单。"""
    try:
        entry = flow_run_service.sync_validation(run_id)
    except Exception as exc:
        return ActionResult(ok=False, message=str(exc))
    return ActionResult(ok=True, message="校验结论已同步到存储落地台账", entry=entry)


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"流程编排 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="流程编排已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.post("/{entry_id}/run")
def run_flow(entry_id: int, payload: FlowRunPayload | None = None) -> ActionResult:
    """执行采集流程，返回流程执行数据快照；支持中断后续跑与失败回退。"""
    payload = payload or FlowRunPayload()
    snapshot_or_none, message = service.execute(
        entry_id,
        resume_from=payload.resume_from,
        fail_at_step=payload.fail_at_step,
        payload=payload.merged_values(),
    )
    if snapshot_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=snapshot_or_none)
