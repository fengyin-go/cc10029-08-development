"""流程执行数据快照：把步骤完成凭据串成可回放的执行记录。

设计要点（与采集流程 / 存储落地共享同一份执行版本）：

* 一条流程执行（run）按「步骤序列」逐级推进，每一步完成后落一份步骤回执，
  回执里带输出目标与输出体；下一步必须读到上一步已完成的回执才能开始。
* 全部步骤完成后才生成流程执行数据快照，快照里的步骤回执按序列号排序且连续。
* 记录中断后再次送交同一快照：只保留一份，后续请求读取已存在结果。
* 落库按事务语义处理：回执、快照、流程状态在同一个临界区里一次提交，
  失败时整份回退，不留下半份数据。
"""
from __future__ import annotations

import copy
import threading
from datetime import datetime, timezone
from typing import Any

from app.store import store

# 采集流程的三类环节：站点采集 / 字段提取 / 结果落地。
STEP_KINDS = ("站点采集", "字段提取", "结果落地")
SNAPSHOT_MODULE = "flow_snapshot"
RECEIPT_MODULE = "flow_receipt"
RUN_MODULE = "flow_run"
LANDING_MODULE = "landing_receipt"
LEDGER_MODULE = "storage_ledger"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class FlowRunError(Exception):
    """携带可读原因的流程执行错误，路由层统一转成 ActionResult。"""


class FlowStepInterrupted(FlowRunError):
    """步骤执行失败：流程中断，已落地的回执保留。"""


class FlowRunService:
    """流程执行的唯一入口：步骤推进、快照生成、落地批次回执。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()

    # ---------- 步骤序列 ----------

    @staticmethod
    def parse_sequence(raw: Any) -> list[str]:
        """把「步骤序列」字段解析成有序步骤名列表，空序列视为非法。"""
        if isinstance(raw, list):
            return [str(item).strip() for item in raw if str(item).strip()]
        text = str(raw or "").strip()
        if not text:
            return []
        for sep in ("->", "→", ",", "，", "、", "|", "/"):
            if sep in text:
                return [part.strip() for part in text.split(sep) if part.strip()]
        return [text]

    @staticmethod
    def step_kind(name: str) -> str:
        """按步骤名归类到站点采集 / 字段提取 / 结果落地，未知步骤按提取处理。"""
        if any(word in name for word in ("采集", "抓取", "站点")):
            return "站点采集"
        if any(word in name for word in ("落地", "写入", "存储", "入库")):
            return "结果落地"
        if "提取" in name or "字段" in name:
            return "字段提取"
        return "字段提取"

    # ---------- 查询 ----------

    def list_snapshots(self, flow_id: int | None = None) -> list[dict[str, Any]]:
        rows = store.rows(SNAPSHOT_MODULE)
        if flow_id is not None:
            rows = [row for row in rows if int(row.get("流程ID", 0)) == flow_id]
        return [self._snapshot_view(row) for row in rows]

    def list_runs(self, flow_id: int | None = None) -> list[dict[str, Any]]:
        """流程运行清单：含已完成、中断的中间状态，便于选定某次运行核对回执。"""
        rows = store.rows(RUN_MODULE)
        if flow_id is not None:
            rows = [row for row in rows if int(row.get("流程ID", 0)) == flow_id]
        return [self._run_view(row) for row in rows]

    def list_receipts(self, run_id: str) -> list[dict[str, Any]]:
        rows = [
            copy.deepcopy(row) for row in store.rows(RECEIPT_MODULE)
            if row.get("执行编号") == run_id
        ]
        return sorted(rows, key=lambda row: int(row.get("步骤序号", 0)))

    def list_landing(
        self,
        snapshot_id: str | None = None,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """存储落地台账里的落地批次回执清单，附带快照校验结论。"""
        rows = store.rows(LANDING_MODULE)
        if snapshot_id is not None:
            rows = [row for row in rows if row.get("快照编号") == snapshot_id]
        if run_id is not None:
            rows = [row for row in rows if row.get("执行编号") == run_id]
        return [copy.deepcopy(row) for row in rows]

    def list_ledger(self) -> list[dict[str, Any]]:
        """存储落地业务台账：同步保存每份落地批次回执的校验结论。"""
        return [copy.deepcopy(row) for row in store.rows(LEDGER_MODULE)]

    def _snapshot_view(self, row: dict[str, Any]) -> dict[str, Any]:
        return copy.deepcopy(row)

    def _run_view(self, row: dict[str, Any]) -> dict[str, Any]:
        view = copy.deepcopy(row)
        view["步骤回执数"] = len(self.list_receipts(str(row.get("执行编号") or "")))
        return view

    def find_snapshot(self, run_id: str) -> dict[str, Any] | None:
        for row in store.rows(SNAPSHOT_MODULE):
            if row.get("执行编号") == run_id:
                return self._snapshot_view(row)
        return None

    def find_run(self, run_id: str) -> dict[str, Any] | None:
        row = self._raw_run(run_id)
        return self._run_view(row) if row is not None else None

    def _raw_run(self, run_id: str) -> dict[str, Any] | None:
        for row in store.rows(RUN_MODULE):
            if row.get("执行编号") == run_id:
                return row
        return None

    # ---------- 执行 ----------

    def execute(
        self,
        flow_id: int,
        *,
        resume_from: int | None = None,
        fail_at_step: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """按步骤序列执行一次流程，返回完成后的流程执行数据快照。

        resume_from 指定从中断处继续的步骤序号：之前已落地的回执原样复用，
        不会被重新生成或覆盖。
        """
        with self._lock:
            flow = store.find("flowline", flow_id)
            if flow is None:
                raise FlowRunError(f"采集流程 {flow_id} 不存在或已归档")

            steps = self.parse_sequence(flow.get("步骤序列"))
            if not steps:
                raise FlowRunError("流程步骤序列为空，无法执行")

            run_id = str(flow.get("流程编号") or f"FLOW-{flow_id:04d}")
            version = str(flow.get("版本号") or "v1")
            existing = self.find_snapshot(run_id)
            if existing is not None and existing.get("流程版本") != version:
                # 执行版本变了：另起一份快照，旧版本结果原样保留。
                run_id = f"{run_id}-{version}"
                existing = self.find_snapshot(run_id)
            if existing is not None:
                # 再次送交：只保留一份，直接读取已存在结果。
                return self._merged_result(existing, reused=True, flow=flow)

            done_receipts = self.list_receipts(run_id)
            next_index = int(resume_from or (len(done_receipts) + 1))
            if done_receipts:
                expected_index = len(done_receipts) + 1
                if next_index < expected_index:
                    raise FlowRunError("不允许重跑已落地的步骤，请从下一个未完成步骤继续")
                if next_index != expected_index:
                    raise FlowRunError("只能从未完成步骤继续执行")
            elif resume_from and resume_from > 1:
                raise FlowRunError("没有已落地的步骤，请从第 1 步开始执行")

            run = self._ensure_run(flow_id, run_id, version, steps, flow)
            previous_state = str(flow.get("执行状态") or "未执行")
            committed = len(done_receipts)
            try:
                for index in range(next_index, len(steps) + 1):
                    if fail_at_step is not None and index == fail_at_step:
                        # 步骤执行失败属于「执行中断」：已完成的回执保留，
                        # 下一次从未完成的这一步继续。
                        raise FlowStepInterrupted(
                            f"步骤 {index}「{steps[index - 1]}」执行失败，流程中断")
                    receipt = self._run_step(
                        flow_id=flow_id,
                        run_id=run_id,
                        version=version,
                        index=index,
                        name=steps[index - 1],
                        payload=payload or {},
                    )
                    store.rows(RECEIPT_MODULE).append(receipt)
                    committed += 1
                snapshot = self._build_snapshot(
                    flow_id, run_id, version, steps, flow, self.list_receipts(run_id))
            except FlowStepInterrupted as exc:
                # 中断只记录状态，已落地的步骤结果保留，供后续续跑复用。
                self._mark_interrupted(run, flow, previous_state, committed, index, exc)
                raise FlowRunError(str(exc)) from exc
            except Exception as exc:
                # 快照组装 / 落库失败按事务处理：本次新增回执整体回退，
                # 采集流程回到执行前状态，读取方看不到半份数据。
                self._rollback(kept=len(done_receipts))
                self._mark_interrupted(run, flow, previous_state, len(done_receipts),
                                       len(done_receipts) + 1, exc)
                raise FlowRunError(f"流程执行数据快照落库失败，已回退：{exc}") from exc

            store.rows(SNAPSHOT_MODULE).append(copy.deepcopy(snapshot))
            flow["执行状态"] = "已完成"
            flow["最近快照"] = snapshot.get("快照编号")
            flow["步骤回执数"] = len(snapshot.get("步骤回执", []))
            self._finish_run(run, snapshot)
            return self._merged_result(snapshot, reused=False, flow=flow)

    def _rollback(self, *, kept: int) -> None:
        """撤销本次执行新增的步骤回执，只保留中断前已落地的部分。"""
        rows = store.rows(RECEIPT_MODULE)
        if len(rows) > kept:
            del rows[kept:]

    def _ensure_run(
        self,
        flow_id: int,
        run_id: str,
        version: str,
        steps: list[str],
        flow: dict[str, Any],
    ) -> dict[str, Any]:
        run = self._raw_run(run_id)
        if run is not None:
            return run
        row = {
            "id": max((int(item.get("id", 0)) for item in store.rows(RUN_MODULE)), default=0) + 1,
            "执行编号": run_id,
            "流程ID": flow_id,
            "流程编号": run_id,
            "流程名称": str(flow.get("流程名称") or ""),
            "流程版本": version,
            "步骤序列": list(steps),
            "步骤总数": len(steps),
            "完成步骤数": 0,
            "执行状态": "执行中",
            "失败步骤": None,
            "失败原因": None,
            "失败处理": str(flow.get("失败处理") or ""),
            "最近快照": None,
            "开始时间": _now(),
            "更新时间": _now(),
        }
        store.rows(RUN_MODULE).append(row)
        return row

    def _run_step(
        self,
        *,
        flow_id: int,
        run_id: str,
        version: str,
        index: int,
        name: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        kind = self.step_kind(name)
        if index > 1:
            prior = [
                row for row in store.rows(RECEIPT_MODULE)
                if row.get("执行编号") == run_id
                and int(row.get("步骤序号", 0)) == index - 1
            ]
            if not prior:
                raise FlowRunError(f"步骤 {index - 1} 尚未完成，不能读取中间数据")
            last = prior[0]
            if last.get("完成状态") != "已完成":
                raise FlowRunError(f"上一步「{last.get('步骤名称')}」未完成，不能继续")
            upstream = last
        else:
            upstream = None

        credential = f"{run_id}-S{index}"
        output = {
            "凭据编号": credential,
            "来源步骤": upstream.get("凭据编号") if isinstance(upstream, dict) else None,
            "记录数": len(str(payload.get("原始数据") or "")) or 3,
            "样例": {
                "步骤": name,
                "环节": kind,
                "版本": version,
            },
        }
        return {
            "id": max((int(row.get("id", 0)) for row in store.rows(RECEIPT_MODULE)), default=0) + 1,
            "执行编号": run_id,
            "流程ID": flow_id,
            "流程版本": version,
            "步骤序号": index,
            "步骤名称": name,
            "环节类型": kind,
            "完成状态": "已完成",
            "完成时间": _now(),
            "凭据编号": credential,
            "输出目标": str(payload.get("输出目标") or f"step-{index}-output"),
            "输出结果": output,
        }

    def _build_snapshot(
        self,
        flow_id: int,
        run_id: str,
        version: str,
        steps: list[str],
        flow: dict[str, Any],
        receipts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        receipts = sorted(receipts, key=lambda row: int(row.get("步骤序号", 0)))
        if len(receipts) != len(steps):
            raise FlowRunError("步骤回执数量与步骤序列不一致，快照不完整")
        indexes = [int(row.get("步骤序号", 0)) for row in receipts]
        if indexes != list(range(1, len(steps) + 1)):
            raise FlowRunError("步骤回执顺序不连续，快照不完整")
        if any(row.get("完成状态") != "已完成" for row in receipts):
            raise FlowRunError("存在未完成的步骤回执，不能生成快照")

        return {
            "id": max((int(row.get("id", 0)) for row in store.rows(SNAPSHOT_MODULE)), default=0) + 1,
            "快照编号": f"{run_id}-SNAP",
            "执行编号": run_id,
            "流程ID": flow_id,
            "流程编号": run_id,
            "流程名称": str(flow.get("流程名称") or ""),
            "流程版本": version,
            "步骤序列": list(steps),
            "步骤总数": len(steps),
            "完成步骤数": len(receipts),
            "完成状态": "已完成",
            "生成时间": _now(),
            "输入站点": str(flow.get("输入站点") or ""),
            "输出目标": str(flow.get("输出目标") or ""),
            "失败处理": str(flow.get("失败处理") or ""),
            "步骤回执": copy.deepcopy(receipts),
        }

    def _mark_interrupted(
        self,
        run: dict[str, Any],
        flow: dict[str, Any],
        previous_state: str,
        committed: int,
        failed_index: int,
        exc: Exception,
    ) -> None:
        """中断回退：流程回到执行前的状态，只保留中断前已落地的回执。"""
        run["执行状态"] = "执行中断"
        run["完成步骤数"] = committed
        run["失败步骤"] = failed_index
        run["失败原因"] = str(exc)
        run["失败处理"] = str(flow.get("失败处理") or "")
        run["更新时间"] = _now()
        flow["执行状态"] = "执行中断" if previous_state == "未执行" else previous_state
        flow["失败原因"] = str(exc)
        flow["未完成步骤"] = int(failed_index)

    def _finish_run(self, run: dict[str, Any], snapshot: dict[str, Any]) -> None:
        run["执行状态"] = "已完成"
        run["完成步骤数"] = int(snapshot.get("完成步骤数") or 0)
        run["失败步骤"] = None
        run["失败原因"] = None
        run["最近快照"] = snapshot.get("快照编号")
        run["更新时间"] = _now()

    def _merged_result(
        self,
        snapshot: dict[str, Any],
        *,
        reused: bool,
        flow: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result = copy.deepcopy(snapshot)
        result["重复送交"] = reused
        result["采集流程"] = {
            "id": result.get("流程ID"),
            "流程编号": result.get("流程编号"),
            "状态": (flow or store.find("flowline", int(result.get("流程ID") or 0)) or {}).get("status"),
            "版本号": result.get("流程版本"),
        }
        return result

    # ---------- 落地批次回执 ----------

    def land(self, run_id: str) -> dict[str, Any]:
        """由流程执行数据快照生成落地批次回执；必须引用步骤输出与流程版本。

        同一份快照只落地一次；校验结论会同步写入存储落地的台账回执清单，
        写入与落地回执在同一把锁内完成，读取方看不到半份数据。
        """
        with self._lock:
            snapshot = self.find_snapshot(run_id)
            if snapshot is None:
                raise FlowRunError(f"流程执行数据快照 {run_id} 不存在，不能生成落地批次")
            if snapshot.get("完成状态") != "已完成":
                raise FlowRunError("流程执行数据快照未完成，不能读取半份数据")
            receipts = snapshot.get("步骤回执", [])
            if len(receipts) != int(snapshot.get("步骤总数", 0)):
                raise FlowRunError("流程执行数据快照的步骤回执不完整，不能生成落地批次")
            indexes = [int(row.get("步骤序号", 0)) for row in receipts]
            if indexes != list(range(1, len(receipts) + 1)):
                raise FlowRunError("流程执行数据快照的步骤顺序不连续，不能生成落地批次")

            existing = [
                row for row in store.rows(LANDING_MODULE)
                if row.get("执行编号") == run_id
            ]
            if existing:
                return copy.deepcopy(existing[0])

            last = receipts[-1]
            landing = {
                "id": max((int(row.get("id", 0)) for row in store.rows(LANDING_MODULE)), default=0) + 1,
                "落地批次号": f"{run_id}-LAND",
                "执行编号": run_id,
                "快照编号": snapshot.get("快照编号"),
                "流程版本": snapshot.get("流程版本"),
                "步骤引用": [
                    {
                        "步骤序号": row.get("步骤序号"),
                        "凭据编号": row.get("凭据编号"),
                        "输出目标": row.get("输出目标"),
                    }
                    for row in receipts
                ],
                "输出凭据": last.get("凭据编号"),
                "输出目标": last.get("输出目标"),
                "步骤总数": snapshot.get("步骤总数"),
                "完成状态": "已落地",
                "落地时间": _now(),
            }
            store.rows(LANDING_MODULE).append(landing)
            self._sync_validation(landing, snapshot)
            return copy.deepcopy(landing)

    def sync_validation(self, run_id: str) -> dict[str, Any]:
        """重新校验快照并把结论同步到存储落地的落地批次回执清单。"""
        with self._lock:
            snapshot = self.find_snapshot(run_id)
            if snapshot is None:
                raise FlowRunError(f"流程执行数据快照 {run_id} 不存在")
            landings = [
                row for row in store.rows(LANDING_MODULE)
                if row.get("执行编号") == run_id
            ]
            if not landings:
                raise FlowRunError("该快照尚未生成落地批次回执，无法同步校验结论")
            for landing in landings:
                self._sync_validation(landing, snapshot)
            return copy.deepcopy(landings[0])

    @staticmethod
    def validate_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
        """校验流程执行数据快照：回执完整、顺序连续、引用与版本一致。"""
        checks: list[dict[str, Any]] = []

        def check(name: str, passed: bool, detail: str) -> None:
            checks.append({"检查项": name, "通过": passed, "说明": detail})

        receipts = list(snapshot.get("步骤回执") or [])
        steps = list(snapshot.get("步骤序列") or [])
        total = int(snapshot.get("步骤总数") or 0)
        check("快照已完成", snapshot.get("完成状态") == "已完成",
              "快照状态为已完成" if snapshot.get("完成状态") == "已完成" else "快照尚未完成")
        expected = len(steps) if steps else total
        check("步骤数量一致", len(receipts) == total == expected,
              f"回执 {len(receipts)} 份 / 步骤 {total} 个")
        indexes = [int(row.get("步骤序号", 0)) for row in receipts]
        check("步骤顺序连续", indexes == list(range(1, len(receipts) + 1)),
              f"步骤序号 {indexes}")
        version = snapshot.get("流程版本")
        check("流程版本一致", all(row.get("流程版本") == version for row in receipts),
              f"流程版本 {version}")
        references_ok = all(
            bool(row.get("输出目标"))
            and isinstance(row.get("输出结果"), dict)
            and bool(row.get("凭据编号"))
            for row in receipts
        )
        check("输出引用完整", references_ok, "每份回执都引用输出目标与完成凭据")

        passed = all(item["通过"] for item in checks)
        return {
            "校验结论": "通过" if passed else "不通过",
            "通过": passed,
            "流程版本": version,
            "步骤回执数": len(receipts),
            "检查项": checks,
        }

    def _sync_validation(self, landing: dict[str, Any], snapshot: dict[str, Any]) -> None:
        conclusion = self.validate_snapshot(snapshot)
        landing["校验结论"] = conclusion["校验结论"]
        landing["校验通过"] = conclusion["通过"]
        landing["校验回执数"] = conclusion["步骤回执数"]
        landing["校验项"] = conclusion["检查项"]
        # 同步写入另一份业务台账：存储落地模块的台账清单。
        ledger = store.rows(LEDGER_MODULE)
        for row in ledger:
            if row.get("执行编号") == landing.get("执行编号"):
                row.update({
                    "校验结论": conclusion["校验结论"],
                    "校验通过": conclusion["通过"],
                    "校验回执数": conclusion["步骤回执数"],
                    "流程版本": landing.get("流程版本"),
                    "落地批次号": landing.get("落地批次号"),
                    "同步时间": _now(),
                })
                return
        ledger.append({
            "id": max((int(row.get("id", 0)) for row in ledger), default=0) + 1,
            "执行编号": landing.get("执行编号"),
            "落地批次号": landing.get("落地批次号"),
            "快照编号": landing.get("快照编号"),
            "流程版本": landing.get("流程版本"),
            "校验结论": conclusion["校验结论"],
            "校验通过": conclusion["通过"],
            "校验回执数": conclusion["步骤回执数"],
            "同步时间": _now(),
        })


flow_run_service = FlowRunService()
