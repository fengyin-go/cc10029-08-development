"""流程执行数据快照的后端验收脚本（标准库 unittest，可离线运行）。

覆盖：步骤回执顺序、快照事务回退、幂等送交、落地回执引用、校验结论同步、
状态单向流转以及并发落地与开始事件冲突。

运行：cd backend && .venv/bin/python -m unittest tests.test_flowrun -v
"""
from __future__ import annotations

import threading
import unittest

from app.seed import SEED_ROWS
from app.services import flowrun as flowrun_module
from app.services.flowline import FlowlineService
from app.services.flowrun import FlowRunError, FlowRunService
from app.store import store


def reset() -> None:
    """每个用例从种子数据重建仓库，避免用例之间共享内存状态。"""
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    for flow in store.rows("flowline"):
        flow["执行状态"] = "未执行"
        flow["最近快照"] = None
        flow.pop("失败原因", None)
        flow.pop("未完成步骤", None)
        flow.pop("步骤回执数", None)


class FlowRunTests(unittest.TestCase):
    def setUp(self) -> None:
        reset()
        self.service = FlowlineService()

    # 1. 快照把各步骤完成凭据按顺序串起来
    def test_snapshot_has_sequential_receipts(self) -> None:
        snapshot, message = self.service.execute(1)
        self.assertIsNotNone(snapshot)
        self.assertEqual(message, "流程执行数据快照已生成")
        self.assertEqual(snapshot["完成状态"], "已完成")
        self.assertEqual(snapshot["步骤序列"], ["站点采集", "字段提取", "结果落地"])
        receipts = snapshot["步骤回执"]
        self.assertEqual([r["步骤序号"] for r in receipts], [1, 2, 3])
        self.assertEqual([r["凭据编号"] for r in receipts], ["FLOW-0001-S1", "FLOW-0001-S2", "FLOW-0001-S3"])
        self.assertEqual([r["环节类型"] for r in receipts], ["站点采集", "字段提取", "结果落地"])
        self.assertTrue(all(r["输出目标"] and r["输出结果"] for r in receipts))
        # 下一步读取的是上一步的完成凭据，而不是未完成中间数据
        self.assertEqual(receipts[1]["输出结果"]["来源步骤"], "FLOW-0001-S1")
        self.assertEqual(receipts[2]["输出结果"]["来源步骤"], "FLOW-0001-S2")
        self.assertTrue(all(r["流程版本"] == "v1" for r in receipts))

    # 2. 再次送交只写入一份，后续请求读取已存在结果
    def test_repeat_submission_is_idempotent(self) -> None:
        first, _ = self.service.execute(1)
        second, _ = self.service.execute(1)
        self.assertFalse(first["重复送交"])
        self.assertTrue(second["重复送交"])
        self.assertEqual(first["快照编号"], second["快照编号"])
        self.assertEqual(len(store.rows("flow_snapshot")), 1)
        self.assertEqual(len(self.service.list_receipts("FLOW-0001")), 3)

    # 3. 中断后从未完成步骤继续，已落地结果不被覆盖
    def test_resume_from_interrupted_step_keeps_receipts(self) -> None:
        snapshot, message = self.service.execute(2, fail_at_step=2)
        self.assertIsNone(snapshot)
        self.assertIn("中断", message)
        first_step = self.service.list_receipts("FLOW-0002")
        self.assertEqual(len(first_step), 1)
        self.assertEqual(first_step[0]["凭据编号"], "FLOW-0002-S1")
        self.assertEqual(self.service.get_run("FLOW-0002")["执行状态"], "执行中断")

        # 不允许重跑已落地的第 1 步
        replayed, message = self.service.execute(2, resume_from=1)
        self.assertIsNone(replayed)
        self.assertIn("不允许重跑", message)
        self.assertEqual(self.service.list_receipts("FLOW-0002")[0]["凭据编号"], "FLOW-0002-S1")

        snapshot, _ = self.service.execute(2, resume_from=2)
        receipts = snapshot["步骤回执"]
        self.assertEqual([r["凭据编号"] for r in receipts], ["FLOW-0002-S1", "FLOW-0002-S2"])
        self.assertEqual(len(self.service.list_receipts("FLOW-0002")), 2)
        self.assertEqual(len(store.rows("flow_snapshot")), 1)

    # 4. 快照落库失败时整体回退，流程状态回退，落地回执不读半份数据
    def test_snapshot_failure_rolls_back(self) -> None:
        original = flowrun_module.FlowRunService._build_snapshot

        def boom(*args, **kwargs):
            raise RuntimeError("模拟快照落库失败")

        flowrun_module.FlowRunService._build_snapshot = boom
        try:
            snapshot, message = self.service.execute(1)
            self.assertIsNone(snapshot)
            self.assertIn("回退", message)
        finally:
            flowrun_module.FlowRunService._build_snapshot = original

        self.assertEqual(self.service.list_receipts("FLOW-0001"), [])
        self.assertEqual(store.rows("flow_snapshot"), [])
        self.assertEqual(store.find("flowline", 1)["执行状态"], "执行中断")
        landing, message = self.service.land("FLOW-0001")
        self.assertIsNone(landing)
        self.assertIn("不存在", message)
        self.assertEqual(store.rows("landing_receipt"), [])

    # 5. 落地批次回执引用步骤输出与流程版本，且只生成一次
    def test_landing_receipt_references_outputs_and_version(self) -> None:
        self.service.execute(1)
        landing, message = self.service.land("FLOW-0001")
        self.assertIsNotNone(landing)
        self.assertEqual(message, "落地批次回执已生成")
        self.assertEqual(landing["流程版本"], "v1")
        self.assertEqual(landing["输出凭据"], "FLOW-0001-S3")
        self.assertEqual([ref["凭据编号"] for ref in landing["步骤引用"]],
                         ["FLOW-0001-S1", "FLOW-0001-S2", "FLOW-0001-S3"])
        self.assertEqual([ref["输出目标"] for ref in landing["步骤引用"]],
                         ["step-1-output", "step-2-output", "step-3-output"])

        again, _ = self.service.land("FLOW-0001")
        self.assertEqual(again["id"], landing["id"])
        self.assertEqual(len(store.rows("landing_receipt")), 1)

    # 6. 校验结论同步到存储落地台账
    def test_validation_syncs_to_storage_ledger(self) -> None:
        snapshot, _ = self.service.execute(1)
        self.assertEqual(self.service.validate("FLOW-0001")["校验结论"], "通过")
        self.service.land("FLOW-0001")
        ledger = flowrun_module.flow_run_service.list_ledger()
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger[0]["校验结论"], "通过")
        self.assertEqual(ledger[0]["校验回执数"], len(snapshot["步骤回执"]))
        self.assertEqual(ledger[0]["流程版本"], "v1")

    # 7. 采集流程状态按顺序推进，完成后不能回到旧环节
    def test_status_flow_is_one_way(self) -> None:
        entry, message = self.service.run_action(1, "提交校验")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["status"], "待校验")
        entry, _ = self.service.run_action(1, "启用配置")
        self.assertEqual(entry["status"], "已启用")
        back, message = self.service.run_action(1, "提交校验")
        self.assertIsNone(back)
        self.assertIn("不能", message)
        entry, _ = self.service.run_action(1, "停用配置")
        self.assertEqual(entry["status"], "已停用")
        blocked, message = self.service.run_action(1, "启用配置")
        self.assertIsNone(blocked)

    # 8. 落地回执与下一步开始事件并发到达，最终只读一份顺序连续的快照
    def test_concurrent_land_and_next_start_yield_single_snapshot(self) -> None:
        runs = FlowRunService()
        results: list[object] = []
        errors: list[object] = []

        def start_next() -> None:
            try:
                results.append(runs.execute(1))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        def land_batch() -> None:
            try:
                results.append(runs.land("FLOW-0001"))
            except FlowRunError as exc:
                errors.append(exc)

        threads = [threading.Thread(target=start_next), threading.Thread(target=land_batch)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        if errors:
            # 落地先到、快照未生成时只允许失败，不允许读到半份数据
            self.assertTrue(all(isinstance(err, FlowRunError) for err in errors))
        snapshots = store.rows("flow_snapshot")
        self.assertEqual(len(snapshots), 1)
        receipt_indexes = [r["步骤序号"] for r in snapshots[0]["步骤回执"]]
        self.assertEqual(receipt_indexes, [1, 2, 3])
        self.assertEqual(snapshots[0]["完成状态"], "已完成")

    # 9. 已有运行时并发落地只生成一份落地回执
    def test_concurrent_landing_single_receipt(self) -> None:
        runs = FlowRunService()
        runs.execute(1)
        outputs: list[dict] = []
        threads = [threading.Thread(target=lambda: outputs.append(runs.land("FLOW-0001")))
                   for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(outputs), 5)
        self.assertEqual(len({item["id"] for item in outputs}), 1)
        self.assertEqual(len(store.rows("landing_receipt")), 1)
        self.assertEqual(len(store.rows("storage_ledger")), 1)

    # 10. 未完成的快照不能落地
    def test_incomplete_snapshot_cannot_land(self) -> None:
        snapshot, _ = self.service.execute(2, fail_at_step=2)
        self.assertIsNone(snapshot)
        landing, message = self.service.land("FLOW-0002")
        self.assertIsNone(landing)
        self.assertIn("不存在", message)
        self.assertEqual(store.rows("landing_receipt"), [])

    # 11. 停用的流程不能再次执行
    def test_disabled_flow_cannot_run(self) -> None:
        self.service.run_action(3, "停用配置")
        snapshot, message = self.service.execute(3)
        self.assertIsNone(snapshot)
        self.assertIn("停用", message)

    # 12. 流程版本变化后另起快照，旧版本结果保留且新快照仍连续
    def test_version_change_creates_new_snapshot(self) -> None:
        first, _ = self.service.execute(1)
        store.find("flowline", 1)["版本号"] = "v9"
        second, _ = self.service.execute(1)
        self.assertNotEqual(first["快照编号"], second["快照编号"])
        self.assertEqual(second["流程版本"], "v9")
        self.assertEqual([r["流程版本"] for r in second["步骤回执"]], ["v9", "v9", "v9"])
        self.assertEqual(len(store.rows("flow_snapshot")), 2)
        self.assertEqual(self.service.list_snapshots(1)[0]["步骤回执"][0]["流程版本"], "v1")


if __name__ == "__main__":
    unittest.main()
