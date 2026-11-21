# 持久待写的进程强杀恢复验证

日期：2026-11-11（Asia/Shanghai）。两个已提交边界各执行三次真实 `SIGKILL`，共 **6/6 通过**。隔离子进程使用生产 `IngressStore`、`SourceStore` 与 `IngressService`；没有操作实机、既有运行数据库、私人会话或模型服务。

## 契约与验证范围

[ADR-0016](../adr/0016-durable-ingress-and-context-completion.md)要求原始入库前保留独立持久待写记录，原始库提交后幂等清除待写；两个数据库不假定原子双写，同来源流保持顺序。[ADR-0015](../adr/0015-durable-maintenance-and-replay.md)要求未过期租约不能被恢复过程抢占。

既有 `test_source_commit_before_ack_crash_replays_without_duplicate` 关闭连接后重新打开数据库并推进测试时钟，其他任务恢复测试使用取消异常；这些证据覆盖逻辑恢复，未覆盖持有未关闭 SQLite 连接的进程被强杀。因此选择以下两个已有契约中的边界，不增加生产恢复机制：

| 边界 | 强杀前已提交状态 | 新进程应完成的行为 |
| --- | --- | --- |
| `ingress_committed` | 同流两条待写均为 queued；原始库为空 | 按顺序写入两条原文，确认并清空待写 |
| `source_committed_before_ack` | 第一条原始记录已提交，待写仍为 running；第二条 queued | 有效租约期间不接管；到期后幂等识别第一条，再写第二条，清空待写 |

探针父进程等待写进程的提交完成信号，确认两个数据库均有非空 WAL 文件后发送 `SIGKILL`。写进程没有机会关闭连接或运行清理；恢复进程使用不同 PID。第二个场景通过公开 `claim` 取得租约，再调用生产 `_capture`；仅在实际 `SourceStore.capture` 返回后、确认待写前插入等待屏障。恢复使用生产 `process_once()`。

测试租约为三秒，使用真实时钟等待到期，没有推进模拟时钟。生产默认三十秒租约未改动。

## 运行结果

| 检查项 | ingress 已提交 | source 已提交但未确认 |
| --- | --- | --- |
| 实际进程强杀次数与退出码 | 3次，均为 `-9` | 3次，均为 `-9` |
| 恢复前待写 / 原始记录数 | 2 / 0 | 2 / 1 |
| 未到期租约阻止接管 | 无运行中租约，不适用 | 3/3 |
| 恢复第一条 / 第二条是否新插入 | true / true | false / true |
| 恢复来源序号 | 1、2 | 1、2 |
| 最终原始记录 / 待写数 | 2 / 0 | 2 / 0 |
| 稳定来源ID与完整合成正文 | 3/3正确 | 3/3正确 |
| 两库完整性 / 外键违规 | 均为 `ok` / 0 | 均为 `ok` / 0 |

强杀前 `ingress.db-wal` 大小分别为53,592和57,712字节，`source.db-wal` 分别为90,672和152,472字节。两库均处于 WAL 模式，ingress 的 synchronous 为 FULL（2），source 为 NORMAL（1）。这些设置与证据不能推导出整机断电时的持久性保证。

运行环境为 Linux、Python 3.12.13、SQLite 3.53.4，测试起始时间 `2026-11-11T17:06:23+08:00`。运行时仓库 HEAD 为 `b1dd2a702ac472c5bda2d693a36bf4dcd9e5e967`，另包含新增探针文件；该 HEAD 不是包含探针的完整快照。探针 SHA256：`79010e0202cf7105d60f963aaa1de6d1a150f5ff6c0087abe96fd3c4e9803460`。HTTP请求数为0。脚本通过 Ruff 的 E/F/I/UP 检查（忽略 E501）。

## 复现与证据

在项目根目录执行，输出目录必须尚不存在：

```bash
/home/coder/apps/miniconda3/envs/astrbot/bin/python scripts/verify_ingress_sigkill.py \
  --output data/evaluations/ingress-sigkill-recheck \
  --repeats 3
```

脚本位于 [scripts/verify_ingress_sigkill.py](../../scripts/verify_ingress_sigkill.py)。本次聚合结果和各场景隔离数据库保留在不入Git的 `data/evaluations/ingress-sigkill-20261111/`，`report.json` 记录提交边界、WAL大小、写进程与恢复进程PID、租约检查及最终完整性结果。脚本只强杀自己启动的合成写进程。

## 结论边界

本次支持的结论是：在这两个确定的提交后边界，进程强杀后持久待写可以经真实数据库及服务恢复，保持同流顺序并避免原始记录重复。六次实验使用每流两条合成输入，不代表压力、容量或多实例验证。

尚未覆盖以下情况：

- 整机断电、内核崩溃、文件系统或硬件损坏；特别是 source 的 NORMAL 同步设置不能据此宣称断电不丢失。
- SQLite提交内部任意时刻的强杀、部分WAL写入和随机故障点。
- 完整AstrBot进程被强杀、真实平台消息采集及宿主钩子的端到端恢复。
- 记忆抽取回执提交到任务完成之间、重构切代、短期摘要回执等其他进程强杀边界；它们不能用本探针结果替代。记忆提交与重构切代已有[独立强杀验证](memory-sigkill-20261111.md)，短期摘要回执仍未覆盖。

未发现这两个边界需要新增生产修复。宿主输入截断问题仍按用户决定暂不处理，本探针不涉及该问题。
