# memory.db v5：任务、派生游标与治理凭据

日期：2026-10-29。基于v4追加迁移，source.db保持v1，cache.db保持v1。启动时自动升级；不自动降级schema。

## 新增权威数据

| 表 | 关键内容 | 生命周期和用途 |
| --- | --- | --- |
| memory_jobs | job_id、kind、scope_key、唯一idempotency_key、状态、payload/checkpoint/result JSON、步骤和进度、尝试次数、退避、owner/token/lease_until、安全错误码 | 排队、恢复、取消和人工重试；成功后清空checkpoint |
| derived_cursors | generation_id → scan_cursor | 与抽取batch receipt同事务推进，不能放可清空缓存 |
| pipeline_state | state_key → 有界JSON | 来源空间发现游标；不是业务成功凭据 |
| rebuild_runs | job_id → 空间、parent generation、target generation | 固定重构任务只能写自己的building代 |
| rebuild_receipts | job_id、两端generation、结果和提交时间 | 切代原子凭据，恢复时避免二次切换 |
| governance_receipts | job_id、空间/代、操作/操作者/原因、输入摘要、结果引用、提交时间 | 事务化治理审计与幂等凭据；结果引用节点ID/正文版本，不复制正文 |

正文、来源、参与者、contains关系、coverage继续使用已有规范化表。任务保存必要的来源/节点引用和单份有界治理命令（幂等请求只存其摘要）；未完成窗口的有界模型输出属于恢复和人工审查材料，成功后从检查点清除。临时相似度、聚类中心与扫描拼接内容不入权威库。

## 写权与事务

任务状态为queued/running/retry_wait/failed/succeeded/cancelled。queued、running、retry_wait占用作用域；`scope_key=space_id`只排斥同空间，`*`排斥全部活动任务。提交、人工重试都重新检查冲突。

每次claim生成独立lease token。抽取、治理、重构和检查点保存在事务中检查running状态、token、期限与scope。取消先撤销写权；正常关闭取消worker并将仍持有的任务释放回queued；进程崩溃由租约过期恢复。模型网络请求始终在事务外。

抽取成功时，节点/画像/关系、extraction_batches、extraction_batch_sources、活动轮次与derived_cursors一起提交。任务finish在后面执行；中间崩溃时先查batch receipt，不能按新round重新抽取。召回强化不改变模型输入，不纳入新持久任务的恢复摘要。

失败任务释放互斥锁；当前代的未处理extract/rebuild/reject失败会暂停该空间自动推导。旧代失败仍可查阅，但不阻挡新代。fresh重新准备和人工拒绝成功后，将原失败任务改为cancelled并保留诊断。人工拒绝只能消费当前代尚未提交的失败抽取窗口，不能撤销已提交内容。

## 重构与清理边界

重构载荷记录每条来源流的sequence高水位，按录入顺序回放。building代完成后，在一个事务内退休旧代、激活新代、删除本空间全部退休代的派生内容与版本历史、写入rebuild receipt。新记录从v1开始，回放中的更新正常递增。笔记独立保留。失败或切换前取消时，旧代继续服务；新消息一直写入source.db，在新代激活后按新游标处理。

退休代仅保留轻量代标识、任务及切换凭据以维持外键与幂等恢复；成功重构后旧记忆不可回切，具体清理范围见[ADR-0051](../../adr/0051-rebuild-discards-retired-content.md)。被放弃的building/failed代可随所属非运行任务删除，已完成任务也可批量清理；原始来源不自动清理。升级不触发旧代内容删除，清理在该空间成功重构时执行。

来源目前只追加，scan_cursor使用SQLite rowid；来源表不能在游标使用期间VACUUM或重写。后续原始清理需要先迁移成稳定录入序号。后补身份才可归属、且位于已消费游标之前的来源通过重构回填。

## 治理与缓存

治理携带expected_generation_id、expected_versions及expected_updated_at。人工修改/拆合默认保护；解除保护显式进行。删除为派生墓碑：保留正文版本和原始来源，不能通过restore恢复；restore只将disabled恢复为active。

人工纠错影响的父摘要标记metadata.summary_stale，排除召回、最终校验、强化和自动抽取上下文。人工确认/修订正文可解除。正文版本或状态变化使旧向量自然失效，治理完成后另按空间/节点清理旧缓存；缓存故障不回滚已提交治理。

index_nodes与retrieval_snapshot共享active+historical节点的有界全集，按importance DESC、updated_at DESC、node_id排序，再对被动召回过滤historical。默认最多2000个节点，超过返回truncated；高优先级historical节点可能减少被动候选数量。索引与查询不会采用两个不同截断顺序。扩大规模需后续专门索引方案。
