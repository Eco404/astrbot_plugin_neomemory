# 主题候选生产路径与弱关联复核

四个合成场景分别复用 44 段 BGE-M3 真实向量。每组选择旧排序或直接调用生产候选入口，经过真实临时来源库、记忆库、预算裁剪与 v7 提示词构建。旧排序对照在单页固定图上计算含待整理节点的相似度。两种排序共用生产预算装配。来源分别采用逐节点摘录和区分用户、人格发言的合成对话。256000 和 18000 字节两档共 32 份最终请求，逐字复现此前的准备结果。[窗口对照](topic-neighbor-window-study.md)

高频第三方新闻与两项陪伴约定同时进入一批时，新排序在两种预算下都将相关的旧约定从 0/2 补至 2/2。区分说话者的默认预算下，两臂均有 30 个可见节点，请求从 43021 增至 43894 字节。两个待整理节点共同命中一项旧约定时，两臂均仅保留一份。

分页压力检查沿用分说话者三话题用例。在 39 条历史前按节点 ID 插入 260 条近义干扰，复用 31 种独立新闻文本。加上 3 个待整理节点，SQLite 共 302 项事件，两条目标旧约定都位于第三页。默认和 18000 字节两档准备出的输入分别含 30、11 个节点，两档旧排序均命中 0/2，新排序均命中 2/2。该图人为放大了近义新闻密度，用于验证候选跨页后仍能入选。

## 弱关联的实际模型输出

两项边界场景在调用前固定预期成员。对于双方约定先听完、后来确实照做的情况，应只用这两项事件建主题。对于各自独立的字幕新闻、临时借伞，预期跳过主题。额外归入候选的共读约定、按时归还字典均缺乏持续关联。两个场景均采用分说话者来源和默认预算。使用 gpt-6-luna、gpt-5.6-luna 各生成两臂输出，8 份首次请求全部通过 schema。每个条件仅观察一次。

| 场景 | 模型 | 含自身的旧排序 | 生产排序 |
| --- | --- | --- | --- |
| 已有约定与实践同在当前批次 | gpt-6-luna | 漏掉倾听主题 | 只生成倾听主题，成员正确 |
| 同上 | gpt-5.6-luna | 生成倾听主题，并将 28 条彼此独立的字幕新闻误归为一个主题 | 只生成倾听主题，成员正确 |
| 临时经历，无跨事件关联 | gpt-6-luna | 无主题 | 无主题 |
| 同上 | gpt-5.6-luna | 将 29 条独立字幕新闻误归为一个主题 | 无主题 |

在这两个场景中，生产排序额外选入的弱邻居都没有被写成主题成员，正文也没有借用其事实。旧排序的一份倾听概况用了“我记得”的包装。模型具有随机性，单个图上的结果不足以估算日常误建率。该组验证到主题提案和可见成员，实机持续运行、实际回答及其他 Embedding 模型需另行评估。

## 复核

在插件根目录使用 conda astrbot 环境和未占用的输出目录。两个判据清单的 SHA-256 前缀分别为 `fe0c58ae1af8`（当批已有约定与实践）、`8fe6f0cfe36a`（无关联）。原始输出保存在本地 `data/evaluations/topic-neighbor-production-model-*-20261119/`。

```bash
export PYTHONPATH=.:../AstrBot
windows=data/evaluations/topic-neighbor-attributed-repeat
python -m scripts.probe_topic_neighbors --attributed-sources --output "$windows" \
  --vectors data/evaluations/topic-neighbor-window-20261119/vectors.json
for scenario in already-in-batch unrelated; do
  inputs="$windows/$scenario/256000"
  output_dir="data/evaluations/topic-neighbor-model-repeat-$scenario"
  python -m scripts.probe_topic_neighbor_generation "$inputs" "$output_dir" --case "$scenario" --freeze
  python -m scripts.probe_topic_neighbor_generation "$inputs" "$output_dir" --case "$scenario"
done
```
