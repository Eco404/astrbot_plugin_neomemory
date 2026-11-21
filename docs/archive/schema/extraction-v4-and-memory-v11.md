# 节点标题与条目内查阅合同

当前代码的生产默认为 `extraction-prompt.v11` / `extraction.v4` / `reference.v2`，派生数据库为memory.db v11。标题是长期原子、事件和主题的展示摘要；正文仍独立承载事实、覆盖判断和召回内容。设计见[ADR-0029](../../adr/0029-memory-titles-and-inline-inspection.md)，完整抽取字段见[提示词与输出协议](../../提示词与输出协议.md)。

## extraction.v4

v4继承v2的节点、关系、画像、人格关系、覆盖和no_change结构，每项node_changes增加必填非空title。它不包含独立[v3证据声明候选](extraction-v3-and-memory-v10.md)的字段，也不增加尚未实现的节点情绪字段。

```json
{
  "local_ref": "local_pottery",
  "node_type": "event",
  "operation": "create",
  "node_id": null,
  "expected_version": null,
  "title": "陶艺体验计划",
  "content": "{{user:u1}}计划下周参加陶艺体验，尚未报名。",
  "importance": 0.4,
  "source_ids": ["s1"],
  "event_time": {"start": null, "end": null, "text": "下周"}
}
```

- create和update都在既有抽取调用内提供title；更新可填写原标题，不为标题增加独立模型请求。
- 标题为1～60个语义字符：完整合法的 `{{user:<id>}}` 计一个，其余Unicode码点逐个计。引用ID最多200个码点，完整编码文本最多12540个码点；短引用还原前后使用同一规则。格式正确不能替代身份存在及作用域校验。
- 标题不能增加正文没有的事实、改变主体、否定或不确定性，不能把计划和提议写成完成。同名不表示同一事件；不使用“我记得”等包装。必要指名沿用身份占位符，明确约定称呼和原话保留。
- 标题不进入Embedding、不参与默认召回排序或注入，不用于证明事件正文覆盖了原子内容。正文不得因已有标题而省略必要限定。
- 仅调整标题时，importance保持原值，event_time=null保留既有时间；正文相同不会推进content_version，因此coverage的最终版本仍使用原值。

生产默认、执行签名、准备快照、批次回执和健康信息使用v11/v4。v4调用返回旧schema会被拒绝，不能静默降级为无标题抽取。旧v1/v2/v3仍可按明确历史版本解析；旧提示词保持原合同。已经提交的回执不重复消费，升级前未提交候选按签名规则重新准备，不改其版本标签冒充v4。

## memory.db v11

| 表 | 字段 | 含义 |
| --- | --- | --- |
| memory_nodes | title TEXT，可空 | 当前展示标题 |
| memory_node_versions | title TEXT，可空 | 该正文版本生成时保存的标题快照 |

迁移只增加可空列，不根据当前正文猜标题，不改写正文、来源、版本、覆盖、强度或向量。旧节点在列表中显示类型与短ID；旧版本title=null显示“未记录标题”。正常后续抽取、人工治理或重构可以产生新标题，不能回填旧正文版本快照。

仅标题变化不推进content_version或updated_at，不重置强度、不清除强化，不改变人工保护、覆盖及节点向量。保持updated_at避免展示性改名改变近期候选排序。正常正文变化仍推进正文版本，并在同一事务保存当时标题快照；重要性、时间等其他实际变化遵守各自既有规则，不能将混合编辑当成仅标题操作。

## 标题治理

标题字段接入既有治理任务：edit.patch.title、split.parts[].title、merge.title。拆合与正文修改继续执行原有保护和来源规则；仅标题编辑不默认增加保护。

```json
{
  "action": "edit",
  "node_id": "node_example",
  "expected_generation_id": "gen_example",
  "expected_versions": {"node_example": 1},
  "expected_updated_at": {"node_example": 1790000000},
  "expected_titles": {"node_example": null},
  "patch": {"title": "陶艺体验计划"}
}
```

expected_titles使用调用方读取的原始title，可为null，不使用昵称渲染后的display_title。edit.patch含title时必须提供目标原标题；merge含title时必须提供保留节点原标题，界面同时提交全部选中节点的原标题。检查与generation、正文版本、updated_at以及同空间互斥一同执行，防止正文版本没有变化时覆盖另一份标题修改。仅标题修改的表单不附带未变的正文与重要性。

标题独立变化使用现有memory_derivations表的operation=title_update，挂在当时正文版本上，semantic_json中保存title_before、title_after，操作记录保存时间、编辑者和治理任务。它不创建第二套标题版本表。查询接口把该字段投影为title_change，semantic对该操作返回空对象，不把标题操作展示为模型事实声明。

旧正文只有title_update而没有正文派生凭据时，provenance_status继续为unrecorded；补标题不能补造旧正文来源。原有直接引用、上下文和继承基线仍按其真实凭据展示。

## 查询与界面

- memory/nodes返回title及昵称渲染的display_title，查询可以匹配标题或正文及相关最新昵称。标题搜索属于浏览能力，不改变向量内容。
- memory/versions返回每个正文版本实际保存的title及display_title。title_update的title_change含before、after，以及对应的display_before、display_after；当前标题不能代替历史快照。
- 长期节点、画像和人格关系在所属卡片内展开版本、直接引用、同窗上下文和继承基线；基线在同一区域导航并可返回。节点累计来源单独标注，原文在对应来源入口下按需读取。
- 每条卡片独立加载、折叠、失败重试；同页折叠可复用结果。空间、分类、分页或刷新后旧页面请求失效，不能写入新卡片。短期只展示实际覆盖信息，不虚构版本链。
- 所有内容作为文本渲染，标题允许换行，不按CSS显示宽度截断存储文本。

## 验证与边界

针对性工程验证覆盖新旧库、仅标题与混合治理、自动抽取、拆合、版本快照、并发原标题检查、稳定引用及搜索；真实Chromium覆盖长列表、多个展开项、原文、基线返回、错误重试、迟到响应及移动布局。真实AstrBot路由与临时数据库验证了后台仅标题治理，实机尚未部署此能力。

两组独立合成真实模型调用中，三条标题经审阅与正文一致；私聊正文仍出现人格第三人称表达。更新标题、计划与完成、复杂条件和多模型稳定性尚未覆盖，不能把标题工程通过当成整体叙述或关系连续性已解决。证据与限制见[标题与条目内查阅验证](../../evaluations/memory-titles-inline-20261113.md)。
