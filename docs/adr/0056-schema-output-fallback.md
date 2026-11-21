# ADR-0056 抽取优先使用 JSON Schema

长期抽取和主题整理默认请求 JSON Schema。缺少兼容的宿主接口，或上游明确拒绝这一输出格式时，使用同一 Provider 和原提示词按文本生成。格式协商共用窗口截止时间和并发名额，返回的候选继续经过同一套本地校验。其他请求错误和候选纠正沿用各自的处理流程。显式选择文本或 JSON 对象时使用所选格式。

本项决策替代 [ADR-0035](0035-format-recovery-and-task-deletion.md) 中关于默认输出格式和 JSON Schema 兼容性的约定。
