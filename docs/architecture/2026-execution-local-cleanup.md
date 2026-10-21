# 生成任务的本地数据清理设计

状态：2026-10-21，实现与验收方案；生产部署及效果以发布验收记录为准。

长期目标：完成交付所需的短暂缓冲之外，本机不保留会话正文、参考图、生成媒体或运行凭据副本；删除不能造成重复生成、重复计费或破坏已有下载接口。

本次实现：执行私有目录即时清理，以及 canonical 终态归并后 runner 重复 payload 的持久队列回收。
客户产物仍按现有 30 分钟下载契约保留，数据库原始请求和受保护备份不在本次删除范围。
第 5、6、7 节中标明的外部存储、正文到期、长期日志/安装包策略是后续设计，不能视为已经上线。

## 1. 已核对的生产基线

本轮 SSH 只读确认 `/opt/ai-image-factory/current` 指向
`v0.1.0-20261020.hotfix.1.05a1485`。下列源码证据来自该 tag，
不是当前工作目录的旧 HEAD `22fe897`；current 路径核对不等同于本轮重新验证全部运行二进制。

- 系统盘约 40G，已用 34G，剩余 4.1G，使用率 90%。
- `executor-runners` 约 11G，主要集中在 `managed.codex.edits`；大文件中实际存在重复的 `provider-workspaces/attempt/input-*.png`。
- Codex 编辑在 `executor/codex_supervisor.rs:472-525,740` 校验并复制参考图/蒙版；`runner/process.rs:521-543` 将其写入 `provider-workspaces/attempt`。
- 完成时调用的 `cleanup_codex_runtime()` 只清 `codex-home/workspace/runtime-home`，见 `executor/codex_supervisor.rs:864`、`runner/process.rs:644-649`。这是已部署 tag 的明确覆盖遗漏，与残留路径吻合；尚未逐单证明这 11G 都满足删除条件。
- 基线已提供 `RunnerOutputRetention`：成功超过 24 小时、平台产物保留状态已为 `deleted`，且 execution/submission/job/output 全部成功、执行租约清空后，才回收 `output.bin`。本轮核对生产 `app.env` 未配置可选的 `RECONCILER_RUNNER_ROOT`；新功能部署需明确配置并核对启动日志，不能仅凭源码判断已启用。
- 现有回收已经包含 runner 排他锁、文件身份/绑定核验及 `unlinkat`/`fsync`，见 `runner/process.rs:1558-1624`。应扩充这条路径。
- 现有回收只处理 `output.bin`，不清请求正文或工作目录，也未在删除路径重新探测实际 PID/提供商进程组是否已退出；提前回收必须补齐这些条件。
- 平台 `ArtifactBlobStore` 目前只有 filesystem/memory 后端；Grok 的 S3/Qiniu 输出配置不是平台通用产物存储后端。

因此本次设计首先补齐现有清理，再把 runner 回收从客户产物 TTL 中解耦。

## 2. 删除策略与时点

这里的“会话完成”由执行、交付、结算的持久状态决定，不能由目录 mtime、CLI 进程退出或 HTTP 200 单独决定。

| 数据 | 删除条件 | 建议默认目标 |
| --- | --- | --- |
| CLI home、history、临时认证副本、工作目录、参考图、蒙版、首帧/中间图 | 提供商进程组退出；必要的结果/恢复证据已可靠提取 | 正常路径当次退出即清，通常数秒内 |
| runner 的 `output.bin`、`provider-request.json` 与含正文的诊断文件 | 对应 execution 的 canonical 终态及 reducer 完成事务已提交；有效结果另有经校验的权威副本 | 立即进入清理队列，正常负载下 60 秒内 |
| 平台 executor artifact 副本 | customer artifact 已验证持久化、对应 reduction 完成，且无其他未完成消费者 | 本次不改变已有 artifact 生命周期；单独验证消费者后再缩短 |
| 输入 session 的原始图片/视频 | 所有引用它的 job/output 已结束，不再有执行/恢复读取者 | 保留现有 60 秒安全间隔，并独立扫尾 |
| 客户可下载的最终图片/视频 | 外部权威存储已验证并记录，下载/重放可从该位置提供 | 后续方案；本次保留现有 30 分钟窗口 |
| 数据库中的原始 prompt/command JSON | parent 终态及全部子输出/恢复工作结束；请求摘要已持久化 | 后续独立 payload 到期迁移；本次不清空受保护正文 |
| 任务 ID、请求哈希、结果哈希、参数摘要、错误码、账单与幂等记录 | 独立业务/审计策略 | 保留最小元数据，不保留媒体/会话正文 |

上述时间是建议的正常运行目标，不是服务不可用时的删除保证；积压超时必须可见。
正式账号凭据与“执行时复制的凭据”分开管理：前者仍由账号生命周期控制，后者立即清理。

## 3. 正常执行流程

```text
生成/编辑过程
  -> 输出校验，写入可恢复的 output.bin 和结果标记
  -> 进程组退出，清 CLI home / 输入副本 / 会话与中间文件
  -> executor artifact 写入、回读校验、authority 落库
  -> reducer 持久化客户产物及结果、用量、结算
  -> 同一事务登记本地 payload 可清理
  -> 后台取得清理权与 runner 锁
  -> 删除 runner payload，保留原有客户下载与 artifact 生命周期
```

第一步优先统一 Codex/Grok 的临时目录清理覆盖范围：
`codex-home`、`workspace`、`runtime-home`、`provider-home`、`provider-workspaces`。
文件不存在视为已完成。只处理 execution 私有目录，不触及账号级凭据根。

保留生产 tag 已有的“先持久化结果，再清临时目录”顺序。清理失败进入待清理状态并告警，
不把已确定的生成成功改成生成失败，也不触发新一次提供商调用。

本次快速回收只接纳 canonical `succeeded` / `failed` 且 reduction 已完成的 execution。
超时/取消/未决只有按既有状态机收敛到这些受支持终态才可进入；收到取消请求不等于可删除。
取消请求必须等提供商进程/任务状态收敛，
不能把 API 收到取消请求当作进程已经退出。多输出任务按 execution 清私有副本，
共享输入及 parent payload 则等待所有引用结束。

## 4. 可重试的后台清理

扩展现有 `RunnerOutputRetention` 和 `reconcilerd`，保持一个清理实现；正常退出和后台扫尾共同调用它。
现有 24h 成功输出 GC 在迁移期间保留作后备，新路径满足更强的交接证据后即可提前清理。

最小新增持久状态为一张 `executor_local_cleanup` 表，主键为 execution ID，记录：

- execution/submission/profile、canonical decision 绑定；实际根目录来自受控配置，不能由 profile key 推测目录名。
- `state = pending | cleaning | cleaned`、`eligible_at_ms`、`next_attempt_at_ms`。
- `lease_owner`、递增 `lease_epoch`、`lease_expires_at_ms`。
- 尝试次数、固定错误码、实际删除字节数、完成时间；不存 prompt、媒体或凭据。

迁移 0135 的 trigger 在 reducer 完成事务内幂等插入清理意图，并回填已有确定终态。
复用产物保留的 DB 时钟、批量领取和 owner/epoch 校验模式，文件 I/O 放在事务外。
不复用 `job_artifact_retention` 的状态，因为客户仍可下载时 runner 副本就已经可以删除。

实现每 30 秒运行独立的后台分支，复用现有 batch-size 配置并限制单轮最多 100 条，串行删除控制 I/O。
每 8 次领取有 7 次优先新完成、1 次服务最旧 due，兼顾历史回填；已清理条目不再扫描。
错误条目按 5 秒指数退避至约 5 分钟并加少量确定性抖动。
日志报告本轮候选/删除字节/阻塞数、总待清理与最老待清理年龄。输入清理独立运行，不受其他 reconciliation 错误阻塞。

删除动作依次执行：

1. 从数据库领取清理租约，验证 canonical 终态、reduction、结果副本与引用关系。
2. 仅在执行目录所属的主机/存储命名空间工作，取得原有 `runner.lock` 排他锁。
3. 复核 helper/provider PID、启动标识和进程组已退出；DB 可用且无活跃恢复/启动权。
4. 再核对 execution/submission、root/目录/锁 inode，按白名单删除 payload 并 `fsync`。
5. 以完全相同的 owner/epoch 完成记账；失败只重试清理。删除后、写回前崩溃可安全重做。

启动准备与清理持有同一个执行锁：只有 `Prepared` 才能写认证/请求/输入；已启动或终态不再重建这些内容。
executor 在异步 prepare/observe/recover 之后再次核对持久终态，迟到观察直接返回既有结果，
不再强制读取已回收的 `output.bin` 或重复上传/调用。混跑不识别新屏障的旧版本时不启用提前回收。

既有 `PostgresExecutorOwnerGuard` 和 lane 顺序执行仍是 launch owner 的串行前提；
本次不新增同一 lease 多个 `AllowLaunch` 调用者的组件级启动协议，不能脱离该生产约束重入。

短期保留极小的 `spec/launch/terminal/lock`，维持“已启动过”的屏障；它们不含会话正文或媒体。
不删除正在被持有的锁文件，以免产生两个锁 inode。要连目录一起删除，必须先实现数据库终态屏障、
所有启动者在共享执行锁下检查该屏障，并完成旧版本/旧租约退出验证；不按天数盲删这些标记。

文件删除继续采用已打开目录描述符、`O_NOFOLLOW`、逐级身份校验和相对目录 unlink，
递归删除也不跟随符号链接或跨越挂载点；后台不得用会创建运行目录的 spool 构造函数做只读盘点。
默认不复制到同盘“回收站”，否则空间和敏感数据都没有真正减少。

## 5. 后续：本机最终媒体也不留存

当前图片幂等重放和异步视频下载仍读取平台 artifact 字节，没有“客户端已经持久保存”的 ACK。
因此先完成下列交付能力，才能对客户产物采用本地零留存：

1. 在现有 `ArtifactBlobStore` 接口下增加受控的外部对象后端，优先复用已配置的业务 S3/Qiniu，
   使用独立的 Factory 对象命名空间，不强制新增存储服务。
2. 上传时计算应用级 SHA-256 与长度；取得持久对象 key/版本和可验证的完整性结果。
   无可靠整对象校验回执时，流式回读并比对 SHA-256，不能只看 HEAD 200 或把 multipart ETag 当整文件哈希。
3. 把不可变对象引用、哈希、长度、media type 与交付状态写入结果事务，然后才将客户产物标为 ready。
4. 现有受鉴权下载和图片重放从外部后端读取，不再落本地缓存；大媒体采用有界流式读取。
   不把长期对象权限或用户提供的任意 URL 当交付凭证。
5. 数据库确认交付后回收本地字节。上传/校验失败只重试交付，不重新生成。

外部后端尚未配置的过渡期，客户产物继续遵守现有 30 分钟保留及 reader-drain 契约；
不能以全局 TTL=0 改坏已签发的异步下载。新增策略按任务创建时快照，旧任务按原契约结束。
外部临时对象的清理沿用应用侧到期/删除状态；对象存储生命周期只作漏删后备，不能承担秒级清理保证。
业务永久作品与 Factory 临时对象使用不同所有权/前缀，Factory 不删除业务已经保存的作品。

若以后加入客户端保存 ACK，必须带 artifact ID、版本/哈希和幂等身份；ACK 只能作为明确新契约的交付信号。
当前实施不依赖新增 ACK API，避免改动所有调用方。

## 6. 后续正文到期与当前失败/未决边界

`provider-request.json` 之外，数据库 `job_payloads.command_json` 也保存原始执行请求。
仅删目录不能宣称会话正文已全清。当前读取/immutable 约束必须通过迁移改为：
身份/哈希等元数据继续保留，原始正文放入可到期的独立 payload 存储；
终态后所有读取者识别 `payload_expired`，重放依赖请求哈希和结果引用，不重新生成。
实施时审计 console/provider journal 等其他正文副本及日志字段，不直接清空受保护表。

会话工作目录与确定终态的本地请求按本次策略回收；其他日志和数据库正文的全面到期还需上述后续迁移。
日志原则是不保留失败会话或整份 stdout/stderr。提取固定错误码、阶段、退出码、提供商调用标识、
请求参数摘要（模型、尺寸、时长、版本）和实际产物元数据，正文及鉴权信息不得进入日志。
需要临时诊断留存时必须显式开启、有到期时间，默认关闭；它不改变正常清理默认值。

`uncertain` 不能仅凭文件很旧就当作失败。先固化远端 task ID、必要回执及最小对账证据，
执行无新增付费的状态对账；仍被恢复读取的唯一输出不删除。可证明不再需要的上下文/输入副本则先清。
恢复依赖尚不明确的条目保留并暴露积压原因；本次输出清理统计，后续接入 15 分钟未进展告警，
也不通过自动重放、重复扣费或无证据改终态来腾空间。

## 7. 运维策略：日志、安装包和磁盘余量

本次已获授权手工执行原生日志归档到期清理，保留近期 30 天；它与软件部署后的执行器回收效果分别计量。
以下长期策略没有在应用中自动启用，也不授权随意删除安装版本或备份：

- 应用事件日志只保留元数据。建议 Factory journal 使用独立 namespace，设置 `SystemMaxUse=256M`、
  `MaxRetentionSec=7day`、`MaxFileSec=1day`、`Compress=yes`；namespace 不可用时采用 Factory 专属轮转。
  不在应用内静默改整台主机的日志策略。容量上限可能因活动 journal 的粒度略超出，不能作为整个磁盘的硬配额。
- 旧安装包/解压 staging 由 updater 按清单清理；保留 current、最近一个已验证回滚版本、
  当前 candidate/pin 引用和进行中的操作。仅在 updater 锁保护且操作完成后回收未引用副本。
  数据库备份保留策略独立，本轮设计不改变现有受保护回滚备份。
- 外部交付模式下，后续新备份排除已清理的 session scratch，并只备份元数据和必要恢复数据。
  若原始 payload 已进入旧备份，本地零留存不能自动覆盖这些历史备份，按独立备份到期策略消除。
- 空间监测优先加速已经满足条件的清理。建议剩余低于 5GiB 或使用率超过 85% 时告警；
  剩余低于 2GiB 或使用率超过 95% 时，在调用提供商/计费之前暂停领取新的媒体任务，
  继续排空在途工作。实际上线前需按最大媒体体积与并发量核算阈值和 scratch 预算。
- 不把整套媒体工作区默认放进 tmpfs；当前媒体体积与并发可能把磁盘压力变成内存耗尽。

## 8. 最小实施顺序与验收

上线顺序：完整 CI → main CI → 签名 candidate 与精确产物校验 → 正常 owner Check/Apply。
Apply 正常排空执行并完成迁移 0135、切换全部 executord/reconcilerd 新二进制后，才启用
`RECONCILER_RUNNER_ROOT=/var/lib/ai-image-factory/executor-runners`。禁止旧 writer 与新快速 GC 混跑。
保留 current/已验证回滚版本与备份；不手工改数据库或 current 指针。回滚前先停用快速 GC，
已删 payload 不恢复，但 canonical 结果、幂等及账单元数据必须仍可用。

1. 修复 Codex edit 临时输入清理，统一目录覆盖。真实 managed-edit 进程测试必须断言参考图、蒙版、
   CLI home、会话及中间媒体内容消失；只检查目录存在/不存在不够。
2. 增加持久清理意图和 reducer 完成后的快速回收，扩充既有扫描器；输入清理成为独立分支，
   避免其他 reconciliation 错误阻塞它。补充失败/取消/未决的安全策略。
3. 接入外部产物后端及正文 payload 到期迁移，使完成交付后的本地媒体和会话正文归零。
4. 对已有目录先盘点确定终态与队列状态，不读取或导出用户正文。历史 task 逐个关联数据库，
   不以文件年龄替代验证；缺目录、无进程退出证据或绑定冲突时保留待重试，不冒充已清理。
5. 正常 CI/发布后分批清理并验证：磁盘实际可用空间增加、文件无已删但仍占用的句柄、
   下载/幂等/计费行为不变、active execution 和回滚点未受影响。

必须覆盖的确定性测试：

- helper 在结果写入前后、authority 提交前后、reducer commit 前后被 SIGKILL。
- 清理时数据库/磁盘/外部存储失败、删除后尚未记 cleaned 即崩溃、重复领取与旧 epoch 写回。
- 活跃任务、锁被持有、旧 helper/attach 并发；锁 inode、根目录、子目录或 symlink 被替换。
- output 已提前回收后的 attach/恢复，不能再生成或收费；同键不同请求仍为冲突。
- 外部上传成功后 DB 失败、checksum 不符、下载中清理、客户端断连、视频稍后下载。
- 多输出任务的共享输入、成功/失败混合输出、确定失败和长期 uncertain；未知历史目录保持 blocked。

本次验收指标：正常任务临时上下文/参考图在退出时清掉；满足交付/收敛条件的 runner payload
正常负载下目标 P95 在 60 秒内消失（容量目标，不作为未测的生产保证）；原有下载/重放正常。
外部模式与数据库正文全部零留存是后续独立验收，不由本次目录清理代替；
清理故障只产生可重试的清理欠账，不产生第二次提供商调用或账单。
监测待清理字节、最老积压、各原因 blocked 数、删除失败及每类磁盘占用；生产验收不得擅自增加付费生成。

## 9. 参考依据

以下采用官方文档中的生命周期、幂等、完整性和容量控制原则；本方案的秒数、容量阈值与分层删除时点为针对 Factory 的设计选择。

- [Kubernetes：TTL after finished](https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/)：终态后开始清理，尊重生命周期完成条件。
- [AWS Builders' Library：安全的幂等重试](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/)：迟到重试仍需识别原始意图，媒体删除不应使同一请求再执行一次。
- [Amazon S3：上传完整性校验](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html)：验证内容完整性，区分整对象校验和 multipart ETag。
- [Amazon S3：对象到期](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html)：生命周期删除异步，版本对象还需处理非当前版本；不作为即时交付清理的唯一执行器。
- [systemd：tmpfiles](https://www.freedesktop.org/software/systemd/man/latest/systemd-tmpfiles.html)：可用于普通临时目录管理，但不具备 Factory 的任务、结算和幂等状态判断。
- [systemd：journald 容量和保留期](https://www.freedesktop.org/software/systemd/man/latest/journald.conf.html)：使用日志服务原生的容量和保留策略。
