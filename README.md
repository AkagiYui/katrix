# Katrix

> 纯 Go 实现的 Matrix 协议 homeserver（服务端）。单二进制、0 cgo、自带 Web 聊天 / 管理面板，对齐 Matrix Client-Server / Server-Server 规范（v1.19）。
>
> 参考实现：[Synapse](https://github.com/element-hq/synapse)（官方 homeserver）、[synapse-admin](https://github.com/Awesome-Technologies/synapse-admin)（管理面板）。

Katrix 是一个从零实现的、尽量贴近 [Matrix 规范](https://spec.matrix.org/v1.19/) 的 homeserver，用纯 Go 编写（`CGO_ENABLED=0`），前端经 `go:embed` 打包进同一个二进制。前端构建产物以 Vite 输出到 `internal/webui/dist`，运行时由 Go 静态服务，API 路由前缀（`/_matrix`、`/_synapse`、`/.well-known`）优先，未命中路径回退 `index.html`。

---

## 目录

- [功能总览](#功能总览)
- [系统设计](#系统设计)
  - [cmd 与 go:embed 约束](#cmd-与-goembed-约束)
  - [版本编号关系](#版本编号关系)
  - [房间版本与状态解析](#房间版本与状态解析)
  - [联邦（Server-Server API）](#联邦server-server-api)
  - [E2EE（服务端中转）](#e2ee服务端中转)
  - [媒体处理策略](#媒体处理策略)
  - [Web 前端](#web-前端)
  - [核心子系统](#核心子系统)
  - [主要风险](#主要风险)
- [如何使用](#如何使用)
- [测试](#测试)
- [已知遗留（未在本阶段完成）](#已知遗留未在本阶段完成)

---

## 功能总览

Katrix 按阶段（P0–P8）实现，每个阶段都自带测试并通过 GitHub Actions CI。

| 模块 | 实现范围 |
|---|---|
| **账户（P1）** | 注册（含 guest）、登录、登出、登出全部、access/refresh token 轮换、`/whoami`、改密、停用、设备管理（列表/查询/改名/删除）、用户资料（昵称/头像）。UIA（m.login.dummy / m.login.password / registration_token，支持单请求完成无参流程）、guest 中间件、soft_logout、登录时序侧信道防护。 |
| **房间核心（P2）** | `createRoom`、join/leave/invite/kick/ban/unban、`send`、`state` 读写、`event`、`members`、`joined_members`、`aliases`、`messages` 分页、`redact`、`typing`、`forget`、目录别名 CRUD。事件授权规则（m.room.create/member/power_levels/join_rules，含 v12 creator 特权）、power levels、成员状态机、knock 支持。 |
| **Sync（P3）** | `/sync` 全量 + 增量、long-poll（经 Notifier 唤醒）、typing 临时状态、receipts、account_data（全局 + 房间）、to-device 派发。 |
| **媒体 + UI（P4）** | 上传/下载/缩略图（v1 + v3 端点）。纯 Go 缩略图（JPEG/PNG/GIF/WebP 解码，JPEG/PNG 输出，scale/crop，DB 缓存）。远程媒体拉取（经联邦 `GET /media/v3/download`，X-Matrix 签名，懒拉取 + 本地缓存，缩略图支持远程）。React 19 + Vite 8 SPA（登录/注册/建房/聊天/实时 sync 循环）。 |
| **联邦（P5）** | 远端 server key 拉取 + Postgres 缓存、`/send/{txnId}`（去重 + PDU 入库 + 入站逐事件 ed25519 签名验证）、`/event`、`/state`、`/state_ids`（auth chain 递归遍历）、`/backfill`、`/get_missing_events`、`/make_join` + `/send_join`、`/make_leave` + `/send_leave`、`/invite`、`/event_auth`。出站 federation Client、状态解析 v2 接入 ingest 路径（入站状态事件入库后经 `eventstate.Maintain` 维护每事件 state-at-event 快照并基于 `forward_extremities` 重算 `room_state`，取代盲目 UpsertState）。**TLS**：联邦端口支持 HTTPS（`federation_tls.cert_path/key_path` 配置 + `katrix gencert` 子命令用 CA 签发 leaf 证书）。 |
| **房间版本回填（P6）** | 旧事件格式（v1–v2：显式 event_id 字段、`[id, hash]` prev/auth）经 `Builder.BuildLegacy`；状态解析 v1（前向时序）。roomver 规则表覆盖 v1–v12。v12 room ID = create reference hash 派生（`BuildInitialEvents` 省略 room_id，`roomver.Default` = 12）。 |
| **状态解析（核心）** | `stateres.ResolveV2`：规范的出度模型反向拓扑 Kahn 排序（祖先优先），**发送者 power-level 为主排序键**（从候选事件的 auth_events 解析 m.room.power_levels 的 users map；v12 creator 无限 power = 2^53），mainline depth 对齐 Synapse（最老祖先 depth=1，head depth=len，无匹配 depth=0）。v1 前向时序。 |
| **E2EE 中转 + 客户端 Megolm（P7）** | 服务端：`keys/upload`（device + one-time + fallback）、`keys/query`、`keys/claim`（原子消费 OTK）、`keys/changes`、`sendToDevice`（经 /sync 派发后删除，`*` 通配扇出）、cross-signing、**密钥备份**（`/room_keys/*`）。服务端不做加密，只中转。**前端**：`@matrix-org/olm`（libolm WASM）完整集成 — Ed25519/Curve25519 设备密钥（canonical JSON 签名 + keys/upload）、one-time/fallback keys、IndexedDB 持久化、Megolm 出站会话（m.megolm.v1.aes-sha2 加密 m.room.message → m.room.encrypted）、Megolm 入站会话（room key 导入 + 解密）、Olm 1:1 会话（keys/claim + sendToDevice m.room_key 分发）、to-device m.encrypted 解密处理、keys/query 设备密钥缓存。 |
| **补全（P8）** | push rules（默认规则集 + 增删改查）、filters、`publicRooms`、`preview_url`（OpenGraph 解析 + SSRF 防护：DNS 解析后 IP 段校验 [loopback/私网/link-local/CGNAT/metadata/IPv6 ULA/TEST-NET] + 连接时校验防 DNS rebinding + 重定向/大小/超时限制）、admin API（whois / 停用 / 改密 / 用户列表 / 房间列表 / 统计，按 `users.admin` 鉴权）。 |
| **性能与可观测性** | `forward_extremities` 表 + `InsertEvent` 维护 extremity 集（/sync 增量 delta 无需全量重扫）；`database.max_conns/min_conns` 配置项；`internal/metrics` 依赖-free Prometheus `/metrics` 端点（Go runtime + katrix events/sync/federation/media 计数器）。 |
| **Web 面板扩展** | TanStack Router（`/` 聊天 / `/devices` 设备与 E2EE / `/admin` 管理面板）；shadcn 风格 UI 原语（Button/Input/Card/Badge/Table + `cn()` + 设计 token CSS）；管理面板（统计卡片、用户停用/改密、房间列表，对齐 synapse-admin）；E2EE 设备密钥自举 + Megolm 加解密全链路。 |
| **登录增强** | `login_tokens` 表 + `POST /login/token` 铸造单次令牌；`/login` 增 `m.login.token` 流程；`/versions` 声明 `org.matrix.msc3886`；`SendToDevice` `*` 通配扇出（打通 `m.secret.send`）；`m.key.verification.*` 透传。 |
| **部署** | 生产 `Containerfile`（alpine 多阶段、0cgo 静态、非 root、healthcheck 子命令）；`Containerfile.complement`（Complement 测试镜像，内置 Postgres per-server 集群 + entrypoint，federation TLS 自动签发）。子命令：serve / healthcheck / genkey / **gencert** / version。 |
| **CI/CD** | `ci.yml`（Go build/vet/gofmt/race test + Web Vite build）；`release.yml`（多架构二进制 + GHCR 镜像，打 tag 触发）；`complement.yml`（官方 Complement 黑盒测试 [核心 + MSC 用例]，federation TLS 就绪，结果在 job summary 报告 PASS/FAIL/SKIP）。 |

### 硬约束

- **0 cgo**：所有依赖纯 Go（pgx、bcrypt、x/image）。CI 用 `CGO_ENABLED=0` 构建产物二进制；race 测试单独开 cgo。
- **单二进制**：源码多包，`go build` 产出一个可执行文件，前端 `go:embed` 进二进制。
- **可 distroless/alpine 部署**：镜像内不依赖外部程序。

---

## 系统设计

### 目录结构

```
katrix/
├── cmd/katrix/main.go          # 入口：serve / healthcheck / genkey / gencert / version
├── internal/
│   ├── config/                 # YAML + env 配置（含 federation_tls）
│   ├── httpserver/             # 顶层路由装配 + SPA fallback
│   ├── csapi/                  # Client-Server API handlers
│   ├── federation/             # Server-Server API（key/发现/PDU/EDU/join/状态解析接入）
│   ├── media/                  # 内容仓库（上传/下载/缩略图/URL preview/远程媒体拉取）
│   ├── rooms/                  # 房间状态机、成员、事件授权规则
│   ├── roomver/                # v1–v12 房间版本规则表
│   ├── stateres/               # 状态解析 v1 / v2（含 sender power-level 主排序键）
│   ├── eventstate/            # 每事件 state-at-event 快照 + 基于 extremity 重算 room_state
│   ├── events/                 # 事件模型、canonical JSON、hashing、redaction、签名
│   ├── crypto/                 # ed25519 签名/验签、密钥管理
│   ├── sync/                   # /sync 引擎（Token、TypingTracker、Response 构建）
│   ├── storage/                # Postgres（pgx）+ 迁移；接口化
│   ├── homeserver/             # 共享状态容器（config/store/key/notifier + 认证中间件）
│   ├── httpx/                  # Matrix 标准错误码 + JSON 响应
│   ├── ids/                    # user/room/alias/device id 解析与生成
│   ├── netutil/ssrf/           # URL preview SSRF 防护
│   ├── metrics/                # Prometheus /metrics 端点
│   ├── testdb/                 # 测试用 Postgres advisory lock（跨进程隔离）
│   └── webui/                  # //go:embed all:dist
├── web/                        # 前端源码（pnpm + Vite + React 19 + libolm WASM）
│   └── src/lib/{e2ee,olm-init,canonical-json,matrix}.ts
├── .github/workflows/          # ci / release / complement
├── Containerfile               # 生产镜像（alpine）
├── Containerfile.complement    # Complement 测试镜像（含 federation TLS 自动签发）
└── README.md
```

### `cmd/` 与 `go:embed` 约束

`go:embed` 只能嵌入指令所在包目录及其子目录，不能用 `..` 向上引用。因此 `cmd/katrix/main.go` 无法直接 embed `web/` 的产物。解决方案：

- 前端源码在 `web/`，Vite 的 `build.outDir` 指向 `internal/webui/dist`。
- `internal/webui/embed.go` 用 `//go:embed all:dist`（`all:` 前缀确保包含 `_` / `.` 开头的产物）。
- `cmd/katrix/main.go` import `internal/webui`，取 `fs.Sub(webui.Dist, "dist")` 作为静态资源根。

### 版本编号关系

Matrix 里有几套相互独立的版本编号，实现时必须分清：

- **规范发布版本（spec release，`v1.x`）**：整份规范的发布快照，当前最新 **v1.19**。一次性覆盖所有子 API。
- **房间版本（room version，`1`–`12`）**：每个房间独立、基本不可变，写在 `m.room.create` 的 `room_version`。固定该房间的：事件格式、event ID 计算方式、auth 规则、redaction 规则、状态解析算法。与 spec 发布号正交。关键分界：v1–v2 最老事件格式（event ID 为独立字段）+ v1 状态解析；v3 event ID = 事件哈希（MSC1659）；v4–v5 URL-safe base64 event ID + 签名密钥有效期；v6–v10 auth 收敛 + knocking + restricted join + 整数 power level（当今网络主流）；v11 redaction 算法澄清；v12 room ID = create 事件哈希（MSC4291）+ 创建者无限 power（MSC4289）+ 状态解析 v2.1。
- **联邦"版本"**：没有全局联邦协议版本号。兼容性由三样承担：端点路径版本（per-endpoint `v1`/`v2`，如 `/_matrix/federation/v1/...`、`send_join`/`invite`/`send_leave` 有 `/v2`、密钥服务 `/_matrix/key/v2/...`）、房间版本（两台服务器能否就某房间达成一致取决于是否都实现了该 `room_version`）、发现 + 密钥（`.well-known/matrix/server` 或 SRV + `/_matrix/key/v2/server`）。`GET /_matrix/federation/v1/version` 只返回软件名/版本（信息性），不是协议协商。

**`/versions` 声明策略**：`GET /_matrix/client/versions` 只声明已实现并通过 CI 的版本号（当前到 `v1.19` + 历史 `r0.6.1`），未完成特性走 `unstable_features`，不虚报。实测（Complement）与自报保持一致。

### 房间版本与状态解析

- **房间版本规则表**（`internal/roomver`）：以数据表方式描述每个版本的差异开关（event ID 格式、auth 规则集、redaction 规则、是否 room-ID-as-hash、状态解析版本等），供其他模块查询。
- **状态解析三套**（`internal/stateres`）：
  - **v1**（房间 v1）：前向时序算法。
  - **v2**（房间 v2–v11）：出度模型反向拓扑 Kahn 排序（祖先优先）+ mainline tie-break。Kahn 选点的**主排序键为发送者 power-level**（从候选事件的 auth_events 解析 m.room.power_levels 的 users map，v12 creator 无限 power = 2^53），tie-break 为 origin_server_ts + event_id。
  - **v2.1**（房间 v12，含 MSC4289 创建者 power）：v2 基础上 creator 无限 power 经 SenderPowerLevel 表达（`stateres.MaxCreatorPowerLevel` = 2^53，对齐 Synapse `CREATOR_POWER_LEVEL`），无需单独代码路径。
  - **mainline depth** 对齐 Synapse：最老祖先 depth=1，head depth=len(mainline)，无匹配 depth=0，升序排序。
- **canonical JSON + 签名**（`internal/canonicaljson` + `internal/events`）：字节级一致是联邦互通的隐形地雷。键按 Unicode code point 排序、无空白、整数渲染无小数点、禁止浮点、UTF-8 原样、仅强制转义。content hash + reference hash 按 room version 派生 event ID；redaction 算法按版本分支（v11+ `UpdatedRedaction` 修剪 origin/membership/prev_state、保留 create 全内容、保留 m.room.member.third_party_invite.signed、保留 m.room.redaction.redacts）。
- **状态快照**（`internal/eventstate`）：每事件入库时维护一份 state-at-event 快照（`event_state_snapshots` 表）：0 prev（create）= `{}` + 自身 tuple；1 prev = 拷贝 prev 快照；>1 prev（merge）= `stateres.Resolve` 合并各 prev 快照。`room_state` 由对所有 `forward_extremities` 的快照做 `stateres.Resolve` 重算（单 extremity 直接用其快照），统一取代各调用点的盲写 `UpsertState`，正确处理深层 fork/merge。

### 联邦（Server-Server API）

- **签名与密钥**（`internal/crypto`）：ed25519（stdlib）；`/_matrix/key/v2/server` 发布本服务器公钥（自签名），验证对端签名。出站请求经 `federation.signRequestWith` 做 X-Matrix 签名（远程媒体拉取复用同一签名路径）。
- **入站验签与成员落地**：`internal/federation/fedverify` 基于远端 server key 逐事件 ed25519 验签（`crypto.VerifyJSONWith` + `events.EventIDFromRaw` 原语），`ingestPDU`/`ingestRemoteMember` 入站前验签，未签名/伪造事件被拒；入站 member 事件经 `federation.applyRemoteMembership` 更新 denormalised membership 并 `notifyRoomMembers` 唤醒 /sync。
- **发现**：`.well-known/matrix/server` 优先，回退 SRV（`_matrix._tcp` / `_matrix-fed._tcp`），再回退 A/AAAA + 默认端口。DNS 用 Go 纯解析器（netgo，0cgo 下即默认），不依赖 libc。
- **收发**：PDU/EDU、`/send/{txnId}`、`/state`、`/state_ids`（auth chain 递归遍历）、`/backfill`、`/get_missing_events`、`/make_join` + `/send_join`（v2）、`/invite`（v2）、`/make_leave` + `/send_leave`（v2）、`/event_auth` 等。入站状态事件入库后经 `internal/eventstate.Maintain` 维护每事件 state-at-event 快照并基于 `forward_extremities` 重算 `room_state`（fork/merge 正确解析）；`federation.stateres.go` 仅保留 `roomRules` 与递归 `authChain` 遍历。
- **端点版本**：按 spec 分别实现各端点的 `v1`/`v2`。
- **监听端口**：联邦默认 `8448`（或经 `.well-known` 委派到 `8008`/反代）。

### E2EE（服务端中转）

服务端**不做加密本身**，只做密钥与消息的中转：

- `/_matrix/client/v3/keys/upload`、`/keys/query`、`/keys/claim`、`/keys/changes`
- one-time keys 计数与原子分发（claim 标记 used）
- `/sendToDevice/{eventType}/{txnId}`（to-device 消息中转，经 /sync 派发后删除，`*` 通配扇出）
- cross-signing 密钥的存储与查询、`/keys/device_signing/upload`、`/keys/signatures/upload`
- **密钥备份** `/room_keys/*`：version 增删改查 + keys 批量上传/查询/删除

**客户端 Megolm 加解密**（`web/src/lib/e2ee.ts`，基于 `@matrix-org/olm` libolm WASM）：

- Ed25519 签名密钥 + Curve25519 身份密钥（canonical JSON 签名后 keys/upload），IndexedDB 持久化
- one-time keys（signed_curve25519）+ fallback key 生成与上传
- Megolm 出站会话（per-room m.megolm.v1.aes-sha2）：encryptRoomMessage 加密 m.room.message → m.room.encrypted
- Megolm 入站会话：importRoomKey 导入 room key，decryptRoomMessage 解密入站 m.room.encrypted
- Olm 1:1 会话：keys/claim + sendToDevice 包装 m.room_key 为 m.encrypted 分发给已知设备
- to-device m.encrypted 解密（建立入站 Olm 会话），keys/query 设备密钥缓存

### 媒体处理策略

Matrix 需要的媒体处理只有"图片缩略图"一项，无需 ImageMagick / ffmpeg。

**纯 Go 覆盖范围（全部 0cgo）**：

| 源 MIME | 输出 |
|---|---|
| `image/jpeg` / `image/jpg` | jpeg |
| `image/webp`（`golang.org/x/image/webp`） | jpeg |
| `image/gif`（`image/gif`） | png（保留透明） |
| `image/png`（stdlib `image/*`） | png |

缩放用 `x/image/draw`（CatmullRom crop / BiLinear scale），输出 JPEG（Q85）或 PNG（保留透明）。缩略图缓存到 `media_thumbnails` 表。

**URL preview**（`preview_url`）：端点 `GET /_matrix/client/v1/media/preview_url`（旧 `/_matrix/media/v3/preview_url` 已弃用）。抓取目标 URL、解析 OpenGraph、对 `og:image` 生成缩略图。**SSRF 防护**：IP 段黑名单（拒绝 loopback / 私网 / link-local），限制大小（1MB）、超时（10s）、重定向。

**AVIF / HEIC**：规范不要求服务端缩略这些格式，遵循规范 = 不生成服务端缩略图，存/发原始媒体（客户端自行渲染）。纯 Go 即满足，无需 cgo/WASM。视频/音频转码或缩略图在 spec 之外，一旦引入 ffmpeg 会同时打破 0cgo、单二进制、最小镜像三条约束；若将来需要做成可选外部 sidecar。

**安全红线**（公开媒体服务器必须）：不光栅化不可信 SVG（XXE/脚本风险），存/发原样；解压炸弹防护（解码前限制文件大小与像素上限）；缩略图统一输出安全格式（JPEG/PNG）。

### Web 前端

| 组件 | 说明 |
|---|---|
| **pnpm 9** | 包管理；CI/容器 `corepack enable` + `--frozen-lockfile` |
| **Vite 8** | 打包（需 Node ≥20）；`build.outDir: '../internal/webui/dist'`、`emptyOutDir: true` |
| **React 19** | `@vitejs/plugin-react` v5 |
| **TanStack Query** | 服务端状态（CS API 拉取、缓存、失效） |

目录结构（`web/`）：`package.json`、`pnpm-lock.yaml`、`vite.config.ts`、`tsconfig.json`、`index.html`、`src/main.tsx`（挂载 + QueryClientProvider）、`src/App.tsx`（登录/注册/建房/聊天/实时 sync 循环）、`src/lib/matrix.ts`（CS API 客户端）、`src/lib/query.ts`（QueryClient）、`src/styles/globals.css`。

- **开发**：`pnpm dev` 起 Vite dev server；`vite.config.ts` 的 `server.proxy` 将 `/_matrix`、`/_synapse`、`/.well-known` 代理到本地 Go 服务。
- **生产**：Go `httpserver` 直接服务 embed 的 `dist`；API 路由前缀优先，未命中路径回退 `index.html`（供客户端路由）。fallback 不得吞掉 `/_matrix`、`/_synapse`、`/.well-known`、`/_matrix/key` 等前缀。

### 核心子系统

- **存储**：单 `*storage.Store` 包裹 pgx 连接池（`storage.OpenWithConfig` 让连接池大小经 `database.max_conns/min_conns` 可配置），按域分文件（accounts、rooms、events、state、memberships、media、e2ee、keybackup、push、federation）。迁移经 `embed` SQL 在 `Open` 时自动应用。
- **事件流水线**：`events.Builder.Build` / `BuildLegacy` 组装 -> canonical JSON -> content hash -> 对 redacted form 签名。事件 ID 按 room version 派生（v1–2 显式、v3 标准 base64 hash、v4+ url-safe hash）。
- **事件授权**：`rooms.Authorize` 按 room version 规则执行 create/member/power_levels/join_rules/generic 五类检查；v12 creator 免 power-level 检查；成员状态机覆盖 join/leave/invite/ban/knock 转换合法性。
- **状态解析**：`stateres.Resolve` 按 state-res 版本分发（v1 前向时序 / v2 出度模型反向拓扑 Kahn 排序 + mainline depth）。Kahn 选点主排序键为发送者 power-level（从 auth_events 解析）。单 extremity 房间走 `PickLatest` 退化路径。每事件入库后由 `internal/eventstate` 维护一份 state-at-event 快照（`event_state_snapshots` 表），`room_state` 由对所有 `forward_extremities` 的快照做 `stateres.Resolve` 重算（单 extremity 直接用其快照），正确处理深层 fork/merge，不再退化为 last-writer-wins。
- **认证**：`homeserver.Authenticate` 解析 bearer token -> access token 行 -> user；`RequireAuth` 放行 guest，`RequireUserAuth` 拒绝 guest。UIA 会话绑定操作 + 用户 + TTL，防止跨端点重放。
- **/sync**：`sync.Engine` 构建完整响应（joined/invited/left、timeline 窗口、account_data、ephemeral typing/receipts、to_device）。long-poll 经 `Notifier.Wait` 唤醒。
- **配置**：YAML 文件 + 环境变量覆盖（`KATRIX_*`），单节点开发零配置。

### 主要风险

1. **状态解析**：v1 / v2 / v2.1 三套并存，算法复杂，是联邦一致性的核心（v2 mainline 已实现完整 Kahn 拓扑 + mainline 链；发送者 power-level 为主排序键，从候选事件的 auth_events 解析；mainline depth 方向对齐 Synapse；每事件 state-at-event 快照表 `event_state_snapshots` 已接入，`room_state` 基于 forward extremities 重算）。
2. **房间版本 v1–v12 全覆盖**：event ID 格式、auth、redaction、room-ID-as-hash 各版本不同，工作量重（v12 room-ID-as-create-hash 已在 createRoom 串联）。
3. **canonical JSON + 签名**：字节级一致性是联邦互通的隐形地雷（入站 PDU 已逐事件验签）。
4. **`/sync` 正确性与性能**：几乎所有客户端行为的基础（`forward_extremities` 表已接入，InsertEvent 维护 extremity 集，/sync 增量 delta 不再全量重扫）。
5. **规模**：整体接近"从零写一个近乎 spec 完整的 homeserver"，为多阶段大型工程。

---

## 如何使用

### 前置

- Go ≥ 1.26
- Node ≥ 24 + pnpm 9（仅构建前端时需要）
- PostgreSQL ≥ 14（Katrix 用 pg18 测试）

### 1. 克隆并构建

```bash
git clone https://github.com/AkagiYui/katrix.git
cd katrix

# 构建 Go 二进制（0cgo）
CGO_ENABLED=0 go build -o katrix ./cmd/katrix

# （可选）构建前端到 internal/webui/dist
pnpm -C web install --frozen-lockfile
pnpm -C web build
```

### 2. 配置

Katrix 开箱即用零配置（开发默认），也可用 YAML 文件或环境变量覆盖：

```bash
# 环境变量（常用）
export KATRIX_SERVER_NAME=localhost          # homeserver 名（user id 域）
export KATRIX_DATABASE_DSN="postgres://user:pass@localhost:5432/katrix?sslmode=disable"
export KATRIX_LISTEN_CLIENT=:8008            # client/admin API
export KATRIX_LISTEN_FEDERATION=:8448        # federation API
export KATRIX_REGISTRATION_ENABLED=true      # 是否开放注册
export KATRIX_FEDERATION_ENABLED=true        # 是否启用联邦
```

或写一个 YAML：

```yaml
server_name: localhost
public_base_url: http://localhost:8008
listen:
  client: ":8008"
  federation: ":8448"
database:
  dsn: postgres://user:pass@localhost:5432/katrix?sslmode=disable
signing_key_path: signing.key
registration:
  enabled: true
  require_token: false
  allow_guest: true
media:
  max_upload_bytes: 52428800
  store_path: media_store
federation_enabled: true
```

### 3. 准备数据库

```bash
createdb katrix
# Katrix 启动时自动跑迁移（embed SQL），无需手动建表。
```

### 4. 运行

```bash
./katrix serve -config /path/to/config.yaml
# 首次运行会自动生成 signing.key
```

打开 `http://localhost:8008` 即可看到 Web 面板（登录 / 注册 / 建房 / 聊天）。

### 5. 其他子命令

```bash
./katrix healthcheck    # 打 /health，用于容器 HEALTHCHECK
./katrix genkey         # 生成并打印一个 ed25519 签名密钥
./katrix version        # 打印版本
```

### 6. 容器部署

```bash
# 构建生产镜像
docker build -f Containerfile -t ghcr.io/akagiyui/katrix:latest .

# 运行（需可达的 Postgres）
docker run -p 8008:8008 -p 8448:8448 \
  -e KATRIX_DATABASE_DSN="postgres://..." \
  -e KATRIX_SERVER_NAME=matrix.example.com \
  ghcr.io/akagiyui/katrix:latest
```

### 7. 开发（前端热重载）

```bash
# 终端 1：跑 Go 后端
./katrix serve
# 终端 2：Vite dev server，代理 /_matrix 到 :8008
pnpm -C web dev
```

---

## 测试

```bash
# 需要可达的 Postgres（测试用 advisory lock 跨包隔离）
export KATRIX_TEST_DSN="postgres://user:pass@localhost:5432/katrix_test?sslmode=disable"

# 全量测试（race 检测器需开 cgo）
CGO_ENABLED=1 go test -race -count=1 ./...

# 单包
go test ./internal/rooms/...
```

测试覆盖（~110 个用例）：canonical JSON spec 向量、crypto 签名、events 哈希/签名/legacy、ids 解析、httpx 错误码（含 soft_logout）、storage 全域 CRUD、rooms 授权规则、csapi 集成（账户/房间/sync/E2EE/key backup/push/filter/admin）、federation key cache + txn 去重、stateres v1/v2、media 上传下载。

测试隔离：`internal/testdb` 用 Postgres `pg_advisory_lock` 序列化跨包并行运行，避免共享测试库被并发 TRUNCATE 干扰；`testdb.Truncate` 覆盖 `forward_extremities` / `media` / `login_tokens` 等新增表。

CI（`.github/workflows/ci.yml`）：
- **go** job：`go build`（0cgo）+ `go vet` + gofmt 检查 + `go test -race`（cgo）。
- **web** job：`pnpm install` + `tsc` + `vite build`，上传 `internal/webui/dist` artifact。

---

## 已知遗留（未在本阶段完成）

- **Complement 逐用例通过率**：federation TLS + UIA 单请求 + per-server 命名 + r0 别名 + 客户端事件格式 + txn 幂等 + receipt/presence 端点 + E2EE 密钥验证/领取 等系统性根因已修复（csapi 通过率从 2 提升至 30/124，联邦测试解锁运行）。剩余逐用例偏差（联邦 profile 查询、远程房间 join、device list 跨服务器更新、canonical_alias 验证、presence sync 传播等）仍需迭代。
- **状态解析快照表（已实现）**：新增 `event_state_snapshots` 表存储每事件的 state-at-event 快照（`internal/eventstate` 包）。`room_state` 不再由各调用点盲写 `UpsertState`，而是在每次事件入库后由 `eventstate.Maintain` 统一维护：先计算并存该事件的 state-at-event 快照（0 prev = `{}`+自身 tuple；1 prev = 拷贝 prev 快照；>1 prev = `stateres.Resolve` 合并 prev 快照），再基于 `forward_extremities` 重算 `room_state`（单 extremity 直接用其快照；多 extremity 走 `stateres.Resolve` 解析各 extremity 快照）。这使深层 fork/merge 能基于真实的 state-before-event 正确解析，不再退化为 last-writer-wins。csapi 与 federation 两条入库路径均接入。
