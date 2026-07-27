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
- [下一阶段目标](#下一阶段目标)

---

## 功能总览

Katrix 按阶段（P0–P8）实现，每个阶段都自带测试并通过 GitHub Actions CI。

| 模块 | 实现范围 |
|---|---|
| **账户（P1）** | 注册（含 guest）、登录、登出、登出全部、access/refresh token 轮换、`/whoami`、改密、停用、设备管理（列表/查询/改名/删除）、用户资料（昵称/头像）。UIA（m.login.dummy / m.login.password / registration_token）、guest 中间件、soft_logout、登录时序侧信道防护。 |
| **房间核心（P2）** | `createRoom`、join/leave/invite/kick/ban/unban、`send`、`state` 读写、`event`、`members`、`joined_members`、`aliases`、`messages` 分页、`redact`、`typing`、`forget`、目录别名 CRUD。事件授权规则（m.room.create/member/power_levels/join_rules，含 v12 creator 特权）、power levels、成员状态机、knock 支持。 |
| **Sync（P3）** | `/sync` 全量 + 增量、long-poll（经 Notifier 唤醒）、typing 临时状态、receipts、account_data（全局 + 房间）、to-device 派发。 |
| **媒体 + UI（P4）** | 上传/下载/缩略图（v1 + v3 端点）。纯 Go 缩略图（JPEG/PNG/GIF/WebP 解码，JPEG/PNG 输出，scale/crop，DB 缓存）。React 19 + Vite 8 SPA（登录/注册/建房/聊天/实时 sync 循环）。 |
| **联邦（P5）** | 远端 server key 拉取 + Postgres 缓存、`/send/{txnId}`（去重 + PDU 入库）、`/event`、`/state`、`/state_ids`、`/backfill`、`/get_missing_events`、`/make_join` + `/send_join`、`/make_leave` + `/send_leave`、`/invite`、`/event_auth`。出站 federation Client、状态解析 v2。 |
| **房间版本回填（P6）** | 旧事件格式（v1–v2：显式 event_id 字段、`[id, hash]` prev/auth）经 `Builder.BuildLegacy`；状态解析 v1（前向时序）。roomver 规则表覆盖 v1–v12。 |
| **E2EE 中转（P7）** | `keys/upload`（device + one-time + fallback）、`keys/query`、`keys/claim`（原子消费 OTK）、`keys/changes`、`sendToDevice`（经 /sync 派发后删除）、cross-signing（device_signing / signatures 上传）。**密钥备份**（`/room_keys/*`：version 增删改查、keys 批量上传/查询/删除）。服务端不做加密，只中转。 |
| **补全（P8）** | push rules（默认规则集 + 增删改查）、filters、`publicRooms`、`preview_url`（OpenGraph 解析 + 粗粒度 SSRF 防护）、admin API（whois / 停用 / 改密 / 用户列表 / 房间列表 / 统计，按 `users.admin` 鉴权）。 |
| **部署** | 生产 `Containerfile`（alpine 多阶段、0cgo 静态、非 root、healthcheck 子命令）；`Containerfile.complement`（Complement 测试镜像）。 |
| **CI/CD** | `ci.yml`（Go build/vet/gofmt/race test + Web Vite build）；`release.yml`（多架构二进制 + GHCR 镜像，打 tag 触发）；`complement.yml`（官方 Complement 黑盒测试，nightly）。 |

### 硬约束

- **0 cgo**：所有依赖纯 Go（pgx、bcrypt、x/image）。CI 用 `CGO_ENABLED=0` 构建产物二进制；race 测试单独开 cgo。
- **单二进制**：源码多包，`go build` 产出一个可执行文件，前端 `go:embed` 进二进制。
- **可 distroless/alpine 部署**：镜像内不依赖外部程序。

---

## 系统设计

### 目录结构

```
katrix/
├── cmd/katrix/main.go          # 入口：serve / healthcheck / genkey / version
├── internal/
│   ├── config/                 # YAML + env 配置
│   ├── httpserver/             # 顶层路由装配 + SPA fallback
│   ├── csapi/                  # Client-Server API handlers
│   ├── federation/             # Server-Server API（key/发现/PDU/EDU/join）
│   ├── media/                  # 内容仓库（上传/下载/缩略图/URL preview）
│   ├── rooms/                  # 房间状态机、成员、事件授权规则
│   ├── roomver/                # v1–v12 房间版本规则表
│   ├── stateres/               # 状态解析 v1 / v2
│   ├── events/                 # 事件模型、canonical JSON、hashing、redaction、签名
│   ├── crypto/                 # ed25519 签名/验签、密钥管理
│   ├── sync/                   # /sync 引擎（Token、TypingTracker、Response 构建）
│   ├── storage/                # Postgres（pgx）+ 迁移；接口化
│   ├── homeserver/             # 共享状态容器（config/store/key/notifier + 认证中间件）
│   ├── httpx/                  # Matrix 标准错误码 + JSON 响应
│   ├── ids/                    # user/room/alias/device id 解析与生成
│   ├── testdb/                 # 测试用 Postgres advisory lock（跨进程隔离）
│   └── webui/                  # //go:embed all:dist
├── web/                        # 前端源码（pnpm + Vite + React 19）
├── .github/workflows/          # ci / release / complement
├── Containerfile               # 生产镜像（alpine）
├── Containerfile.complement    # Complement 测试镜像
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
  - **v2**（房间 v2–v11）：幂事件逆时序排序 + mainline tie-break。
  - **v2.1**（房间 v12，含 MSC4289 创建者 power）：在 v2 基础上，creator 经授权规则层（`rooms.Authorize` 的 `CreatorPrivileged` 分支）豁免 power-level 检查。
- **canonical JSON + 签名**（`internal/canonicaljson` + `internal/events`）：字节级一致是联邦互通的隐形地雷。键按 Unicode code point 排序、无空白、整数渲染无小数点、禁止浮点、UTF-8 原样、仅强制转义。content hash + reference hash 按 room version 派生 event ID；redaction 算法按版本分支（v11+ `UpdatedRedaction` 修剪 origin/membership/prev_state、保留 create 全内容、保留 m.room.member.third_party_invite.signed、保留 m.room.redaction.redacts）。

### 联邦（Server-Server API）

- **签名与密钥**（`internal/crypto`）：ed25519（stdlib）；`/_matrix/key/v2/server` 发布本服务器公钥（自签名），验证对端签名。
- **发现**：`.well-known/matrix/server` 优先，回退 SRV（`_matrix._tcp` / `_matrix-fed._tcp`），再回退 A/AAAA + 默认端口。DNS 用 Go 纯解析器（netgo，0cgo 下即默认），不依赖 libc。
- **收发**：PDU/EDU、`/send/{txnId}`、`/state`、`/state_ids`、`/backfill`、`/get_missing_events`、`/make_join` + `/send_join`（v2）、`/invite`（v2）、`/make_leave` + `/send_leave`（v2）、`/event_auth` 等。
- **端点版本**：按 spec 分别实现各端点的 `v1`/`v2`。
- **监听端口**：联邦默认 `8448`（或经 `.well-known` 委派到 `8008`/反代）。

### E2EE（服务端中转）

服务端**不做加密本身**，只做密钥与消息的中转：

- `/_matrix/client/v3/keys/upload`、`/keys/query`、`/keys/claim`、`/keys/changes`
- one-time keys 计数与原子分发（claim 标记 used）
- `/sendToDevice/{eventType}/{txnId}`（to-device 消息中转，经 /sync 派发后删除）
- cross-signing 密钥的存储与查询、`/keys/device_signing/upload`、`/keys/signatures/upload`
- **密钥备份** `/room_keys/*`：version 增删改查 + keys 批量上传/查询/删除（`/room_keys/version`、`/room_keys/keys`，含 room/session 维度过滤）

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

- **存储**：单 `*storage.Store` 包裹 pgx 连接池，按域分文件（accounts、rooms、events、state、memberships、media、e2ee、keybackup、push、federation）。迁移经 `embed` SQL 在 `Open` 时自动应用。
- **事件流水线**：`events.Builder.Build` / `BuildLegacy` 组装 -> canonical JSON -> content hash -> 对 redacted form 签名。事件 ID 按 room version 派生（v1–2 显式、v3 标准 base64 hash、v4+ url-safe hash）。
- **事件授权**：`rooms.Authorize` 按 room version 规则执行 create/member/power_levels/join_rules/generic 五类检查；v12 creator 免 power-level 检查；成员状态机覆盖 join/leave/invite/ban/knock 转换合法性。
- **状态解析**：`stateres.Resolve` 按 state-res 版本分发（v1 前向时序 / v2 幂事件逆时序 + mainline）。单 extremity 房间走 `PickLatest` 退化路径。
- **认证**：`homeserver.Authenticate` 解析 bearer token -> access token 行 -> user；`RequireAuth` 放行 guest，`RequireUserAuth` 拒绝 guest。UIA 会话绑定操作 + 用户 + TTL，防止跨端点重放。
- **/sync**：`sync.Engine` 构建完整响应（joined/invited/left、timeline 窗口、account_data、ephemeral typing/receipts、to_device）。long-poll 经 `Notifier.Wait` 唤醒。
- **配置**：YAML 文件 + 环境变量覆盖（`KATRIX_*`），单节点开发零配置。

### 主要风险

1. **状态解析**：v1 / v2 / v2.1 三套并存，算法复杂，是联邦一致性的核心（当前 v2 mainline 为退化实现，见下一阶段目标）。
2. **房间版本 v1–v12 全覆盖**：event ID 格式、auth、redaction、room-ID-as-hash 各版本不同，工作量重。
3. **canonical JSON + 签名**：字节级一致性是联邦互通的隐形地雷。
4. **`/sync` 正确性与性能**：几乎所有客户端行为的基础（当前每次重算 delta，缺 extremities 表，见下一阶段目标）。
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

测试隔离：`internal/testdb` 用 Postgres `pg_advisory_lock` 序列化跨包并行运行，避免共享测试库被并发 TRUNCATE 干扰。

CI（`.github/workflows/ci.yml`）：
- **go** job：`go build`（0cgo）+ `go vet` + gofmt 检查 + `go test -race`（cgo）。
- **web** job：`pnpm install` + `tsc` + `vite build`，上传 `internal/webui/dist` artifact。

---

## 下一阶段目标

以下是当前实现的已知限制，作为下一阶段的优化与补全目标：

1. **联邦入站签名验证**：P5 的入站 PDU 当前信任 origin、未做逐事件签名校验。需配合完整状态解析 v2 收敛，对每个入站 PDU 用缓存远端 key 验签。
2. **完整状态解析 v2 mainline**：当前 v2 实现为单 extremity 退化版（幂事件逆时序 + 简化 mainline）。需实现完整的 reverse-chronological power-event DAG 排序 + mainline 距离计算，覆盖联邦冲突场景。
3. **远程媒体拉取**：P4 仅支持本服务器媒体，`download/{serverName}/{mediaId}` 对远端 server 直接返回 `M_NOT_FOUND`。需经联邦 fetch 通路拉取并缓存远端媒体。
4. **v12 room ID 哈希派生串联**：`BuildV12RoomID` 已实现 create 哈希派生，但建房流程暂用随机 ID。需在 createRoom 流程中先生成 create 事件 → 算哈希 → 回填 room_id → 再签名后续事件。
5. **URL preview SSRF 强化**：当前为粗粒度字符串黑名单（localhost / 私网段 / link-local）。生产应改为 DNS 解析后做 IP 段（含 IPv6 ULA/link-local）校验 + 重定向次数/大小/超时限制。
6. **Complement 验收**：已接入 `complement.yml`（nightly 跑官方黑盒测试），但尚未对齐通过率里程碑。需按模块开启 Complement 用例并修复偏差，作为 spec 合规的正式验收。
7. **Web 面板扩展**：当前为最小可用（登录/注册/建房/聊天/sync 循环）。待补：管理面板（用户/房间/媒体/统计图表，对齐 synapse-admin）、shadcn/ui 组件、TanStack Router 文件路由、E2EE 客户端加密（Olm/Megolm）。
8. **性能与可观测性**：`/sync` 当前每次重算全量 delta，缺少 `forward_extremities` 表与增量索引；DB 连接池大小、token TTL 硬编码。需加 extremities 表、连接池配置项、metrics/tracing。
