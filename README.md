# Katrix

> 纯 Go 实现的 Matrix 协议 homeserver（服务端）。单二进制、0 cgo、自带 Web 聊天 / 管理面板，对齐 Matrix Client-Server / Server-Server 规范（v1.19）。

Katrix 是一个从零实现的、尽量贴近 [Matrix 规范](https://spec.matrix.org/v1.19/) 的 homeserver，用纯 Go 编写（`CGO_ENABLED=0`），前端经 `go:embed` 打包进同一个二进制。前端构建产物以 Vite 输出到 `internal/webui/dist`，运行时由 Go 静态服务，API 路由前缀（`/_matrix`、`/_synapse`、`/.well-known`）优先，未命中路径回退 `index.html`。

详细设计基线见 [`DESIGN.md`](./DESIGN.md)。

---

## 目录

- [功能总览](#功能总览)
- [系统设计](#系统设计)
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
└── DESIGN.md                   # 设计基线
```

### 核心子系统

- **存储**：单 `*storage.Store` 包裹 pgx 连接池，按域分文件（accounts、rooms、events、state、memberships、media、e2ee、keybackup、push、federation）。迁移经 `embed` SQL 在 `Open` 时自动应用。
- **事件流水线**：`events.Builder.Build` / `BuildLegacy` 组装 → canonical JSON → content hash → 对 redacted form 签名。事件 ID 按 room version 派生（v1–2 显式、v3 标准 base64 hash、v4+ url-safe hash）。
- **事件授权**：`rooms.Authorize` 按 room version 规则执行 create/member/power_levels/join_rules/generic 五类检查；v12 creator 免 power-level 检查；成员状态机覆盖 join/leave/invite/ban/knock 转换合法性。
- **状态解析**：`stateres.Resolve` 按 state-res 版本分发（v1 前向时序 / v2 幂事件逆时序 + mainline）。单 extremity 房间走 `PickLatest` 退化路径。
- **认证**：`homeserver.Authenticate` 解析 bearer token → access token 行 → user；`RequireAuth` 放行 guest，`RequireUserAuth` 拒绝 guest。UIA 会话绑定操作 + 用户 + TTL，防止跨端点重放。
- **/sync**：`sync.Engine` 构建完整响应（joined/invited/left、timeline 窗口、account_data、ephemeral typing/receipts、to_device）。long-poll 经 `Notifier.Wait` 唤醒。
- **配置**：YAML 文件 + 环境变量覆盖（`KATRIX_*`），单节点开发零配置。

### 版本声明策略

`GET /_matrix/client/versions` 只声明已实现并通过 CI 的版本号（当前到 `v1.19` + 历史 `r0.6.1`），未完成特性走 `unstable_features`，不虚报。

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
