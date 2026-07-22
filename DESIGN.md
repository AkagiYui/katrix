# Katrix 设计文档

> 纯 Go 实现的 Matrix 协议服务器（homeserver），单二进制、0cgo、自带 Web 管理/聊天面板。
> 参考实现：[Synapse](https://github.com/element-hq/synapse)（官方 homeserver）、[synapse-admin](https://github.com/Awesome-Technologies/synapse-admin)（管理面板）。
> 本文档为实现前的基线设计，随开发迭代更新。

---

## 1. 目标与范围

实现一个尽量对齐 [Matrix 规范](https://spec.matrix.org/) 的 homeserver，具备联邦能力，并自带轻量 Web 面板。

### 锁定的设计决策

| 维度 | 决定 | 说明 |
|---|---|---|
| 语言 / 构建 | 纯 Go，`CGO_ENABLED=0` | 全程无 cgo；多包源码，`go build` 产出单个可执行文件 |
| 部署形态 | 单个自包含二进制 | 前端经 `go:embed` 打包进二进制 |
| Web UI | 自研轻量面板 | 注册/登录/建房/聊天 + 基础管理（用户/房间/媒体/统计） |
| 联邦 | **需要** | 完整 Server-Server API，可与其他 homeserver 互通 |
| CS API | 对齐 spec **v1.19** | Client-Server API 尽量完整 |
| E2EE | 服务端**中转** | 只转发设备密钥/一次性密钥/to-device，不解密 |
| 数据库 | **Postgres**（`pgx`，纯 Go 驱动） | |
| 房间版本 | **v1–v12 全量** | 最大兼容性 |
| 验收标准 | 官方 **Complement** 测试套件 | |
| 基础镜像 | **alpine** | 运行镜像 `alpine:3.24` |
| Go / Node | Go **1.26** / Node **24** | |

### 硬约束（不可违反）

1. **0cgo**：任何依赖都必须纯 Go；不链接 libc、libheif、ffmpeg 等原生库。
2. **单二进制**：源码可拆多包，但最终产物是一个可执行文件（含前端）。
3. **可 distroless/alpine 部署**：不依赖镜像内的外部程序。

---

## 2. 版本编号关系（重要概念）

Matrix 里有多套相互独立的版本编号，实现时必须分清：

### 2.1 规范发布版本（spec release，`v1.x`）
- 整份规范的发布快照，当前最新 **v1.19**（2026-07-08）。
- 一次性覆盖所有子 API（Client-Server、Server-Server、Application Service、Identity、Push Gateway）。
- 2021-06 起统一为全局 `v1.x`；此前是各 API 各自的 `r0.x.x`。

### 2.2 房间版本（room version，`1`–`12`）
- **每个房间独立**、基本不可变，写在 `m.room.create` 的 `room_version`。
- 固定该房间的：事件格式、event ID 计算方式、auth 规则、redaction 规则、状态解析算法。
- 与 spec 发布号**正交**：新房间版本不跟随 spec 发布号。
- 当前最新稳定房间版本 = **12**，spec 现建议新建房间默认用 v12。
- 关键分界：
  - `v1–v2`：最老事件格式（event ID 为独立字段）、旧 auth、v1 状态解析
  - `v3`：event ID = 事件哈希（MSC1659）
  - `v4–v5`：URL-safe base64 event ID、签名密钥有效期
  - `v6–v10`：auth 规则收敛、knocking、restricted join、整数 power level（当今网络主流）
  - `v11`：redaction 算法澄清（此前长期默认）
  - `v12`：room ID = create 事件哈希（MSC4291）、创建者无限 power（MSC4289）、状态解析 v2.1

### 2.3 联邦"版本"
- **没有全局的联邦协议版本号。** 兼容性由三样承担：
  1. **端点路径版本**（per-endpoint `v1`/`v2`）：多数 `/_matrix/federation/v1/...`，少数如 `send_join`/`invite`/`send_leave` 有 `/v2`；密钥服务 `/_matrix/key/v2/...`。
  2. **房间版本**：事实上的"联邦算法版本"——两台服务器能否就某房间达成一致，取决于是否都实现了该 `room_version`。
  3. **发现 + 密钥**：`.well-known/matrix/server`（或 SRV）+ `/_matrix/key/v2/server`。
- `GET /_matrix/federation/v1/version` 只返回软件名/软件版本（信息性），**不是协议协商**。

### 2.4 `/versions` 声明策略
- `GET /_matrix/client/versions` 是服务器对 **Client-Server API** 的自我声明。
- 参照：当前 Synapse checkout 只声明到 `v1.12`（+ 历史 `r0.x`），更新特性放在 `unstable_features` 的 MSC 开关里。
- **本项目策略**：`/versions` 只声明**已实现并通过 Complement** 的版本号；未完成的走 `unstable_features`。**实测（Complement）与自报（/versions）保持一致，不虚报。**

---

## 3. 总体架构与目录结构

```
katrix/
├── cmd/
│   └── katrix/
│       └── main.go                 # 入口：config、启动 http server、healthcheck 子命令、import internal/webui
├── internal/
│   ├── config/                     # server_name、签名密钥、Postgres DSN、监听端口
│   ├── httpserver/                 # 路由装配 + SPA fallback（API 前缀优先）
│   ├── csapi/                      # Client-Server API handlers
│   ├── federation/                 # Server-Server API：签名、key 服务、发现、PDU/EDU、make/send_join、backfill
│   ├── media/                      # 上传/下载/缩略图（纯 Go）
│   ├── rooms/                      # 房间状态机、成员、事件授权
│   ├── roomver/                    # v1–v12 版本规则表（auth/redaction/eventid/state-res 选择）
│   ├── stateres/                   # 状态解析 v1 / v2 / v2.1
│   ├── events/                     # 事件模型、canonical JSON、hashing、redaction
│   ├── crypto/                     # ed25519 签名/验签、密钥管理
│   ├── sync/                       # /sync 引擎（全量 + 增量、long-poll、since token）
│   ├── e2ee/                       # 设备密钥、one-time keys、to-device 中转、cross-signing
│   ├── storage/                    # Postgres（pgx）+ 迁移；接口化
│   └── webui/
│       ├── embed.go                # //go:embed all:dist
│       └── dist/                   # ← Vite 构建产物（.gitignore，CI/容器构建时生成）
├── web/                            # 前端源码（pnpm + Vite）
├── .github/workflows/
│   ├── ci.yml
│   ├── complement.yml
│   └── release.yml
├── Containerfile                   # 生产镜像（alpine）
├── Containerfile.complement        # Complement 专用镜像
├── go.mod
├── go.sum
└── DESIGN.md
```

### `cmd/` 与 `go:embed` 约束（关键）
`go:embed` **只能嵌入指令所在包目录及其子目录，不能用 `..` 向上引用**。因此 `cmd/katrix/main.go` 无法直接 embed `web/` 的产物。解决方案：

- 前端源码在 `web/`，Vite 的 `build.outDir` 指向 `internal/webui/dist`。
- `internal/webui/embed.go`：
  ```go
  package webui

  import "embed"

  //go:embed all:dist
  var Dist embed.FS   // all: 前缀确保包含 _ / . 开头的产物
  ```
- `cmd/katrix/main.go` import `internal/webui`，取 `fs.Sub(webui.Dist, "dist")` 作为静态资源根。

---

## 4. 数据存储（Postgres）

- 驱动：`github.com/jackc/pgx/v5`（纯 Go，0cgo）。
- `internal/storage` 定义接口，Postgres 为唯一实现（后续如需其他后端可再加）。
- 内置迁移（embed 的 SQL 或代码迁移），启动时自动升级 schema。
- 主要数据域：账户/设备/token、房间/事件/状态、成员、账户数据、receipts/typing/presence、媒体元数据、E2EE 密钥、联邦目标/重试、注册令牌等。

---

## 5. 房间版本与状态解析

这是"联邦互通"的真正承载点，也是工作量最重的部分（对应 v1–v12 全量选择）。

- `internal/roomver`：以数据表方式描述每个版本的差异开关（event ID 格式、auth 规则集、redaction 规则、是否 room-ID-as-hash、状态解析版本等），供其他模块查询。
- `internal/stateres`：实现三套状态解析：
  - **v1**（房间 v1）
  - **v2**（房间 v2–v11）
  - **v2.1**（房间 v12，含 MSC4289 创建者 power 等）
- `internal/events`：**canonical JSON**（字节级一致，签名正确性的基础）、内容哈希、reference 哈希、按版本的 redaction 算法。
- 事件授权（auth rules）按 `roomver` 选择对应规则集执行。

---

## 6. 联邦（Server-Server API）

- **签名与密钥**（`internal/crypto`）：ed25519（stdlib）；`/_matrix/key/v2/server` 发布公钥，验证对端签名。
- **发现**：`.well-known/matrix/server` 优先，回退 SRV（`_matrix._tcp` / `_matrix-fed._tcp`），再回退 A/AAAA + 默认端口。
  - DNS 用 Go 纯解析器（0cgo 下即 netgo），不依赖 libc。
- **收发**：PDU/EDU、`/send/{txnId}`、`/state`、`/state_ids`、`/backfill`、`/get_missing_events`、`/make_join` + `/send_join`（v2）、`/invite`（v2）、`/make_leave` + `/send_leave`（v2）、`/event_auth`、`/user/keys/*` 等。
- **端点版本**：按 spec 分别实现各端点的 `v1`/`v2`。
- 监听端口：联邦默认 `8448`（或经 `.well-known` 委派到 `8008`/反代）。

---

## 7. E2EE（服务端中转）

服务端**不做加密本身**，只做密钥与消息的中转：

- `/_matrix/client/v3/keys/upload`、`/keys/query`、`/keys/claim`、`/keys/changes`
- one-time keys 计数与分发
- `/sendToDevice/{eventType}/{txnId}`（to-device 消息中转，含跨服务器经联邦转发）
- cross-signing 密钥的存储与查询、`/keys/device_signing/upload`、`/keys/signatures/upload`
- （可选）密钥备份 `/room_keys/*`

---

## 8. 媒体处理策略

**Matrix 需要的媒体处理只有"图片缩略图"一项**——无需 ImageMagick / ffmpeg。参照 Synapse（核心用 Pillow，仅处理图片，不依赖 ffmpeg）。

### 纯 Go 覆盖范围（全部 0cgo）

| 格式 | 方案 | 能力 |
|---|---|---|
| JPEG / PNG / GIF | stdlib `image/*` | 解码 + 编码 |
| WebP | `golang.org/x/image/webp` | 仅解码（缩略图输出 JPEG/PNG） |
| 缩放 | `x/image/draw`（CatmullRom）或 `disintegration/imaging` | 纯 Go |
| EXIF 方向 | `rwcarlsen/goexif` | 纯 Go |
| blurhash | 纯 Go 实现 | 计算 |

### 缩略图格式（对齐官方规范）

- 规范只要求**缩略图输出为 `image/jpeg` 或 `image/png`**，并未强制服务端能缩略任意源格式。
- 对齐参考实现 synapse 的可缩略源格式（`THUMBNAIL_SUPPORTED_MEDIA_FORMAT_MAP`）：

  | 源 MIME | 输出 |
  |---|---|
  | `image/jpeg` / `image/jpg` | jpeg |
  | `image/webp` | jpeg |
  | `image/gif` | png（保留透明） |
  | `image/png` | png |

- 动图（GIF/animated）缩略图输出静态首帧（png），或按需生成动态 webp（可选）。

### URL 预览（`preview_url`，官方端点）

- 端点：`GET /_matrix/client/v1/media/preview_url`（旧 `/_matrix/media/v3/preview_url` 已弃用）。属 Content Repository 模块**正式端点**，纳入实现范围。
- 抓取目标 URL、解析 OpenGraph、对 `og:image` 生成缩略图。
- **SSRF 防护（必须）**：IP 段黑名单，禁止解析/访问内网、环回、链路本地等地址；限制大小、超时、重定向次数。

### 摩擦点与决策

- **AVIF / HEIC**：规范不要求服务端缩略这些格式，synapse 亦不缩略。**遵循规范 = 不生成服务端缩略图**，存/发原始媒体（客户端自行渲染）。纯 Go 即满足，无需 cgo/WASM。
- **视频 / 音频转码或缩略图**：spec 之外。**不作为硬依赖**——一旦引入 ffmpeg 会同时打破 0cgo、单二进制、最小镜像三条约束。若将来需要，做成**可选外部 sidecar**（检测 PATH 上的 `ffmpeg`/`vips`，有则用、无则降级）+ 专门镜像变体，核心保持纯 Go。

### 架构口子

- `internal/media/thumbnailer` 抽象为接口 + **解码器注册表**，新增格式只改一处。
- 默认后端 = 纯 Go；exotic 编解码走可选外部工具。

### 安全红线（公开媒体服务器必须）

- **不光栅化不可信 SVG**（XXE/脚本风险），存/发原样。
- **解压炸弹防护**：解码前限制文件大小与像素上限。
- 缩略图统一输出安全格式（JPEG/PNG）。

---

## 9. Web 前端

### 技术栈

| 组件 | 说明 |
|---|---|
| **pnpm** | 包管理；`packageManager` 字段锁定，CI/容器 `corepack enable` + `--frozen-lockfile` |
| **Vite 8** | 打包（需 Node ≥20）；`build.outDir: '../internal/webui/dist'`、`emptyOutDir: true` |
| **React 19** | `@vitejs/plugin-react` |
| **TanStack Router（file route）** | `@tanstack/router-plugin/vite`，插件放在 react 之前；`src/routes/` 生成 `routeTree.gen.ts` |
| **TanStack Query** | 服务端状态（CS/admin API 拉取、缓存、失效） |
| **shadcn/ui** | Tailwind + Radix；`components.json` |
| **vite-plugin-iconify-offline** | 离线打包 Iconify 图标，运行时不联网（契合自包含二进制） |
| **shadcn charts** | 基于 Recharts，放 `components/charts/`，用于 admin 统计 |

### 目录结构（`web/`）

```
web/
├── package.json                    # packageManager: pnpm
├── pnpm-lock.yaml
├── vite.config.ts
├── tsconfig.json
├── tailwind.config.ts
├── components.json                 # shadcn/ui
├── index.html
├── .gitignore                      # 含 src/routeTree.gen.ts
└── src/
    ├── main.tsx                    # 挂载 Router + QueryClientProvider
    ├── routeTree.gen.ts            # 自动生成（忽略 VCS）
    ├── routes/                     # 文件路由
    │   ├── __root.tsx
    │   ├── index.tsx
    │   ├── login.tsx
    │   ├── chat/$roomId.tsx
    │   └── admin/{users,rooms,media,stats}.tsx
    ├── components/
    │   ├── ui/                     # shadcn/ui 基础组件
    │   └── charts/                 # shadcn charts
    ├── lib/
    │   ├── matrix.ts               # CS API 客户端
    │   └── query.ts                # TanStack Query client
    └── styles/globals.css
```

- **`routeTree.gen.ts` 忽略 VCS**：由 `@tanstack/router-plugin` 在 `dev`/`build` 时自动生成，无需额外步骤。

### 开发 vs 生产

- **开发**：`pnpm dev` 起 Vite dev server；`vite.config.ts` 的 `server.proxy` 将 `/_matrix`、`/_synapse`、`/.well-known` 代理到本地 Go 服务。
- **生产**：Go `httpserver` 直接服务 embed 的 `dist`；**API 路由前缀优先**，未命中路径回退 `index.html`（供客户端路由）。fallback 不得吞掉 `/_matrix`、`/_synapse`、`/.well-known`、`/_matrix/key` 等前缀。

---

## 10. 构建与部署

### Containerfile（生产，alpine 多阶段，0cgo 静态）

```dockerfile
# 1) 构建前端
FROM node:24-alpine AS web
RUN corepack enable
WORKDIR /src/web
COPY web/package.json web/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY web/ ./
RUN pnpm build                       # 输出到 /src/internal/webui/dist

# 2) 构建 Go 单二进制（CGO 关闭）
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /src/internal/webui/dist ./internal/webui/dist
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /katrix ./cmd/katrix

# 3) 运行镜像（alpine）
FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S katrix && adduser -S -G katrix katrix
COPY --from=build /katrix /usr/local/bin/katrix
USER katrix
EXPOSE 8008 8448
HEALTHCHECK --interval=30s --timeout=5s CMD ["katrix", "healthcheck"]
ENTRYPOINT ["katrix"]
```

- alpine 默认不带 CA 证书，需 `apk add ca-certificates`（联邦出站 TLS）；`tzdata` 供时区。
- 非 root 运行（`adduser`）。
- **HEALTHCHECK 用 `katrix healthcheck` 子命令**（二进制内部打 `/health`），跨镜像可移植；alpine 亦可用 busybox `wget`，但子命令方案更干净、且换基础镜像不受影响。

### `Containerfile.complement`
Complement 专用镜像：带 Complement 约定的入口（读取 `SERVER_NAME` 环境变量、注入测试 CA、`/data` 卷、开放联邦端口），用于跑官方测试。

---

## 11. CI/CD（GitHub Actions）

Action 版本为 2026-07 核实的最新 major（建仓时再核）：

| Action | 版本 |
|---|---|
| actions/checkout | v7 |
| actions/setup-go | v7 |
| actions/setup-node | v7 |
| pnpm/action-setup | v6 |
| actions/cache | v6 |
| docker/setup-qemu-action | v4 |
| docker/setup-buildx-action | v4 |
| docker/login-action | v4 |
| docker/build-push-action | v7 |
| goreleaser/goreleaser-action | v7 |
| softprops/action-gh-release | v3 |

### `ci.yml`（push / PR）

```yaml
name: ci
on: [push, pull_request]
jobs:
  web:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: pnpm/action-setup@v6
      - uses: actions/setup-node@v7
        with: { node-version: 24, cache: pnpm, cache-dependency-path: web/pnpm-lock.yaml }
      - run: pnpm -C web install --frozen-lockfile
      - run: pnpm -C web build         # 生成 routeTree.gen.ts + 输出 internal/webui/dist
      - uses: actions/upload-artifact@v4
        with: { name: web-dist, path: internal/webui/dist }
  go:
    needs: web
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: actions/download-artifact@v4
        with: { name: web-dist, path: internal/webui/dist }
      - uses: actions/setup-go@v7
        with: { go-version: '1.26', cache: true }
      - run: CGO_ENABLED=0 go build ./...
      - run: go vet ./...
      - run: go test ./...
```

### `complement.yml`（push / 定时）
- 用 `Containerfile.complement` 构建镜像。
- 跑官方 [Complement](https://github.com/matrix-org/complement)（Go 黑盒测试，需 Docker-in-Docker），产出通过率报告。**这是验收标准。**

### `release.yml`（打 tag）
- 多架构静态二进制（`linux/amd64`、`linux/arm64`，可加 darwin/windows）。
- 构建并推容器镜像到 **GHCR**（`ghcr.io/<owner>/katrix`）。
- 可选 goreleaser 统一二进制 + 镜像 + changelog。

---

## 12. 验收：Complement

- 官方黑盒集成测试套件，覆盖注册/登录、房间、sync、媒体、联邦、E2EE、多房间版本等。
- 实现推进时按模块开启对应用例；`/versions` 声明与 Complement 通过范围保持一致。

---

## 13. 分期路线图

每期挂一个 Complement 里程碑；建议 P0–P3 跑通（可用单服务器 + sync）后再进联邦。

| 阶段 | 内容 | Complement 里程碑 |
|---|---|---|
| **P0 骨架** | cmd/ 布局、config、PG schema、路由、`/versions`、`.well-known`、healthcheck | 起服务、被发现 |
| **P1 账户** | 注册/登录/登出、access token、device 管理、`/whoami` | 登录类用例 |
| **P2 房间核心** | 建房、成员、消息、状态事件、事件授权（先 v10–v12） | 房间基础用例 |
| **P3 Sync** | `/sync` 全量 + 增量、typing/receipts/account_data | sync 用例 |
| **P4 媒体 + UI** | 上传/下载/缩略图、自研 Web 面板打通 | 媒体用例 |
| **P5 联邦** | 签名/keys、发现、make/send_join、send、backfill、EDU、状态解析 v2 | 联邦互通用例 |
| **P6 房间版本回填** | v1–v9、v1 状态解析、旧事件格式 | 多版本用例 |
| **P7 E2EE 中转** | keys/upload\|query\|claim、to-device、cross-signing、key backup | E2EE 用例 |
| **P8 补全** | push rules、filters、capabilities、URL preview（`preview_url` + SSRF 防护）、admin API 补全 | 剩余 spec 用例 |

---

## 14. 主要风险

1. **状态解析**：v1 / v2 / v2.1 三套并存，算法复杂，是联邦一致性的核心。
2. **房间版本 v1–v12 全覆盖**：event ID 格式、auth、redaction、room-ID-as-hash 各版本不同，工作量重。
3. **canonical JSON + 签名**：字节级一致性是联邦互通的隐形地雷。
4. **`/sync` 正确性与性能**：几乎所有客户端行为的基础。
5. **规模**：整体接近"从零写一个近乎 spec 完整的 homeserver"（参照 Go 的 Dendrite、Rust 的 conduwuit），为多阶段大型工程。

---

## 15. 已确定的补充决策

- **镜像仓库**：锁定 **GHCR**（`ghcr.io/<owner>/katrix`）。
- **`release.yml` 目标平台**：保持默认 **linux amd64 / arm64**。
- **密钥备份**（E2EE `/room_keys/*`）：**纳入 P7**。
- **URL preview**（`preview_url`）：官方规范端点，**纳入实现**（P8），含 SSRF 防护。
- **AVIF/HEIC 缩略图**：**遵循规范不缩略**（存/发原图），无需 cgo/WASM。

当前无遗留开放项。

---

## 附录：参考

- Matrix 规范：<https://spec.matrix.org/>（当前 v1.19）
- 房间版本：<https://spec.matrix.org/latest/rooms/>（当前稳定 v1–v12）
- Complement：<https://github.com/matrix-org/complement>
- Synapse（参考实现）：`tmp/synapse/`
- synapse-admin（参考面板）：`tmp/synapse-admin/`
