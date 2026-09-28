# Miora 画布查看器（`.miora` 预览）

本目录存放 Miora 画布的**离线单文件查看器**产物，用于在 WorkBuddy 中预览
`.miora` 文件（Miora kiwi 画布数据）。

> **位置**：本目录原在 `apps/workbuddy-desktop/resources/miora-viewer/`，
> 现已随 Miora 集成方式改造搬到 `mcps/miora-mcp/assets/` —— 查看器属于 Miora
> 模块的 Widget 资源，按接入规范应与 `cli.cjs` 同处一个插件目录，不再散落在
> Desktop 的通用 resources 下。
>
> 放 `assets/` 而不是 `dist/`：`apps/workbuddy-desktop/.gitignore` 忽略 `dist/`，
> 而本产物是**必须入库**的 vendored 文件（理由见下节）。`dist/` 留给由
> `scripts/build-miora-mcp-canvas.js` 现场生成、无需入库的 `canvas.cjs`。

## 产物从哪来

由 **miora 仓**构建，不在本仓维护源码：

```bash
# miora 仓（分支 feature/workbuddy-embeb-local-v1）
cd packages/frontend/web-flow
pnpm build:viewer          # 产出 dist-viewer/
```

然后把 `dist-viewer/` 的内容同步到本目录（`miora-viewer.html` 及其同级资源）。

### 为什么产物直接入 git

已确认走**产物入库**，不走 npm 包 / 构建期拉取。理由：viewer 与 `.miora` 格式
（kiwi schema 中的 `MioraLocalFile`）强耦合，格式变更时二者必须同版本；入库能保证
任一 commit 都可复现出配套的预览能力，也免去打包机访问外部制品库的依赖。

代价是仓库体积，而 **git 历史无法回收** —— 一旦把无用的兆级产物提进去就永久留存。
因此同步前必须先完成瘦身，并核对：

1. miora 仓跑 `node scripts/analyze-viewer-size.mjs` 确认没有重型依赖回流
   （该脚本按模块在产物中的**实际字节数**归因；注意"模块数减少"不等于"体积减少"）；
2. 本目录不应出现 viewer 用不到的 `.pag` / `.wasm`（见下节）；
3. 在提交信息里记录本次同步对应的 miora 仓 commit，便于回溯。


## 为什么是"目录"而不是单个文件

产物主体是一个自包含 HTML（JS/CSS 全部内联），另有 4 个外部文件（合计约 1.6MB）：

| 文件 | 来源 | 为什么不能去掉 |
|---|---|---|
| `viewer2.js` / `viewer.js` | `iconfont.js` ×2 | 图标字体，界面必需 |
| `viewer.wasm` | `utils/vtracer/vtracer_webapp_bg.wasm` | 矢量化 glue 靠 `new URL` 自动定位；`vectorize-tool` 在 `from-node-data` 数据转换链路上 |
| `viewer.jpg` / `viewer.png` | 默认头像 / 空面板占位图 | `workflow-error-page` 等错误路径可能用到，200KB 不值得冒险 |

不强行内联这些：`assetsInlineLimit` 曾设为 `MAX_SAFE_INTEGER`（"全部内联"），
实测反而是体积主因 —— 41.5MB 产物里 20.9MB 是 data URI（base64 额外膨胀 ~33%）。

### 排查兄弟资产的正确方法（重要）

若发现本目录出现新的大文件，**不要用 `grep 产物文件名`判断是否被引用**：
产物名是按 emit 顺序编号的 `viewer{N}.{ext}`，**每次构建都会变**（实测同一份代码
两次构建，`viewer.pag` 与 `viewer5.pag` 互换）。

正确做法是在 miora 仓跑 `node scripts/trace-viewer-assets.mjs`，它从 rollup bundle
元信息读 `originalFileNames`，直接给出每个产物文件的真实源路径。

另外注意 `analyze-viewer-size.mjs` **看不到这些资产** —— 它统计的是 JS 模块的
`renderedLength`，而 `emitFile` 出来的资产不在 `chunk.modules` 里（在 ROI 表里显示为 0 KB）。
两个脚本各管一半，缺一不可。

### 两个反复踩到的 vite 机制

1. **`@vite-ignore` 只抑制警告，不阻止 emit。** 想让 `new URL(x, import.meta.url)`
   不产出资产，必须把 URL 表达式本身替换掉。
2. **改写这类 URL 的插件必须 `enforce: 'pre'`。** vite 内置 asset 插件在 transform
   阶段就登记资产，普通插件排在其后，此时改源码已无效 —— 症状是"正则命中、替换生效、
   transform 确认被调用，但产物里资产照旧"。
3. **`inlineDynamicImports` 让所有动态 import 懒加载失效。** 主站用
   `import('pinyin-pro')`、`import('@/services/sentry')` 做的体积隔离，在 viewer 里
   全部被内联进同一 chunk。看到 `import(...)` 不能认为该依赖已被隔离。

## 运行时如何被使用

预览走 MCP Apps 标准链路（`mcps/miora-mcp/cli.cjs`）：

1. 模型调 `miora_open_canvas`，Host 据 `_meta.ui.resourceUri` 向插件请求
   `resources/read`；
2. `cli.cjs` 返回一段 **~1KB 的薄壳 HTML**，里面用 `<iframe>` 指向本目录的
   `miora-viewer.html`；
3. 薄壳里的 iframe 地址由 daemon 把**本目录**与**用户 `.miora` 所在目录**分别注册到
   `StaticHtmlHttpServer` 后拼出（各一个随机 token），并带上
   `?data=<画布字节地址>&assetsBase=<资源基址>`，viewer 自己 fetch 取数。

### 为什么是薄壳 + 内层 iframe，而不是直接返回本产物

`miora-viewer.html` 有 **11MB**，且用 `new URL("viewer2.js", import.meta.url)`
这类**相对定位**加载 4 个兄弟资源。若把它整份塞进 `resources/read`：

- Host 会用 `document.write` 把 HTML 写进 iframe（见 `mcp-apps/sandbox-proxy.ts`，
  刻意不用 srcdoc），此时 `import.meta.url` 指向宿主 proxy 页地址 ——
  `viewer2.js` / `viewer.js` / `viewer.wasm` / `viewer.png` 会全部 404，viewer 起不来；
- 11MB 也远超 Agent CLI 的 256KiB 预取阈值。

薄壳让内层 iframe 拿到**真实 loopback URL**，相对定位与 origin 都天然正确。

iframe src **必须**是 loopback：主窗口 CSP 的 `frame-src` 只放行
`http://127.0.0.1`，不含 `file:` / `blob:` / `data:`
（见 `src/main/window/csp/setup-csp.ts`）。而薄壳这一层还需 resource 的
`_meta.ui.csp.frameDomains` 声明 loopback —— 宿主有**两层** `frame-src`
（HTTP 响应头 + 注入的 meta），缺任一层内层 iframe 都会被静默拦掉。

## 打包注意

搬进插件目录后，本目录**不再需要单独登记** —— 它随整个 builtin 插件树被覆盖：

- `runtimeAssetDirectories` 的 `'plugins'` 条目 —— build 期同步进
  `dist/resources/plugins/`
- `asarUnpackEntries` 的 `'dist/resources/plugins/**/*'` —— **必须 unpack**：
  `StaticHtmlHttpServer` 用 `node:fs` 的 `readFile`/`realpath` 读盘，asar 虚拟
  文件系统下 `realpath` 无法解析，会被其 traversal 守卫判为 403。

⚠️ 因此**不要**把那条 asarUnpack 规则收窄成更细的子路径，否则本目录会退回 asar
内部，表现为 iframe 403、画布打不开。该约束已在 manifest 里注明。
