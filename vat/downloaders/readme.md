# VAT 模块文档：Download（下载阶段）

> **阶段定义**：`TaskStep.DOWNLOAD` 是一个单一阶段（非阶段组）
> 
> 职责：从外部来源（YouTube / 本地文件 / HTTP 直链）获取视频文件，为后续 ASR/翻译/嵌入阶段准备输入

---

## 1. 整体流程图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DOWNLOAD 阶段                                        │
│                         (TaskStep.DOWNLOAD)                                  │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    CLI / Pipeline / WebUI 入口                       │    │
│  │  vat pipeline -u <source>  (source = 路径 / YouTube URL / 直链)     │    │
│  └───────────────────────────────┬─────────────────────────────────────┘    │
│                                  ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 1. detect_source_type(source) → SourceType                         │    │
│  │ 2. create_video_from_source(source, db, source_type, title)        │    │
│  │    - LOCAL:      video_id = content_hash(文件前1MB + size)[:16]     │    │
│  │    - YOUTUBE:    video_id = md5(url)[:16]                           │    │
│  │    - DIRECT_URL: video_id = md5(url)[:16]                           │    │
│  │    - 创建 DB Video 记录 + 输出目录                                  │    │
│  └───────────────────────────────┬─────────────────────────────────────┘    │
│                                  ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 3. VideoProcessor._run_download()                                   │    │
│  │    │                                                                │    │
│  │    ├─ 根据 source_type 选择下载器 (downloader 属性)                  │    │
│  │    │      YOUTUBE    → YouTubeDownloader                            │    │
│  │    │      LOCAL      → LocalImporter                                │    │
│  │    │      DIRECT_URL → DirectURLDownloader                          │    │
│  │    │                                                                │    │
│  │    ├─ downloader.download(source, output_dir, **kwargs)             │    │
│  │    │      └─ 返回标准化 result dict (video_path, title, metadata)   │    │
│  │    │                                                                │    │
│  │    ├─ 验证 guaranteed_fields 契约                                   │    │
│  │    │                                                                │    │
│  │    ├─ 更新 DB: Video.title, Video.metadata                          │    │
│  │    │                                                                │    │
│  │    └─ Metadata 增强（按数据可用性，非按 source_type 分支）           │    │
│  │           ├─ if subtitles: 处理字幕信息                              │    │
│  │           ├─ if title: SceneIdentifier → metadata['scene']          │    │
│  │           ├─ if title: VideoInfoTranslator → metadata['translated'] │    │
│  │           └─ if thumbnail: 下载封面                                  │    │
│  └───────────────────────────────┬─────────────────────────────────────┘    │
│                                  ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 输出文件                                                            │    │
│  │   <output_dir>/<video_id>/                                          │    │
│  │       ├─ original.mp4        (LOCAL: 软链接; DIRECT_URL: 下载文件)  │    │
│  │       └─ <youtube_id>.mp4    (YouTube: yt-dlp 下载)                 │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 下载器架构

### 2.1 两层接口层级

```
BaseDownloader (抽象基类)
├── LocalImporter              # 本地文件导入（软链接 + ffprobe）
├── DirectURLDownloader        # HTTP/HTTPS 直链下载
└── PlatformDownloader (抽象)  # 平台下载器，增加 playlist 能力
    └── YouTubeDownloader      # YouTube 下载（yt-dlp）
    └── (未来) BilibiliDownloader
```

**设计动机**：`get_playlist_urls()` 仅对平台源有意义。将其放在 `PlatformDownloader` 子接口中，避免 `LocalImporter`/`DirectURLDownloader` 被迫实现不相关的方法（避免 LSP 违反）。

### 2.2 BaseDownloader 接口

所有下载器的通用契约，定义在 `vat/downloaders/base.py`：

| 方法 | 说明 |
|------|------|
| `download(source, output_dir, **kwargs)` | 下载/导入视频，返回标准化结果字典 |
| `validate_source(source)` | 验证源是否有效（路径存在 / URL 格式正确） |
| `extract_video_id(source)` | 从源提取/生成稳定的 16 字符 hex ID |
| `guaranteed_fields` (property) | 声明该下载器保证返回的字段集合 |
| `probe_video_metadata(path)` (static) | 通过 ffprobe 提取视频元数据（共享工具方法） |

### 2.3 PlatformDownloader 接口

在 BaseDownloader 基础上增加平台特有能力，定义在 `vat/downloaders/base.py`：

| 方法 | 说明 |
|------|------|
| `get_playlist_urls(playlist_url)` | 获取播放列表中的所有视频 URL |

### 2.4 download() 返回值标准化

所有下载器的 `download()` 方法返回统一结构：

```python
{
    'video_path': Path,               # output_dir 内的视频文件路径（必须）
    'title': str,                     # 视频标题
    'subtitles': Dict[str, Path],     # {lang: path}，无字幕则空 dict
    'metadata': {
        'duration': float,            # 时长（秒），ffprobe 提取
        'url': str,                   # 原始 source URL / 路径
        'video_id': str,              # 平台视频 ID（非平台源为空）
        'description': str,           # 视频描述
        'uploader': str,              # 上传者/频道名
        'upload_date': str,           # 上传日期 YYYYMMDD
        'thumbnail': str,             # 封面 URL
        'channel_id': str,            # 频道 ID
        'subtitle_source': str,       # 字幕来源标记
        'available_subtitles': list,
        'available_auto_subtitles': list,
    }
}
```

### 2.5 guaranteed_fields 契约

每个下载器声明保证返回的字段。executor 在 download() 后断言这些字段非空：

| 下载器 | guaranteed_fields | 说明 |
|--------|-------------------|------|
| **YouTubeDownloader** | `{'title', 'duration', 'description', 'uploader', 'thumbnail'}` | 平台应提供完整元数据 |
| **LocalImporter** | `{'duration'}` | 仅 ffprobe 可得的信息 |
| **DirectURLDownloader** | `{'duration'}` | 仅 ffprobe 可得的信息 |

- 保证字段缺失 → 下载器 bug → **fail-fast**
- 非保证字段缺失 → 正常情况 → 跳过对应后续步骤并 log

---

## 3. 三个下载器实现

### 3.1 YouTubeDownloader

**文件**：`vat/downloaders/youtube.py`  
**继承**：`PlatformDownloader`

通过 yt-dlp 下载 YouTube 视频、字幕和元数据。

**yt-dlp 关键参数**：
- **format**：来自 `downloader.youtube.format`
- **proxy**：来自全局配置 `proxy.http_proxy`
- **outtmpl**：`output_dir / '%(id)s.%(ext)s'`
- **日志适配**：`YtDlpLogger` 将大量 yt-dlp info 降级为 debug

**字幕下载**：
- 默认启用 (`download_subs=True`)
- 默认语言：`['ja', 'ja-orig', 'en']`
- 格式：VTT
- 来源优先级：手动上传字幕 > YouTube 自动生成字幕

### 3.2 LocalImporter

**文件**：`vat/downloaders/local.py`  
**继承**：`BaseDownloader`

将本地视频文件导入 pipeline，不执行下载：
1. 验证文件存在且格式受支持（`.mp4`, `.mkv`, `.webm`, `.avi`, `.mov`, `.flv`, `.ts`, `.m4v`）
2. 在 output_dir 创建**软链接** `original.{ext} → 原始路径`（零磁盘开销）
3. 通过 ffprobe 提取 duration 等元数据
4. 标题从文件名推导（未手动指定时）

**Video ID 生成**：基于文件内容哈希（`md5(前1MB + file_size)[:16]`），同文件不同路径产生相同 ID。

### 3.3 DirectURLDownloader

**文件**：`vat/downloaders/direct_url.py`  
**继承**：`BaseDownloader`

从 HTTP/HTTPS 直链下载视频文件：
1. 流式下载（chunk_size=8192，避免内存爆炸）
2. 从 Content-Disposition 或 URL 路径推导文件扩展名
3. 支持进度回调和代理
4. 保存为 `original.{ext}`
5. 通过 ffprobe 提取元数据

---

## 4. 入口与调用链

### 4.1 CLI 入口

```bash
# 统一通过 --url 参数，内部自动检测源类型
vat pipeline -u "https://www.youtube.com/watch?v=xxx"    # YouTube
vat pipeline -u "/path/to/video.mp4"                      # 本地文件
vat pipeline -u "https://example.com/video.mp4"           # 直链
vat pipeline -u "/path/to/video.mp4" --title "我的视频"   # 手动指定标题
```

实现位置：`vat/cli/commands.py`

流程：
1. `detect_source_type(url)` 自动识别源类型
2. `create_video_from_source(url, db, source_type, title)` 创建 DB 记录
3. `schedule_videos(...)` 调度执行

### 4.2 WebUI 入口

WebUI 通过 `POST /api/videos` 添加视频，支持四种方式：
- **平台链接**：YouTube / Bilibili URL（自动检测）
- **直链**：HTTP/HTTPS 视频文件直链
- **服务器路径**：服务器上的视频文件绝对路径
- **上传视频**：先 `POST /api/videos/upload-file` 上传文件，再创建 LOCAL 记录

### 4.3 核心执行入口

```
VideoProcessor.process()
  → _execute_step(TaskStep.DOWNLOAD)
    → VideoProcessor._run_download()
      → self.downloader.download(source, output_dir, **kwargs)
```

实现位置：`vat/pipeline/executor.py`

---

## 5. Video ID 生成策略

| SourceType | ID 生成方式 | 理由 |
|---|---|---|
| **YOUTUBE** | `md5(url)[:16]` | URL 天然稳定标识 |
| **BILIBILI** | `md5(url)[:16]` | URL 天然稳定标识 |
| **DIRECT_URL** | `md5(url)[:16]` | URL 是稳定标识 |
| **LOCAL** | `md5(文件前1MB + file_size)[:16]` | 基于内容，同文件不同路径 → 同 ID |

注意：内部 `video_id`（16 字符 hex）**不是** YouTube 的 11 位 video id。

---

## 6. 输出目录规范

所有源类型在 download 阶段完成后，output_dir 内都有可被 `_find_video_file()` 找到的视频文件：

| SourceType | download 行为 | 输出文件 |
|---|---|---|
| **YOUTUBE** | yt-dlp 下载 | `{yt_video_id}.mp4`（yt-dlp 命名，`_find_video_file` 兼容） |
| **LOCAL** | 创建软链接 | `original.{ext} → /原始/路径/video.mp4` |
| **DIRECT_URL** | HTTP 下载 | `original.{ext}` |

`_find_video_file()` 查找规则：优先 `original.*`，否则扫描目录内除 `final.*` 外的视频文件。

---

## 7. Metadata 增强（LLM，按数据可用性触发）

`_run_download()` 在下载成功后按**数据是否存在**（而非 source_type）触发后续步骤：

### 7.1 场景识别（SceneIdentifier）
- **触发条件**：下载器返回了 `title`
- 调用：`vat/llm/scene_identifier.py` `SceneIdentifier.detect_scene()`
- 结果写入：`metadata['scene']`, `metadata['scene_name']`, `metadata['scene_auto_detected']`
- 缓存：通过 `call_llm()` 的 `diskcache` memoize

### 7.2 视频信息翻译（VideoInfoTranslator）
- **触发条件**：`config.llm.is_available()` 为真 **且** 没有已有翻译结果
- 调用：`vat/llm/video_info_translator.py` `VideoInfoTranslator.translate()`
- 结果写入：`metadata['translated'] = translated_info.to_dict()`
- 翻译复用：如果 Playlist sync 已异步翻译完成，则跳过
- 缓存：不走 `diskcache`，每次真实发请求

### 7.3 封面下载
- **触发条件**：`metadata['thumbnail']` 非空（仅 YouTube 等平台源有）

---

## 8. 缓存与重复执行语义

### 8.1 步骤级（数据库控制）
- 非 `--force`：若 DB 记录显示 `download` 已完成，则跳过
- `--force`：强制重新执行

### 8.2 下载器级
- YouTube：yt-dlp 有内部缓存策略，已有文件时可能不重新下载
- LOCAL：软链接创建是幂等的（已存在则先删除再创建）
- DIRECT_URL：每次都重新下载

---

## 9. 常见问题与 Debug Checklist

### 9.1 "下载完成但后续找不到视频文件"
- 检查 `<output_dir>/<video_id>/` 是否有视频文件
- 了解 `_find_video_file()` 的查找规则：优先 `original.*`，否则扫描除 `final.*` 外的视频文件

### 9.2 "内部 video_id 和 YouTube video_id 对不上"
- 内部 ID：`md5(url)[:16]`（DB 主键、输出目录名）
- YouTube ID：11 位（下载文件名、`metadata['video_id']`）

### 9.3 LOCAL 软链接问题
- 跨文件系统时软链接可能失败，此时 LocalImporter 会 fallback 为复制并 warn
- 源文件被删除后软链接断裂，重新处理会报明确错误

### 9.4 代理/网络
- 检查 `proxy.http_proxy` 配置
- YouTube 和 DIRECT_URL 共用全局代理配置
- LOCAL 不需要网络

### 9.5 YouTube 风控
- playlist 列表获取：基本无风控
- 视频 info 获取：并发 10 偶尔触发验证，yt-dlp 可自动处理
- 视频下载：风控较明显，建议并发=1
- 失败重试：Pipeline 层面有"失败放队尾"重试机制（最多 2 轮）

### 9.6 Debug Checklist
1. 确认 DB 记录：`video_id` 存在、`source_url`/`source_type` 正确
2. 确认输出目录：`<output_dir>/<video_id>/` 存在
3. 确认视频文件：目录内有视频文件（`original.*` 或 `<youtube_id>.*`）
4. 确认 `_find_video_file()` 能找到文件
5. 确认 LLM 副作用（可选）：场景识别/视频信息翻译是否触发

---

## 10. 关键代码索引

| 组件 | 文件位置 | 函数/类 |
|------|----------|---------|
| 下载器基类 | `vat/downloaders/base.py` | `BaseDownloader`, `PlatformDownloader` |
| YouTube 下载器 | `vat/downloaders/youtube.py` | `YouTubeDownloader` |
| 本地文件导入器 | `vat/downloaders/local.py` | `LocalImporter` |
| HTTP 直链下载器 | `vat/downloaders/direct_url.py` | `DirectURLDownloader` |
| 源类型检测 | `vat/pipeline/executor.py` | `detect_source_type()` |
| 创建视频记录 | `vat/pipeline/executor.py` | `create_video_from_source()` |
| 执行下载 | `vat/pipeline/executor.py` | `VideoProcessor._run_download()` |
| 查找视频文件 | `vat/pipeline/executor.py` | `_find_video_file()` |
| 场景识别 | `vat/llm/scene_identifier.py` | `SceneIdentifier.detect_scene()` |
| 视频信息翻译 | `vat/llm/video_info_translator.py` | `VideoInfoTranslator.translate()` |
| Content Hash | `vat/downloaders/local.py` | `generate_content_based_id()` |

---

## 11. 修改指南

| 如果你想... | 应该看/改哪里 |
|-------------|---------------|
| 添加新的下载源 | 继承 `BaseDownloader`（通用源）或 `PlatformDownloader`（有 playlist 的平台） |
| 改 video_id 生成规则 | `create_video_from_source()` 或各下载器的 `extract_video_id()` |
| 改下载文件名格式 | `YouTubeDownloader._get_ydl_opts()` 的 `outtmpl` |
| 改场景识别逻辑 | `vat/llm/scene_identifier.py` + `vat/llm/scenes.yaml` |
| 改视频信息翻译格式 | `vat/llm/video_info_translator.py` |
| 改 yt-dlp 参数 | `config.downloader.youtube.*` |
| 改 LOCAL 软链接行为 | `LocalImporter.download()` |
| 改直链下载行为 | `DirectURLDownloader.download()` |
| 改 WebUI 添加视频弹窗 | `vat/web/templates/index.html` + `vat/web/routes/videos.py` |
