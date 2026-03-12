# 翻译模型评测 & ASR质量评估 综合报告

**评测时间**：2026年2月—3月
**评测目标**：评估替代模型做VTuber字幕翻译的可行性，以及ASR质量改进方向

---

## 一、背景

当前pipeline使用Google Gemini-3-flash做日语→中文字幕翻译，质量好但成本高。评测了以下替代方案：

| 方案 | 来源 | 月成本 | 独有模型 |
|------|------|--------|--------|
| 火山引擎方舟 | 火山引擎(字节) | 按量付费 | deepseek-v3.2, doubao系列 |
| DashScope | 阿里云 | 按量付费 | qwen3-max, qwen3.5-plus, glm-5, MiniMax-M2.5 |
| gpt-5-nano | videocaptioner中转站 | 按量付费 | gpt-5-nano |
| **phpth 中转站** | **第三方(免费)** | **免费** | **gpt-5.4（标称含 claude 但均为 GPT 马甲）** |
| Gemini-3-flash | Google API | ~¥150/20k次 | Gemini-3-flash |

火山引擎与 DashScope 共有模型：kimi-k2.5, glm-4.7。跨厂商验证确认同模型质量完全一致。

---

## 二、评测方法

- 独立评测脚本 `scripts/translation_benchmark.py`，不走生产pipeline
- 逐条定性阅读评估（非机械指标），对照原文日语+Gemini翻译+测试模型翻译
- 中文版提示词和few-shot示例存放在 `scripts/translation_benchmark_prompts/`

### 测试视频（共16个）

| 视频ID | 内容类型 | 字幕条数 | 采样 |
|--------|---------|---------|------|
| _QOMPli80JA | 双人联动对话 | 155 | 全量 |
| czIBPN1eCbU | 美食实况 | 448 | 全量 |
| k8CCqKYx2Pk | 马里奥卡丁车(ASR差) | 234 | 全量 |
| i-EreP4zejg | 混合(歌词+叙事) | 892 | 100 |
| enAZ8wvvgl4 | 直播开箱 | 242 | 100 |
| oE8HLVwpimA | 多人正式公告 | 103 | 全量 |
| q2B_u_wZWAQ | 恐怖游戏直播 | 511 | 80 |
| X53mU_mxCDQ | 杂谈直播 | 406 | 80 |
| CMvE8F9INDM | 赛车30分挑战 | 84 | 80 |
| DoDiPWf0Rg4 | 恐怖游戏杂谈 | 401 | 80 |
| IEYQQQFmxak | 马里奥卡丁车REVENGE | 212 | 100 |
| ajda3lbz6Mk | 风来のシレン | 237 | 100 |
| 8Q6WtSDdbNk | 初配信(Live2D展示) | 313 | 全量(phpth) |
| cgbcUfgQF6M | Only Up实况 | 509 | 全量(phpth) |
| jjPBUsN5jl4 | 杂谈配信 | 725 | 全量(phpth) |

---

## 三、翻译参数优化

在 `_QOMPli80JA` 50条样本上系统测试了22+组配置：

| 因素 | 测试范围 | 结论 |
|------|---------|------|
| **提示词语言** | 英文/中文 | 中文推荐（更好），英文也可接受 |
| **Few-shot示例数** | 0/6/12/25+条 | **12条最优**：6→12提升显著，25+反而下降 |
| **翻译模式** | reflect/standard | **reflect更好**（更生动、语义错误更少） |
| **Batch size** | 10/30/50/100 | **质量无差异，BS越大越好**（减少API调用次数） |
| **Temperature** | 0.3/0.7/1.0 | 差异不大，默认1.0即可 |
| **Optimize** | 有/跳过 | 跳过影响微小，但保留为好 |
| **Thinking模式** | enabled/disabled | **应关闭**（对ASR纠错无帮助，慢50-100倍） |
| **混合方案** | Gemini opt+kimi trans | 无额外收益（Gemini纠错在translate阶段） |
| **ASR-aware prompt** | 告知模型输入是ASR | 无效（模型能力硬限制） |

**关于Batch size**：逐条阅读BS=30/50/100三组翻译，差异仅为随机措辞变化（如"诶"vs"咦"），无质量退化。BS越大API调用次数越少，推荐BS=100。

**关于Thinking模式**：用7个ASR乱码案例做AB对比，thinking=enabled对ASR纠错完全无帮助，且单条延迟从0.4-4.5s增至16-270s。ASR纠错是模型知识层面的差距，不是推理深度问题。

**关于reflect vs standard的修正**：初始实验用"Gemini完全一致率"得出standard>reflect的错误结论。逐条定性阅读后确认reflect更好——standard在美食实况视频中将拉面品牌"スミレ"翻译成"紫罗兰"（连续8条错误），reflect正确保留了品牌名。

---

## 四、模型横向对比

### 全模型排名（按翻译质量）

| 排名 | 模型 | 来源 | 翻译风格 | 速度(155条) | 推荐度 |
|------|------|------|---------|------------|--------|
| 1 | **kimi-k2.5** | **火山引擎方舟** | 最活泼可爱，VTuber味浓 | 176s | ⭐⭐⭐⭐½ |
| 2 | **deepseek-v3.2** | **火山引擎方舟** | 语义准确，风格偏正经 | 145s | ⭐⭐⭐⭐ |
| 3 | **qwen3-max** | **DashScope** | 自然流畅，偶尔更准确 | ~180s | ⭐⭐⭐⭐ |
| 4 | **phpth-gpt-5.4** | **phpth中转站(免费)** | prompt遵循强，人名偏罗马字 | 69s(bs30,t10) | ⭐⭐⭐⭐ |
| 5 | qwen3.5-plus | DashScope | 偏正式/平淡 | 慢 | ⭐⭐⭐ |
| 6 | doubao-seed-code | 火山引擎方舟 | 中规中矩 | 93s(50条) | ⭐⭐⭐ |
| 7 | gpt-5-nano | 中转站 | 平淡，招牌语丢失 | 快 | ⭐⭐⭐ |
| 8 | glm-5 | DashScope | 可用但无特色 | 很慢 | ⭐⭐½ |
| 9 | doubao-seed-2.0-code | 火山引擎方舟 | 最快但有错译风险 | 59s(50条) | ⭐⭐½ |
| 10 | MiniMax-M2.5 | DashScope | optimize无效，翻译勉强 | 快 | ⭐⭐ |
| - | glm-4.7 | 火山引擎方舟/DashScope | 可用但太慢(668s/50条) | - | ⭐ |
| - | kimi-k2-thinking | 火山引擎方舟 | 翻译任务失败 | - | ❌ |
| - | auto | 火山引擎方舟 | 不支持指定模型 | - | ❌ |

### kimi-k2.5 vs deepseek-v3.2 关键差异

两者都是火山引擎方舟的可用模型，风格取向不同：

| 案例 | DeepSeek | Kimi | 判定 |
|------|----------|------|------|
| #72 ツンデレセリフ(傲娇台词) | "来段傲娇台词！" ✅ | "来段傲娇自拍吧～" ❌(误解为selfie) | DeepSeek |
| #148 ガチャ(开门声) | "咔嚓（开门声）啊" ✅ | "是抽卡呀——" ❌(误解为gacha) | DeepSeek |
| #3 招牌语 | "空空狐狸～" ✅ | "狐狐——狐狸——" △ | DeepSeek |
| #123 モコ(气鼓鼓) | "已经是Moko了" △(直译) | "气鼓鼓了哟～" ✅(生动) | Kimi |
| 傲娇台词整体风格 | 自然流畅，偏正经 | 更活泼可爱，VTuber味浓 | 看需求 |

| 维度 | DeepSeek-v3.2 | kimi-k2.5 |
|------|--------------|-----------|
| 语义准确性 | ⭐⭐⭐⭐ | ⭐⭐⭐½ |
| VTuber风格/活泼度 | ⭐⭐⭐½ | ⭐⭐⭐⭐½ |
| 速度 | ⭐⭐⭐⭐½ | ⭐⭐⭐⭐ |
| ASR纠错 | ⭐⭐ | ⭐⭐ |

**选择建议**：追求VTuber风格→kimi-k2.5；追求语义准确/速度→deepseek-v3.2。

### nano vs kimi-k2.5 对比（6个视频，470条）

| 维度 | gpt-5-nano | kimi-k2.5 | Gemini(参考) |
|------|-----------|-----------|-------------|
| VTuber风格/活泼度 | ⭐⭐⭐ | ⭐⭐⭐⭐½ | ⭐⭐⭐⭐⭐ |
| 忠实度/准确性 | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| 术语/招牌语处理 | ⭐⭐ | ⭐⭐⭐½ | ⭐⭐⭐⭐⭐ |
| 语气还原 | ⭐⭐½ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

nano核心问题：VTuber招牌语丢失（`こんこんきーつね` → "嗨，大家好～"❌），语义错误频率高。

### phpth gpt-5.4 vs Gemini 对比（7个视频全量，fubuki prompt）

phpth 中转站标称提供 claude-opus-4-6 / claude-sonnet-4-6 / gpt-5.4 等模型，实测 claude 均为 GPT 马甲（claude-opus-4-6 → gpt-5.4, claude-sonnet-4-6 → gpt-5.2-codex）。sonnet 路由到的 gpt-5.2-codex 在 optimize 阶段出现灾难级错误（"白上フブキ"→"白髪ロボ子"），不可用。以下仅分析 gpt-5.4。

测试配置：batch_size=30, thread_num=10, fubuki prompt(en), reflect mode。batch_size 受 API 端 nginx 超时限制，translate+reflect 模式下 bs≥80 会 504，推荐 bs=30。

#### phpth 胜出的场景

```
jjPBUsN5jl4: "FANBOXはいつ更新"
  phpth:  FANBOX什么时候更新 ✅    ← ASR纠错正确
  Gemini: HOMEBOX什么时候更新 ❌

jjPBUsN5jl4: "Cですよね"（クリスマスのク→C双关）
  phpth:  应该是C吧 ✅             ← 捕捉到双关语
  Gemini: 对吧？ ❌                ← 完全漏掉

_QOMPli80JA: "こんこんきーつね"
  phpth:  晚上好狐～ ✅             ← 严格遵循fubuki prompt
  Gemini: 空空狐狸～
```

#### Gemini 胜出的场景

```
8Q6WtSDdbNk: "ルズルルズです"
  phpth:  我是Ruzururuzu ❌         ← 未识别画师名
  Gemini: 我是Rurudo ✅

DoDiPWf0Rg4: "ケイコさん/マサイさん"
  phpth:  Keiko/Masai（罗马字）      ← 人名未汉化
  Gemini: 惠子/昌江（汉字） ✅

jjPBUsN5jl4: "冬コミ出ない"
  phpth:  不参加冬Comic Market      ← 直译
  Gemini: 不去冬漫展 ✅              ← 本地化
```

#### 综合评分

| 维度 | phpth gpt-5.4 | Gemini |
|------|--------------|--------|
| ASR 纠错 | 70 | **85** |
| 人名处理 | 60（偏罗马字） | **90**（汉化） |
| VTuber 语感 | 75 | **85** |
| prompt 遵循 | **90** | 75 |
| 双关/梗识别 | **80** | 70 |
| 事实准确性 | 85 | 85 |
| **综合** | **~77** | **~82** |

日常对话、游戏实况、叙事准确性基本持平，差距集中在人名汉化和 ASR 纠错。

#### 并发能力

100 API 并发（10视频×10线程）零失败，吞吐上限约 3.6 段/s。稳定性良好，但存在总吞吐天花板。

---

## 五、全流程定性评估（3个视频，837条逐条阅读）

对3个视频的全量翻译，逐条对照原文日语、Gemini翻译、Volc kimi-k2.5翻译，基于语义准确性、VTuber风格、专有名词处理做人工定性评估。

### 整体质量分布

| 视频 | 条数 | 持平 | Gemini更好 | Volc更好 | 两者都差 |
|------|------|------|-----------|---------|---------|
| _QOMPli80JA (对话) | 155 | ~120(77%) | ~15(10%) | ~8(5%) | ~12(8%) |
| czIBPN1eCbU (美食) | 448 | ~380(85%) | ~35(8%) | ~15(3%) | ~18(4%) |
| k8CCqKYx2Pk (ASR差) | 234 | ~180(77%) | ~35(15%) | ~8(3%) | ~11(5%) |
| **总计** | **837** | **~680(81%)** | **~85(10%)** | **~31(4%)** | **~41(5%)** |

### Gemini更好的模式（~85条，占10%）

**1. ASR乱码纠错（占差距的~60%）**

Gemini能"看穿"ASR乱码推断正确含义，kimi/deepseek直译乱码：

```
k8CCqKYx2Pk #14: "アバフライサー"
  Gemini: 马力欧卡丁车！ ← 看穿乱码
  Kimi:   ABoFlieser！ ← 直译乱码

czIBPN1eCbU #5: "えーしょうゆです"（实际是自我介绍）
  Gemini: 我是白上吹雪。 ← 识别ASR错误
  Kimi:   诶——是酱油哟～ ← 直译"しょうゆ=酱油"
```

**2. VTuber专有知识（占差距的~25%）**

```
k8CCqKYx2Pk #2: "snacksどもねーす"
  Gemini: すこん部各位好呀—— ← 识别粉丝团名
  Kimi:   snacks的大家好呀—— ← 直译

k8CCqKYx2Pk #101: "ボギーゾーだ"
  Gemini: 是耀西耶！ ← 识别马里奥角色名
  Kimi:   是博基哦～ ← 音译
```

**3. 品牌名/文化引用（占差距的~15%）**

```
czIBPN1eCbU #62-71: "スミレ"（拉面品牌名）
  Gemini: Sumire ← 全程正确保留品牌名
  Kimi:   紫罗兰 ← 连续10条错译（后半段#330+自行修正）
```

### Volc kimi-k2.5更好的模式（~31条，占4%）

**1. VTuber风格更活泼**
```
_QOMPli80JA #38: "痛い"
  Gemini: 好疼。      ← 平实
  Kimi:   痛痛～      ← 更可爱

_QOMPli80JA #123: "もうモコだよモコですよ"
  Gemini: 就是说呀，真是的。       ← 丢失拟声意象
  Kimi:   已经气鼓鼓了哟～气鼓鼓的！ ← 生动保留
```

**2. 数字/事实更忠实**
```
czIBPN1eCbU #197: "10分で来た来た来た来た！"
  Gemini: 15分啦！        ← 错误修改原文数字
  Kimi:   10分钟到啦到啦！ ← 保留原文
```

---

## 六、ASR质量评估

### 6.1 大规模参数评测（350个VTuber视频）

从Hololive旗下49203个视频中筛选350个有YouTube人工标注日语字幕的视频，用CER(字符错误率)评估。

**CER分布**：
- 平均CER约50-60%，但有误导性——45%样本是歌曲/音乐视频（ASR无法处理）
- 剩余正常样本CER主要来自汉字vs假名写法差异、标点差异
- 实际对比ASR和GT文本，内容基本正确

**参数效果排名**（167个正常样本）：

| 排名 | 参数 | 平均CER | 相对baseline |
|------|------|---------|-------------|
| 1 | copt=false + HST=2 | 49.2% | +2.8% |
| 2 | HST=2 | 49.9% | +1.4% |
| 3 | copt=false | 50.5% | +0.4% |
| 11 | baseline | 50.7% | 0% |
| 14 | repetition_penalty=1.2 | 51.3% | -1.2% |

关键结论：`condition_on_previous_text=false`和`hallucination_silence_threshold=2`轻微改善；`repetition_penalty>1.0`反而有害；`initial_prompt`无益处。参数调优总收益有限（~1-3% CER）。

### 6.2 模型对比（large-v3 vs large-v3-turbo）

| 指标 | large-v3 beam7 | turbo beam5 |
|------|---------------|-------------|
| 速度 | 基准 | 快**3-4倍** |
| 质量 | 基准 | 基本相当 |
| 核心乱码 | 无改善 | 无改善 |
| 断句 | 正常 | 更碎（笑声/语气词独立成段） |

### 6.3 beam_size对比

beam=5是性价比最优（与beam=7差异微小，速度提升约10%）。

### 6.4 当前生产配置

```yaml
asr:
  model: "large-v3"
  beam_size: 7                    # 建议改为5
  condition_on_previous_text: false
  temperature: [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
  no_speech_threshold: 0.6
  initial_prompt: ""
  repetition_penalty: 1.0
  hallucination_silence_threshold: 2
```

### 6.5 已知问题

- **temperature=0.0 + condition_on_previous_text=false**：会产生重复字符幻觉，需用温度回退列表
- **ASR乱码根因**：Whisper对快语速日语口语（尤其有BGM时）的架构层面限制，换模型/调参数无法突破

---

## 七、核心瓶颈分析

**整个pipeline的最大瓶颈是ASR质量，不是翻译模型。**

基于837条全量定性评估的量化数据：
- **81%的翻译持平**——绝大多数观众不会感知 kimi 与 Gemini 的差异
- Gemini更好的10%中，**60%源于ASR纠错**——这是模型知识层面的差距，thinking模式也无法弥补
- 对于ASR质量正常的视频（约80-90%），Volc翻译效果完全可用于生产
- ASR质量差的视频是真正的瓶颈，改善方向应聚焦于ASR后处理

---

## 八、推荐方案

### 各环节推荐模型

| 环节 | 推荐模型 | 来源 | 理由 |
|------|---------|------|------|
| **翻译(translate)** | gemini-3-flash | Google API | 质量最优，ASR纠错能力强 |
| **优化(optimize)** | **kimi-k2.5** | **火山引擎方舟** | **已集成**。同语言optimize阶段有良好ASR纠错能力 |
| **断句(split)** | gpt-4o-mini | 中转站 | 测试国产模型效果不佳（过度断句），保持不变 |
| **场景识别** | gpt-4o-mini | 中转站 | 对模型要求低 |
| **翻译(备用)** | phpth-gpt-5.4 | phpth中转站(免费) | Gemini不可用时的后备，bs≤50，质量~77分(Gemini~82) |

> **关于translate是否替换**：从837条定性评估看，81%持平、10%Gemini更好（主要是ASR纠错）。phpth gpt-5.4 免费但人名汉化和 ASR 纠错不如 Gemini，适合做备用通道而非主力。

### Split 阶段国产模型测试

用 _QOMPli80JA 的 raw ASR 文本，对比 gpt-4o-mini / volc-kimi-k2.5 / volc-deepseek-v3.2 的断句效果：

| 模型 | 断句数(前100段raw) | 问题 |
|------|-------------------|------|
| gpt-4o-mini (生产) | 72句 | 语义完整，断句合理 |
| deepseek-v3.2 | 79句(+10%) | 过度断句：在条件句中间(～ば后)、宾格助词(を后)错误断开 |
| kimi-k2.5 | 35句/50段(更碎) | 更严重的过度断句，且速度慢(48s) |

```
#1 "今回何回私たちが起こせば気が済むんですか？"（完整条件句）
  gpt-4o-mini: 保持完整 ✅
  deepseek:    拆成 "起こせば" / "気が済むんですか？" ❌ (条件句中间断开)

#7 "もうでは早速ロボ子先輩をびっくりさせちゃいましょーう"
  gpt-4o-mini: 保持完整 ✅  
  deepseek:    拆成 "ロボ子先輩を" / "びっくりさせ..." ❌ (宾语を后断开)
```

**结论：国产模型对日语语法结构（条件句、助词）的断句判断不准确，split 继续使用 gpt-4o-mini。**

### Optimize 阶段国产模型测试

kimi-k2.5 在同语言 optimize 阶段展现了良好的 ASR 纠错能力：

```
#4: "ちらかみむむきでーす" → "白上フブキです"     ← 识别出自我介绍！
#28: "キュウリさん来ましたよ" → "フブキさん来ましたよ" ← 修正人名ASR错误
#27: "ロコ先輩きたー！" → "ロボ先輩きたー！"         ← 修正人名
#3:  "こんこんきーっすねー" → "こんこんきーつねー"     ← 修正招牌语发音
```

这是一个重要发现：**虽然kimi在translate阶段（跨语言）无法纠正ASR乱码，但在optimize阶段（同语言日语→日语）可以修正许多ASR错误。** optimize用国产模型既节省Gemini额度，又能在翻译前先修正一部分ASR问题。

**已集成到 `config/default.yaml`**：optimize 使用 volc-kimi-k2.5，BS=100，thread=3。

### Optimize 对 Translate 的影响验证

初始观察到"optimize纠正ASR后translate反而翻错"的现象，经控制实验排查，确认为**误判**：

- **根因**：diskcache使旧版翻译结果来自缓存；新版optimize改变输入→cache miss→temperature=1.0下产生不同随机结果
- **控制实验**：对问题条目禁用缓存、固定输入做3轮batch翻译
  - `白上フブキですー`(#4)：3轮全部正确 "我是白上吹雪～" ✅
  - `フブキさん来ましたよ`(#28)：3轮全部正确 "吹雪来啦～" ✅（且比Gemini翻的"突然冒出来了"更准确）
  - `こんこんきーつねー`(#3)：3轮3种不同结果——但单独测5次也产生5种结果，这是招牌语翻译的**固有不稳定性**，与optimize无关

**结论：optimize没有负面影响。kimi-k2.5在optimize阶段的ASR纠错对后续翻译是正面贡献。**

### Translate 替换为国产模型的 Trade-off 分析

基于30条控制实验（kimi optimize后，Gemini translate vs kimi translate）：

| 分类 | 比例 | 说明 |
|------|------|------|
| 完全一致 | 30% | 翻译完全相同 |
| 持平（仅措辞差异） | 47% | "诶"vs"咦"、"～"vs"——"级别 |
| Gemini略好 | 20% | 语境补充更完整（如"好像不在家呀"vs"不在呢"） |
| Kimi更好 | 3% | optimize纠正ASR后kimi翻译更准确 |

**成本差异**：当前配置下Gemini translate月消耗仅~160次调用（BS=100，20个视频）≈ ¥1.2/月。切换到kimi translate只能再省¥1.2/月。

**结论：保持 kimi optimize + Gemini translate 的混合配置。** 理由：
1. Gemini translate成本已极低（optimize已省了一半调用量）
2. Gemini在复杂ASR乱码上的纠错能力无可替代（如"アバフライサー"→马里奥卡丁车）
3. 切换到全国产translate质量降幅小但收益也小，不值得trade

**切换到全国产的适用场景**：Gemini API额度不够 / 需要完全脱离Google依赖。此时大部分观众感知不到差异（只有ASR差的视频会退化）。

### 当前生产配置

```yaml
# translate: Gemini（质量最优，保持不变）
translator.llm.model: gemini-3-flash-preview
translator.llm.enable_reflect: true
translator.llm.batch_size: 100
translator.llm.thread_num: 10

# optimize: 火山引擎 kimi-k2.5（已替换，节省Gemini额度）
translator.llm.optimize.model: kimi-k2.5
translator.llm.optimize.base_url: ark.cn-beijing.volces.com/api/v3
translator.llm.optimize.batch_size: 100
translator.llm.optimize.thread_num: 3

# split: gpt-4o-mini（不变）
asr.split.model: gpt-4o-mini
asr.split.base_url: api.videocaptioner.cn
```

技术细节：`call_llm` 中已内置火山引擎 `thinking=disabled` 自动注入，无需额外配置。

Few-shot prompt文件：`scripts/translation_benchmark_prompts/custom_translate_fubuki_zh_fewshot_plus.md`

### 成本对比

| 方案 | 月费 | 翻译质量 | 备注 |
|------|------|---------|------|
| Gemini-3-flash | ~¥150/20k次 | ⭐⭐⭐⭐⭐ | 取决于额度 |
| **火山引擎 kimi-k2.5** | **按量付费** | **⭐⭐⭐⭐** | **推荐 optimize 用** |
| DashScope kimi-k2.5 | 按量付费 | ⭐⭐⭐⭐ | 备用 |
| 中转站 nano | 按量付费 | ⭐⭐⭐ | 取决于余额 |
| **phpth gpt-5.4** | **免费** | **⭐⭐⭐⭐** | **免费备用，bs≤50，人名偏罗马字** |

### 混合方案可行性

用kimi处理大部分内容、Gemini只处理ASR乱码段落，可将Gemini调用量降至~10%同时维持接近全Gemini的效果。

检测"ASR乱码段落"的可能方案：
- 基于Whisper输出的置信度（avg_logprob）标记低置信度段
- 基于文本特征（拉丁字符、重复字符、罕见假名组合）
- 翻译后检测（输出中的音译乱码、长度异常）

当前判断：可行但需单独开发。短期内直接用volc-kimi-k2.5全量翻译最简单。

---

## 九、后续改进方向

| 方向 | 优先级 | 状态 | 说明 |
|------|--------|------|------|
| **optimize集成** | 高 | ✅ 已完成 | volc-kimi-k2.5 已集成到 config，`call_llm` 已内置thinking自动关闭 |
| **translate替换测试** | 高 | 已测 | kimi opt+kimi trans 全流程已测，与纯Gemini对比见下方。当前保持Gemini translate |
| **ASR后处理改进** | 高 | 进行中 | 由另一人同步探究 |
| **翻译后异常检测** | 中 | 待做 | 自动检测乱码/幻觉/长度异常 |
| **混合方案原型** | 中 | 待做 | ASR低置信度段落检测 + kimi+Gemini混合调用 |
| **并发控制测试** | 低 | 待做 | thread_num=5稳定性（当前thread=3无问题） |
| **phpth 免费中转站评估** | 中 | ✅ 已完成 | 7视频全量+并发压测，质量~77分（Gemini~82分），适合做备用通道。详见第四章 |

---

## 十、Gemini 系列型号横向评测

**评测时间**：2026年3月10日
**评测目标**：对比 Gemini 系列 4 个型号的翻译质量与成本效益，寻找比当前生产模型 gemini-3-flash 更具性价比的替代方案。

### 10.1 测试模型与定价

| 模型 | API Model ID | Input $/1M | Output $/1M | 相对成本 | 定位 |
|------|-------------|------------|-------------|---------|------|
| **Gemini 3 Flash** | `gemini-3-flash-preview` | $0.50 | $3.00 | 1.00x | 当前生产模型 |
| **Gemini 3.1 Flash-Lite** | `gemini-3.1-flash-lite-preview` | $0.25 | $1.50 | 0.50x | 3代最新轻量版 |
| **Gemini 2.5 Flash** | `gemini-2.5-flash` | $0.30 | $2.50 | 0.80x | 上一代主力 (thinking 模型) |
| **Gemini 2.5 Flash-Lite** | `gemini-2.5-flash-lite` | $0.10 | $0.40 | 0.14x | 上一代轻量版 |

### 10.2 测试配置

完全匹配生产 `config/default.yaml` 配置：

| 参数 | 值 | 说明 |
|------|-----|------|
| optimize 模型 | kimi-k2.5 (volc) | 所有视频统一用 kimi-k2.5 做 optimize |
| translate 模型 | 4 个 Gemini 模型分别测试 | 仅 translate 阶段使用不同 Gemini 模型 |
| reflect | true | 反思模式 |
| batch_size | 100 | 与生产一致 |
| thread_num | 10 | 与生产一致 |
| custom_prompt | fubuki (English) | 与生产一致 |
| sample | 0 (全量) | 不采样 |
| proxy | http://localhost:7990 | Google API 代理 |

测试覆盖全部 15 个视频（共 5462 条字幕），包括双人对话、美食实况、赛车游戏（ASR差）、恐怖游戏、杂谈、初配信等多种场景。

### 10.3 速度与吞吐量对比

| 模型 | 总翻译时间 | 平均速度(条/秒) | 相对速度 |
|------|-----------|---------------|---------|
| Gemini 3 Flash | 695s (11.6min) | 7.9 | 1.0x |
| Gemini 3.1 Flash-Lite | **313s (5.2min)** | **17.4** | **2.2x** |
| Gemini 2.5 Flash | 1072s (17.9min) | 5.1 | 0.6x |
| Gemini 2.5 Flash-Lite | 380s (6.3min) | 14.4 | 1.8x |

- **3.1-flash-lite 速度最快**，是 3-flash 的 2.2 倍
- **2.5-flash 最慢**，比 3-flash 还慢 54%，因为它是 thinking 模型，内部有推理过程
- 两个 lite 模型速度都远快于非 lite 版本

### 10.4 翻译质量逐条定性评估

#### Gemini 3 Flash（当前生产模型）

整体最优。翻译自然流畅、VTuber 风格把握准确，专有名词处理正确：

| 表现维度 | 评价 |
|---------|------|
| VTuber 口吻/语感 | 活泼自然，符合白上吹雪人设（"耶——！"、"嘿咻——"、"真的好厉害呢～"） |
| 专有名词 | 正确翻译 Roboco前辈、すこん部等 VTuber 术语 |
| ASR 纠错补偿 | 能较好地推断 ASR 错误的真实含义 |
| 语义准确性 | 高，极少出现误译 |
| 空翻译/乱码 | 0 条（5462 条中） |

#### Gemini 3.1 Flash-Lite（半价模型）

质量可接受，是性价比最优候选。翻译基本正确但风格略显平淡：

| 表现维度 | 评价 |
|---------|------|
| VTuber 口吻/语感 | 偏朴素，少了些活泼感（"我是白上吹雪。"vs 3-flash 的"我是白上吹雪～！"） |
| 专有名词 | 大部分正确，偶尔丢失细节（"硬币"vs"金币"） |
| ASR 纠错补偿 | 一般，对严重 ASR 错误的推断能力弱于 3-flash |
| 语义准确性 | 较高，少数口语化表达翻译偏机械 |
| 空翻译/乱码 | 0 条 |

典型案例对比（_QOMPli80JA #7）：
- 原文：もう、では早速ロボ子先輩をびっくりさせちゃいましょーう
- 3-flash：那么这就赶快去吓Roboco前辈一跳吧～
- 3.1-lite：那么赶紧来吓Roboco前辈一跳吧！
- 评价：3.1-lite 翻译正确但少了"这就"等连接词带来的口语自然感

#### Gemini 2.5 Flash（上一代 thinking 模型）

质量与 3-flash 接近，但速度最慢、性价比低：

| 表现维度 | 评价 |
|---------|------|
| VTuber 口吻/语感 | 较好，但有时偏书面化（"事不宜迟"等略正式的表达） |
| 专有名词 | 基本正确 |
| ASR 纠错补偿 | 与 3-flash 相当 |
| 语义准确性 | 高，但偶有空翻译（3条/5462条） |
| 速度 | **最慢**（thinking 模型开销），比 3-flash 慢 54% |

不推荐：质量与 3-flash 持平，但速度慢 54%、价格仅便宜 20%，性价比最差。

#### Gemini 2.5 Flash-Lite（最便宜模型）

**存在严重质量问题，不推荐用于生产**：

| 表现维度 | 评价 |
|---------|------|
| 致命问题 | 部分字幕输出罗马字而非中文翻译（如 "kon kon ki~su ne~"、"Ko chi?"） |
| 致命问题 | 部分字幕保留日文原文不翻译（如 "よしよし"、"よししょー"） |
| VTuber 口吻 | 即使翻译成功的部分，风格也偏平淡 |
| 空翻译 | 2 条 |
| 未翻译/罗马字 | 多个视频出现，k8CCqKYx2Pk 中高达 11 处 |

典型失败案例（_QOMPli80JA）：
- #3 こんこんきーつねー → "kon kon ki~su ne~"（应为"晚上好狐～"）
- #18 コチ？ → "Ko chi?"（应为"这边？"）
- #52 よしよし → "よしよしよし"（未翻译）
- #53 よいしょー → "よししょー"（未翻译）

虽然价格仅为 3-flash 的 14%，但翻译可靠性不足以用于生产。

### 10.5 性价比综合矩阵

| 模型 | 质量 | 速度 | 单价 | 性价比评级 | 推荐场景 |
|------|------|------|------|-----------|---------|
| **3-flash** | ★★★★★ | ★★★☆☆ | ★★☆☆☆ | **生产首选** | 当前生产环境，质量要求高的场景 |
| **3.1-flash-lite** | ★★★★☆ | ★★★★★ | ★★★★☆ | **最佳性价比** | 成本敏感场景、批量处理 |
| 2.5-flash | ★★★★☆ | ★★☆☆☆ | ★★★☆☆ | 不推荐 | 无明显优势 |
| 2.5-flash-lite | ★★☆☆☆ | ★★★★☆ | ★★★★★ | 不推荐 | 翻译可靠性不足 |

### 10.6 结论与建议

1. **当前生产配置维持不变**：gemini-3-flash-preview 仍是质量最优的翻译模型
2. **gemini-3.1-flash-lite 值得关注**：
   - 质量约为 3-flash 的 85-90%，对大部分内容可接受
   - 速度 2.2 倍、成本 50%，综合性价比最优
   - 适合作为成本优化方案或大批量处理的备选
   - 主要弱点：口语化表达、VTuber 特殊术语的处理不如 3-flash 细腻
3. **gemini-2.5-flash 不推荐**：thinking 模型导致速度最慢，质量未见优势
4. **gemini-2.5-flash-lite 不可用**：存在翻译失败（输出罗马字/不翻译）的致命问题

### 10.7 Gemini 横评复现命令

```bash
# 批量运行全部 Gemini 横评（已有 run_gemini_benchmark.sh 脚本）
bash scripts/run_gemini_benchmark.sh

# 单个模型手动运行（示例：gemini-3.1-flash-lite）
# 步骤1: optimize
python scripts/translation_benchmark.py --video-id VIDEO_ID --model volc-kimi-k2.5 \
    --sample 0 --batch-size 100 --thread-num 10 --optimize-only --tag gemini_bench

# 步骤2: translate（使用步骤1的 optimized.srt）
python scripts/translation_benchmark.py --video-id VIDEO_ID --model gemini-3.1-flash-lite \
    --sample 0 --batch-size 100 --thread-num 10 \
    --optimized-input scripts/translation_benchmark_results/VIDEO_ID/volc-kimi-k2.5_full_gemini_bench/optimized.srt \
    --tag gemini_bench
```

新增模型配置名：`gemini-3-flash`、`gemini-3.1-flash-lite`、`gemini-2.5-flash`、`gemini-2.5-flash-lite`

---

## 十一、API 端点信息

| 服务 | 提供商 | Base URL | 主要用途 | 环境变量 |
|------|--------|----------|--------|--------|
| 火山引擎方舟 | 火山引擎 | `https://ark.cn-beijing.volces.com/api/coding/v3` | **optimize 主力** | `VAT_VOLC_APIKEY` |
| Google Gemini | Google | `https://generativelanguage.googleapis.com/v1beta/openai` | **translate 主力** | `VAT_GOOGLE_APIKEY` |
| DashScope | 阿里云 | `https://dashscope.aliyuncs.com/compatible-mode/v1` | 备用 | `VAT_DASHSCOPE_APIKEY` |
| videocaptioner中转站 | 第三方 | `https://api.videocaptioner.cn` | split/scene_identify | - |
| **phpth 中转站** | **第三方(免费)** | **`https://ai.phpth.cn/v1`** | **translate 备用** | **`VAT_PHPTH_APIKEY`** |

**注意**：
- 火山引擎 thinking 自动关闭已内置到 `call_llm`（`vat/llm/client.py`），无需手动配置
- phpth 中转站使用时 batch_size 不超过 50（translate+reflect 模式，详见第四章 phpth 对比小节）
- 所有 API 密钥通过环境变量管理，不写入代码/配置的明文中

---

## 十二、评测工具说明

### 翻译评测

```bash
# 运行单个模型测试
python scripts/translation_benchmark.py --video-id _QOMPli80JA --model volc-kimi-k2.5 \
    --sample 0 --prompt-lang zh --batch-size 100 \
    --custom-translate-prompt scripts/translation_benchmark_prompts/custom_translate_fubuki_zh_fewshot_plus.md

# 生成对比表
python scripts/translation_benchmark.py --video-id _QOMPli80JA --compare

# 可用模型
python scripts/translation_benchmark.py --list-models
```

```bash
# phpth 中转站模型测试（免费备用通道）
python scripts/translation_benchmark.py --video-id VIDEO_ID --model phpth-gpt-5.4 \
    --sample 0 --prompt-lang en --batch-size 30 --thread-num 10 --tag "描述"
# 可选模型名: phpth-gpt-5.4, phpth-claude-opus-4-6
# 注意: batch_size 不要超过 50（translate+reflect 模式下会 504 超时）
```

支持的参数：`--no-reflect`(standard模式)、`--temperature`、`--skip-optimize`、`--tag`(区分实验)、`--thread-num`

### ASR参数评测

```bash
python scripts/asr_evaluation/run_param_experiment.py --phase full --gpus 4
python scripts/asr_evaluation/evaluate_params.py
```

### 文件结构

```
scripts/
├── translation_benchmark.py                           # 翻译评测脚本
├── run_gemini_benchmark.sh                            # Gemini 横评批量运行脚本
├── translation_benchmark_prompts/
│   ├── optimize_system_zh.md                          # 中文optimize系统提示词
│   ├── translate_reflect_zh.md                        # 中文translate系统提示词
│   ├── custom_translate_fubuki_zh_fewshot_plus.md     # 12条few-shot（最优）
│   └── ...
├── translation_benchmark_results/                     # 所有翻译实验的原始数据
│   ├── _QOMPli80JA/                                   # 主测试视频（22+组配置）
│   ├── k8CCqKYx2Pk/                                   # ASR差案例
│   ├── czIBPN1eCbU/                                   # 美食实况
│   └── ...（共15个视频目录）
└── asr_evaluation/                                    # ASR参数评测脚本

docs/
├── TRANSLATION_AND_ASR_EVALUATION.md                  # 本文档
└── ASR_EVALUATION_REPORT.md                           # ASR 350视频参数评测详细数据
```

---

*最后更新: 2026-03-10*
