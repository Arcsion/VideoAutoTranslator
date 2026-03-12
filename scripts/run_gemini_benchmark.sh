#!/bin/bash
# Gemini 多型号翻译质量横评 批量运行脚本
# 步骤1: 用 kimi-k2.5 对所有视频跑 optimize (保存 optimized.srt)
# 步骤2: 用 4 个 Gemini 模型对所有视频跑 translate (使用步骤1的optimized.srt)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/translation_benchmark_results"
BENCHMARK="$SCRIPT_DIR/translation_benchmark.py"

# 15个测试视频
VIDEOS=(
    _QOMPli80JA    # 双人联动对话 155条
    czIBPN1eCbU    # 美食实况 448条
    k8CCqKYx2Pk    # 马里奥卡丁车(ASR差) 234条
    oE8HLVwpimA    # 多人正式公告 103条
    CMvE8F9INDM    # 赛车30分挑战 84条
    enAZ8wvvgl4    # 直播开箱 242条
    IEYQQQFmxak    # 马里奥卡丁车REVENGE 212条
    ajda3lbz6Mk    # 风来のシレン 237条
    DoDiPWf0Rg4    # 恐怖游戏杂谈 401条
    8Q6WtSDdbNk    # 初配信 313条
    q2B_u_wZWAQ    # 恐怖游戏直播 511条
    X53mU_mxCDQ    # 杂谈直播 406条
    cgbcUfgQF6M    # Only Up实况 509条
    jjPBUsN5jl4    # 杂谈配信 725条
    i-EreP4zejg    # 混合(歌词+叙事) 892条
)

# 4个 Gemini 模型
GEMINI_MODELS=(
    gemini-3-flash
    gemini-3.1-flash-lite
    gemini-2.5-flash
    gemini-2.5-flash-lite
)

TAG="gemini_bench"
BATCH_SIZE=100
THREAD_NUM=10

echo "=========================================="
echo "Gemini 多型号翻译质量横评"
echo "视频数: ${#VIDEOS[@]}"
echo "模型数: ${#GEMINI_MODELS[@]}"
echo "总运行数: optimize=${#VIDEOS[@]}, translate=$((${#VIDEOS[@]} * ${#GEMINI_MODELS[@]}))"
echo "=========================================="

# ============================================
# 步骤1: Optimize (kimi-k2.5)
# ============================================
echo ""
echo "===== 步骤1: Optimize with kimi-k2.5 ====="
OPT_MODEL="volc-kimi-k2.5"

for vid in "${VIDEOS[@]}"; do
    OPT_DIR="$RESULTS_DIR/$vid/${OPT_MODEL}_full_${TAG}"
    OPT_SRT="$OPT_DIR/optimized.srt"
    
    if [ -f "$OPT_SRT" ]; then
        echo "[SKIP] $vid - optimized.srt already exists"
        continue
    fi
    
    echo "[RUN] optimize: $vid"
    python3 "$BENCHMARK" \
        --video-id "$vid" \
        --model "$OPT_MODEL" \
        --sample 0 \
        --batch-size "$BATCH_SIZE" \
        --thread-num "$THREAD_NUM" \
        --optimize-only \
        --tag "$TAG" \
    || echo "[WARN] optimize failed for $vid, continuing..."
done

echo ""
echo "===== 步骤1完成 ====="

# ============================================
# 步骤2: Translate (各 Gemini 模型)
# ============================================
echo ""
echo "===== 步骤2: Translate with Gemini models ====="

for model in "${GEMINI_MODELS[@]}"; do
    echo ""
    echo "--- 模型: $model ---"
    
    for vid in "${VIDEOS[@]}"; do
        # 检查是否已有翻译结果
        TRANS_DIR="$RESULTS_DIR/$vid/${model}_full_${TAG}"
        TRANS_SRT="$TRANS_DIR/translated.srt"
        
        if [ -f "$TRANS_SRT" ]; then
            echo "[SKIP] $vid/$model - translated.srt already exists"
            continue
        fi
        
        # 查找 optimized.srt
        OPT_SRT="$RESULTS_DIR/$vid/${OPT_MODEL}_full_${TAG}/optimized.srt"
        if [ ! -f "$OPT_SRT" ]; then
            echo "[WARN] $vid - optimized.srt not found, skipping"
            continue
        fi
        
        echo "[RUN] translate: $vid with $model"
        python3 "$BENCHMARK" \
            --video-id "$vid" \
            --model "$model" \
            --sample 0 \
            --batch-size "$BATCH_SIZE" \
            --thread-num "$THREAD_NUM" \
            --optimized-input "$OPT_SRT" \
            --tag "$TAG" \
        || echo "[WARN] translate failed for $vid/$model, continuing..."
    done
done

echo ""
echo "=========================================="
echo "全部完成！"
echo "=========================================="
