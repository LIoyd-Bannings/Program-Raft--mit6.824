#!/bin/bash

# 设定运行次数
runs=100
# 设定要跑的测试名称（支持正则）
test_name="2B"

echo "开始自动测试，共计 $runs 次..."

for ((i=1; i<=runs; i++))
do
    echo "正在进行第 $i 次测试..."
    # -race 是竞态检查，强烈建议加上
    # > out.txt 2>&1 是把所有输出（包括错误）重定向到 out.txt
    go test -run $test_name -race > last_run_log.txt 2>&1
    
    # 检查上一条命令的退出状态
    if [ $? -ne 0 ]; then
        echo "❌ 第 $i 次测试失败了！"
        # 失败时把日志改名保存下来，方便后续分析
        mv last_run_log.txt "error_log_run_$i.txt"
        exit 1
    fi
done

echo "✅ 全部 $runs 次测试顺利通过！"