#!/bin/bash

# 这是一个简单的运行脚本示例

# 设置 Python 虚拟环境的路径，如果有的话
# 这里假设虚拟环境在项目目录下的 venv 文件夹中
VIRTUAL_ENV="./venv/bin/activate"

# 设置你的 Python 脚本路径
SCRIPT_PATH="./main.py"

# 激活虚拟环境
source $VIRTUAL_ENV

pip3 install -r requirements.txt

# 定义输入参数
DATABASE_FILE="./database.txt"
PATTERN_FILE="./pattern.txt"
K=1

# 获取虚拟环境中的 Python 命令路径
PYTHON_COMMAND=$(which python3)

# 运行 Python 脚本并传递参数
$PYTHON_COMMAND $SCRIPT_PATH -database $DATABASE_FILE -pattern $PATTERN_FILE --k $K
