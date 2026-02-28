#!/bin/bash

# 启动脚本
set -e

# 等待数据库启动
echo "等待数据库启动..."
while ! nc -z db 5432; do
  sleep 1
done
echo "数据库已启动"

# 等待Redis启动
echo "等待Redis启动..."
while ! nc -z redis 6379; do
  sleep 1
done
echo "Redis已启动"

# 运行数据库迁移（如果需要）
echo "运行数据库迁移..."
python -c "
from agents.utils.db import get_database_manager
db = get_database_manager()
print('数据库初始化完成')
"

# 启动应用
echo "启动应用..."
exec uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4
