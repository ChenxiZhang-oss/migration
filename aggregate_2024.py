#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
聚合武汉市迁出省级数据（20240110-20240120）
计算各省的平均占比和权重
"""

import json
import pandas as pd
from pathlib import Path

# 数据目录
data_dir = Path('./data')

# 定义日期范围 (10号到20号)
dates = [f'202401{i}' for i in range(10, 21)]

# 收集所有日期的数据
all_data = []

for date in dates:
    # 文件名: 武汉市_迁出_省百分比_YYYYMMDD.json
    json_file = data_dir / f'武汉市_迁出_省百分比_{date}.json'
    
    if not json_file.exists():
        print(f'警告: 文件不存在 {json_file}')
        continue
    
    # 读取JSON文件
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 提取list数据
    if data.get('errmsg') == 'SUCCESS' and data.get('data', {}).get('list'):
        for item in data['data']['list']:
            all_data.append({
                'date': date,
                'province_name': item.get('province_name', ''),
                'value': item.get('value', 0)
            })
        print(f'✓ 已处理: {date} ({len(data["data"]["list"])} 个省份)')

# 转换为DataFrame
df = pd.DataFrame(all_data)

if df.empty:
    print('没有数据可处理')
    exit(1)

# 按省份计算平均值
province_stats = df.groupby('province_name')['value'].agg(['mean', 'count']).reset_index()
province_stats.columns = ['province_name', 'avg_value', 'days_count']

# 计算总迁出量（所有日期所有省份的总和）
total_outflow = df['value'].sum()

# 计算每省的权重（所有日期该省的总占比 / 总占比）
province_stats['total_value'] = df.groupby('province_name')['value'].sum().values
province_stats['weight'] = (province_stats['total_value'] / total_outflow * 100).round(2)

# 保留必要列并排序
result_df = province_stats[['province_name', 'avg_value', 'weight', 'days_count']].sort_values('weight', ascending=False).reset_index(drop=True)

# 重命名列
result_df.columns = ['省份', '平均占比(%)', '权重(%)', '数据天数']

# 保存结果CSV
output_file = Path('./data/武汉市迁出省级汇总_20240110-20240120.csv')
result_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f'\n✓ 数据已保存到: {output_file}')
print(f'总共统计: {len(result_df)} 个省份')
print(f'统计日期: {len(dates)} 天 (20240110 - 20240120)')
print(f'\n数据预览:')
print(result_df.head(10))
