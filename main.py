import requests
import json
import re
import time
import os
from datetime import datetime, timedelta
import pandas as pd
# import pyarrow
from typing import List
from admin_codes import CITY_CODES

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False


# 用来把英文和数字翻译成中文存到文件名里。也许还有别的用途。
translation_table = {
    'lastdate': '最后日期',
    'historycurve': '迁徙规模指数',
    'cityrank': '市百分比',
    'provincerank': '省百分比',
    'country': '国家',
    'province': '省',
    'city': '市',
    'move_in': '迁入',
    'move_out': '迁出',
}

# 从 admin_codes 导入城市代码映射
translation_table.update(CITY_CODES)

# 特定覆盖确保关键城市显示正确
translation_table['420100'] = '武汉市'


class Types:
    """
    存储我们已知和要请求的一些数据。
    """
    # 请不要修改data_type
    data_type = {
        'provincerank'
    }
    # 请不要修改dt
    dt = {'country',
          'province',
          'city'}
    # 如果你有其他省市数据需要获取，请修改这里
    # 地级市及以上城市代码映射（从 admin_codes.py 提取）
    region = {
        '420100': 'city',  # 武汉市
    }
        
    # 请不要修改move_type
    # 只抓取迁出情况
    move_type = {'move_out'}


def generate_date_range(start_date_str:"20190201", end_date_str:"20190204"):
    """
    输入两端的日期，输出包括这两天在内的所有日期。

    :param start_date_str: 起始日期，长度为8、格式为年月日的字符串，如"20201231"。
    :param end_date_str: 结束日期，格式同上。
    :return: 一个list，包含从起始日期开始到结束日期为止的所有日期，格式同上。
    """
    # 将输入的日期字符串转换为datetime对象
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')

    # 初始化日期列表，包含起始日期
    date_list = [start_date.strftime('%Y%m%d')]

    # 生成日期范围
    current_date = start_date
    while current_date < end_date:
        current_date += timedelta(days=1)
        date_list.append(current_date.strftime('%Y%m%d'))

    return date_list


def get_timestamp():
    """
    获得当前时间戳。

    :return: 长度为13的时间戳
    """
    return str(int(time.time() * 1000))  # * 1000获取毫秒级时间戳


def get_lastdate():
    """
    从API获得目前API有的最晚的日期。

    :return: 长度为8、格式为年月日的字符串，如"20201231"。
    """
    url = f'http://huiyan.baidu.com/migration/lastdate.jsonp'
    response = requests.get(url)
    json_data_match = re.search(r'{.*}', response.text)
    if json_data_match:
        json_data_str = json_data_match.group()

        # Decode JSON string, automatically handling Unicode characters
        json_data = json.loads(json_data_str)
        return_value = json_data['data']['lastdate']
        print(f"成功获取到API最晚日期：{return_value}")
        return return_value
    else:
        print('获取API最晚日期时失败')


def get_historycurve(region: str, move_type: str):
    """
    从API获得一个区域（省或市）的迁入或迁出的迁徙规模指数。historycurve API会直接返回所有日期。存储文件到文件名。

    :param region: 行政区域编码，如300001。
    :param move_type: 'move_in' 或 'move_out'
    :return: 成功返回 True，失败返回 False
    """
    # 注意：这个文件要覆写，因为每次抓到的可能不一样。不需要判断是否已存在文件。
    output_file = f'./data/{translation_table[region]}_{translation_table[move_type]}_{translation_table["historycurve"]}.json'

    print(f"正在获取"
          f' {translation_table[region]}'
          f' {translation_table[move_type]} 的'
          f' {translation_table["historycurve"]}')

    url = f"http://huiyan.baidu.com/migration/historycurve.jsonp?dt={Types.region[region]}&id={region}&type={move_type}"
    response = requests.get(url)
    json_data_match = re.search(r'{.*}', response.text)
    if json_data_match:
        json_data_str = json_data_match.group()

        # Decode JSON string, automatically handling Unicode characters
        json_data = json.loads(json_data_str)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        print('成功')
        return True
    else:
        print('失败')
        return False


# http://huiyan.baidu.com/migration/cityrank.jsonp?
# dt=country&id=0&type=move_in&date=20240130&callback=jsonp_1706674779082_8488633
def download_and_convert_jsonp(data_type: str, dt: str, id: str, move_type: str, date: str, callback: str = ''):
    """
    从百度慧眼的API上获得数据并存到本地。

    :param data_type: 就是你要获取的信息类型，有且仅有下列几个值：
        lastdate，返回最后有数据的日期（昨天）；
        historycurve，返回从20190112到lastdate为止的迁徙规模指数；
        cityrank，返回迁入xx来源地（城市级别）；
        provincerank，返回xx来源地（省份级别）。
    :param dt: 级别。可选的值有：country，province，city。
    :param id: 哪里的数据（若dt为国家则不需要id）。参照"行政区划乡镇清单201910"表。级别要和上面的id对应。
    :param move_type: 迁入还是迁出。可选的值有：move_in, move_out。
    :param date: 要的日期，格式：八位数的年月日，如20240131。data_type为lastdate或historycurve时不需要这个参数。
    :param callback: 没什么用，只是为了确认返回值是我这次要的数据罢了。给个时间戳就行。
    :param output_file: 该文件要存到的文件名。
    :return: boolean 成功返回True，失败返回False
    """
    # TODO: 如果要爬大量数据，搞多线程
    output_file = f'./data/{translation_table.get(id, id)}_' \
                  f'{translation_table.get(move_type, move_type)}_' \
                  f'{translation_table.get(data_type, data_type)}_' \
                  f'{date}.json'

    # Construct the full URL with query parameters (no callback to match endpoint)
    url = f'http://huiyan.baidu.com/migration/{data_type}.jsonp?dt={dt}&id={id}&type={move_type}&date={date}'

    response = requests.get(url)

    # Extract JSON data part using regular expression
    json_data_match = re.search(r'{.*}', response.text)
    if json_data_match:
        json_data_str = json_data_match.group()

        # Decode JSON string, automatically handling Unicode characters
        json_data = json.loads(json_data_str)

        # 导出 CSV 文件
        if json_data.get('errmsg') == 'SUCCESS':
            data_list = json_data.get('data', {}).get('list', [])
            # data_list 是一个列表，元素包含 name, value 字段（省/市名称与占比）
            df = pd.DataFrame(data_list)
            csv_filename = (
                f'./data/{translation_table.get(id, id)}_'
                f'{translation_table.get(move_type, move_type)}_'
                f'{translation_table.get(data_type, data_type)}_'
                f'{date}.csv'
            )
            # 保存为 UTF-8，包含表头
            df.to_csv(csv_filename, index=False, encoding='utf-8')

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)

        print('成功')
        return json_data
    else:
        print('失败')
        return None


def get_data(region: str, data_type: str, move_type: str, date: str):
    timestamp = get_timestamp()
    print(f"正在获取"
          f' {translation_table[region]}'
          f' {translation_table[move_type]} 的'
          f' {translation_table[data_type]}，日期为{date}')
    return download_and_convert_jsonp(
        data_type=data_type,
        dt=Types.region[region],
        id=region,
        move_type=move_type,
        date=date,
        callback=timestamp
    )


def get_by_date(from_date, to_date, lastdate):
    # 将日期字符串转换为datetime对象
    from_date_dt = datetime.strptime(from_date, '%Y%m%d')
    to_date_dt = datetime.strptime(to_date, '%Y%m%d')
    lastdate_dt = datetime.strptime(lastdate, '%Y%m%d')

    print("正在检查日期输入合法性")

    # 检查from_date是否小于'20190112'
    earlydate = datetime(2019, 1, 12)
    if from_date_dt < earlydate:
        print("你的起始日期比API目前有的最早日期还早。已经将起始日期设置成API的最早日期，并尝试继续运行。")
        from_date_dt = earlydate

    # 检查to_date是否大于lastdate
    if to_date_dt > lastdate_dt:
        print("你的结束日期比API目前有的最晚日期还晚。已经将结束日期设置成API的最晚日期，并尝试继续运行。")
        to_date_dt = lastdate_dt

    # 检查输入合法性
    if from_date_dt > to_date_dt:
        print("你的起始日期需要比结束日期早。")
        return

    print("成功")

    # 转换为字符串，以便生成日期范围
    from_date = from_date_dt.strftime('%Y%m%d')
    to_date = to_date_dt.strftime('%Y%m%d')

    # 如需迁徙规模指数，可取消注释
    # for region in Types.region.keys():
    #     for move_type in Types.move_type:
    #         get_historycurve(region, move_type)

    for date in generate_date_range(from_date, to_date):  # 日期
        for region in Types.region.keys():
            for move_type in Types.move_type:
                for data_type in Types.data_type:
                    get_data(region, data_type, move_type, date)
    print("完成")




if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Baidu Huiyan migration cityrank crawler')
    parser.add_argument('--from-date', dest='from_date', default='20190201', help='开始日期 YYYYMMDD，默认 20190201')
    parser.add_argument('--to-date', dest='to_date', default='20190204', help='结束日期 YYYYMMDD，默认 20190204')
    args = parser.parse_args()

    # 确保输出目录存在
    os.makedirs('./data', exist_ok=True)

    # 获取API最晚日期
    lastdate = get_lastdate()
    if not lastdate:
        raise SystemExit('无法获取最晚日期，程序退出。')

    # 执行数据抓取（可通过命令行参数覆盖默认日期）
    get_by_date(args.from_date, args.to_date, lastdate)



