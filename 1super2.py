import threading

from machine_lib import *

import random

import datetime

import requests

import json

import time

import os

import sys

from os.path import expanduser

from requests.auth import HTTPBasicAuth
import logging
from pathlib import Path
import getpass  # 用于安全输入密码
import builtins


def print(*args, **kwargs):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    builtins.print(f"[{timestamp}]", *args, **kwargs)


# 字段选择条件字典 - 每个字段包含其可能的条件

field_conditions = {

    # 所有权条件 - 必须放在最前面
    'ownership': [
        '(own)',
        # 'not(own)',
    ],

    # 基础Alpha属性字段
    'turnover': [
        'turnover <= 0.1',
        'turnover <= 0.3',
        'turnover >= 0.1',
        'turnover >= 0.2',
        'turnover >= 0.05',
        'turnover <= 0.5'
    ],

    'long_count': [
        'long_count >= 100',
        'long_count >= 1000',
        'long_count >= 500',
        'long_count >= 200',
        'long_count <= 2000',
        'long_count >= 50'
    ],

    'short_count': [
        'short_count >= 100',
        'short_count >= 1000',
        'short_count >= 500',
        'short_count >= 200',
        'short_count <= 2000',
        'short_count >= 50'
    ],

    'truncation': [
        'truncation <= 0.06',
        'truncation <= 0.1',
        'truncation <= 0.05',
        'truncation >= 0.01',
        'truncation <= 0.2',
        'truncation >= 0.02'
    ],

    'decay': [
        'decay <= 2',
        'decay <= 5',
        'decay >= 1',
        'decay <= 10',
        'decay >= 0.5',
        'decay <= 1'
    ],

    'operator_count': [
        'operator_count <= 6',
        'operator_count <= 4',
        'operator_count <= 3',
        'operator_count <= 5',
        'operator_count >= 2',
        'operator_count <= 8'
    ],

    'dataset_count': [
        'dataset_count == 1',
        'dataset_count <= 2',
        'dataset_count >= 1',
        'dataset_count <= 3',
        'dataset_count == 2',
        'dataset_count >= 2'
    ],

    'self_correlation': [
        'self_correlation <= 0.6',
        'self_correlation <= 0.5',
        'self_correlation <= 0.3',
        'self_correlation <= 0.4',
        'self_correlation <= 0.7',
        'self_correlation >= 0.1'
    ],

    'prod_correlation': [
        'prod_correlation < 0.5',
        'prod_correlation <= 0.3',
        'prod_correlation <= 0.4',
        'prod_correlation <= 0.6',
        'prod_correlation >= 0.1',
        'prod_correlation <= 0.7'
    ],

    'datacategory_count': [
        'datacategory_count < 5',
        'datacategory_count <= 3',
        'datacategory_count <= 2',
        'datacategory_count >= 1',
        'datacategory_count == 1',
        'datacategory_count <= 4'
    ],

    'datafield_count': [
        'datafield_count < 2',
        'datafield_count <= 3',
        'datafield_count <= 4',
        'datafield_count >= 1',
        'datafield_count == 1',
        'datafield_count <= 5'
    ],

    # 分类和标签字段
    'category': [
        'category == "NONE"',
        'category == "PRICE_REVERSION"',
        'category == "PRICE_MOMENTUM"',
        'category == "VOLUME"',
        'category == "FUNDAMENTAL"',
        'category == "ANALYST"'
    ],

    'color': [
        'color == "GREEN"',
        'color == "RED"',
        'color == "YELLOW"',
        'color == "BLUE"',
        'color == "PURPLE"',
        'color == "NONE"'
    ],

    'favorite': [
        'not(favorite)',
        'favorite == 1',
        'favorite == 0'
    ],

    # 数据集和字段相关
    'dataset': [
        'in(dataset, "fundamental6")',
        'in(dataset, "analyst4")',
        'in(dataset, "model26")',
        'in(dataset, "fundamental1")',
        'in(dataset, "analyst1")',
        'in(dataset, "model1")'
    ],

    'datafields': [
        'in(datafields, "returns")',
        'in(datafields, "assets")',
        'in(datafields, "debt")',
        'in(datafields, "volume")',
        'in(datafields, "price")',
        'in(datafields, "market_cap")'
    ],

    'datacategories': [
        'not(in(datacategories, "fundamental"))',
        'in(datacategories, "analyst")',
        'in(datacategories, "earnings")',
        'in(datacategories, "imbalance")',
        'in(datacategories, "institutions")',
        'in(datacategories, "macro")'
    ],

    # 分类和竞赛相关
    'classifications': [
        'in(classifications, "POWER_POOL")',
        'in(classifications, "ATOM")',
        'not(in(classifications, "POWER_POOL"))',
        'not(in(classifications, "ATOM"))'
    ],

    'competitions': [
        'in(competitions, "HCAC2025")',
        'in(competitions, "ACE2023")',
        'not(in(competitions, "HCAC2025"))',
        'not(in(competitions, "ACE2023"))'
    ],

    # 宇宙和中性化设置
    'universe': [
        'universe == "TOP1000"',
        'universe == "TOP3000"',
        'universe == "TOP2000"',
        'universe == "TOP500"',
        'universe == "TOP200"',
        'universe == "TOP5000"'
    ],

    'universe_size': [
        'universe_size(universe) >= 2000',
        'universe_size(universe) >= 1000',
        'universe_size(universe) >= 500',
        'universe_size(universe) <= 3000',
        'universe_size(universe) <= 5000',
        'universe_size(universe) >= 3000'
    ],

    'neutralization': [
        'neutralization == "MARKET"',
        'neutralization == "SECTOR"',
        'neutralization == "INDUSTRY"',
        'neutralization == "SUBINDUSTRY"',
        'neutralization == "NONE"',
        'neutralization == "COUNTRY"'
    ],

    # 日期相关
    'os_start_date': [
        'os_start_date > "2020-01-01"',
        'os_start_date > "2021-01-01"',
        'os_start_date > "2022-01-01"',
        'os_start_date < "2024-01-01"',
        'os_start_date < "2023-01-01"',
        'os_start_date > "2019-01-01"'
    ],

    # 名称和标签
    'name': [
        'name == "good_alpha"',
        'name != ""',
        'name != "untitled"',
        'name != "alpha"',
        'name != "test"',
        'name != "new"'
    ],

    'tags': [
        'in(tags, "my_example_tag")',
        'in(tags, "good")',
        'in(tags, "test")',
        'in(tags, "production")',
        'in(tags, "experimental")',
        'in(tags, "stable")'
    ]

}


def generate_selection(num_conditions=3):
    """生成选择表达式，从不同字段中随机选择指定数量的条件并用 && 连接"""

    # 获取所有字段（排除ownership，因为它是必须的）

    all_fields = [field for field in field_conditions.keys() if field != 'ownership']

    # 首先添加ownership条件（必须包含）

    selected_conditions = []

    ownership_condition = random.choice(field_conditions['ownership'])

    selected_conditions.append(ownership_condition)

    # 然后选择其他字段条件

    remaining_conditions = num_conditions - 1  # 减去ownership条件

    if remaining_conditions > 0:

        if remaining_conditions <= len(all_fields):

            chosen_fields = random.sample(all_fields, remaining_conditions)

        else:

            # 如果需要的条件数大于字段数，先选择所有字段，然后重复选择

            chosen_fields = all_fields.copy()

            remaining = remaining_conditions - len(all_fields)

            chosen_fields.extend(random.choices(all_fields, k=remaining))

        for field in chosen_fields:
            condition = random.choice(field_conditions[field])

            selected_conditions.append(condition)

    # 用 && 连接

    return ' && '.join(selected_conditions)


# 简单的selection表达式列表（已清空，改用动态生成）

simple_selections = []


# def login():

#     """登录WorldQuant BRAIN平台（支持cookie验证）"""

#     cookie_path = os.path.join(os.path.dirname(__file__), "cookie.json")

#     session = requests.Session()

#     if not os.path.exists(cookie_path):

#         print("cookie文件不存在，直接使用账号密码登陆")

#         env_dist = os.environ

#         username = env_dist.get("WQ_USERNAME")

#         password = env_dist.get("WQ_PASSWORD")

#         if username is None or password is None:

#             with open(expanduser("denglu.txt")) as f:

#                 credentials = json.load(f)

#             username, password = credentials

#         session = relogin(username, password)

#     else:

#         with open(cookie_path, "r") as f:

#             cookies = requests.utils.cookiejar_from_dict(json.load(f))

#             session.cookies = cookies

#         response = session.get("https://api.worldquantbrain.com/operators")

#         if response.status_code in (401, 403):

#             print("cookie文件失效，使用账号密码重新登陆")

#             env_dist = os.environ

#             username = env_dist.get("WQ_USERNAME")

#             password = env_dist.get("WQ_PASSWORD")

#             if username is None or password is None:

#                 with open(expanduser("denglu.txt")) as f:

#                     credentials = json.load(f)

#                 username, password = credentials

#             session = relogin(username, password)

#         else:

#             print("cookie文件有效")

#     return session

def relogin(username: str, password: str):
    """重新登录并保存cookie"""

    session = requests.Session()

    session.auth = (username, password)

    while True:

        try:

            response = session.post("https://api.worldquantbrain.com/authentication")

            response.raise_for_status()

            print("登录成功, cookie文件更新")

            session.cookies = response.cookies

            cookie_path = os.path.join(os.path.dirname(__file__), "cookie.json")

            with open(cookie_path, "w") as f:

                json.dump(requests.utils.dict_from_cookiejar(response.cookies), f)

            return session

        except requests.exceptions.RequestException as e:

            print("登录失败，等待10s后再次尝试登录")

            time.sleep(10)


def get_simple_selection():
    """动态生成选择表达式"""

    # 随机选择 1-2 个条件

    num_conditions = random.randint(1, 2)

    return generate_selection(num_conditions)


def get_combo_code_list():
    """动态生成随机的组合代码列表，每次调用都会生成不同的组合"""
    # 随机时间窗口池
    time_windows_short = [20, 40, 60, 80, 100]
    time_windows_medium = [120, 180, 250, 300, 400]
    time_windows_long = [500, 600, 750, 1000, 1200]
    time_windows_rank = [250, 500, 750, 1000]
    
    # 随机阈值池
    thresholds_high = [0.7, 0.75, 0.8, 0.85, 0.9]
    thresholds_low = [0.1, 0.15, 0.2, 0.25, 0.3]
    thresholds_mid_high = [0.6, 0.65, 0.7]
    thresholds_mid_low = [0.3, 0.35, 0.4]
    
    # 随机系数池
    risk_coeffs = [0.3, 0.4, 0.5, 0.6, 0.7]
    
    ret = []
    
    # 随机决定是否包含基础组合
    if random.random() < 0.3:  # 30%概率包含基础组合
        ret.append('1')
    
    # 动态生成自相关性组合（使用随机窗口）
    if random.random() < 0.7:  # 70%概率包含
        window = random.choice(time_windows_long)
        ret.append(f'stats = generate_stats(alpha); a = self_corr(stats.returns, {window}); b = if_else(a == 1.0, nan, a); c = reduce_max(b); 1 - c')
    
    if random.random() < 0.5:  # 50%概率包含
        window = random.choice(time_windows_long)
        ret.append(f'stats = generate_stats(alpha); innerCorr = self_corr(stats.returns, {window}); ic = if_else(innerCorr == 1.0, nan, innerCorr); maxCorr = reduce_max(ic); 1 - maxCorr')
    
    # 动态生成时间序列排名组合（使用随机窗口和阈值）
    if random.random() < 0.6:
        window_sum = random.choice(time_windows_medium)
        window_rank = random.choice(time_windows_rank)
        thresh_high = random.choice(thresholds_high)
        thresh_low = random.choice(thresholds_low)
        ret.append(f'stats = generate_stats(alpha); a = ts_sum(stats.returns, {window_sum}); b = ts_rank(a, {window_rank}); if_else(b>{thresh_high}, 1, if_else(b<{thresh_low}, -1, 0))')
    
    # 动态生成波动率组合
    if random.random() < 0.6:
        window = random.choice(time_windows_medium)
        window_rank = random.choice(time_windows_rank)
        ret.append(f'stats = generate_stats(alpha); a = ts_std_dev(stats.returns, {window}); b = a / ts_delay(a, {window}); ts_rank(-b, {window_rank})')
    
    # 动态生成交易价值组合
    if random.random() < 0.5:
        window = random.choice(time_windows_medium)
        window_rank = random.choice(time_windows_rank)
        ret.append(f'stats = generate_stats(alpha); a = ts_mean(stats.trade_value, {window}); b = a / ts_delay(a, {window}); ts_rank(-b, {window_rank})')
    
    # Combo算法组合（随机选择参数）
    if random.random() < 0.6:
        nlength = random.choice([250, 255, 500, 750, 1000])
        mode = random.choice(["algo1", "algo2"]) if random.random() < 0.3 else None
        if mode:
            ret.append(f'combo_a(alpha, nlength = {nlength}, mode = "{mode}")')
        else:
            ret.append(f'combo_a(alpha, nlength = {nlength})')
    
    if random.random() < 0.4:
        ret.append('combo_a(alpha)')
    
    if random.random() < 0.3:
        ret.append('combo_a(normalize(alpha))')
    
    # 动态生成夏普比率组合（PNL）
    if random.random() < 0.5:
        window = random.choice(time_windows_short + time_windows_medium)
        ret.append(f'stats = generate_stats(alpha); a = stats.pnl; ts_mean(a, {window}) / ts_std_dev(a, {window})')
    
    # 动态生成夏普比率组合（Returns）
    if random.random() < 0.5:
        window = random.choice(time_windows_short + time_windows_medium)
        ret.append(f'stats = generate_stats(alpha); a = stats.returns; ts_mean(a, {window}) / ts_std_dev(a, {window})')
    
    # 动态生成动量组合
    if random.random() < 0.5:
        window = random.choice(time_windows_medium)
        window_rank = random.choice(time_windows_rank)
        ret.append(f'stats = generate_stats(alpha); a = ts_momentum(stats.returns, {window}); ts_rank(a, {window_rank})')
    
    # 动态生成均值回归组合
    if random.random() < 0.5:
        window = random.choice(time_windows_medium)
        window_rank = random.choice(time_windows_rank)
        ret.append(f'stats = generate_stats(alpha); a = ts_mean_reversion(stats.returns, {window}); ts_rank(-a, {window_rank})')
    
    # 动态生成波动率调整组合
    if random.random() < 0.5:
        window = random.choice(time_windows_medium)
        window_rank = random.choice(time_windows_rank)
        ret.append(f'stats = generate_stats(alpha); a = stats.returns; b = ts_std_dev(a, {window}); c = a / b; ts_rank(c, {window_rank})')
    
    # 动态生成相关性组合
    if random.random() < 0.4:
        window = random.choice(time_windows_medium)
        window_rank = random.choice(time_windows_rank)
        ret.append(f'stats = generate_stats(alpha); a = self_corr(stats.returns, {window}); b = if_else(a == 1, nan, a); ts_rank(-reduce_min(b), {window_rank})')
    
    # 动态生成复合指标组合
    if random.random() < 0.4:
        window = random.choice(time_windows_medium)
        window_rank = random.choice(time_windows_rank)
        thresh_high = random.choice(thresholds_high)
        thresh_low = random.choice(thresholds_low)
        ret.append(f'stats = generate_stats(alpha); a = stats.returns; b = ts_mean(a, {window}); c = ts_std_dev(a, {window}); d = b / c; e = ts_rank(d, {window_rank}); if_else(e > {thresh_high}, 1, if_else(e < {thresh_low}, -1, 0))')
    
    # 动态生成风险调整组合
    if random.random() < 0.4:
        window = random.choice(time_windows_medium)
        window_rank = random.choice(time_windows_rank)
        risk_coeff = random.choice(risk_coeffs)
        ret.append(f'stats = generate_stats(alpha); a = stats.returns; b = ts_mean(a, {window}); c = ts_std_dev(a, {window}); d = b - {risk_coeff} * c; ts_rank(d, {window_rank})')
    
    # 动态生成趋势跟踪组合
    if random.random() < 0.4:
        window1 = random.choice(time_windows_short)
        window2 = random.choice(time_windows_medium)
        window_rank = random.choice(time_windows_rank)
        ret.append(f'stats = generate_stats(alpha); a = stats.returns; b = ts_sum(a, {window1}); c = ts_sum(a, {window2}); d = b - c; ts_rank(d, {window_rank})')
    
    # 动态生成波动率预测组合
    if random.random() < 0.3:
        window = random.choice(time_windows_medium)
        window_rank = random.choice(time_windows_rank)
        thresh_high = random.choice(thresholds_high)
        thresh_low = random.choice(thresholds_low)
        ret.append(f'stats = generate_stats(alpha); a = ts_std_dev(stats.returns, {window}); b = ts_delay(a, 1); c = a / b; d = ts_rank(c, {window_rank}); if_else(d > {thresh_high}, 1, if_else(d < {thresh_low}, -1, 0))')
    
    # 动态生成多时间框架组合
    if random.random() < 0.4:
        window1 = random.choice(time_windows_short + time_windows_medium)
        window2 = random.choice(time_windows_medium + time_windows_long)
        window_rank = random.choice(time_windows_rank)
        ret.append(f'stats = generate_stats(alpha); a = ts_rank(stats.returns, {window1}); b = ts_rank(stats.returns, {window2}); c = a + b; ts_rank(c, {window_rank})')
    
    # 动态生成非线性组合
    if random.random() < 0.3:
        window1 = random.choice(time_windows_short + time_windows_medium)
        window2 = random.choice(time_windows_medium + time_windows_long)
        window_rank = random.choice(time_windows_rank)
        ret.append(f'stats = generate_stats(alpha); a = stats.returns; b = ts_rank(a, {window1}); c = ts_rank(a, {window2}); d = b * c; ts_rank(d, {window_rank})')
    
    # 动态生成条件组合
    if random.random() < 0.3:
        window_short = random.choice(time_windows_short + time_windows_medium)
        window_long = random.choice(time_windows_long)
        window_rank = random.choice(time_windows_rank)
        ret.append(f'stats = generate_stats(alpha); a = stats.returns; b = ts_std_dev(a, {window_short}); c = if_else(b > ts_mean(b, {window_long}), ts_rank(a, {window_rank}), -ts_rank(a, {window_rank})); c')
    
    # 随机打乱顺序
    random.shuffle(ret)
    
    # 随机选择部分组合（保留60%-100%的组合）
    keep_ratio = random.uniform(0.6, 1.0)
    keep_count = max(1, int(len(ret) * keep_ratio))
    ret = random.sample(ret, keep_count) if len(ret) > keep_count else ret
    
    return ret


class cfg:
    # 从当前目录下的 brain.txt 文件读取账号密码
    brain_file = os.path.join(os.path.dirname(__file__), 'brain.txt')
    
    # 检查文件是否存在
    if not os.path.exists(brain_file):
        raise FileNotFoundError(
            f"配置文件 {brain_file} 不存在！\n"
            f"请在该路径创建 brain.txt 文件，内容格式为 JSON 数组：\n"
            f'["your_username", "your_password"]\n'
            f"用户名和密码用双引号包围，不要有额外空格或换行。\n"
            f"例如：[\"john.doe@example.com\", \"your_password\"]"
        )
    
    # 读取账号密码
    try:
        with open(brain_file, 'r', encoding='utf-8') as f:
            credentials = json.load(f)
        
        if not isinstance(credentials, list) or len(credentials) != 2:
            raise ValueError(
                f"brain.txt 文件格式错误！\n"
                f"应该是包含两个元素的 JSON 数组：[\"username\", \"password\"]"
            )
        
        username, password = credentials
    except json.JSONDecodeError as e:
        raise ValueError(
            f"brain.txt 文件 JSON 格式错误：{str(e)}\n"
            f"请确保文件内容是有效的 JSON 格式：[\"username\", \"password\"]"
        )
    except Exception as e:
        raise RuntimeError(f"读取 brain.txt 文件时出错：{str(e)}")
    
    data_path = Path('.')


def sign_in(username, password, max_retries=5):
    """登录WorldQuant BRAIN平台，带重试机制"""
    last_error_type = None
    last_status_code = None

    for attempt in range(max_retries):
        try:
            s = requests.Session()
            s.auth = (username, password)
            response = s.post('https://api.worldquantbrain.com/authentication', timeout=30)

            last_status_code = response.status_code

            if response.status_code in [200, 201]:
                print(f"✅ 登录成功 (状态码: {response.status_code}, 尝试 {attempt + 1}/{max_retries})")
                logging.info(f"Successfully signed in with status code {response.status_code}")
                return s

            elif response.status_code in [401, 403]:
                last_error_type = "认证错误"
                print(f"❌ 登录失败：认证信息错误 (状态码: {response.status_code})")
                print(f"   📧 账号: {username}")
                print(f"   🔑 请检查账号密码是否正确")
                print(f"   💡 提示: 如果密码包含特殊字符，请确认是否正确转义")
                # 认证错误不需要重试，直接返回
                return None

            elif response.status_code == 429:
                last_error_type = "限流"
                # 特殊处理429限流错误
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    wait_time = int(retry_after)
                else:
                    # 如果没有Retry-After，使用更长的等待时间
                    wait_time = 60 * (attempt + 1)  # 60秒, 120秒, 180秒...

                print(f"🚦 登录受限：请求过于频繁 (429) (尝试 {attempt + 1}/{max_retries})")

                if attempt < max_retries - 1:
                    print(f"   ⏰ 等待 {wait_time} 秒后重试...")
                    print(f"   💡 建议：减少登录频率，避免频繁请求")
                    time.sleep(wait_time)
                else:
                    print(f"   ❌ 已达到最大重试次数")
                    print(f"   ⏰ 请稍后再试（建议等待5-10分钟）")
                    print(f"   💡 原因: API限流保护，频繁登录被暂时限制")

            else:
                last_error_type = "HTTP错误"
                print(f"⚠️ 登录失败：HTTP {response.status_code} (尝试 {attempt + 1}/{max_retries})")
                print(f"   响应内容: {response.text[:200]}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    print(f"   ⏰ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)

        except requests.exceptions.Timeout:
            last_error_type = "超时"
            print(f"⏰ 登录超时 (30秒) (尝试 {attempt + 1}/{max_retries})")
            print(f"   💡 可能原因: 网络连接慢或服务器响应慢")
            if attempt < max_retries - 1:
                wait_time = 10
                print(f"   ⏰ 等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)

        except requests.exceptions.ConnectionError as e:
            last_error_type = "连接错误"
            print(f"🌐 连接失败: {e} (尝试 {attempt + 1}/{max_retries})")
            print(f"   💡 可能原因: 网络断开或无法访问API服务器")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"   ⏰ 等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)

        except requests.exceptions.RequestException as e:
            last_error_type = "网络异常"
            print(f"❌ 登录异常: {e} (尝试 {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"   ⏰ 等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)

    # 最终失败总结
    logging.error(
        f"Login failed after {max_retries} attempts. Last error: {last_error_type}, Status: {last_status_code}")
    print(f"\n{'=' * 60}")
    print(f"❌ 登录失败：已达到最大重试次数 ({max_retries})")
    print(f"{'=' * 60}")
    print(f"📊 失败原因分析:")
    print(f"   • 错误类型: {last_error_type or '未知'}")
    print(f"   • HTTP状态码: {last_status_code or 'N/A'}")
    print(f"   • 账号: {username}")

    if last_error_type == "认证错误":
        print(f"\n💡 解决方案:")
        print(f"   1. 检查账号密码是否正确")
        print(f"   2. 确认密码中特殊字符是否正确输入")
        print(f"   3. 尝试在浏览器中登录平台验证账号")
    elif last_error_type == "限流":
        print(f"\n💡 解决方案:")
        print(f"   1. 等待5-10分钟后重试")
        print(f"   2. 减少登录频率，避免频繁请求")
        print(f"   3. 检查是否有其他程序也在使用相同账号")
    elif last_error_type in ["超时", "连接错误", "网络异常"]:
        print(f"\n💡 解决方案:")
        print(f"   1. 检查网络连接是否正常")
        print(f"   2. 检查防火墙/代理设置")
        print(f"   3. 尝试使用VPN或更换网络")
    else:
        print(f"\n💡 建议:")
        print(f"   1. 等待几分钟后重新运行脚本")
        print(f"   2. 检查WorldQuant BRAIN平台是否正常")
        print(f"   3. 查看错误日志获取更多信息")

    print(f"{'=' * 60}\n")
    return None


def multi_simulate2_sa(alpha_pools, neut, region, universe, start, selection_limits, selection_handling_options):
    """

    改进的并发模拟函数 - 学习machine_lib.py的进度监控方式

    保持并发数恒定，通过进度监控动态提交新任务

    """

    global s

    print(f"🔐 正在登录 WorldQuant BRAIN 平台...")
    s = sign_in(cfg.username, cfg.password)

    if s is None:
        print("❌ 登录失败，无法继续执行模拟")
        raise Exception("登录失败，请检查账号密码")

    brain_api_url = 'https://api.worldquantbrain.com'

    all_sa_pairs = alpha_pools[0]

    total_tasks = len(all_sa_pairs)

    print(f'📊 总任务数: {total_tasks}, 开始位置: {start}, 并发限制: 3')

    task_queue = all_sa_pairs[start:]

    active_tasks = {}  # {task_index: progress_url}
    task_check_counts = {}  # {task_index: check_count} - 记录每个任务的检查次数
    task_start_times = {}  # {task_index: start_time} - 记录每个任务的开始时间
    max_task_duration = 3600  # 最大任务时长（秒），30分钟 = 1800秒，这里设为1小时
    max_check_count = 240  # 最大检查次数（15秒一次，240次 = 1小时）

    completed_count = 0

    failed_count = 0
    rate_limit_log = {}
    completed_alpha_notice = set()

    def log_with_throttle(key, message, interval=30, once=False):
        entry = rate_limit_log.get(key, {"last_time": 0, "last_message": None, "count": 0})
        if once and entry.get("count", 0) > 0:
            return False
        now = time.time()
        should_log = once or (now - entry.get("last_time", 0) > interval) or (entry.get("last_message") != message)
        if should_log:
            print(message)
            entry["last_time"] = now
            entry["last_message"] = message
            entry["count"] = entry.get("count", 0) + 1
            rate_limit_log[key] = entry
            return True
        rate_limit_log[key] = entry
        return False

    def submit_simulation(task_index, task_data):
        global s  # 必须在函数开头声明global

        max_retries = 5

        base_delay = 30

        for attempt in range(max_retries):

            try:

                sim_data_list = generate_sim_data_sa([task_data], region, universe, neut, selection_limits,
                                                     selection_handling_options)

                sim_data = sim_data_list[0]

                simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=sim_data)

                # 检查认证错误
                if simulation_response.status_code in [401, 403]:
                    print(f"🔐 任务{task_index} 认证失败，重新登录...")
                    s = sign_in(cfg.username, cfg.password)
                    if s is None:
                        print(f"❌ 任务{task_index} 重新登录失败")
                        return None
                    # 重新提交
                    simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=sim_data)

                if simulation_response.status_code == 429:

                    if attempt < max_retries - 1:

                        retry_after = simulation_response.headers.get('Retry-After')

                        if retry_after:

                            delay = int(retry_after)

                        else:

                            delay = base_delay * (2 ** attempt)

                        time.sleep(delay)

                        continue

                    else:

                        print(f"❌ 任务{task_index} 429限流，已达到最大重试次数")

                        return None

                if simulation_response.status_code == 400:

                    print(f"❌ 任务{task_index} 400错误: {simulation_response.text}")

                    return None

                elif simulation_response.status_code != 201:

                    print(f"❌ 任务{task_index} 状态码错误: {simulation_response.status_code}")

                    return None

                progress_url = simulation_response.headers.get('Location')

                if progress_url:
                    full_progress_url = progress_url if progress_url.startswith(
                        'http') else f"{brain_api_url}{progress_url}"
                    ui_progress_url = full_progress_url.replace('https://api.worldquantbrain.com',
                                                                'https://platform.worldquantbrain.com')

                    print(f"✅ 任务{task_index} 已提交: {full_progress_url}")
                    print(f"   🔗 浏览器链接: {ui_progress_url}")

                    return progress_url

                else:

                    print(f"❌ 任务{task_index} 无进度URL")

                    return None

            except Exception as e:

                if attempt < max_retries - 1:

                    delay = base_delay * (2 ** attempt)

                    print(f"❌ 任务{task_index} 提交异常: {e}，{delay}秒后重试 ({attempt + 1}/{max_retries})")

                    time.sleep(delay)

                    continue

                else:

                    print(f"❌ 任务{task_index} 提交失败，已达到最大重试次数: {e}")

                    return None

        return None

    def check_simulation_status(task_index, progress_url):
        global s  # 必须在函数开头声明global

        max_retries = 3

        base_delay = 5
        base_timeout = 30

        for attempt in range(max_retries):

            try:
                # 处理Retry-After头
                while True:
                    response = s.get(progress_url, timeout=base_timeout)

                    # 检查Retry-After头
                    if "retry-after" in response.headers or "Retry-After" in response.headers:
                        retry_after = float(
                            response.headers.get("retry-after") or response.headers.get("Retry-After", 0))
                        if retry_after > 0:
                            message = f"   ⏰ 任务{task_index} API限流，等待 {retry_after:.1f} 秒..."
                            log_with_throttle((task_index, "status_rate_limit"), message, once=True)
                            time.sleep(retry_after)
                            continue  # 继续重试同一个请求
                    break  # 没有Retry-After，退出循环

                # 检查认证错误
                if response.status_code in [401, 403]:
                    print(f"🔐 任务{task_index} 状态检查认证失败，重新登录...")
                    s = sign_in(cfg.username, cfg.password)
                    if s is None:
                        print(f"❌ 任务{task_index} 重新登录失败")
                        return "ERROR"
                    # 重新检查（也需要处理Retry-After）
                    while True:
                        response = s.get(progress_url, timeout=base_timeout)
                        if "retry-after" in response.headers or "Retry-After" in response.headers:
                            retry_after = float(
                                response.headers.get("retry-after") or response.headers.get("Retry-After", 0))
                            if retry_after > 0:
                                message = f"   ⏰ 任务{task_index} API限流，等待 {retry_after:.1f} 秒..."
                                log_with_throttle((task_index, "status_rate_limit"), message, once=True)
                                time.sleep(retry_after)
                                continue
                        break

                if response.status_code == 504:

                    if attempt < max_retries - 1:

                        delay = base_delay * (2 ** attempt)

                        print(f"⏳ 任务{task_index} 504超时，{delay}秒后重试 ({attempt + 1}/{max_retries})")

                        time.sleep(delay)

                        continue

                    else:

                        print(f"❌ 任务{task_index} 504超时，已达到最大重试次数")

                        return "ERROR"

                if response.status_code == 200:

                    data = response.json()

                    status = data.get("status", "UNKNOWN")

                    # 打印详细状态信息（仅对非RUNNING状态）
                    if status != "RUNNING":
                        print(f"   📋 任务{task_index} 状态: {status}")

                    # WARNING状态可能是完成但有警告，检查是否有结果URL或完成标记
                    if status == "WARNING":
                        # 检查是否有location或result字段，表明任务已完成
                        if "location" in data or "result" in data:
                            print(f"   ⚠️ 任务{task_index} WARNING状态但有结果，视为完成")
                            return "COMPLETE"
                        # 检查progress中的completion状态
                        if "progress" in data:
                            progress_info = data.get("progress", {})
                            if isinstance(progress_info, dict):
                                completion = progress_info.get("completion", 0)
                                if completion >= 100:
                                    print(f"   ⚠️ 任务{task_index} WARNING状态但完成度100%，视为完成")
                                    return "COMPLETE"
                        # 如果WARNING状态持续一定时间，也视为完成（可能是警告但已生成结果）
                        print(f"   ⚠️ 任务{task_index} WARNING状态，检查是否有可用的结果...")

                    if status in ["ERROR", "FAILED"]:

                        error_msg = data.get("error", "Unknown error")

                        log_with_throttle((task_index, "status_error_detail"),
                                          f"❌ 任务{task_index} 失败详情: {error_msg}", interval=60)

                        if "progress" in data:

                            progress_info = data["progress"]

                            if "errors" in progress_info:

                                for error in progress_info["errors"]:
                                    print(f"   错误: {error}")
                        if task_index == 0:
                            result_info = data.get("result") or {}
                            alpha_id = (
                                    result_info.get("alphaId")
                                    or result_info.get("alpha_id")
                                    or result_info.get("id")
                                    or (result_info.get("alpha") or {}).get("id")
                            )
                            if alpha_id and alpha_id not in completed_alpha_notice:
                                print(f"⚠️ 任务0 失败，alphaId: {alpha_id}")
                                completed_alpha_notice.add(alpha_id)

                        return "FAILED"

                    # COMPLETE状态直接返回
                    if status == "COMPLETE":
                        if task_index == 0:
                            result_info = data.get("result") or {}
                            alpha_id = (
                                    result_info.get("alphaId")
                                    or result_info.get("alpha_id")
                                    or result_info.get("id")
                                    or (result_info.get("alpha") or {}).get("id")
                            )
                            if alpha_id and alpha_id not in completed_alpha_notice:
                                print(f"🎉 任务0 完成，alphaId: {alpha_id}")
                                completed_alpha_notice.add(alpha_id)
                        return status

                    return status

                else:

                    print(f"⚠️ 任务{task_index} 状态检查失败: HTTP {response.status_code}")

                    try:

                        error_data = response.json()

                        print(f"   错误详情: {error_data}")

                    except:

                        print(f"   响应内容: {response.text[:200]}...")

                    return "ERROR"

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    print(
                        f"⏰ 任务{task_index} 请求超时 ({base_timeout}秒)，{delay}秒后重试 ({attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    print(f"❌ 任务{task_index} 请求超时，已达到最大重试次数")
                    return "ERROR"

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    print(f"❌ 任务{task_index} 网络异常: {e}，{delay}秒后重试 ({attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    print(f"❌ 任务{task_index} 网络异常，已达到最大重试次数: {e}")
                    return "ERROR"

            except Exception as e:

                if attempt < max_retries - 1:

                    delay = base_delay * (2 ** attempt)

                    print(f"❌ 任务{task_index} 异常: {e}，{delay}秒后重试 ({attempt + 1}/{max_retries})")

                    time.sleep(delay)

                    continue

                else:

                    print(f"❌ 任务{task_index} 异常，已达到最大重试次数: {e}")

                    return "ERROR"

        return "ERROR"

    while task_queue or active_tasks:

        while len(active_tasks) < 3 and task_queue:

            task_data = task_queue.pop(0)

            task_index = start + completed_count + failed_count + len(active_tasks)

            progress_url = submit_simulation(task_index, task_data)

            if progress_url:
                active_tasks[task_index] = progress_url
                task_check_counts[task_index] = 0
                task_start_times[task_index] = time.time()

            if task_queue:
                time.sleep(2)

        completed_tasks = []

        if active_tasks:
            print(f"📊 检查 {len(active_tasks)} 个任务状态...")

        for task_index, progress_url in list(active_tasks.items()):  # 使用list()避免迭代时修改字典

            # 增加检查计数
            task_check_counts[task_index] = task_check_counts.get(task_index, 0) + 1

            # 检查是否超时
            elapsed_time = time.time() - task_start_times.get(task_index, time.time())
            check_count = task_check_counts.get(task_index, 0)

            if elapsed_time > max_task_duration:
                print(f"⏰ 任务{task_index} 运行时间过长 ({elapsed_time / 60:.1f}分钟)，标记为超时")
                completed_tasks.append(task_index)
                failed_count += 1
                continue

            if check_count > max_check_count:
                print(f"⏰ 任务{task_index} 检查次数过多 ({check_count}次)，标记为超时")
                completed_tasks.append(task_index)
                failed_count += 1
                continue

            print(f"🔍 检查任务{task_index}状态... (第{check_count}次检查, 已运行{elapsed_time / 60:.1f}分钟)")
            status = check_simulation_status(task_index, progress_url)

            if status == "COMPLETE":

                print(f"✅ 任务{task_index} 完成")

                completed_tasks.append(task_index)

                completed_count += 1

                time.sleep(15)

            elif status in ["ERROR", "FAILED"]:

                print(f"❌ 任务{task_index} 失败")

                completed_tasks.append(task_index)

                failed_count += 1

                time.sleep(5)

            elif status == "RUNNING":

                print(f"⏳ 任务{task_index} 运行中 (已检查{check_count}次, 已运行{elapsed_time / 60:.1f}分钟)")

            elif status == "WARNING":
                # WARNING状态：如果持续超过一定时间，视为完成（有警告但已生成结果）
                warning_threshold_minutes = 30  # WARNING状态超过30分钟视为完成
                if elapsed_time > warning_threshold_minutes * 60:
                    print(f"⚠️ 任务{task_index} WARNING状态持续{elapsed_time / 60:.1f}分钟，视为完成（有警告）")
                    completed_tasks.append(task_index)
                    completed_count += 1
                else:
                    print(
                        f"⚠️ 任务{task_index} WARNING状态 (已检查{check_count}次, 已运行{elapsed_time / 60:.1f}分钟，继续等待)")

            else:
                # 处理其他未知状态
                print(f"⚠️ 任务{task_index} 状态: {status} (未知状态，继续等待)")

                # 如果状态是UNKNOWN且持续很长时间，标记为错误
                if status == "UNKNOWN" and elapsed_time > 1800:  # 30分钟
                    print(f"⏰ 任务{task_index} UNKNOWN状态持续30分钟，标记为错误")
                    completed_tasks.append(task_index)
                    failed_count += 1

        for task_index in completed_tasks:
            del active_tasks[task_index]
            if task_index in task_check_counts:
                del task_check_counts[task_index]
            if task_index in task_start_times:
                del task_start_times[task_index]

        if active_tasks:
            print(f"⏳ 等待 {len(active_tasks)} 个任务完成...")
            time.sleep(15)
        else:
            print(f"✅ 所有任务已完成或失败")

        # 每20个任务后重新登录，防止session过期
        if (completed_count + failed_count) % 20 == 0 and (completed_count + failed_count) > 0:
            print(f"🔄 已完成 {completed_count + failed_count} 个任务，重新登录以刷新session...")
            s = sign_in(cfg.username, cfg.password)
            if s is None:
                print("❌ 重新登录失败，停止处理")
                break

    print(f"🎉 模拟完成! 成功: {completed_count}, 失败: {failed_count}, 总计: {completed_count + failed_count}")


def generate_sim_data_sa(alpha_list, region, uni, neut, selection_limits, selection_handling_options):
    # 如果 selection_limits 是列表，随机选择一个；否则直接使用

    if isinstance(selection_limits, list):

        selection_limit = random.choice(selection_limits)

    else:

        selection_limit = selection_limits

    # 随机选择 selection handling

    selection_handling = random.choice(selection_handling_options)

    sim_data_list = []

    if isinstance(alpha_list, list) and len(alpha_list) == 1 and isinstance(alpha_list[0], tuple):

        selection_exp, combo_exp = alpha_list[0]

        simulation_data = {

            'type': 'SUPER',

            'settings': {

                'instrumentType': 'EQUITY',

                'region': region,

                'universe': uni,

                'delay': 1,

                'decay': 5,

                'neutralization': neut,

                'truncation': 0.08,

                'pasteurization': 'ON',

                'unitHandling': 'VERIFY',

                'nanHandling': 'ON',

                'language': 'FASTEXPR',

                'visualization': False,
                'MaxTrade': 'ON',

                'selectionHandling': selection_handling,

                'selectionLimit': selection_limit

            },

            'selection': selection_exp,

            'combo': combo_exp

        }

        sim_data_list.append(simulation_data)

    else:

        for selection_exp, combo_exp in alpha_list:
            simulation_data = {

                'type': 'SUPER',

                'settings': {

                    'instrumentType': 'EQUITY',

                    'region': region,

                    'universe': uni,

                    'delay': 1,

                    'decay': 5,

                    'neutralization': neut,

                    'truncation': 0.08,

                    'pasteurization': 'ON',

                    'unitHandling': 'VERIFY',

                    'nanHandling': 'ON',

                    'language': 'FASTEXPR',

                    'visualization': False,

                    'selectionHandling': selection_handling,

                    'selectionLimit': selection_limit,

                },

                'selection': selection_exp,

                'combo': combo_exp

            }

            sim_data_list.append(simulation_data)

    return sim_data_list


def save_progress(session_seed, completed_configs, current_region, current_universe, current_neutralization):
    progress_data = {

        "session_seed": session_seed,

        "completed_configs": completed_configs,

        "current_region": current_region,

        "current_universe": current_universe,

        "current_neutralization": current_neutralization,

        "timestamp": datetime.datetime.now().isoformat(),

        "version": "1.0"

    }

    progress_file = os.path.join(os.path.dirname(__file__), "sa_progress.json")

    try:

        with open(progress_file, 'w', encoding='utf-8') as f:

            json.dump(progress_data, f, ensure_ascii=False, indent=2)

        print(f"💾 进度已保存: {len(completed_configs)} 个配置已完成")

    except Exception as e:

        print(f"⚠️ 保存进度失败: {e}")


def load_progress():
    progress_file = os.path.join(os.path.dirname(__file__), "sa_progress.json")

    if not os.path.exists(progress_file):
        return None

    try:

        with open(progress_file, 'r', encoding='utf-8') as f:

            progress_data = json.load(f)

        print(f"📂 发现进度文件: {len(progress_data.get('completed_configs', []))} 个配置已完成")

        return progress_data

    except Exception as e:

        print(f"⚠️ 加载进度失败: {e}")

        return None


def parse_arguments():
    import argparse

    parser = argparse.ArgumentParser(description='SA模拟自动化工具')

    parser.add_argument('--seed', type=int, help='随机种子，用于重现相同序列')

    parser.add_argument('--resume', action='store_true', help='从上次进度继续')

    parser.add_argument('--fresh', action='store_true', help='重新开始，忽略进度文件')

    return parser.parse_args()


if __name__ == '__main__':

    # ==================== 使用预定义账号密码 ====================
    print("=" * 60)
    print("🔐 WorldQuant BRAIN - Super Alpha 自动化工具")
    print("=" * 60)
    print()

    # 检查配置中的账号密码
    if not cfg.username or not cfg.password:
        print("❌ 错误：配置中缺少账号或密码")
        print("   请在代码中设置 cfg.username 和 cfg.password")
        sys.exit(1)

    print(f"✅ 使用预定义账号: {cfg.username}")
    print("=" * 60)
    print()
    # ========================================================

    args = parse_arguments()

    progress_data = None

    if args.resume and not args.fresh:
        progress_data = load_progress()

    if args.fresh:
        progress_data = None

        print("🆕 强制重新开始，忽略进度文件")

    if args.seed:

        session_seed = args.seed

        print(f"🎲 使用指定种子: {session_seed}")

    elif progress_data and 'session_seed' in progress_data:

        session_seed = progress_data['session_seed']

        print(f"🎲 使用进度文件中的种子: {session_seed}")

    else:

        session_seed = random.randint(1, 1000000)

        print(f"🎲 生成新随机种子: {session_seed}")

    random.seed(session_seed)

    # Selection limits 设置

    # selection_limits = [10, 20, 30, 40, 50, 100, 200, 300]
    selection_limits = [300, 600, 1000]

    print(f"🎯 Selection Limits: {selection_limits}")

    # Selection handling options 设置

    # selection_handling_options = ['POSITIVE', 'NON_ZERO', 'NON_NAN']

    selection_handling_options = ['POSITIVE', 'NON_ZERO']

    print(f"🎯 Selection Handling Options: {selection_handling_options}")

    completed_configs = progress_data.get('completed_configs', []) if progress_data else []

    print(f"🔄 会话种子: {session_seed}")

    print(f"✅ 已完成配置: {len(completed_configs)}")

    # 记录程序启动时间
    program_start_time = datetime.datetime.now()
    restart_interval = 3600  # 1小时 = 3600秒

    # 错误计数相关
    consecutive_errors = 0
    max_consecutive_errors = 30  # 连续错误阈值

    print(f"⏰ 程序启动时间: {program_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔄 重启间隔: {restart_interval / 3600:.1f} 小时")
    print(f"⚠️ 连续错误阈值: {max_consecutive_errors} 个")

    while True:
        # 检查是否需要重启
        current_time = datetime.datetime.now()
        elapsed_time = (current_time - program_start_time).total_seconds()

        if elapsed_time >= restart_interval:
            print(f"\n🔄 程序运行时间已达到 {elapsed_time / 3600:.1f} 小时，准备重启...")
            print(f"⏰ 当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"💾 保存最终进度...")
            save_progress(session_seed, completed_configs, "restart", "restart", "restart")
            print(f"🔄 程序重启中...")
            break

        # 显示剩余时间
        remaining_time = restart_interval - elapsed_time
        if remaining_time > 0:
            print(f"⏰ 距离下次重启还有 {remaining_time / 3600:.1f} 小时 ({remaining_time / 60:.0f} 分钟)")

        # 显示连续错误计数
        if consecutive_errors > 0:
            print(f"⚠️ 当前连续错误数: {consecutive_errors}/{max_consecutive_errors}")

        selection_exp = []

        exp = get_simple_selection()

        selection_exp.append(exp)

        combo_exp = get_combo_code_list()

        sa_list = [(i, j) for i in selection_exp for j in combo_exp]

        print(f"\n📋 本轮生成: {len(sa_list)} 个SA策略")

        print(f"Sample Selection: {exp}")

        pools = [sa_list]

        print(f"🔄 总共 {len(sa_list)} 个SA对，将分批并发处理（最多3个并发）")

        region_dict = {
            "usa": ("USA", ["TOP3000", "TOP1000", "TOP500", "TOP200", "ILLIQUID_MINVOL1M", "TOPSP200"]),
            "eur": ("EUR", ["TOP2500", "TOP1200", "TOP800", "TOP400", "ILLIQUID_MINVOL1M"]),
            "glb": ("GLB", ["TOPDIV3000", "TOP3000", "MINVOL1M"]),
            "asi": ("ASI", ["MINVOL1M", "ILLIQUID_MINVOL1M"]),
            "chn": ("CHN", ["TOP2000U"]),
            "jpn": ("JPN", ["TOP1600", "TOP1200"]),
            "amr": ("AMR", ["TOP600"])
        }

        neut_opt = {
            "USA": [  # USA支持的所有neutralization选项
                "NONE", "REVERSION_AND_MOMENTUM", "STATISTICAL", "CROWDING", "FAST", "SLOW", "MARKET",
                "SECTOR", "INDUSTRY", "SUBINDUSTRY", "SLOW_AND_FAST"
            ],
            "EUR": [  # EUR支持的所有neutralization选项
                "NONE", "REVERSION_AND_MOMENTUM", "STATISTICAL", "CROWDING", "FAST", "SLOW", "MARKET",
                "SECTOR", "INDUSTRY", "SUBINDUSTRY", "COUNTRY", "SLOW_AND_FAST"
            ],
            "GLB": [  # GLB支持的所有neutralization选项
                "NONE", "REVERSION_AND_MOMENTUM", "STATISTICAL", "CROWDING", "FAST", "SLOW", "MARKET",
                "SECTOR", "INDUSTRY", "SUBINDUSTRY", "COUNTRY", "SLOW_AND_FAST"
            ],
            "ASI": [  # ASI支持的所有neutralization选项
                "NONE", "REVERSION_AND_MOMENTUM", "STATISTICAL", "CROWDING", "FAST", "SLOW", "MARKET",
                "SECTOR", "INDUSTRY", "SUBINDUSTRY", "COUNTRY", "SLOW_AND_FAST"
            ],
            "CHN": [  # CHN支持的所有neutralization选项
                "NONE", "REVERSION_AND_MOMENTUM", "CROWDING", "FAST", "SLOW",
                "MARKET", "SECTOR", "INDUSTRY", "SUBINDUSTRY", "SLOW_AND_FAST"
            ],
            "JPN": [  # JPN支持的所有neutralization选项
                "SUBINDUSTRY", "INDUSTRY", "SECTOR", "MARKET", "NONE"
            ],
            "AMR": [  # AMR支持的所有neutralization选项
                "NONE", "MARKET", "SECTOR", "INDUSTRY", "SUBINDUSTRY", "COUNTRY"
            ]
        }

        regi = ['usa', 'eur', 'glb', 'asi', 'chn', 'jpn', 'amr']

        random.shuffle(regi)

        for k in regi:

            region_name = region_dict[k][0]

            universe_list = region_dict[k][1]  # 处理所有universe

            neutralization_list = neut_opt[k.upper()]
            random.shuffle(neutralization_list)
            print('neutralization_list' + str(neutralization_list))

            print(f"\n🌍 开始处理地区: {region_name}")

            print(f"   Universes: {universe_list}")

            print(f"   Neutralizations: {neutralization_list}")

            for universe in universe_list:

                print(f"\n📊 处理Universe: {universe} ({region_name})")

                start_neut_index = 0

                if (progress_data and

                        progress_data.get('current_region') == k and

                        progress_data.get('current_universe') == universe):

                    current_neut = progress_data.get('current_neutralization')

                    if current_neut in neutralization_list:
                        start_neut_index = neutralization_list.index(current_neut)

                        print(f"⏭️ 跳过已完成的neutralization: {neutralization_list[:start_neut_index]}")

                for i, neutralization in enumerate(neutralization_list[start_neut_index:], start_neut_index):

                    print(f"\n⚙️ 配置: {neutralization} neutralization")

                    print(f"   地区: {region_name}")

                    print(f"   Universe: {universe}")

                    try:
                        multi_simulate2_sa(pools, neutralization, region_name, universe, 0, selection_limits,
                                           selection_handling_options)

                        # 成功执行，重置错误计数
                        if consecutive_errors > 0:
                            print(f"✅ 成功执行，重置连续错误计数: {consecutive_errors} -> 0")
                            consecutive_errors = 0

                        config_key = f"{region_name}-{universe}-{neutralization}"

                        if config_key not in completed_configs:
                            completed_configs.append(config_key)

                        save_progress(session_seed, completed_configs, k, universe, neutralization)

                        print(f"✅ 完成配置: {neutralization} - {region_name} - {universe}")

                    except Exception as e:
                        # 捕获异常，增加错误计数
                        consecutive_errors += 1
                        error_msg = str(e)
                        print(
                            f"❌ 执行异常 (连续错误 {consecutive_errors}/{max_consecutive_errors}): {error_msg[:100]}...")

                        # 特殊处理：如果是登录失败（可能是429限流），等待更长时间
                        if "登录失败" in error_msg or "429" in error_msg:
                            wait_minutes = 5
                            print(f"🚦 检测到登录限流问题，等待 {wait_minutes} 分钟后继续...")
                            print(
                                f"   预计恢复时间: {(datetime.datetime.now() + datetime.timedelta(minutes=wait_minutes)).strftime('%Y-%m-%d %H:%M:%S')}")
                            time.sleep(wait_minutes * 60)
                            print(f"⏰ 等待完成，继续处理...")

                        # 检查是否达到错误阈值
                        if consecutive_errors >= max_consecutive_errors:
                            print(f"\n🚨 连续错误达到阈值 {max_consecutive_errors}，准备重启...")
                            print(f"⏰ 当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                            print(f"💾 保存最终进度...")
                            save_progress(session_seed, completed_configs, "error_restart", "error_restart",
                                          "error_restart")
                            print(f"🔄 因连续错误重启程序...")
                            break  # 退出主循环

                print(f"🎯 完成Universe: {universe} 的所有配置")

            print(f"🏁 完成地区: {region_name} 的所有处理")

        print("\n🎉 本轮循环完成，准备下一轮...")

        print(f"💾 最终进度已保存: {len(completed_configs)} 个配置已完成")

        # 检查是否需要重启
        current_time = datetime.datetime.now()
        elapsed_time = (current_time - program_start_time).total_seconds()

        if elapsed_time >= restart_interval:
            print(f"\n🔄 程序运行时间已达到 {elapsed_time / 3600:.1f} 小时，准备重启...")
            print(f"⏰ 当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"💾 保存最终进度...")
            save_progress(session_seed, completed_configs, "restart", "restart", "restart")
            print(f"🔄 程序重启中...")
            break
