from os.path import expanduser

import requests
from os import environ
from time import sleep
import time
import json
import pandas as pd
import random
import pickle
from itertools import product
from itertools import combinations
from collections import defaultdict
import pickle
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import os


def time_it(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        print('start_time')
        print(start_time)
        print('end_time')
        print(end_time)
        print(f"Function {func.__name__} took {end_time - start_time}")
        return result

    return wrapper


# basic_ops = ["reverse", "inverse", "rank", "zscore", "quantile", "normalize"]

# ts_ops = ["ts_rank", "ts_zscore", "ts_delta",  "ts_sum", "ts_delay",
#           "ts_std_dev", "ts_mean",  "ts_arg_min", "ts_arg_max","ts_scale", "ts_quantile"]
basic_ops = ["log", "sqrt", "reverse", "inverse", "rank", "zscore",
             'quantile', "normalize", 'sign', 'pasteurize', 'max', 'abs', 'min', 'power', 'hump_decay', 'hump']

# basic_ops = ['add', 'multiply', 'sign', 'pasteurize',
# 'log', 'max', 'abs', 'divide', 'signed_power', 'inverse',
# 'sqrt', 'reverse', 'power', 'densify', 'or', 'and', 'not',
# 'is_nan', 'less', 'equal', 'greater', 'if_else', 'not_equal',
# 'less_equal', 'greater_equal', 'ts_zscore', 'ts_returns', 'ts_product', 'ts_std_dev',
# 'ts_backfill', 'days_from_last_change', 'ts_scale', 'ts_sum',
# 'ts_decay_exp_window', 'ts_av_diff', 'ts_kurtosis', 'ts_mean', 'ts_rank', 'ts_ir',
# 'ts_delay', 'hump_decay', 'ts_quantile', 'ts_count_nans', 'ts_decay_linear', 'ts_arg_min', 'ts_max_diff', 'kth_element', 'hump', 'ts_delta'
# , 'winsorize', 'rank', 'zscore', 'scale',
# 'normalize', 'quantile', 'vec_sum', 'vec_avg', 'trade_when', 'group_rank', 'group_scale', 'group_count', 'group_zscore', 'group_std_dev', 'group_sum', 'group_neutralize']

# 'ts_arg_max', 'group_mean','ts_regression', 'bucket', 'subtract', 'ts_step', 'last_diff_value', 'tail','group_backfill', 'min'
# 'ts_target_tvr_delta_limit', 'ts_target_tvr_hump', 'ts_target_tvr_decay',
# ts_ops = ["ts_rank", "ts_zscore", "ts_delta", "ts_sum", "ts_product",
#           "ts_ir", "ts_std_dev", "ts_mean", "ts_arg_min", "ts_arg_max", "ts_min_diff",
#           "ts_max_diff", "ts_returns", "ts_scale", "ts_skewness", "ts_kurtosis",
#           "ts_quantile"]
# ts_ops = ["ts_rank", "ts_zscore", "ts_delta", "ts_sum", "ts_product",
#           "ts_ir", "ts_std_dev", "ts_mean", "ts_arg_min", "ts_arg_max",
#           "ts_max_diff", "ts_returns", "ts_scale", "ts_kurtosis",
#           "ts_quantile"]

ts_ops = ["ts_arg_max", "ts_arg_min", "ts_av_diff", "ts_backfill", "ts_count_nans",
          "ts_decay_exp_window", "ts_decay_linear", "ts_delay", "ts_delta", "ts_ir",
          "ts_kurtosis", "ts_max_diff", "ts_mean", "ts_product",
          "ts_quantile", "ts_rank", "ts_returns", "ts_scale", "ts_std_dev", "ts_sum",
          "ts_zscore"]

ts_not_use = ["ts_delay"]

# arsenal = ["ts_moment", "ts_entropy", "ts_min_max_cps", "ts_min_max_diff", "inst_tvr", 'sigmoid',
#            "ts_decay_exp_window", "ts_percentage", "vector_neut", "vector_proj", "signed_power"]
arsenal = ["ts_decay_exp_window", "signed_power"]

# twin_field_ops = ["ts_corr", "ts_covariance", "ts_co_kurtosis", "ts_co_skewness", "ts_theilsen"]

group_ops = ["group_count", "group_neutralize", "group_rank", "group_scale", "group_std_dev", "group_sum",
             "group_zscore"]

# group_ac_ops = ["group_sum", "group_max", "group_mean", "group_median", "group_min", "group_std_dev", ]
group_ac_ops = ["group_sum", "group_std_dev"]
# , "group_mean"

ops_set = basic_ops + ts_ops + group_ac_ops + group_ac_ops


def login():
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

    with open(brain_file) as f:
        credentials = json.load(f)

    # Extract username and password from the list
    username, password = credentials

    # Create a session to persistently store the headers
    s = requests.Session()

    # Save credentials into session
    s.auth = (username, password)

    # Send a POST request to the /authentication API
    response = s.post('https://api.worldquantbrain.com/authentication')
    print(f"登录响应状态: {response.status_code}")
    # 修复：HTTP 201 Created 也是成功的认证响应
    if response.status_code in [200, 201]:
        print("✅ 登录成功！")
        # 额外验证：测试一个简单API调用
        try:
            test_resp = s.get('https://api.worldquantbrain.com/users/self/alphas?limit=1&offset=0')
            print(f"   API测试响应: {test_resp.status_code}（如果为200，表示认证完全正常）")
            if test_resp.status_code == 200:
                test_data = test_resp.json()
                print(f"   测试数据: {len(test_data.get('results', []))} 个alpha可用")
            else:
                print(f"   ⚠️  API测试警告: {test_resp.status_code} - {test_resp.text[:100]}")
        except Exception as test_e:
            print(f"   ⚠️  API测试异常: {test_e}")
    else:
        print(f"❌ 登录失败 (状态码 {response.status_code}): {response.text[:200]}")
        print("请检查用户名和密码是否正确，或联系WorldQuant BRAIN支持")

    return s


# def locate_alpha(s, alpha_id):
#     alpha = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id)
#     string = alpha.content.decode('utf-8')
#     metrics = json.loads(string)
#     #print(metrics["regular"]["code"])
#
#     dateCreated = metrics["dateCreated"]
#     sharpe = metrics["is"]["sharpe"]
#     fitness = metrics["is"]["fitness"]
#     turnover = metrics["is"]["turnover"]
#     margin = metrics["is"]["margin"]
#
#     triple = [sharpe, fitness, turnover, margin, dateCreated]
#
#     return triple
def get_prod_corr(s, alpha_id):
    """
    Function gets alpha's prod correlation
    and save result to dataframe
    """

    while True:
        result = s.get(
            "https://api.worldquantbrain.com/alphas/" + alpha_id + "/correlations/prod"
        )
        if "retry-after" in result.headers:
            time.sleep(float(result.headers["Retry-After"]))
        else:
            break
    if result.json().get("records", 0) == 0:
        return pd.DataFrame()
    columns = [dct["name"] for dct in result.json()["schema"]["properties"]]
    prod_corr_df = pd.DataFrame(result.json()["records"], columns=columns).assign(alpha_id=alpha_id)

    return prod_corr_df


def get_self_corr(s, alpha_id):
    """
    Function gets alpha's self correlation
    and save result to dataframe
    """

    while True:

        result = s.get(
            "https://api.worldquantbrain.com/alphas/" + alpha_id + "/correlations/self"
        )
        if "retry-after" in result.headers:
            time.sleep(float(result.headers["Retry-After"]))
        else:
            break
    if result.json().get("records", 0) == 0:
        return pd.DataFrame()

    records_len = len(result.json()["records"])
    if records_len == 0:
        return pd.DataFrame()

    columns = [dct["name"] for dct in result.json()["schema"]["properties"]]
    self_corr_df = pd.DataFrame(result.json()["records"], columns=columns).assign(alpha_id=alpha_id)

    return self_corr_df


def check_self_corr_test(s, alpha_id, threshold: float = 0.7):
    """
    Function checks if alpha's self_corr test passed
    Saves result to dataframe
    """

    self_corr_df = get_self_corr(s, alpha_id)
    if self_corr_df.empty:
        result = [{"test": "SELF_CORRELATION", "result": "PASS", "limit": threshold, "value": 0, "alpha_id": alpha_id}]
    else:
        value = self_corr_df["correlation"].max()
        result = [
            {
                "test": "SELF_CORRELATION",
                "result": "PASS" if value < threshold else "FAIL",
                "limit": threshold,
                "value": value,
                "alpha_id": alpha_id
            }
        ]
    return pd.DataFrame(result)


def get_simulation_result_json(s, alpha_id):
    return s.get("https://api.worldquantbrain.com/alphas/" + alpha_id).json()


def locate_alpha(s, alpha_id):
    while True:
        alpha = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id)
        if "retry-after" in alpha.headers:
            time.sleep(float(alpha.headers["Retry-After"]))
        else:
            break
    string = alpha.content.decode('utf-8')
    metrics = json.loads(string)
    # print(metrics["regular"]["code"])

    dateCreated = metrics["dateCreated"]
    sharpe = metrics["is"]["sharpe"]
    fitness = metrics["is"]["fitness"]
    turnover = metrics["is"]["turnover"]
    margin = metrics["is"]["margin"]
    decay = metrics["settings"]["decay"]
    exp = metrics['regular']['code']

    triple = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
    return triple


# 获取当前时间
current_time = datetime.now()

# 用最简单的数字格式显示：年月日时分秒连续排列
simple_format = f"{current_time.year}{current_time.month:02d}{current_time.day:02d}{current_time.hour:02d}{current_time.minute:02d}{current_time.second:02d}"


def set_alpha_properties(
        s,
        alpha_id,
        name: str = None,
        color: str = None,
        selection_desc: str = "311111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111",
        combo_desc: str = "322222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222",
        description: str = 'None',
        tags=None,
):
    """
    Function changes alpha's description parameters
    """

    # 如果tags为None，则不在请求中包含tags字段（避免设置空标签）
    params = {
        "color": color,
        "name": name,
        "category": None,
        "regular": {"description": description},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }

    # 只有当tags不为None时才添加tags字段
    if tags is not None:
        params["tags"] = tags

    response = s.patch(
        "https://api.worldquantbrain.com/alphas/" + alpha_id, json=params
    )


def check_submission(alpha_bag, gold_bag, start):
    depot = []
    s = login()
    progress_summary = []  # 收集进度信息，避免频繁输出

    for idx, g in enumerate(alpha_bag):
        if idx < start:
            continue
        if idx % 5 == 0:
            print(f"   检查进度: {idx}/{len(alpha_bag)} ({idx / len(alpha_bag) * 100:.1f}%)", end="\r")
        if idx % 200 == 0:
            s = login()
        # print(idx)
        pc = get_check_submission(s, g)
        if pc == "sleep":
            sleep(100)
            s = login()
            alpha_bag.append(g)
        elif pc != pc:
            # pc is nan
            print(f"\n   ⚠️  check self-correlation error at {idx}")
            sleep(100)
            alpha_bag.append(g)
        elif pc == "fail":
            continue
        elif pc == "error":
            depot.append((g, pc, "检查错误"))  # 添加错误信息
        else:
            # print('g')
            # print(g)
            gold_bag.append((g, pc))

    print()  # 换行，结束进度显示
    # print('depot')
    # if depot:
    #     print(f"   错误的alpha数量: {len(depot)}")
    return gold_bag


def get_check_submission(s, alpha_id, max_retries=3):
    """
    获取alpha的提交检查结果，包含重试逻辑

    Args:
        s: session对象
        alpha_id: alpha ID
        max_retries: 最大重试次数，默认3次

    Returns:
        pc: PROD_CORRELATION值（成功）
        "fail": 检查失败
        "sleep": 登出状态
        "error": 错误（重试失败后）
    """
    for attempt in range(max_retries):
        try:
            # 获取检查结果（带重试等待）
            while True:
                result = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/check")
                if "retry-after" in result.headers:
                    time.sleep(float(result.headers["Retry-After"]))
                else:
                    break

            # 检查是否登出
            if result.json().get("is", 0) == 0:
                print(f"   ⚠️  {alpha_id[:8]}: 登出状态，需要重新登录")
                return "sleep"

            # 解析检查结果
            checks_df = pd.DataFrame(
                result.json()["is"]["checks"]
            )

            # 获取PROD_CORRELATION值
            pc_rows = checks_df[checks_df.name == "PROD_CORRELATION"]
            if len(pc_rows) == 0:
                raise ValueError("PROD_CORRELATION field not found in checks")

            pc = pc_rows["value"].values[0]

            # 检查是否有FAIL结果
            if not any(checks_df["result"] == "FAIL"):
                # 成功时不打印，减少输出（由调用者统一打印）
                return pc
            else:
                # 失败时只记录简要信息（由调用者统一打印）
                failed_checks = checks_df[checks_df["result"] == "FAIL"]
                failed_names = ", ".join(failed_checks['name'].tolist()[:3])  # 只取前3个
                # print(f"   ❌ {alpha_id[:8]}: 检查失败 - {failed_names}")
                return "fail"

        except KeyError as e:
            # 数据结构错误（减少输出，只在最后一次重试失败时打印）
            if attempt == max_retries - 1:
                print(f"   ⚠️  {alpha_id[:8]}: 字段缺失 {str(e)}")
                return "error"
            time.sleep(2 ** attempt)  # 指数退避：1秒、2秒、4秒

        except ValueError as e:
            # PROD_CORRELATION字段不存在
            if attempt == max_retries - 1:
                print(f"   ⚠️  {alpha_id[:8]}: {str(e)}")
                return "error"
            time.sleep(2 ** attempt)

        except Exception as e:
            # 其他未知错误
            if attempt == max_retries - 1:
                error_type = type(e).__name__
                print(f"   ⚠️  {alpha_id[:8]}: {error_type} - {str(e)[:50]}")
                return "error"
            time.sleep(2 ** attempt)

    # 理论上不会到这里
    return "error"


def get_vec_fields(fields):
    vec_ops = ["vec_avg", "vec_sum"]
    vec_fields = []

    for field in fields:
        for vec_op in vec_ops:
            if vec_op == "vec_choose":
                vec_fields.append("%s(%s, nth=-1)" % (vec_op, field))
                vec_fields.append("%s(%s, nth=0)" % (vec_op, field))
            else:
                vec_fields.append("%s(%s)" % (vec_op, field))

    return (vec_fields)


@time_it
def multi_simulate(alpha_pools, neut, region, universe, start, delay):
    x = 0
    s = login()

    brain_api_url = 'https://api.worldquantbrain.com'

    for x, pool in enumerate(alpha_pools):
        if x < start: continue
        progress_urls = []
        for y, task in enumerate(pool):
            # 10 tasks, 10 alpha in each task
            sim_data_list = generate_sim_data(task, region, universe, neut, delay)
            try:
                simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=sim_data_list)
                simulation_progress_url = simulation_response.headers['Location']
                progress_urls.append(simulation_progress_url)
            except:
                print("loc key error: %s" % simulation_response.content)
                sleep(600)
                s = login()
        print(progress_urls)
        print("pool %d task %d post done" % (x, y))

        for j, progress in enumerate(progress_urls):
            try:
                while True:
                    simulation_progress = s.get(progress)
                    if simulation_progress.headers.get("Retry-After", 0) == 0:
                        break
                    # print("Sleeping for " + simulation_progress.headers["Retry-After"] + " seconds")
                    sleep(float(simulation_progress.headers["Retry-After"]))

                status = simulation_progress.json().get("status", 0)
                if status != "COMPLETE":
                    print("Not complete : %s" % (progress))

                """
                #alpha_id = simulation_progress.json()["alpha"]
                children = simulation_progress.json().get("children", 0)
                children_list = []
                for child in children:
                    child_progress = s.get(brain_api_url + "/simulations/" + child)
                    alpha_id = child_progress.json()["alpha"]

                    set_alpha_properties(s,
                            alpha_id,
                            name = "%s"%name,
                            color = None,)
                """
            except KeyError:
                print("look into: %s" % progress)
            except:
                print("other")
        print("pool %d task %d simulate done" % (x, j))
    print("Simulate done")
    return x


def generate_sim_data(alpha_list, region, uni, neut, delay):
    sim_data_list = []
    for alpha, decay in alpha_list:
        simulation_data = {
            'type': 'REGULAR',
            'settings': {
                'instrumentType': 'EQUITY',
                'region': region,
                'universe': uni,
                'delay': delay,
                'decay': decay,
                'neutralization': neut,
                'truncation': 0.08,
                'pasteurization': 'ON',
                # 'testPeriod': 'P2Y',
                'testPeriod': 'P0Y',
                'unitHandling': 'VERIFY',
                'nanHandling': 'ON',
                'language': 'FASTEXPR',
                'visualization': False,
                'MaxTrade': 'ON',
            },
            'regular': alpha}

        sim_data_list.append(simulation_data)
    return sim_data_list


def load_task_pool(alpha_list, limit_of_children_simulations, limit_of_multi_simulations):
    '''
    Input:
        alpha_list : list of (alpha, decay) tuples
        limit_of_multi_simulations : number of children simulation in a multi-simulation
        limit_of_multi_simulations : number of simultaneous multi-simulations
    Output:
        task : [10 * (alpha, decay)] for a multi-simulation
        pool : [10 * [10 * (alpha, decay)]] for simultaneous multi-simulations
        pools : [[10 * [10 * (alpha, decay)]]]

    '''
    tasks = [alpha_list[i:i + limit_of_children_simulations] for i in
             range(0, len(alpha_list), limit_of_children_simulations)]
    pools = [tasks[i:i + limit_of_multi_simulations] for i in range(0, len(tasks), limit_of_multi_simulations)]
    return pools


def get_datasets(
        s,
        instrument_type: str = 'EQUITY',
        region: str = 'USA',
        delay: int = 1,
        universe: str = 'TOP3000'
):
    url = "https://api.worldquantbrain.com/data-sets?" + \
          f"instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}"
    result = s.get(url)
    datasets_df = pd.DataFrame(result.json()['results'])
    return datasets_df


def get_datafields(
        s,
        instrument_type: str = 'EQUITY',
        region: str = 'USA',
        delay: int = 1,
        universe: str = 'TOP3000',
        dataset_id: str = '',
        search: str = ''
):
    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" + \
                       "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count']

    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" + \
                       f"&search={search}" + \
                       "&offset={x}"
        count = 100

    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])

    datafields_list_flat = [item for sublist in datafields_list for item in sublist]

    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df


def process_datafields(df, data_type):
    if data_type == "matrix":
        datafields = df[df['type'] == "MATRIX"]["id"].tolist()
    elif data_type == "vector":
        datafields = get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())

    tb_fields = []
    for field in datafields:
        tb_fields.append("winsorize(ts_backfill(%s, 120), std=4)" % field)
    return tb_fields


def process_datafields_two(first, second, data_type):
    if data_type == "matrix":
        datafield_first = first[first['type'] == "MATRIX"]["id"].tolist()
        datafield_second = second[second['type'] == "MATRIX"]["id"].tolist()
    elif data_type == "vector":
        # datafields = get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())
        datafield_first = get_vec_fields(first[first['type'] == "MATRIX"]["id"].tolist())
        datafield_second = get_vec_fields(second[second['type'] == "MATRIX"]["id"].tolist())

    tb_fields = []
    for field_f in datafield_first:
        for field_s in datafield_second:
            tb_fields.append(f"winsorize(ts_backfill({field_s}/{field_s}, 120), std=4)")
    return tb_fields


def view_alphas(gold_bag):
    s = login()
    sharp_list = []
    for gold, pc in gold_bag:
        triple = locate_alpha(s, gold)
        info = [triple[0], triple[2], triple[3], triple[4], triple[5], triple[6], triple[1]]
        info.append(pc)
        sharp_list.append(info)

    sharp_list.sort(reverse=True, key=lambda x: x[1])
    for i in sharp_list:
        print(i)


def get_alphas(start_date, end_date, sharpe_th, fitness_th, region, alpha_num, usage):
    s = login()
    output = []
    # 3E large 3C less
    count = 0
    for i in range(0, alpha_num, 100):
        print(i)
        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d" % (i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=-is.sharpe&hidden=false&type!=SUPER"
        print('url_e')
        print(url_e)
        url_c = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d" % (i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3C-" + str(fitness_th) + "&is.sharpe%3C-" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=is.sharpe&hidden=false&type!=SUPER"
        print('url_c')
        print(url_c)
        urls = [url_e]
        if usage != "submit":
            urls.append(url_c)
        for url in urls:
            response = s.get(url)
            # print(response.json())
            try:
                alpha_list = response.json()["results"]
                # print(response.json())
                for j in range(len(alpha_list)):
                    alpha_id = alpha_list[j]["id"]
                    name = alpha_list[j]["name"]
                    dateCreated = alpha_list[j]["dateCreated"]
                    sharpe = alpha_list[j]["is"]["sharpe"]
                    fitness = alpha_list[j]["is"]["fitness"]
                    turnover = alpha_list[j]["is"]["turnover"]
                    margin = alpha_list[j]["is"]["margin"]
                    longCount = alpha_list[j]["is"]["longCount"]
                    shortCount = alpha_list[j]["is"]["shortCount"]
                    decay = alpha_list[j]["settings"]["decay"]
                    exp = alpha_list[j]['regular']['code']
                    count += 1
                    # if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
                    if (longCount + shortCount) > 100:
                        if sharpe < -sharpe_th:
                            exp = "-%s" % exp
                        rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                        print(rec)
                        if turnover > 0.7:
                            rec.append(decay * 4)
                        elif turnover > 0.6:
                            rec.append(decay * 3 + 3)
                        elif turnover > 0.5:
                            rec.append(decay * 3)
                        elif turnover > 0.4:
                            rec.append(decay * 2)
                        elif turnover > 0.35:
                            rec.append(decay + 4)
                        elif turnover > 0.3:
                            rec.append(decay + 2)
                        output.append(rec)
            except:
                print("%d finished re-login" % i)
                s = login()

    print("count: %d" % count)
    return output


def get_super_alphas(start_date, end_date, sharpe_th, fitness_th, region, alpha_num, usage):
    s = login()
    output = []
    # 3E large 3C less
    count = 0
    for i in range(0, alpha_num, 20):
        print(f"📥 Offset {i}: 获取 {region} 地区的 SuperAlpha...")
        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d" % (i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=-is.sharpe&hidden=false&type=SUPER"
        # print('url_e')
        # print(url_e)
        url_c = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d" % (i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3C-" + str(fitness_th) + "&is.sharpe%3C-" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=is.sharpe&hidden=false&type=SUPER"
        # print('url_c')
        # print(url_c)
        urls = [url_e]
        if usage != "submit":
            urls.append(url_c)
        for url in urls:
            response = s.get(url)
            # print(response.json())
            try:
                alpha_list = response.json()["results"]
                batch_ids = []  # 收集本批次的ID
                # print(alpha_list)
                for j in range(len(alpha_list)):
                    alpha_id = alpha_list[j]["id"]
                    batch_ids.append(alpha_id)
                    output.append(alpha_id)

                # 横向显示本批次ID（每行10个）
                if batch_ids:
                    print(f"   获取到 {len(batch_ids)} 个: ", end="")
                    print(" | ".join([aid[:8] for aid in batch_ids[:10]]))
                    if len(batch_ids) > 10:
                        print(f"   ... 及其他 {len(batch_ids) - 10} 个")
                    # print(alpha_list[j]["is"]["sharpe"])
                    # # print(alpha_list[j]["is"]["longCount"])
                    # name = alpha_list[j]["name"]
                    # dateCreated = alpha_list[j]["dateCreated"]
                    # sharpe = alpha_list[j]["is"]["sharpe"]
                    # fitness = alpha_list[j]["is"]["fitness"]
                    # turnover = alpha_list[j]["is"]["turnover"]
                    # margin = alpha_list[j]["is"]["margin"]
                    # longCount = alpha_list[j]["is"]["longCount"]
                    # shortCount = alpha_list[j]["is"]["shortCount"]
                    # decay = alpha_list[j]["settings"]["decay"]
                    # exp = alpha_list[j]['regular']['code']
                    # count += 1
                    # #if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
                    # print(longCount)
                    # print(shortCount)
                    # if (longCount + shortCount) > 100:
                    #     print(alpha_list[j]["is"]["longCount"])
                    #     if sharpe < -sharpe_th:
                    #         exp = "-%s"%exp
                    #     rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                    #     print(rec)
                    #     if turnover > 0.7:
                    #         rec.append(decay*4)
                    #     elif turnover > 0.6:
                    #         rec.append(decay*3+3)
                    #     elif turnover > 0.5:
                    #         rec.append(decay*3)
                    #     elif turnover > 0.4:
                    #         rec.append(decay*2)
                    #     elif turnover > 0.35:
                    #         rec.append(decay+4)
                    #     elif turnover > 0.3:
                    #         rec.append(decay+2)
                    #     output.append(rec)
            except:
                print("%d finished re-login" % i)
                s = login()

    print("count: %d" % count)
    return output


def get_super_alphas_color(start_date, end_date, sharpe_th, fitness_th, region, alpha_num, usage, color):
    s = login()
    output = []
    # 3E large 3C less
    count = 0
    for i in range(0, alpha_num, 10):
        print(f"📥 Offset {i}: 获取 {region} 地区的 {color} 色 SuperAlpha...")
        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d" % (i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(
            sharpe_th) + "&settings.region=" + region + "&is.color=" + color + "&order=-is.sharpe&hidden=false&type=SUPER"
        # print('url_e')
        # print(url_e)
        url_c = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d" % (i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3C-" + str(fitness_th) + "&is.sharpe%3C-" \
                + str(
            sharpe_th) + "&settings.region=" + region + "&is.color=" + color + "&order=is.sharpe&hidden=false&type=SUPER"
        # print('url_c')
        # print(url_c)
        urls = [url_e]
        if usage != "submit":
            urls.append(url_c)
        for url in urls:
            response = s.get(url)
            # print(response.json())
            try:
                alpha_list = response.json()["results"]
                batch_ids = []  # 收集本批次的ID
                # print(alpha_list)
                for j in range(len(alpha_list)):
                    alpha_id = alpha_list[j]["id"]
                    batch_ids.append(alpha_id)
                    output.append(alpha_id)

                # 横向显示本批次ID（每行10个）
                if batch_ids:
                    print(f"   获取到 {len(batch_ids)} 个: ", end="")
                    print(" | ".join([aid[:8] for aid in batch_ids[:10]]))
                    if len(batch_ids) > 10:
                        print(f"   ... 及其他 {len(batch_ids) - 10} 个")
                    # print(alpha_list[j]["is"]["sharpe"])
                    # # print(alpha_list[j]["is"]["longCount"])
                    # name = alpha_list[j]["name"]
                    # dateCreated = alpha_list[j]["dateCreated"]
                    # sharpe = alpha_list[j]["is"]["sharpe"]
                    # fitness = alpha_list[j]["is"]["fitness"]
                    # turnover = alpha_list[j]["is"]["turnover"]
                    # margin = alpha_list[j]["is"]["margin"]
                    # longCount = alpha_list[j]["is"]["longCount"]
                    # shortCount = alpha_list[j]["is"]["shortCount"]
                    # decay = alpha_list[j]["settings"]["decay"]
                    # exp = alpha_list[j]['regular']['code']
                    # count += 1
                    # #if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
                    # print(longCount)
                    # print(shortCount)
                    # if (longCount + shortCount) > 100:
                    #     print(alpha_list[j]["is"]["longCount"])
                    #     if sharpe < -sharpe_th:
                    #         exp = "-%s"%exp
                    #     rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                    #     print(rec)
                    #     if turnover > 0.7:
                    #         rec.append(decay*4)
                    #     elif turnover > 0.6:
                    #         rec.append(decay*3+3)
                    #     elif turnover > 0.5:
                    #         rec.append(decay*3)
                    #     elif turnover > 0.4:
                    #         rec.append(decay*2)
                    #     elif turnover > 0.35:
                    #         rec.append(decay+4)
                    #     elif turnover > 0.3:
                    #         rec.append(decay+2)
                    #     output.append(rec)
            except:
                print("%d finished re-login" % i)
                s = login()

    print("count: %d" % count)
    return output


def get_alphas_posit(start_date, end_date, sharpe_th, fitness_th, region, alpha_num):
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] get_alphas_posit开始处理地区 {region}，目标数量: {alpha_num}")
    s = login()
    output = []
    count = 0

    for i in range(0, alpha_num, 40):
        offset_start = time.time()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 地区 {region} API请求，offset={i}")

        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d" % (i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(
            sharpe_th) + "&settings.region=" + region + "&is.color!=RED" + "&order=-is.sharpe&hidden=false&type!=SUPER"

        urls = [url_e]

        for url in urls:  # 修复缩进，确保这个循环正确执行
            req_start = time.time()
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 发送API请求到: {url[:80]}...")  # 只打印URL前80字符

            try:
                # 添加超时30秒，避免无限挂起；如果需要重试机制，可以用wait_get替换
                response = s.get(url, timeout=30)
                req_time = time.time() - req_start
                print(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API响应状态: {response.status_code}，响应时间: {req_time:.2f}秒")

                if response.status_code != 200:
                    print(
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API错误 {response.status_code}: {response.text[:200]}")  # 只打印前200字符错误信息
                    # 如果非200，尝试重登录
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 尝试重新登录...")
                    s = login()
                    continue  # 重试这个请求

                # 检查Retry-After头，如果有等待时间
                retry_after = response.headers.get("Retry-After", 0)
                if int(retry_after) > 0:
                    wait_time = int(retry_after)
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API要求等待 {wait_time} 秒...")
                    time.sleep(wait_time)

                alpha_list = response.json()["results"]
                offset_count = len(alpha_list)
                print(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] offset={i} 获取到 {offset_count} 个alpha，总计数: {count + offset_count}")

                for j in range(len(alpha_list)):
                    alpha_id = alpha_list[j]["id"]
                    name = alpha_list[j]["name"]
                    dateCreated = alpha_list[j]["dateCreated"]
                    sharpe = alpha_list[j]["is"]["sharpe"]
                    fitness = alpha_list[j]["is"]["fitness"]
                    turnover = alpha_list[j]["is"]["turnover"]
                    margin = alpha_list[j]["is"]["margin"]
                    longCount = alpha_list[j]["is"]["longCount"]
                    shortCount = alpha_list[j]["is"]["shortCount"]
                    decay = alpha_list[j]["settings"]["decay"]
                    exp = alpha_list[j]['regular']['code']
                    count += 1

                    if (longCount + shortCount) > 100:
                        if sharpe < -sharpe_th:
                            exp = "-%s" % exp
                        rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                        print(
                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 添加alpha {alpha_id} (Sharpe: {sharpe:.3f})")

                        if turnover > 0.7:
                            rec.append(decay * 4)
                        elif turnover > 0.6:
                            rec.append(decay * 3 + 3)
                        elif turnover > 0.5:
                            rec.append(decay * 3)
                        elif turnover > 0.4:
                            rec.append(decay * 2)
                        elif turnover > 0.35:
                            rec.append(decay + 4)
                        elif turnover > 0.3:
                            rec.append(decay + 2)
                        output.append(rec)

                offset_time = time.time() - offset_start
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] offset={i} 处理完成，耗时: {offset_time:.2f}秒")

            except requests.exceptions.Timeout:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API请求超时 (30秒)，offset={i}，跳过")
                continue
            except requests.exceptions.RequestException as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API请求异常: {e}，offset={i}")
                # 尝试重登录
                try:
                    s = login()
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 重登录成功，继续")
                except Exception as login_e:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 重登录失败: {login_e}，跳过此offset")
                continue
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] JSON解析或其他错误: {e}")
                # 原有重登录逻辑
                s = login()
                continue

    total_time = time.time() - offset_start  # 注意：这里offset_start是最后一个循环的，实际应从函数开始计算
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] get_alphas_posit for {region} 完成，总计数: {count}，输出: {len(output)}，总耗时约: {total_time:.2f}秒 (估算)")
    return output


def get_alphas_posit_color(start_date, end_date, sharpe_th, fitness_th, region, alpha_num, color):
    s = login()
    output = []
    # 3E large 3C less
    count = 0
    for i in range(0, alpha_num, 20):
        # print(i)
        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d" % (i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(
            sharpe_th) + "&settings.region=" + region + "&is.color=" + color + "&order=is.sharpe&hidden=false&type!=SUPER"
        # url_c = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d" % (i) \
        #         + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date \
        #         + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
        #         + "T00:00:00-04:00&is.fitness%3C-" + str(fitness_th) + "&is.sharpe%3C-" \
        #         + str(sharpe_th) + "&settings.region=" + region + "&order=is.sharpe&hidden=false&type!=SUPER"
        urls = [url_e]
        # if usage != "submit":
        #     urls.append(url_c)
        for url in urls:
            response = s.get(url)
            # print(response.json())
            try:
                alpha_list = response.json()["results"]
                # print(response.json())
                for j in range(len(alpha_list)):
                    alpha_id = alpha_list[j]["id"]
                    name = alpha_list[j]["name"]
                    dateCreated = alpha_list[j]["dateCreated"]
                    sharpe = alpha_list[j]["is"]["sharpe"]
                    fitness = alpha_list[j]["is"]["fitness"]
                    turnover = alpha_list[j]["is"]["turnover"]
                    margin = alpha_list[j]["is"]["margin"]
                    longCount = alpha_list[j]["is"]["longCount"]
                    shortCount = alpha_list[j]["is"]["shortCount"]
                    decay = alpha_list[j]["settings"]["decay"]
                    exp = alpha_list[j]['regular']['code']
                    count += 1
                    # if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
                    if (longCount + shortCount) > 100:
                        if sharpe < -sharpe_th:
                            exp = "-%s" % exp
                        rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                        print(rec)
                        if turnover > 0.7:
                            rec.append(decay * 4)
                        elif turnover > 0.6:
                            rec.append(decay * 3 + 3)
                        elif turnover > 0.5:
                            rec.append(decay * 3)
                        elif turnover > 0.4:
                            rec.append(decay * 2)
                        elif turnover > 0.35:
                            rec.append(decay + 4)
                        elif turnover > 0.3:
                            rec.append(decay + 2)
                        output.append(rec)
            except:
                print("%d finished re-login" % i)
                s = login()

    print("count: %d" % count)
    return output


def transform(next_alpha_recs, region):
    output = []
    for rec in next_alpha_recs:
        decay = rec[-1]
        exp = rec[1]
        output.append([exp, decay])
    output_dict = {region: output}
    return output_dict


def prune(next_alpha_recs, prefix, keep_num):
    # prefix is the datafield prefix, fnd6, mdl175 ...
    # keep_num is the num of top sharpe same-datafield alpha
    output = []
    num_dict = defaultdict(int)
    for rec in next_alpha_recs:
        exp = rec[1]
        field = exp.split(prefix)[-1].split(",")[0]
        sharpe = rec[2]
        if sharpe < 0:
            field = "-%s" % field
        if num_dict[field] < keep_num:
            num_dict[field] += 1
            decay = rec[-1]
            exp = rec[1]
            output.append([exp, decay])
    return output


def first_order_factory(fields, ops_set):
    alpha_set = []
    # for field in fields:
    for field in fields:
        # reverse op does the work
        alpha_set.append(field)
        # alpha_set.append("-%s"%field)
        for op in ops_set:

            if op == "ts_percentage":

                # lpha_set += ts_comp_factory(op, field, "percentage", [0.2, 0.5, 0.8])
                alpha_set += ts_comp_factory(op, field, "percentage", [0.5])

            elif op == "ts_target_tvr_hump":

                # alpha_set += ts_comp_factory(op, field, "factor", [0.2, 0.5, 0.8])
                alpha_set += ts_comp_factory(op, field, "lambda_min=0, lambda_max=1, target_tvr=0.1", [0.5])

            elif op == "ts_decay_exp_window":

                # alpha_set += ts_comp_factory(op, field, "factor", [0.2, 0.5, 0.8])
                alpha_set += ts_comp_factory(op, field, "factor", [0.5])


            elif op == "ts_moment":

                alpha_set += ts_comp_factory(op, field, "k", [2, 3, 4])

            elif op == "ts_entropy":

                # alpha_set += ts_comp_factory(op, field, "buckets", [5, 10, 15, 20])
                alpha_set += ts_comp_factory(op, field, "buckets", [10])

            elif op.startswith("ts_") or op == "inst_tvr":

                alpha_set += ts_factory(op, field)

            elif op.startswith("group_"):

                alpha_set += group_factory(op, field, "usa")

            elif op.startswith("vector"):

                alpha_set += vector_factory(op, field)

            elif op == "signed_power":

                alpha = "%s(%s, 2)" % (op, field)
                alpha_set.append(alpha)

            else:
                alpha = "%s(%s)" % (op, field)
                alpha_set.append(alpha)

    return alpha_set


def first_order_factory_undo(fields):
    alpha_set = []
    # for field in fields:
    for field in fields:
        # reverse op does the work
        alpha_set.append(field)
        alpha_set.append("-%s" % field)

    return alpha_set


def get_group_second_order_factory(first_order, group_ops, region):
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory(group_op, fo, region)
    return second_order


def get_ts_second_order_factory(first_order, ts_ops):
    second_order = []
    for fo in first_order:
        for ts_op in ts_ops:
            second_order += ts_factory(ts_op, fo)
    return second_order


def get_data_fields_csv(filename, prefix):
    '''
    inputs:
    CSV file with header 'field'
    outputs:
    A list of string
    '''
    df = pd.read_csv(filename, header=0, encoding='unicode_escape')
    collection = []
    for _, row in df.iterrows():
        if row['field'].startswith(prefix):
            collection.append(row['field'])

    return collection


def ts_arith_factory(ts_op, arith_op, field):
    first_order = "%s(%s)" % (arith_op, field)
    second_order = ts_factory(ts_op, first_order)
    return second_order


def arith_ts_factory(arith_op, ts_op, field):
    second_order = []
    first_order = ts_factory(ts_op, field)
    for fo in first_order:
        second_order.append("%s(%s)" % (arith_op, fo))
    return second_order


def ts_group_factory(ts_op, group_op, field, region):
    second_order = []
    first_order = group_factory(group_op, field, region)
    for fo in first_order:
        second_order += ts_factory(ts_op, fo)
    return second_order


def group_ts_factory(group_op, ts_op, field, region):
    second_order = []
    first_order = ts_factory(ts_op, field)
    for fo in first_order:
        second_order += group_factory(group_op, fo, region)
    return second_order


def vector_factory(op, field):
    output = []
    vectors = ["cap"]

    for vector in vectors:
        alpha = "%s(%s, %s)" % (op, field, vector)
        output.append(alpha)

    return output


def trade_when_factory(op, field, region):
    output = []
    open_events = ["ts_arg_max(volume, 5) == 0", "ts_corr(close, volume, 20) < 0",
                   "ts_corr(close, volume, 5) < 0", "ts_mean(volume,10)>ts_mean(volume,60)",
                   "group_rank(ts_std_dev(returns,60), sector) > 0.7", "ts_zscore(returns,60) > 2",
                   "ts_arg_min(volume, 5) > 3",
                   "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
                   "ts_arg_max(close, 5) == 0", "ts_arg_max(close, 20) == 0",
                   "ts_corr(close, volume, 5) > 0", "ts_corr(close, volume, 5) > 0.3",
                   "ts_corr(close, volume, 5) > 0.5",
                   "ts_corr(close, volume, 20) > 0", "ts_corr(close, volume, 20) > 0.3",
                   "ts_corr(close, volume, 20) > 0.5",
                   "ts_regression(returns, %s, 5, lag = 0, rettype = 2) > 0" % field,
                   "ts_regression(returns, %s, 20, lag = 0, rettype = 2) > 0" % field,
                   "ts_regression(returns, ts_step(20), 20, lag = 0, rettype = 2) > 0",
                   "ts_regression(returns, ts_step(5), 5, lag = 0, rettype = 2) > 0"]

    exit_events = ["abs(returns) > 0.1", "-1"]

    usa_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8",
                  "rank(vec_avg(mws82_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mws82_sentiment),22) > 0.8", "rank(vec_avg(nws48_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws48_ssc),22) > 0.8", "rank(vec_avg(mws50_ssc)) > 0.8",
                  "ts_rank(vec_avg(mws50_ssc),22) > 0.8",
                  "ts_rank(vec_sum(scl12_alltype_buzzvec),22) > 0.9", "pcr_oi_270 < 1", "pcr_oi_270 > 1", ]

    asi_events = ["rank(vec_avg(mws38_score)) > 0.8", "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    eur_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8",
                  "rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos)) > 0.8",
                  "ts_rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos),22) > 0.8",
                  "rank(vec_avg(mws84_sentiment)) > 0.8", "ts_rank(vec_avg(mws84_sentiment),22) > 0.8",
                  "rank(vec_avg(mws85_sentiment)) > 0.8", "ts_rank(vec_avg(mws85_sentiment),22) > 0.8",
                  "rank(mdl110_analyst_sentiment) > 0.8", "ts_rank(mdl110_analyst_sentiment, 22) > 0.8",
                  "rank(vec_avg(nws3_scores_posnormscr)) > 0.8",
                  "ts_rank(vec_avg(nws3_scores_posnormscr),22) > 0.8",
                  "rank(vec_avg(mws36_sentiment_words_positive)) > 0.8",
                  "ts_rank(vec_avg(mws36_sentiment_words_positive),22) > 0.8"]

    glb_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(vec_avg(nws20_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws20_ssc),22) > 0.8",
                  "vec_avg(nws20_ssc) > 0",
                  "rank(vec_avg(nws20_bee)) > 0.8",
                  "ts_rank(vec_avg(nws20_bee),22) > 0.8",
                  "rank(vec_avg(nws20_qmb)) > 0.8",
                  "ts_rank(vec_avg(nws20_qmb),22) > 0.8"]

    chn_events = ["rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform),22) > 0.8"]

    kor_events = ["rank(vec_avg(mdl110_analyst_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mdl110_analyst_sentiment),22) > 0.8",
                  "rank(vec_avg(mws38_score)) > 0.8",
                  "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    twn_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(rp_ess_business) > 0.8",
                  "ts_rank(rp_ess_business,22) > 0.8"]

    for oe in open_events:
        for ee in exit_events:
            alpha = "%s(%s, %s, %s)" % (op, oe, field, ee)
            output.append(alpha)
    return output


def ts_factory(op, field):
    output = []
    # days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 120, 240]

    for day in days:
        alpha = "%s(%s, %d)" % (op, field, day)
        output.append(alpha)

    return output


def ts_comp_factory(op, field, factor, paras):
    output = []
    if factor == 'lambda_min=0, lambda_max=1, target_tvr=0.1':
        alpha = "%s(%s, %s)" % (op, field, factor)
        output.append(alpha)
    else:
        # l1, l2 = [3, 5, 10, 20, 60, 120, 240], paras
        l1, l2 = [5, 22, 66, 240], paras
        comb = list(product(l1, l2))
        for day, para in comb:

            if type(para) == float:
                alpha = "%s(%s, %d, %s=%.1f)" % (op, field, day, factor, para)
            elif type(para) == int:
                alpha = "%s(%s, %d, %s=%d)" % (op, field, day, factor, para)

            output.append(alpha)

    return output


def twin_field_factory(op, field, fields):
    output = []
    # days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 240]
    outset = list(set(fields) - set([field]))

    for day in days:
        for counterpart in outset:
            alpha = "%s(%s, %s, %d)" % (op, field, counterpart, day)
            output.append(alpha)

    return output


def group_factory(op, field, region):
    output = []
    vectors = ["cap"]

    chn_group_13 = ['pv13_h_min2_sector', 'pv13_di_6l', 'pv13_rcsed_6l', 'pv13_di_5l', 'pv13_di_4l',
                    'pv13_di_3l', 'pv13_di_2l', 'pv13_di_1l', 'pv13_parent', 'pv13_level']

    chn_group_1 = ['sta1_top3000c30', 'sta1_top3000c20', 'sta1_top3000c10', 'sta1_top3000c2', 'sta1_top3000c5']

    chn_group_2 = ['sta2_top3000_fact4_c10', 'sta2_top2000_fact4_c50', 'sta2_top3000_fact3_c20']

    hkg_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_20_minvol_1m_sector',
                    'pv13_2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_1l_scibr', 'pv13_3l_scibr',
                    'pv13_2l_scibr', 'pv13_4l_scibr', 'pv13_5l_scibr']

    hkg_group_1 = ['sta1_allc50', 'sta1_allc5', 'sta1_allxjp_513_c20', 'sta1_top2000xjp_513_c5']

    hkg_group_2 = ['sta2_all_xjp_513_all_fact4_c10', 'sta2_top2000_xjp_513_top2000_fact3_c10',
                   'sta2_allfactor_xjp_513_13', 'sta2_top2000_xjp_513_top2000_fact3_c20']

    twn_group_13 = ['pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                    'pv13_5_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector']

    twn_group_1 = ['sta1_allc50', 'sta1_allxjp_513_c50', 'sta1_allxjp_513_c20', 'sta1_allxjp_513_c2',
                   'sta1_allc20', 'sta1_allxjp_513_c5', 'sta1_allxjp_513_c10', 'sta1_allc2', 'sta1_allc5']

    twn_group_2 = ['sta2_allfactor_xjp_513_0', 'sta2_all_xjp_513_all_fact3_c20',
                   'sta2_all_xjp_513_all_fact4_c20', 'sta2_all_xjp_513_all_fact4_c50']

    usa_group_13 = ['pv13_h_min2_3000_sector', 'pv13_r2_min20_3000_sector', 'pv13_r2_min2_3000_sector',
                    'pv13_r2_min2_3000_sector', 'pv13_h_min2_focused_pureplay_3000_sector']

    usa_group_1 = ['sta1_top3000c50', 'sta1_allc20']
    usa_group_other = ['oth455_competitor_n2v_p10_q200_w1_kmeans_cluster_10',
                       'oth455_competitor_n2v_p10_q200_w1_kmeans_cluster_20',
                       'oth455_competitor_n2v_p10_q200_w4_pca_fact1_cluster_5',
                       'oth455_competitor_n2v_p10_q200_w4_pca_fact1_value',
                       'oth455_competitor_n2v_p10_q200_w4_pca_fact2_cluster_10',
                       'oth455_competitor_n2v_p10_q50_w3_pca_fact1_cluster_10',
                       'oth455_competitor_n2v_p10_q50_w3_pca_fact1_cluster_20',
                       'oth455_competitor_n2v_p50_q50_w2_pca_fact2_value',
                       'oth455_competitor_n2v_p50_q50_w2_pca_fact3_cluster_10',
                       'oth455_competitor_n2v_p50_q50_w2_pca_fact3_cluster_20',
                       'oth455_partner_n2v_p10_q50_w3_pca_fact1_cluster_5',
                       'oth455_partner_n2v_p10_q50_w3_pca_fact1_value',
                       'oth455_partner_roam_w1_kmeans_cluster_20', 'oth455_partner_roam_w1_kmeans_cluster_5',
                       'oth455_relation_n2v_p10_q200_w1_pca_fact1_value',
                       'oth455_relation_n2v_p10_q200_w1_pca_fact2_cluster_10',
                       'oth455_relation_n2v_p50_q200_w2_pca_fact3_cluster_5',
                       'oth455_relation_n2v_p50_q200_w2_pca_fact3_value',
                       'oth455_relation_n2v_p50_q50_w5_pca_fact3_value', 'oth455_relation_roam_w1_kmeans_cluster_10',
                       'oth455_relation_roam_w5_pca_fact3_cluster_5', 'oth455_relation_roam_w5_pca_fact3_value']
    # usa_group_1 = ['sta1_top3000c50','sta1_allc20','sta1_allc10','sta1_top3000c20','sta1_allc5']

    usa_group_2 = ['sta2_top3000_fact3_c50', 'sta2_top3000_fact4_c20', 'sta2_top3000_fact4_c10']

    usa_group_6 = ['mdl10_group_name']

    asi_group_13 = ['pv13_20_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_5_minvol_1m_sector']

    asi_group_1 = ['sta1_allc50', 'sta1_allc10', 'sta1_minvol1mc50', 'sta1_minvol1mc20',
                   'sta1_minvol1m_normc20', 'sta1_minvol1m_normc50']

    jpn_group_1 = ['sta1_alljpn_513_c5', 'sta1_alljpn_513_c50', 'sta1_alljpn_513_c2', 'sta1_alljpn_513_c20']

    jpn_group_2 = ['sta2_top2000_jpn_513_top2000_fact3_c20', 'sta2_all_jpn_513_all_fact1_c5',
                   'sta2_allfactor_jpn_513_9', 'sta2_all_jpn_513_all_fact1_c10']

    jpn_group_13 = ['pv13_2_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                    'pv13_10_f3_g2_minvol_1m_sector', 'pv13_all_delay_1_parent', 'pv13_all_delay_1_level']

    kor_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector']

    kor_group_1 = ['sta1_allc20', 'sta1_allc50', 'sta1_allc2', 'sta1_allc10', 'sta1_minvol1mc50',
                   'sta1_allxjp_513_c10', 'sta1_top2000xjp_513_c50']

    kor_group_2 = ['sta2_all_xjp_513_all_fact1_c50', 'sta2_top2000_xjp_513_top2000_fact2_c50',
                   'sta2_all_xjp_513_all_fact4_c50', 'sta2_all_xjp_513_all_fact4_c5']

    # eur_group_13 = ['pv13_5_sector', 'pv13_2_sector', 'pv13_v3_3l_scibr', 'pv13_v3_2l_scibr', 'pv13_2l_scibr',
    #                 'pv13_52_sector', 'pv13_v3_6l_scibr', 'pv13_v3_4l_scibr', 'pv13_v3_1l_scibr']

    eur_group_13 = ['pv13_22_800_sector', 'pv13_2_sector', 'pv13_5_sector']

    # eur_group_1 = ['sta1_allc10', 'sta1_allc2', 'sta1_top1200c2', 'sta1_allc20', 'sta1_top1200c10']
    eur_group_1 = ['sta1_top800c50', 'sta1_top800c2', 'sta1_top400c2', 'sta1_allc20', 'sta1_top1200c10']

    # eur_group_2 = ['sta2_top1200_fact3_c50','sta2_top1200_fact3_c20','sta2_top1200_fact4_c50']
    eur_group_2 = ['sta2_top400_fact2_c20', 'sta2_top400_fact4_c2', 'sta2_top800_fact1_c5']

    glb_group_13 = ["pv13_10_f2_g3_sector", "pv13_2_f3_g2_sector", "pv13_2_sector", "pv13_52_all_delay_1_sector"]

    glb_group_1 = ['sta1_allc20', 'sta1_allc10', 'sta1_allc50', 'sta1_allc5']

    glb_group_2 = ['sta2_all_fact4_c50', 'sta2_all_fact4_c20', 'sta2_all_fact3_c20', 'sta2_all_fact4_c10']

    glb_group_13 = ['pv13_2_sector', 'pv13_10_sector', 'pv13_3l_scibr', 'pv13_2l_scibr', 'pv13_1l_scibr',
                    'pv13_52_minvol_1m_all_delay_1_sector', 'pv13_52_minvol_1m_sector', 'pv13_52_minvol_1m_sector']

    amr_group_13 = ['pv13_4l_scibr', 'pv13_1l_scibr', 'pv13_hierarchy_min51_f1_sector',
                    'pv13_hierarchy_min2_600_sector', 'pv13_r2_min2_sector', 'pv13_h_min20_600_sector']

    bps_group = "bucket(rank(fnd28_value_05480), range='0.1, 1, 0.1')"
    pb_group = "bucket(rank(close/fnd28_value_05480), range='0.1, 1, 0.1')"
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    # asset_group = "bucket(rank(assets),range='0.1, 1, 0.1')"
    sector_cap_group = "bucket(group_rank(cap, sector),range='0.1, 1, 0.1')"
    # sector_asset_group = "bucket(group_rank(assets, sector),range='0.1, 1, 0.1')"

    vol_group = "bucket(rank(ts_std_dev(returns,20)),range = '0.1, 1, 0.1')"

    liquidity_group = "bucket(rank(close*volume),range = '0.1, 1, 0.1')"

    # groups = ["market","sector", "industry", "subindustry",
    #           pb_group, bps_group, cap_group, asset_group, sector_cap_group, sector_asset_group, vol_group, liquidity_group]
    groups = ["market", "sector", "industry", "subindustry"]

    if region == "CHN":
        # groups += chn_group_13 + chn_group_1 + chn_group_2
        groups += chn_group_1
    if region == "TWN":
        groups += twn_group_13 + twn_group_1 + twn_group_2
    if region == "ASI":
        groups += asi_group_13 + asi_group_1
        # groups += usa_group_other
    if region == "USA":
        # groups += usa_group_other
        # groups += usa_group_1
        groups += usa_group_13 + usa_group_1 + usa_group_2
    if region == "HKG":
        groups += hkg_group_13 + hkg_group_1 + hkg_group_2
    if region == "KOR":
        groups += kor_group_13 + kor_group_1 + kor_group_2
    if region == "EUR":
        groups += eur_group_13 + eur_group_1 + eur_group_2
        # groups += usa_group_other
    if region == "GLB":
        groups += glb_group_13 + glb_group_1 + glb_group_2
        # groups += usa_group_other
    if region == "AMR":
        groups += amr_group_13
    if region == "JPN":
        groups += jpn_group_1 + jpn_group_2 + jpn_group_13

    for group in groups:
        if op.startswith("group_vector"):
            for vector in vectors:
                alpha = "%s(%s,%s,densify(%s))" % (op, field, vector, group)
                output.append(alpha)
        elif op.startswith("group_percentage"):
            alpha = "%s(%s,densify(%s),percentage=0.5)" % (op, field, group)
            output.append(alpha)
        else:
            alpha = "%s(%s,densify(%s))" % (op, field, group)
            output.append(alpha)

    return output


def filter_alpha(tracker, keys, return_type):
    if return_type == 'tracker':
        def contains_keyword(sublist, keys):
            return any(key in " ".join(map(str, sublist)) for key in keys)

        tracker['next'] = [item for item in tracker['next'] if not contains_keyword(item, keys)]
        tracker['decay'] = [item for item in tracker['decay'] if not contains_keyword(item, keys)]
        return tracker
    elif return_type == 'expression':
        original_list = tracker['next'] + tracker['decay']
        expression_list = [item[1] for item in original_list]
        result = [expr for expr in expression_list if not any(key in expr for key in keys)]
        return result