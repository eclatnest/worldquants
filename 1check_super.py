from datetime import datetime, timedelta
import random
import requests
import pandas as pd
import logging
import time
import warnings
from typing import Optional, Tuple
from typing import Tuple, Dict, List
from typing import Union, List, Tuple
from concurrent.futures import ThreadPoolExecutor
import pickle
from collections import defaultdict
import numpy as np
from pathlib import Path
import json
import os


def sign_in(username, password, max_retries: int = 5, base_delay: float = 2.0):
    """
    登录到 WorldQuant BRAIN 平台，带429错误重试机制
    
    Args:
        username: 用户名
        password: 密码
        max_retries: 最大重试次数，默认5次
        base_delay: 基础延迟时间（秒），默认2秒，使用指数退避
    
    Returns:
        Session对象（成功）或None（失败）
    """
    s = requests.Session()
    s.auth = (username, password)
    
    for attempt in range(max_retries):
        try:
            response = s.post('https://api.worldquantbrain.com/authentication', timeout=30)
            
            # 处理429限流错误
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    wait_time = float(retry_after)
                else:
                    # 使用指数退避：2秒、4秒、8秒、16秒、32秒
                    wait_time = base_delay * (2 ** attempt)
                
                if attempt < max_retries - 1:
                    logging.warning(f"Login rate limited (429), waiting {wait_time:.1f}s before retry ({attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"Login failed after {max_retries} retries: 429 Too Many Requests")
                    return None
            
            # 处理其他HTTP错误
            if response.status_code >= 400:
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt)
                    logging.warning(f"Login failed with status {response.status_code}, retrying in {wait_time:.1f}s ({attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"Login failed after {max_retries} retries: HTTP {response.status_code}")
                    return None
            
            # 成功登录
            response.raise_for_status()
            logging.info("Successfully signed in")
            return s
            
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt)
                logging.warning(f"Login timeout, retrying in {wait_time:.1f}s ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                logging.error(f"Login failed after {max_retries} retries: Timeout")
                return None
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt)
                logging.warning(f"Login request failed: {str(e)[:80]}, retrying in {wait_time:.1f}s ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                logging.error(f"Login failed after {max_retries} retries: {e}")
                return None
    
    return None


def save_obj(obj: object, name: str) -> None:
    """
    保存对象到文件中，以 pickle 格式序列化。
    Args:
        obj (object): 需要保存的对象。
        name (str): 文件名（不包含扩展名），保存的文件将以 '.pickle' 为扩展名。
    Returns:
        None: 此函数无返回值。
    Raises:
        pickle.PickleError: 如果序列化过程中发生错误。
        IOError: 如果文件写入过程中发生 I/O 错误。
    """
    with open(name + '.pickle', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name: str) -> object:
    """
    加载指定名称的 pickle 文件并返回其内容。
    此函数会打开一个以 `.pickle` 为扩展名的文件，并使用 `pickle` 模块加载其内容。
    Args:
        name (str): 不带扩展名的文件名称。
    Returns:
        object: 从 pickle 文件中加载的 Python 对象。
    Raises:
        FileNotFoundError: 如果指定的文件不存在。
        pickle.UnpicklingError: 如果文件内容无法被正确反序列化。
    """
    with open(name + '.pickle', 'rb') as f:
        return pickle.load(f)


def wait_get(url: str, max_retries: int = 10) -> "requests.Response":
    """
    发送带有重试机制的 GET 请求，直到成功或达到最大重试次数。
    此函数会根据服务器返回的 `Retry-After` 头信息进行等待，并在遇到 401 状态码时重新初始化配置。

    Args:
        url (str): 目标 URL。
        max_retries (int, optional): 最大重试次数，默认为 10。

    Returns:
        Response: 请求的响应对象。
    """
    retries = 0
    while retries < max_retries:
        while True:
            simulation_progress = sess.get(url)
            if simulation_progress.headers.get("Retry-After", 0) == 0:
                break
            time.sleep(float(simulation_progress.headers["Retry-After"]))
        if simulation_progress.status_code < 400:
            break
        else:
            time.sleep(2 ** retries)
            retries += 1
    return simulation_progress


def _get_alpha_pnl(alpha_id: str) -> pd.DataFrame:
    """
    获取指定 alpha 的 PnL数据，并返回一个包含日期和 PnL 的 DataFrame。
    此函数通过调用 WorldQuant Brain API 获取指定 alpha 的 PnL 数据，
    并将其转换为 pandas DataFrame 格式，方便后续数据处理。
    Args:
        alpha_id (str): Alpha 的唯一标识符。
    Returns:
        pd.DataFrame: 包含日期和对应 PnL 数据的 DataFrame，列名为 'Date' 和 alpha_id。
    """
    pnl = wait_get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/recordsets/pnl").json()
    df = pd.DataFrame(pnl['records'], columns=[item['name'] for item in pnl['schema']['properties']])
    df = df.rename(columns={'date': 'Date', 'pnl': alpha_id})
    df = df[['Date', alpha_id]]
    return df


def get_alpha_pnls(
        alphas: list[dict],
        alpha_pnls: Optional[pd.DataFrame] = None,
        alpha_ids: Optional[dict[str, list]] = None
) -> Tuple[dict[str, list], pd.DataFrame]:
    """
    获取 alpha 的 PnL 数据，并按区域分类 alpha 的 ID。
    Args:
        alphas (list[dict]): 包含 alpha 信息的列表，每个元素是一个字典，包含 alpha 的 ID 和设置等信息。
        alpha_pnls (Optional[pd.DataFrame], 可选): 已有的 alpha PnL 数据，默认为空的 DataFrame。
        alpha_ids (Optional[dict[str, list]], 可选): 按区域分类的 alpha ID 字典，默认为空字典。
    Returns:
        Tuple[dict[str, list], pd.DataFrame]:
            - 按区域分类的 alpha ID 字典。
            - 包含所有 alpha 的 PnL 数据的 DataFrame。
    """
    if alpha_ids is None:
        alpha_ids = defaultdict(list)
    if alpha_pnls is None:
        alpha_pnls = pd.DataFrame()

    # 验证alphas数据结构并过滤有效数据
    valid_alphas = []
    for item in alphas:
        if not isinstance(item, dict):
            print(f"   ⚠️  跳过无效数据（非字典类型）: {type(item)}")
            continue

        if 'id' not in item:
            print(f"   ⚠️  跳过无效数据（缺少id字段）: {item}")
            continue

        if 'settings' not in item or 'region' not in item.get('settings', {}):
            print(f"   ⚠️  跳过无效数据（缺少settings.region）: {item.get('id', 'unknown')}")
            continue

        valid_alphas.append(item)

    if not valid_alphas:
        print(f"   ⚠️  没有有效的alpha数据")
        return alpha_ids, alpha_pnls

    new_alphas = [item for item in valid_alphas if item['id'] not in alpha_pnls.columns]
    if not new_alphas:
        return alpha_ids, alpha_pnls

    # 按区域分类alpha ID
    for item_alpha in new_alphas:
        try:
            alpha_ids[item_alpha['settings']['region']].append(item_alpha['id'])
        except Exception as e:
            print(f"   ⚠️  [get_alpha_pnls] 分类alpha时出错，跳过 {item_alpha.get('id', 'unknown')}: {type(e).__name__}")
            continue

    # 获取PnL数据（带错误处理）
    def safe_get_pnl(alpha_id):
        try:
            return _get_alpha_pnl(alpha_id).set_index('Date')
        except Exception as e:
            print(f"   ⚠️  [get_alpha_pnls] 获取 {alpha_id} 的PnL失败，跳过: {type(e).__name__} - {str(e)[:50]}")
            return None

    fetch_pnl_func = safe_get_pnl
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_pnl_func, [item['id'] for item in new_alphas])

    # 过滤掉None结果
    valid_results = [r for r in results if r is not None]
    if valid_results:
        alpha_pnls = pd.concat([alpha_pnls] + valid_results, axis=1)
        alpha_pnls.sort_index(inplace=True)

    return alpha_ids, alpha_pnls


def get_os_alphas(limit: int = 100, get_first: bool = False) -> List[Dict]:
    """
    获取OS阶段的alpha列表。
    此函数通过调用WorldQuant Brain API获取用户的alpha列表，支持分页获取，并可以选择只获取第一个结果。
    Args:
        limit (int, optional): 每次请求获取的alpha数量限制。默认为100。
        get_first (bool, optional): 是否只获取第一次请求的alpha结果。如果为True，则只请求一次。默认为False。
    Returns:
        List[Dict]: 包含alpha信息的字典列表，每个字典表示一个alpha。
    """
    fetched_alphas = []
    offset = 0
    retries = 0
    total_alphas = 100

    try:
        while len(fetched_alphas) < total_alphas:
            print(f"Fetching alphas from offset {offset} to {offset + limit}")
            url = f"https://api.worldquantbrain.com/users/self/alphas?stage=OS&limit={limit}&offset={offset}&order=-dateSubmitted"

            try:
                res = wait_get(url).json()
            except Exception as e:
                print(f"   ⚠️  [get_os_alphas] API请求失败: {type(e).__name__} - {str(e)[:100]}")
                break

            # 检查响应格式
            if not isinstance(res, dict):
                print(f"   ⚠️  [get_os_alphas] API响应不是字典格式: {type(res)}")
                break

            # 检查是否有错误信息
            if 'error' in res or 'message' in res:
                error_msg = res.get('error', res.get('message', 'Unknown error'))
                print(f"   ⚠️  [get_os_alphas] API返回错误: {error_msg}")
                break

            # 检查是否有 'count' 字段（只在第一次请求时）
            if offset == 0:
                if 'count' not in res:
                    print(f"   ⚠️  [get_os_alphas] API响应缺少'count'字段，使用实际获取的数量")
                    # 如果没有count字段，尝试从results长度推断，或使用默认值
                    if 'results' in res and isinstance(res['results'], list):
                        total_alphas = len(res['results'])
                        if len(res['results']) < limit:
                            # 如果第一次获取的数量小于limit，说明已经获取完了
                            fetched_alphas.extend(res['results'])
                            break
                    else:
                        total_alphas = limit * 10  # 设置一个合理的上限
                else:
                    total_alphas = res['count']

            # 检查是否有 'results' 字段
            if 'results' not in res:
                print(f"   ⚠️  [get_os_alphas] API响应缺少'results'字段")
                print(f"   响应keys: {list(res.keys())[:10]}")
                break

            alphas = res["results"]
            if not isinstance(alphas, list):
                print(f"   ⚠️  [get_os_alphas] 'results'不是列表格式: {type(alphas)}")
                break

            fetched_alphas.extend(alphas)

            if len(alphas) < limit:
                break

            offset += limit
            if get_first:
                break

    except Exception as e:
        print(f"   ⚠️  [get_os_alphas] 处理过程中出错: {type(e).__name__} - {str(e)[:100]}")

    # 如果获取到了数据，返回数据；否则返回空列表
    if fetched_alphas:
        return fetched_alphas[:total_alphas] if total_alphas > 0 else fetched_alphas
    else:
        print(f"   ⚠️  [get_os_alphas] 未获取到任何alpha数据")
        return []


def calc_self_corr(
        alpha_id: str,
        os_alpha_rets: pd.DataFrame | None = None,
        os_alpha_ids: dict[str, str] | None = None,
        alpha_result: dict | None = None,
        return_alpha_pnls: bool = False,
        alpha_pnls: pd.DataFrame | None = None
) -> float | tuple[float, pd.DataFrame]:
    """
    计算指定 alpha 与其他 alpha 的最大自相关性。
    Args:
        alpha_id (str): 目标 alpha 的唯一标识符。
        os_alpha_rets (pd.DataFrame | None, optional): 其他 alpha 的收益率数据，默认为 None。
        os_alpha_ids (dict[str, str] | None, optional): 其他 alpha 的标识符映射，默认为 None。
        alpha_result (dict | None, optional): 目标 alpha 的详细信息，默认为 None。
        return_alpha_pnls (bool, optional): 是否返回 alpha 的 PnL 数据，默认为 False。
        alpha_pnls (pd.DataFrame | None, optional): 目标 alpha 的 PnL 数据，默认为 None。
    Returns:
        float | tuple[float, pd.DataFrame]: 如果 `return_alpha_pnls` 为 False，返回最大自相关性值；
            如果 `return_alpha_pnls` 为 True，返回包含最大自相关性值和 alpha PnL 数据的元组。
    """
    try:
        if alpha_result is None:
            print(f"   [calc_self_corr] 获取 alpha {alpha_id} 的详细信息...")
            alpha_result = wait_get(f"https://api.worldquantbrain.com/alphas/{alpha_id}").json()

        # 验证alpha_result数据结构
        if not isinstance(alpha_result, dict):
            print(f"   ❌ alpha_result不是字典类型: {type(alpha_result)}")
            return 0.0

        if 'id' not in alpha_result:
            print(f"   ❌ alpha_result缺少id字段")
            print(f"   alpha_result keys: {alpha_result.keys()}")
            return 0.0

        if 'settings' not in alpha_result or 'region' not in alpha_result.get('settings', {}):
            print(f"   ❌ alpha_result缺少settings.region字段")
            return 0.0

        if alpha_pnls is not None:
            if len(alpha_pnls) == 0:
                alpha_pnls = None

        if alpha_pnls is None:
            try:
                print(f"   [calc_self_corr] 获取 alpha {alpha_id} 的PnL数据...")
                _, alpha_pnls = get_alpha_pnls([alpha_result])
                if alpha_id not in alpha_pnls.columns:
                    print(f"   ⚠️  [calc_self_corr] PnL数据中找不到 {alpha_id}")
                    return 0.0 if not return_alpha_pnls else (0.0, pd.DataFrame())
                alpha_pnls = alpha_pnls[alpha_id]
            except Exception as e:
                print(f"   ⚠️  [calc_self_corr] 获取 {alpha_id} 的PnL数据失败: {type(e).__name__} - {str(e)[:50]}")
                return 0.0 if not return_alpha_pnls else (0.0, pd.DataFrame())

        alpha_rets = alpha_pnls - alpha_pnls.ffill().shift(1)
        alpha_rets = alpha_rets[
            pd.to_datetime(alpha_rets.index) > pd.to_datetime(alpha_rets.index).max() - pd.DateOffset(years=4)]

        # 获取当前区域的其他alpha收益率数据
        region = alpha_result['settings']['region']
        if region not in os_alpha_ids or len(os_alpha_ids[region]) == 0:
            print(f"   ⚠️  [calc_self_corr] 区域 {region} 没有可用的OS alpha数据")
            return 0.0 if not return_alpha_pnls else (0.0, alpha_pnls)

        region_os_rets = os_alpha_rets[os_alpha_ids[region]]

        # 过滤掉标准差为0或NaN的alpha（避免除以零警告）
        valid_cols = region_os_rets.columns[
            (region_os_rets.std() > 1e-10) & (region_os_rets.std().notna())
            ]

        # 检查目标alpha的标准差是否有效
        if len(alpha_rets.dropna()) > 0 and alpha_rets.std() > 1e-10:
            # 只计算与有效alpha的相关性
            if len(valid_cols) > 0:
                region_os_rets_valid = region_os_rets[valid_cols]

                # 使用警告上下文管理器抑制预期的除以零警告
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=RuntimeWarning, message='invalid value encountered')
                    corr_results = region_os_rets_valid.corrwith(alpha_rets)
                    corr_results = corr_results.dropna()  # 移除NaN结果

                    if len(corr_results) > 0:
                        corr_results.sort_values(ascending=False).round(4).to_csv(
                            str(cfg.data_path / 'os_alpha_corr.csv'))
                        self_corr = corr_results.max()
                    else:
                        self_corr = 0
            else:
                self_corr = 0
        else:
            # 目标alpha标准差无效，无法计算相关性
            self_corr = 0

        if np.isnan(self_corr):
            self_corr = 0

        if return_alpha_pnls:
            return self_corr, alpha_pnls
        else:
            return self_corr

    except KeyError as e:
        print(f"   ❌ [calc_self_corr] KeyError for {alpha_id}: {e}")
        print(f"   alpha_result type: {type(alpha_result)}")
        if isinstance(alpha_result, dict):
            print(f"   alpha_result keys: {list(alpha_result.keys())[:10]}")  # 只显示前10个key
        return 0.0

    except Exception as e:
        print(f"   ❌ [calc_self_corr] Error for {alpha_id}: {type(e).__name__} - {str(e)[:100]}")
        return 0.0


def download_data(flag_increment=True):
    """
    下载数据并保存到指定路径。
    此函数会检查数据是否已经存在，如果不存在，则从 API 下载数据并保存到指定路径。
    Args:
        flag_increment (bool): 是否使用增量下载，默认为 True。
    """
    if flag_increment:
        try:
            os_alpha_ids = load_obj(str(cfg.data_path / 'os_alpha_ids'))
            os_alpha_pnls = load_obj(str(cfg.data_path / 'os_alpha_pnls'))
            ppac_alpha_ids = load_obj(str(cfg.data_path / 'ppac_alpha_ids'))
            exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
        except Exception as e:
            logging.error(f"Failed to load existing data: {e}")
            os_alpha_ids = None
            os_alpha_pnls = None
            exist_alpha = []
            ppac_alpha_ids = []
    else:
        os_alpha_ids = None
        os_alpha_pnls = None
        exist_alpha = []
        ppac_alpha_ids = []

    if os_alpha_ids is None:
        alphas = get_os_alphas(limit=100, get_first=False)
    else:
        alphas = get_os_alphas(limit=30, get_first=True)

    alphas = [item for item in alphas if item['id'] not in exist_alpha]
    ppac_alpha_ids += [item['id'] for item in alphas for item_match in item['classifications'] if
                       item_match['name'] == 'Power Pool Alpha']

    os_alpha_ids, os_alpha_pnls = get_alpha_pnls(alphas, alpha_pnls=os_alpha_pnls, alpha_ids=os_alpha_ids)
    save_obj(os_alpha_ids, str(cfg.data_path / 'os_alpha_ids'))
    save_obj(os_alpha_pnls, str(cfg.data_path / 'os_alpha_pnls'))
    save_obj(ppac_alpha_ids, str(cfg.data_path / 'ppac_alpha_ids'))
    print(f'新下载的alpha数量: {len(alphas)}, 目前总共alpha数量: {os_alpha_pnls.shape[1]}')


def load_data(tag=None):
    """
    加载数据。
    此函数会检查数据是否已经存在，如果不存在，则从 API 下载数据并保存到指定路径。
    Args:
        tag (str): 数据标记，默认为 None。
    """
    os_alpha_ids = load_obj(str(cfg.data_path / 'os_alpha_ids'))
    os_alpha_pnls = load_obj(str(cfg.data_path / 'os_alpha_pnls'))
    ppac_alpha_ids = load_obj(str(cfg.data_path / 'ppac_alpha_ids'))
    if tag == 'PPAC':
        for item in os_alpha_ids:
            os_alpha_ids[item] = [alpha for alpha in os_alpha_ids[item] if alpha in ppac_alpha_ids]
    elif tag == 'SelfCorr':
        for item in os_alpha_ids:
            os_alpha_ids[item] = [alpha for alpha in os_alpha_ids[item] if alpha not in ppac_alpha_ids]
    else:
        os_alpha_ids = os_alpha_ids
    exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
    os_alpha_pnls = os_alpha_pnls[exist_alpha]
    os_alpha_rets = os_alpha_pnls - os_alpha_pnls.ffill().shift(1)
    os_alpha_rets = os_alpha_rets[
        pd.to_datetime(os_alpha_rets.index) > pd.to_datetime(os_alpha_rets.index).max() - pd.DateOffset(years=4)]
    return os_alpha_ids, os_alpha_rets


def get_simulation_result_json(s, alpha_id, max_retries: int = 5, base_delay: float = 2.0):
    """
    获取alpha的模拟结果JSON，带错误处理和限流重试
    """
    for attempt in range(max_retries):
        try:
            response = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id, timeout=30)

            # 429 限流处理
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    wait_time = float(retry_after)
                else:
                    wait_time = base_delay * (2 ** attempt)
                print(f"   ⏳  [get_simulation_result_json] {alpha_id} 限流，等待 {wait_time:.1f} 秒后重试 ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue

            # 401/403 重新登录
            if response.status_code in (401, 403):
                print(f"   🔐  [get_simulation_result_json] {alpha_id} 认证失败，尝试重新登录...")
                new_session = sign_in(cfg.username, cfg.password)
                if new_session is None:
                    print(f"   ❌  [get_simulation_result_json] {alpha_id} 重新登录失败，放弃")
                    break
                s.cookies = new_session.cookies
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            wait_time = base_delay * (2 ** attempt)
            print(f"   ⏰  [get_simulation_result_json] {alpha_id} 请求超时，{wait_time:.1f} 秒后重试 ({attempt + 1}/{max_retries})")
            time.sleep(wait_time)

        except requests.exceptions.RequestException as e:
            wait_time = base_delay * (2 ** attempt)
            print(f"   ⚠️  [get_simulation_result_json] {alpha_id} 网络异常: {str(e)[:80]}，{wait_time:.1f} 秒后重试 ({attempt + 1}/{max_retries})")
            time.sleep(wait_time)

        except Exception as e:
            print(f"   ⚠️  [get_simulation_result_json] 获取 {alpha_id} 失败: {type(e).__name__} - {str(e)[:80]}")
            break

    print(f"   ❌  [get_simulation_result_json] {alpha_id} 多次重试后仍失败")
    return {}  # 返回空字典而不是None，避免后续判断出错


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


def set_alpha_properties(
        s,
        alpha_id,
        name: str = None,
        color: str = None,
        selection_desc: str = "311111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111",
        combo_desc: str = "322222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222",
        description: str = 'None',
        tags=['c1'],
):
    """
    Function changes alpha's description parameters
    """

    if tags is None:
        tags = ["c2"]
    params = {
        "color": color,
        "name": name,
        "tags": tags,
        "category": None,
        "regular": {"description": description},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }

    # 处理Retry-After头
    while True:
        response = s.patch(
            "https://api.worldquantbrain.com/alphas/" + alpha_id, json=params
        )
        if "retry-after" in response.headers:
            time.sleep(float(response.headers["Retry-After"]))
        else:
            break

    # 检查响应状态
    if response.status_code >= 400:
        raise Exception(f"API错误 {response.status_code}: {response.text[:200]}")

    return response


def check_submission(alpha_bag, gold_bag, start):
    depot = []
    s = sign_in(cfg.username, cfg.password)
    for idx, g in enumerate(alpha_bag):
        if idx < start:
            continue
        if idx % 5 == 0:
            print(idx)
        if idx % 200 == 0:
            s = sign_in(cfg.username, cfg.password)
        # print(idx)
        pc = get_check_submission(s, g)
        if pc == "sleep":
            time.sleep(100)
            s = sign_in(cfg.username, cfg.password)
            alpha_bag.append(g)
        elif pc != pc:
            # pc is nan
            print("check self-corrlation error")
            time.sleep(100)
            alpha_bag.append(g)
        elif pc == "fail":
            continue
        elif pc == "error":
            depot.append(g)
        else:
            # print('g')
            # print(g)
            gold_bag.append((g, pc))
    # print('depot')
    # print(depot)
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
                print(f"   ⚠️  {alpha_id}: logged out")
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
                print(f"   ✅ {alpha_id}: PC={pc}")
                return pc
            else:
                print(f"   ❌ {alpha_id}: 检查失败 (PC={pc})")
                return "fail"

        except KeyError as e:
            # 数据结构错误
            print(f"   ⚠️  catch {alpha_id} (尝试 {attempt + 1}/{max_retries}): 字段缺失 {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 指数退避：1秒、2秒、4秒
                print(f"   🔄 等待 {2 ** attempt} 秒后重试...")
            else:
                print(f"   ❌ {alpha_id}: 重试失败，返回error")
                return "error"

        except ValueError as e:
            # PROD_CORRELATION字段不存在
            print(f"   ⚠️  catch {alpha_id} (尝试 {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                print(f"   🔄 等待 {2 ** attempt} 秒后重试...")
            else:
                print(f"   ❌ {alpha_id}: 重试失败，返回error")
                return "error"

        except Exception as e:
            # 其他未知错误
            error_type = type(e).__name__
            print(f"   ⚠️  catch {alpha_id} (尝试 {attempt + 1}/{max_retries}): {error_type} - {str(e)[:50]}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                print(f"   🔄 等待 {2 ** attempt} 秒后重试...")
            else:
                print(f"   ❌ {alpha_id}: 重试失败，返回error")
                return "error"

    # 理论上不会到这里
    return "error"


def get_alphas_posit(start_date, end_date, sharpe_th, fitness_th, region, alpha_num):
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] get_alphas_posit开始处理地区 {region}，目标数量: {alpha_num}")
    s = sign_in(cfg.username, cfg.password)
    output = []
    count = 0

    for i in range(0, alpha_num, 40):
        offset_start = time.time()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 地区 {region} API请求，offset={i}")

        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d" % (i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2026-" + start_date \
                + "T00:00:00-04:00&dateCreated%3C2026-" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(
            sharpe_th) + "&settings.region=" + region + "&is.color!=RED&is.color!=YELLOW" + "&order=-is.sharpe&hidden=false&type=SUPER"

        urls = [url_e]

        for url in urls:  # 修复缩进，确保这个循环正确执行
            req_start = time.time()
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 发送API请求到: {url}")  # 打印完整URL

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
                    s = sign_in(cfg.username, cfg.password)
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

                    # SUPER类型的alpha使用combo代码，REGULAR类型使用regular代码
                    if 'combo' in alpha_list[j] and alpha_list[j]['combo']:
                        exp = alpha_list[j]['combo'].get('code', 'SUPER_ALPHA')
                    elif 'regular' in alpha_list[j] and alpha_list[j]['regular']:
                        exp = alpha_list[j]['regular'].get('code', 'REGULAR_ALPHA')
                    else:
                        exp = 'UNKNOWN'

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
                    s = sign_in(cfg.username, cfg.password)
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 重登录成功，继续")
                except Exception as login_e:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 重登录失败: {login_e}，跳过此offset")
                continue
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] JSON解析或其他错误: {e}")
                # 原有重登录逻辑
                s = sign_in(cfg.username, cfg.password)
                continue

    total_time = time.time() - offset_start  # 注意：这里offset_start是最后一个循环的，实际应从函数开始计算
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] get_alphas_posit for {region} 完成，总计数: {count}，输出: {len(output)}，总耗时约: {total_time:.2f}秒 (估算)")
    return output


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


def get_date_range_from_user():
    """
    获取用户自定义的日期范围
    Returns:
        tuple: (start_date, end_date, description, rolling_window) - 格式化的开始日期、结束日期、描述和滚动窗口设置
    """
    print("\n" + "=" * 80)
    print("📅 请设置查询日期范围（设置后将持续使用此范围运行）")
    print("=" * 80)
    print("选择输入方式：")
    print("  1. 使用天数偏移（推荐 - 如：从5天前到明天）")
    print("  2. 使用具体日期（如：01-20 到 01-25）")
    print("  3. 使用默认设置（5天前到明天）")
    print("  4. 使用滚动窗口（每轮自动更新为最近N天）")
    print("\n💡 提示：选项1-3设置后固定不变，选项4每轮自动更新")

    choice = input("\n请选择 [1/2/3/4，默认3]: ").strip() or "3"

    today = datetime.now()

    if choice == "1":
        # 天数偏移方式
        print("\n输入天数偏移（负数表示过去，正数表示未来）：")
        try:
            start_days = int(input("  开始日期偏移天数（如：-5 表示5天前）[默认-5]: ").strip() or "-5")
            end_days = int(input("  结束日期偏移天数（如：1 表示明天）[默认1]: ").strip() or "1")

            start_date_obj = today + timedelta(days=start_days)
            end_date_obj = today + timedelta(days=end_days)

            start_date = start_date_obj.strftime("%m-%d")
            end_date = end_date_obj.strftime("%m-%d")

            desc = f"{abs(start_days)}天前到{abs(end_days)}天后 (固定)" if end_days > 0 else f"{abs(start_days)}天前到{abs(end_days)}天前 (固定)"
            if start_days == 0:
                desc = f"今天到{abs(end_days)}天后 (固定)" if end_days > 0 else f"今天到{abs(end_days)}天前 (固定)"

            print(f"\n✅ 设置成功: {start_date} 到 {end_date} ({desc})")
            return start_date, end_date, desc, False  # False表示不自动更新

        except ValueError:
            print("❌ 输入无效，使用默认设置")

    elif choice == "2":
        # 具体日期方式
        print("\n输入具体日期（格式：MM-DD，如：01-20）：")
        try:
            start_input = input("  开始日期 [默认5天前]: ").strip()
            end_input = input("  结束日期 [默认明天]: ").strip()

            if start_input and end_input:
                # 验证日期格式
                datetime.strptime(start_input, "%m-%d")
                datetime.strptime(end_input, "%m-%d")
                start_date = start_input
                end_date = end_input
                desc = f"{start_date} 到 {end_date} (固定)"
                print(f"\n✅ 设置成功: {desc}")
                return start_date, end_date, desc, False  # False表示不自动更新
            else:
                print("❌ 日期不完整，使用默认设置")
        except ValueError:
            print("❌ 日期格式错误，使用默认设置")

    elif choice == "4":
        # 滚动窗口方式
        print("\n设置滚动窗口（每轮自动更新）：")
        try:
            days_back = int(input("  查询最近多少天的数据？[默认7]: ").strip() or "7")
            if days_back < 1:
                print("❌ 天数必须大于0，使用默认7天")
                days_back = 7

            # 返回特殊标记，表示需要每轮更新
            desc = f"滚动窗口(最近{days_back}天)"
            print(f"\n✅ 设置成功: {desc} - 每轮自动更新日期范围")
            return None, None, desc, days_back  # days_back作为滚动窗口的天数

        except ValueError:
            print("❌ 输入无效，使用默认设置")

    # 默认设置（选项3或其他情况）
    five_days_ago = today - timedelta(days=5)
    tomorrow = today + timedelta(days=1)
    start_date = five_days_ago.strftime("%m-%d")
    end_date = tomorrow.strftime("%m-%d")
    desc = "5天前到明天 (固定)"
    print(f"\n✅ 使用默认设置: {start_date} 到 {end_date} ({desc})")
    return start_date, end_date, desc, False  # False表示不自动更新


sess = sign_in(cfg.username, cfg.password)

# 在循环开始前获取日期范围设置
print("\n" + "🎯" * 40)
print("欢迎使用 Alpha 自动筛选和标记系统")
print("🎯" * 40)
start_date, end_date, date_desc, rolling_window = get_date_range_from_user()

# 无限循环处理所有地区
loop_count = 0
while True:
    loop_count += 1
    print("\n" + "=" * 80)
    print(f"🔄 开始第 {loop_count} 轮处理 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")

    # 每轮开始时更新数据
    download_data(flag_increment=True)

    # 如果是滚动窗口模式，每轮更新日期范围
    if rolling_window and isinstance(rolling_window, int):
        today = datetime.now()
        days_ago = today - timedelta(days=rolling_window)
        start_date = days_ago.strftime("%m-%d")
        end_date = today.strftime("%m-%d")
        print(f"📅 查询日期范围: {start_date} 到 {end_date} ({date_desc}) - 已自动更新\n")
    else:
        # 使用固定的日期范围
        print(f"📅 查询日期范围: {start_date} 到 {end_date} ({date_desc})\n")

    region_list = ['USA', 'ASI', 'EUR', 'GLB', 'CHN', 'JPN', 'AMR']
    random.shuffle(region_list)
    for region in region_list:
        alpha_records = get_alphas_posit(start_date, end_date, 1, 0.5, region, 100)

        # 提取alpha ID（第一个元素）并去重保序
        alpha_ids = []
        for rec in alpha_records:
            alpha_id = rec[0]  # alpha_id是第一个元素
            if alpha_id not in alpha_ids:
                alpha_ids.append(alpha_id)

        print(f"地区 {region} 获取到 {len(alpha_ids)} 个唯一alpha")

        alpha_bag = []
        gold_bag = []
        prod_corr_dict = {}  # 存储每个alpha的生产相关性值

        # 检查是否有fail
        for idx, alpha_id in enumerate(alpha_ids, 1):
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            try:
                result_fail = get_simulation_result_json(sess, alpha_id)
                if result_fail and "FAIL" not in str(result_fail).upper():
                    print(f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 不包含 FAIL，继续")
                    os_alpha_ids, os_alpha_rets = load_data()
                    self_corr = calc_self_corr(
                        alpha_id=alpha_id,
                        os_alpha_rets=os_alpha_rets,
                        os_alpha_ids=os_alpha_ids,
                    )
                    if self_corr < 0.7:
                        print(
                            f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 自相关性: {self_corr} 符合条件")
                        # 直接调用 API 获取生产相关性
                        try:
                            # 记录开始时间，用于超时检查
                            start_time = time.time()
                            timeout_seconds = 600  # 10分钟 = 600秒
                            
                            while True:
                                # 检查是否超时
                                elapsed_time = time.time() - start_time
                                if elapsed_time > timeout_seconds:
                                    raise TimeoutError(f"获取生产相关性超时（超过{timeout_seconds}秒）")
                                
                                response = sess.get(
                                    "https://api.worldquantbrain.com/alphas/" + alpha_id + "/correlations/prod"
                                )
                                if "retry-after" in response.headers:
                                    retry_after = float(response.headers["Retry-After"])
                                    # 检查等待后是否会超时
                                    if elapsed_time + retry_after > timeout_seconds:
                                        raise TimeoutError(f"获取生产相关性超时（等待Retry-After后超过{timeout_seconds}秒）")
                                    time.sleep(retry_after)
                                else:
                                    break

                            # 从 JSON 响应中直接获取 max 值
                            prod_corr_data = response.json()
                            prod_corr_value = prod_corr_data.get('max', None)

                            # print(f"生产相关性响应: {prod_corr_data}")

                            if prod_corr_value is not None and float(prod_corr_value) < 0.7:
                                print(
                                    f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 生产相关性: {prod_corr_value} 符合条件")
                                alpha_bag.append(alpha_id)
                                prod_corr_dict[alpha_id] = prod_corr_value  # 保存生产相关性值
                            else:
                                print(
                                    f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 生产相关性: {prod_corr_value} 不符合条件")
                                # 标记为黄色，name里写上时间和生产相关性
                                try:
                                    current_time_name = datetime.now().strftime("%Y%m%d_%H%M%S")
                                    prod_corr_str = f"{float(prod_corr_value):.3f}" if prod_corr_value is not None else "None"
                                    alpha_name = f"{current_time_name}_{prod_corr_str}"  # 格式：20250127_123456_0.750
                                    set_alpha_properties(sess, alpha_id,
                                                        name=alpha_name,
                                                        color='YELLOW',
                                                        tags=['prod_corr_fail'])
                                    print(f"   🟡 {alpha_id[:8]}... → YELLOW (原因: 生产相关性 {prod_corr_str} 不符合条件, Name: {alpha_name})")
                                except Exception as tag_e:
                                    print(f"   ⚠️  标记YELLOW失败 {alpha_id[:8]}...: {str(tag_e)[:50]}")

                        except TimeoutError as e:
                            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            print(
                                f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 获取生产相关性超时: {str(e)[:50]}")
                            # 标记为黄色，tag记录超时原因
                            try:
                                set_alpha_properties(sess, alpha_id,
                                                    color='YELLOW',
                                                    tags=['prod_corr_timeout'])
                                print(f"   🟡 {alpha_id[:8]}... → YELLOW (原因: 获取生产相关性超时)")
                            except Exception as tag_e:
                                print(f"   ⚠️  标记YELLOW失败 {alpha_id[:8]}...: {str(tag_e)[:50]}")
                        except Exception as e:
                            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            print(
                                f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 获取生产相关性失败: {str(e)[:50]}")
                            # 标记为黄色，tag记录原因
                            try:
                                set_alpha_properties(sess, alpha_id,
                                                    color='YELLOW',
                                                    tags=['prod_corr_error'])
                                print(f"   🟡 {alpha_id[:8]}... → YELLOW (原因: 获取生产相关性失败)")
                            except Exception as tag_e:
                                print(f"   ⚠️  标记YELLOW失败 {alpha_id[:8]}...: {str(tag_e)[:50]}")
                    else:
                        print(
                            f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 自相关性: {self_corr} 不符合条件")
                        # 标记为黄色，tag记录原因
                        try:
                            tag_name = f"self_corr_{self_corr:.2f}"  # 格式：self_corr_0.75
                            set_alpha_properties(sess, alpha_id,
                                                color='YELLOW',
                                                tags=[tag_name])
                            print(f"   🟡 {alpha_id[:8]}... → YELLOW (原因: 自相关性 {self_corr:.3f} 不符合条件)")
                        except Exception as tag_e:
                            print(f"   ⚠️  标记YELLOW失败 {alpha_id[:8]}...: {str(tag_e)[:50]}")
                else:
                    print(f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 包含 FAIL，跳过")
                    # 标记为黄色，name里写上时间和has_fail
                    try:
                        current_time_name = datetime.now().strftime("%Y%m%d_%H%M%S")
                        alpha_name = f"{current_time_name}_has_fail"  # 格式：20250127_143020_has_fail
                        set_alpha_properties(sess, alpha_id,
                                            name=alpha_name,
                                            color='YELLOW',
                                            tags=['has_fail'])
                        print(f"   🟡 {alpha_id[:8]}... → YELLOW (原因: 包含FAIL, Name: {alpha_name})")
                    except Exception as tag_e:
                        print(f"   ⚠️  标记YELLOW失败 {alpha_id[:8]}...: {str(tag_e)[:50]}")

            except Exception as e:
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(
                    f"[{current_time}] [{idx}/{len(alpha_ids)}] ❌ 处理 alpha_id: {alpha_id} 时出错: {type(e).__name__} - {str(e)[:100]}")
                continue

        print("添加描述")
        project_spec = "Idea: 111111111111111\n" + \
                       "Rationale for data used: 11111111111111\n" + \
                       "Rationale for operators used: 111111111111111"
        c_d = "1Short descriptions of your Selection Expression and Combo Expression are required to submit this SuperAlpha."
        s_d = "1Short descriptions of your Selection Expression and Combo Expression are required to submit this SuperAlpha."

        for alpha_id in alpha_bag:
            set_alpha_properties(sess, alpha_id, description=project_spec)
        print("添加描述完成")

        print("提交检查")
        result = check_submission(alpha_bag, gold_bag, 0)
        print("提交检查完成")
        print(f"   📊 检查结果: {len(result)}/{len(alpha_bag)} 个alpha通过检查")

        # 提取通过检查的alpha ID
        li2 = []
        for j in range(0, len(result)):
            li2.append(result[j][0])
        li2 = list(set(li2))

        # 分离通过和失败的alpha
        passed_alphas = set(li2)
        failed_alphas = [aid for aid in alpha_bag if aid not in passed_alphas]

        if failed_alphas:
            print(f"🟡 标记 {len(failed_alphas)} 个失败的alpha为YELLOW...")
            current_time_name = datetime.now().strftime("%Y%m%d_%H%M%S")  # 在循环外生成时间戳
            yellow_success_count = 0
            yellow_fail_count = 0
            for alpha in failed_alphas:
                try:
                    # 获取生产相关性值
                    prod_corr_value = prod_corr_dict.get(alpha, 0.0)

                    # 使用同一时间戳 + 生产相关性作为name
                    alpha_name = f"{current_time_name}_{prod_corr_value:.3f}"  # 格式：20250127_123456_0.350

                    response = set_alpha_properties(sess, alpha,
                                                    name=alpha_name,
                                                    description=project_spec,
                                                    combo_desc=c_d,
                                                    color='YELLOW',
                                                    selection_desc=s_d,
                                                    tags=['c1'])  # 设置tags参数
                    yellow_success_count += 1
                    print(f"   🟡 {alpha[:8]}... → YELLOW (状态: {response.status_code})")
                except Exception as e:
                    yellow_fail_count += 1
                    error_msg = str(e)
                    print(f"   ❌ 标记YELLOW失败 {alpha[:8]}...: {error_msg[:100]}")
                    # 如果是401或403，尝试重新登录
                    if "401" in error_msg or "403" in error_msg:
                        print(f"   🔄 检测到认证错误，尝试重新登录...")
                        sess = sign_in(cfg.username, cfg.password)
                    continue
            print(f"   🟡 YELLOW标记完成: 成功 {yellow_success_count}/{len(failed_alphas)}，失败 {yellow_fail_count}")

        alpha_lis = li2

        # 显示最终选中的alpha列表
        print(f"\n🌟 地区 {region} 最终选中的 Alpha 列表（共 {len(alpha_lis)} 个）:")
        for idx_alpha, alpha_id in enumerate(alpha_lis, 1):
            # 从result中获取PC值
            pc_value = None
            for r_alpha, r_pc in result:
                if r_alpha == alpha_id:
                    pc_value = r_pc
                    break
            print(f"   {idx_alpha:2d}. {alpha_id} (PC: {pc_value})")

        # ✅ 标记为绿色 (通过检查的alpha)
        print(f"\n🟢 开始标记GREEN...")
        current_time_name = datetime.now().strftime("%Y%m%d_%H%M%S")  # 在循环外生成时间戳
        green_success_count = 0
        green_fail_count = 0
        for alpha in alpha_lis:
            try:
                # 从result中获取PC值作为tag
                pc_value = None
                for r_alpha, r_pc in result:
                    if r_alpha == alpha:
                        pc_value = r_pc
                        break

                if pc_value is not None:
                    # 将PC值格式化为tag（保留2位小数）
                    tag_name = f"PC{float(pc_value):.2f}"  # 格式：PC0.35
                else:
                    tag_name = "PC0.00"

                # 从字典中获取生产相关性值（已在第一步筛选时获取并保存）
                prod_corr_value = prod_corr_dict.get(alpha, 0.0)

                # 使用同一时间戳 + 生产相关性作为name
                alpha_name = f"{current_time_name}_{prod_corr_value:.3f}"  # 格式：20250129_143020_0.350

                # ✅ 设置为GREEN色
                response = set_alpha_properties(sess, alpha,
                                                name=alpha_name,
                                                description=project_spec,
                                                combo_desc=c_d,
                                                selection_desc=s_d,
                                                color='GREEN',  # ✅ 确保是GREEN
                                                tags=[tag_name])
                green_success_count += 1

                # 打印前5个确认信息
                if green_success_count <= 5:
                    print(
                        f"   ✅ {alpha[:8]}... → GREEN | Name: {alpha_name} | Tag: {tag_name} (状态: {response.status_code})")

            except Exception as e:
                green_fail_count += 1
                error_msg = str(e)
                print(f"   ❌ 标记GREEN失败 {alpha[:8]}...: {error_msg[:100]}")
                # 如果是401或403，尝试重新登录
                if "401" in error_msg or "403" in error_msg:
                    print(f"   🔄 检测到认证错误，尝试重新登录...")
                    sess = sign_in(cfg.username, cfg.password)
                continue

        print(f"   🟢 GREEN标记完成: 成功 {green_success_count}/{len(alpha_lis)}，失败 {green_fail_count}")

        print(f"\n✅ 地区 {region} 完成: 通过 {len(alpha_lis)} 个，失败 {len(failed_alphas)} 个")
        print("=" * 60)

    # 一轮完成后的统计和等待
    print("\n" + "=" * 80)
    print(f"🎉 第 {loop_count} 轮所有地区处理完成！- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # 等待30分钟后开始下一轮（可根据需要调整）
    wait_minutes = 240
    print(f"\n⏰ 等待 {wait_minutes} 分钟后开始下一轮...")
    print(f"   下一轮预计开始时间: {(datetime.now() + timedelta(minutes=wait_minutes)).strftime('%Y-%m-%d %H:%M:%S')}")
    time.sleep(wait_minutes * 60)
