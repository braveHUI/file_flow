# -*- coding: utf-8 -*-
import requests
import json
import urllib
import hashlib
import base64
import logging
import time

from api import SyncConfig, forSyncAPI
logger = logging.getLogger(__name__)


class RequestApi(object):
    def __init__(self, diy):
        self.diy = diy
        self.request_data = json.dumps(diy)
        # md5夹名计算结果
        self.md5 = hashlib.md5((self.request_data + SyncConfig.api_key).encode("utf-8"))
        # base64计算结果
        self.data_sign = base64.b64encode(self.md5.hexdigest().encode("utf-8"))
        self.re_data = forSyncAPI(self.request_data, self.data_sign, SyncConfig.api_user)

    # 发送新增run请求
    def request_add_run(self):
        logger.debug("进入request_add_run方法")
        item = {}
        flag = False
        try:
            for i in range(0, 3):
                response = requests.post(url=SyncConfig.run_check, data=self.re_data, headers={})
                status_code = response.status_code
                if status_code == 200:
                    item["flat"] = True
                    re_dict = json.loads(response.content.decode())
                    if 'data' in re_dict:
                        item["run_info_id"] = str(re_dict["data"]["run_info_id"])
                        logger.info(re_dict)
                        logger.info(self.diy["runid"] + ": Success!->add_dir")
                        flag = True
                else:
                    time.sleep(2)
                    item["flat"] = False
                    item["run_info_id"] = "-1"
                if flag:
                    break
                else:
                    logger.error(response.content.decode())
                    logger.error("发送%d次新增run api请求失败，请检查你的数据或者网络" % i)

        except Exception as e:
            logger.info(e)
        finally:
            logger.debug("推出request_add_run方法")
            return item

    # 发送更新run的状态
    def request_update_run(self):
        logger.debug("进入request_update_run方法")
        flag = False
        try:
            for i in range(0, 3):
                response = requests.post(url=SyncConfig.run_update, data=self.re_data, headers={})
                status_code = response.status_code
                if status_code == 200:
                    re_dict = json.loads(response.content.decode())
                    logger.info(re_dict)
                    flag = True
                    logger.info(str(self.diy["run_info_id"]) + ": Success!->update_state")
                    break
                else:
                    logger.warning(status_code)
                    time.sleep(2)

                if not flag:
                    logger.error("发送%d次更新run状态 api请求失败，请检查你的数据或者网络" % i)
        except Exception as e:
            logger.error(e)
        finally:
            logger.debug("退出request_update_run方法")
            return flag

    def run(self, method):
        method = int(method)
        run_info_id = -1
        try:
            temp = 1
            if method == 1:
                while temp <= 3:
                    flat, run_info_id = self.request_add_run()
                    if flat:
                        return True, run_info_id
                    else:
                        time.sleep(2)
                        temp += 1
                logger.error("发送3次新增run api请求失败，请检查你的数据或者网络")
                return False, run_info_id
            elif method == 2:
                while temp <= 3:
                    flae = self.request_update_run()
                    if flae:
                        return True
                    else:
                        time.sleep(2)
                        temp += 1
                logger.error("发送3次更新run api请求失败，请检查你的数据或者网络")
                return False
            else:
                logger.error("method的方式不对")
                return False
        except Exception as e:
            logger.error(e)




