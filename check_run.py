import time
import argparse
import os
import logging
import schedule
from datetime import date, datetime
from logging.handlers import TimedRotatingFileHandler
import re
from request_api import RequestApi
logger = logging.getLogger(__name__)


class CheckRun(object):
    def __init__(self, path, outpath):
        self.path = path
        self.diritems = []
        self.fastaitems = []
        self.filename = outpath

    # 根据给定的路径获取文件夹
    def get_dirs(self):
        logger.debug("进入get_dirs方法")
        diritems = []
        try:
            list_dirs = os.listdir(self.path)
            for de in list_dirs:
                item = {}

                list_p = de.split("_")
                if len(list_p) > 2:
                    list_r = list_p[1]
                    if "MN00302" == list_r:
                        item["sequencer"] = 3
                        item["runid"] = de
                        diritems.append(item)
                        logger.debug("增加%s成功" % de)
                    elif "M04840" == list_r:
                        item["sequencer"] = 2
                        item["runid"] = de
                        diritems.append(item)
                        logger.debug("增加%s成功" % de)
        except Exception as e:
            logger.error(e)
        finally:
            logger.debug("退出get_dirs方法")
            return diritems

    # 寻找存放fastq文件的最新的Alignment文件夹
    def find_alignment_dir(self, runid):
        logger.debug("进入find_alignment_dir方法")
        air_dirs = []
        num = 0
        try:
            ali_path = os.path.join(self.path,  runid)
            dire_al = os .listdir(ali_path)
            for ali in dire_al:
                if ali.startswith("Alignment"):
                    air_dirs.append(ali)
            if len(air_dirs):
                for ae in air_dirs:
                    ali_num = int(ae.split("_")[1])
                    if ali_num > num:
                        num = ali_num
        except Exception as e:
            logger.error(e)
        finally:
            ali_dirs_name = "Alignment" + "_" + str(num)
            logger.debug("最新的文件夹是%s" % ali_dirs_name)
            logger.debug("退出find_alignment_dir方法")
            return ali_dirs_name

    # 在文件夹中找出fastq文件
    def get_fastq_file(self, diritem):
        logger.debug("进入get_fastq_file方法")
        fastq_path_list = []
        if diritem["sequencer"] == 3:
            fastqtt_path = self.find_alignment_dir(diritem["runid"])
            fastq_path = os.path.join(self.path,  diritem["runid"],  fastqtt_path)
            if os.path.exists(fastq_path):
                try:
                    temp = os.listdir(fastq_path)[0]
                    fastq_path = os.path.join(fastq_path, temp, 'Fastq')
                    fastq_list = os.listdir(fastq_path)
                    if len(fastq_list):

                        fastq_list = [faq for faq in fastq_list if faq.endswith("fastq.gz")]
                        fastq_list = sorted(fastq_list)
                        for fastq in fastq_list:
                            fastq = os.path.join(fastq_path, fastq)
                            fastq_path_list.append(fastq)
                except Exception as e:
                    logger.error("%s查找失败,错误信息：%s" % (diritem["runid"], e))
            logger.debug("退出get_fastq_file方法")
        elif diritem["sequencer"] == 2:
            fastq_path = os.path.join(self.path, diritem["runid"], 'Data/Intensities/BaseCalls')
            if os.path.exists(fastq_path):
                try:
                    fastq_list = os.listdir(fastq_path)
                    if len(fastq_list):
                        fastq_list = [faq for faq in fastq_list if faq.endswith("fastq.gz")]
                        fastq_list = sorted(fastq_list)
                        fastq_list = sorted(fastq_list)
                        for fastq in fastq_list:
                            fastq = os.path.join(fastq_path, fastq)
                            fastq_path_list.append(fastq)
                except Exception as e:
                    logger.error("%s查找失败,错误信息：%s" % (diritem["runid"], e))
            logger.debug("退出get_fastq_file方法")

        return fastq_path_list

    # 根据文件夹的修改时间判断文件是否完成
    def get_fastqdir_status(self, diritem):
        logger.debug("进入get_fastq_file方法")
        fastq_path_list = []
        status = ["0"]
        if diritem["sequencer"] == 3:
            fastqtt_path = self.find_alignment_dir(diritem["runid"])
            fastq_path = os.path.join(self.path, diritem["runid"], fastqtt_path)
            if os.path.exists(fastq_path):
                try:
                    temp = os.listdir(fastq_path)[0]
                    samplesheetused_path = os.path.join(fastq_path, temp, 'SampleSheetUsed.csv')
                    fastq_path = os.path.join(fastq_path, temp, 'Fastq')
                    update_time = os.path.getmtime(fastq_path)
                    time.sleep(30)
                    update_time1 = os.path.getmtime(fastq_path)
                    logger.debug("{}文件夹30s前的时间戳是，30s之后的时间戳是".format(fastq_path, update_time, update_time))
                    if update_time == update_time1:
                        fastq_name_list = os.listdir(fastq_path)
                        sampale_name_list = {sname.split("_")[0]: 1 for sname in fastq_name_list}
                        fastq_path_list = [os.path.join(fastq_path, name) for name in fastq_name_list if name.endswith("fastq.gz")]
                        flag = self.read_samplesheetused_data(samplesheetused_path, sampale_name_list)
                        if flag:
                            status[0] = "1"
                            return status, fastq_path_list
                        else:
                            return status, fastq_path_list
                    else:
                        return status, fastq_path_list
                except Exception as e:
                    logger.error("%s查找失败,错误信息：%s" % (diritem["runid"], e))
            else:
                return status, fastq_path_list
            logger.debug("退出get_fastq_file方法")
        elif diritem["sequencer"] == 2:
            fastq_path = os.path.join(self.path, diritem["runid"], 'Data/Intensities/BaseCalls')

            if os.path.exists(fastq_path):
                try:
                    # print("#"*100)
                    # print(fastq_path)
                    # print("#"*100)
                    update_time = os.path.getmtime(fastq_path)
                    time.sleep(30)
                    update_time1 = os.path.getmtime(fastq_path)
                    logger.debug("{}文件夹30s前的时间戳是，30s之后的时间戳是".format(fastq_path, update_time, update_time))
                    if update_time == update_time1:
                        fastq_list = os.listdir(fastq_path)
                        if len(fastq_list):
                            fastq_path_list = [os.path.join(fastq_path, faq) for faq in fastq_list if faq.endswith("fastq.gz")]
                        return status, fastq_path_list
                    else:
                        return status, fastq_path_list
                except Exception as e:
                    logger.error("%s查找失败,错误信息：%s" % (diritem["runid"], e))
            else:
                return status, fastq_path_list

    def read_samplesheetused_data(self, samplesheetused_path, sampale_name_list):
        fasta_name = []
        flag = False
        if os.path.exists(samplesheetused_path):
            with open(samplesheetused_path, 'r') as pf:
                for line in pf:
                    line_list = line.strip("\n").split(",")
                    if len(line_list) == 6:
                        if line_list[0] != "Sample_ID":
                            fasta_name.append(line_list[1])
            try:
                for name in fasta_name:
                    num = sampale_name_list[name]
                flag = True
            except:
                pass
        return flag

    # 根据fastq文件的大小是否有变动来决定run的状态码0代表测序中 1代表测序完成
    def get_fastq_status(self,fastq_path_list):
        logger.debug("进入get_fastq_status方法")
        run_status = 0
        temp = 1
        fasta_file_size = []
        if len(fastq_path_list):
            fasta_file_size = [os.path.getsize(fastaf) for fastaf in fastq_path_list]
            # for sie_f in fasta_file_size:
            #     if sie_f < 100:
            #         return run_status
        else:
            run_status = -1
            return run_status
        time.sleep(30)
        while temp <= 3:
            flag = True
            for i in range(len(fastq_path_list)):
                if fasta_file_size[i] != os.path.getsize(fastq_path_list[i]):
                    flag = False
                    fasta_file_size[i] = os.path.getsize(fastq_path_list[i])
            if flag:
                run_status = 1
                return run_status

            else:
                temp += 1
                time.sleep(30)
        logger.debug("退出get_fastq_status方法")
        return run_status

    # 读取已存在的文件名
    def read_filename(self, filename):
        logger.debug("进入read_filename方法")
        iaccs = []
        file = open(filename, 'r')
        logger.info("开始读取文件")
        for line in file:
            iaccs.append(line.rstrip('\n'))
        file.close()
        logger.info("读取%d条数据" % len(iaccs))
        logger.debug("退出read_filename方法")
        return iaccs

    # 跟新已存在的文件的运行状态，输入的是一个字典
    def update_dir_status(self, dirstate):
        logger.debug("进入update_dir_status方法")
        try:
            dirs_list = []
            dirslist = self.read_filename(self.filename)
            if len(dirslist):
                for dit in dirslist:
                    if dirstate["runid"] in dit:
                        dits = dit.split(",")
                        dits[3] = str(dirstate["status"])
                        dit = ",".join(dits)
                    dirs_list.append(dit)
            if len(dirs_list):
                with open(self.filename, 'w') as f:
                    for rew in dirs_list:
                        f.write(rew + "\n")
                f.close()
            logger.debug("退出update_dir_status方法")
        except Exception as e:
            logger.error(e)

    # 写入存在的文件名
    def write_filename(self, listdirs):
        logger.debug("进入write_filename方法")
        # 把序列文件名写入文件
        try:
            logger.debug("开始向文件文件名")
            recordid = open(self.filename, 'a')
            for idr in listdirs:
                recordid.write(str(idr["sequencer"]) + "," + idr["runid"] + "," + idr["run_info_id"] + "," + "0" + "\n")
            recordid.close()
            logger.debug("文件名写入成功共%d条" % len(listdirs))
            logger.debug("退出write_filename方法")
        except Exception as e:
            logger.info(e)

    # 判断是否有新的文件夹生成
    def judge_dir(self, listdirs):
        logger.debug("进入judge_dir方法")
        adddirs = []  # 用于保存新增的文件夹
        try:
            old_listdir = self.read_filename(self.filename)
            old_listdir = ",".join(old_listdir)
            for diy in listdirs:
                if diy["runid"] not in old_listdir:
                    reqapi = RequestApi(diy)
                    api_item = reqapi.request_add_run()
                    if api_item["flat"]:
                        diy["run_info_id"] = api_item["run_info_id"]
                        adddirs.append(diy)
            if len(adddirs):
                self.write_filename(adddirs)
            logger.debug("退出judge_dir方法")
        except Exception as e:
            logger.log(e)

    # 获取文件夹名的状态不为1的文件夹
    def get_state_dir(self):
        logger.debug("进入get_state_dir方法")
        list_start = time.process_time()
        items = []
        try:
            old_listdir = self.read_filename(self.filename)
            for odf in old_listdir:
                item = {}
                ders = odf.split(",")
                if int(ders[3]) == 0:
                    item["sequencer"] = int(ders[0])
                    item["runid"] = ders[1]
                    item["run_info_id"] = int(ders[2])
                    item["status"] = ders[3]
                    items.append(item)
        except Exception as e:
            logger.error(e)
        finally:
            listq_end = time.process_time()
            logger.info("获取文件夹名的状态为0的文件夹的方法开始运行时间{}，结束时间{}，运行时间{}".format(list_start, listq_end, listq_end - list_start))
            logger.debug("退出get_state_dir方法")
            return items

    # 匹配fastq双端文件
    def match_fastq_path(self, fastq_path_list, fq_name, i):
        fae_path = ""
        num = 0
        try:
            for j in range(0, len(fastq_path_list)):
                if i != j:
                    faqf_name = fastq_path_list[j].split("/")[-1]
                    if faqf_name.startswith(fq_name):
                        fae_path = fastq_path_list[j]
                        num = j
                        break
        except Exception as e:
            logger.error(e)
        finally:
            return fae_path, num

    # 对文件状态为1的文件夹的fastq文件路径发送api
    def send_fastq_path(self, fastq_path_list, fate):
        items = []
        request_data = {}
        request_data["run_info_id"] = fate["run_info_id"]
        request_data["status"] = fate["status"]
        num_list = []
        request_data["run_seqs"] = items
        try:
            for i in range(0, len(fastq_path_list)):
                item = {}
                if i not in num_list:
                    faq_name = fastq_path_list[i].split("/")[-1]
                    if not faq_name.startswith("Undetermined"):
                        faq_name_list = faq_name.split("_")
                        faq_name_three = "_".join([faq_name_list[i] for i in range(0, 3)])
                        fastq2_path, j = self.match_fastq_path(fastq_path_list, faq_name_three,i)
                        item["py_code"] = faq_name_list[0]
                        if len(fastq2_path):
                            if faq_name_list[3] == "R1":
                                item["fastq1_path"] = fastq_path_list[i]
                                item["fastq2_path"] = fastq_path_list[j]

                            else:
                                item["fastq1_path"] = fastq_path_list[j]
                                item["fastq2_path"] = fastq_path_list[i]
                            num_list.append(j)
                        else:
                            item["fastq1_path"] = fastq_path_list[i]
                        items.append(item)
        except Exception as e:
            logger.error(e)
        finally:
            request_data["run_seqs"] = items
            return request_data

    # 对需要更新的状态的文件夹，发送更新api
    def get_find_fastq_dirs(self, list_dires):
        logger.debug("进入get_find_fastq_dirs方法")
        try:
            for adre in list_dires:
                item = {}
                logger.debug(adre)
                status, fastq_path_list = self.get_fastqdir_status(adre)
                logger.debug(status, fastq_path_list)
                # if len(fastq_path_list):
                #     run_status = int(self.get_fastq_status(fastq_path_list))
                logger.debug(status)
                run_status = int(status[0])
                item["run_info_id"] = int(adre["run_info_id"])
                item["status"] = run_status
                if run_status:
                    # fastq_path_list = self.get_fastq_file(adre)
                    request_data = self.send_fastq_path(fastq_path_list, item)
                    upapi = RequestApi(request_data)
                    flae = upapi.request_update_run()
                    if flae:
                        adre["status"] = run_status
                        self.update_dir_status(adre)
                        logger.info("{}文件下发送{}个文件".format(os.path.dirname(fastq_path_list[0]), len(fastq_path_list)))
                    # else:
                    #     upapi = RequestApi(item)
                    #     flae = upapi.request_update_run()
                else:
                    logger.debug("fastq文件找不到，文件状态为非1状态")
                    dirpath = os.path.join(self.path, adre["runid"])
                    logger.debug(dirpath)
                    dirtime = datetime.fromtimestamp(os.path.getctime(dirpath))
                    todaytime = datetime.now()
                    counttime = (todaytime - dirtime).days
                    if counttime >= 1:
                        run_status = -2
                        flag = True
                        old_listdir = self.read_filename(self.filename)
                        for dire in old_listdir:
                            if adre["runid"] in dire:
                                state = int(dire.split(",")[3])
                                if state == -2:
                                    flag = False
                                break
                        if flag:
                            item["run_info_id"] = int(adre["run_info_id"])
                            item["status"] = run_status
                            upapi = RequestApi(item)
                            flae = upapi.request_update_run()
                            if flae:
                                adre["status"] = run_status
                                self.update_dir_status(adre)
            logger.debug("退出get_find_fastq_dirs方法")
        except Exception as e:
            logger.error(e)

    def run(self):
        # 1根据路径获取该路径下文件夹名字
        listdirs = self.get_dirs()
        adddirs = []   # 用于保存新增的文件夹
        # 2是否是第一次运行 是第一次运行就直接读这个文件夹里面的文件
        if os.path.exists(self.filename):
            # 3判断是否有新的文件夹生成
            self.judge_dir(listdirs)
            # 4获取文件夹名的状态为0的文件名列表
            list_dires = self.get_state_dir()
            # 5对需要更新的状态的文件夹，发送更新api
            logger.debug(list_dires)
            if len(list_dires):
                self.get_find_fastq_dirs(list_dires)
        else:
            for diy in listdirs:
                reqapi = RequestApi(diy)
                api_start = time.process_time()
                api_item = reqapi.request_add_run()
                api_end = time.process_time()
                logger.debug("发送api接口的开始运行时间{}，结束时间{}，运行时间{}".format(api_start, api_end, api_end-api_start))
                if api_item["flat"]:
                    diy["run_info_id"] = api_item["run_info_id"]
                    adddirs.append(diy)
            if len(adddirs):
                self.write_filename(adddirs)
                self.get_find_fastq_dirs(adddirs)


class ScheduleRun(object):
    def run(self, path, outpath):
        checkr = CheckRun(path, outpath)
        checkr.run()

    def schedule_local(self, path, outpath, schedule_time):
        schedule.every(int(schedule_time)).minutes.do(self.run, path, outpath)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scan Illumina Runs and send status to Promegene BMS')
    parser.add_argument('-r', '--runpath', required=True, help='下机数据路径', default="/share/data4/illumina")
    parser.add_argument('-s', '--scanlog', default='scan.log', help='扫描记录日志文件。默认为scan.log')
    parser.add_argument('-t', '--schedule_time', default='5', help='间隔多少分钟再运行脚本')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='是否打开调试模式。默认关闭')
    args = parser.parse_args()
    logging_level = logging.DEBUG if args.debug else logging.INFO
    handler = TimedRotatingFileHandler('root.log',
                                       when="midnight",
                                       interval=1,
                                       backupCount=5)

    logging.basicConfig(level=logging_level,
                        format='[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
                        handlers=[handler])
    schedule_hour = ScheduleRun()
    schedule_hour.schedule_local(args.runpath, args.scanlog, args.schedule_time)
    # schedule_hour.run(args.runpath, args.scanlog)


