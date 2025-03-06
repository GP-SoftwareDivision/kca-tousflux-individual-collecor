from channel.accc import ACCC
from channel.accp import ACCP
from channel.afsca import AFSCA
from channel.baua import BAUA
from channel.bvl import BVL
from channel.caa import CAA
from channel.ccpc import CCPC
from channel.cfs import CFS
from channel.consumerCouncil import ConsumerCouncil
from channel.cpsc_alert import CPSCAlert
from channel.cpsc_recall import CPSCRecall
from channel.ctsi import CTSI
# from channel.dti import DTI
from channel.fda_alert import FDAAlert
from channel.fda_recall import FDARecall
from channel.fsa import FSA
from channel.fsai_foodAlerts import FSAIFoodAlerts
from channel.fsai_foodAllergenAlerts import FSAIFoodAllergenAlerts
from channel.fsanz import FSANZ
from channel.healthCanada_medicine import HCMedicine
from channel.healthCanada_industrialProducts import HCIP
from channel.healthCanada_food import HCFood
from channel.healthCanada_vehicle import HCVehicle
from channel.meti import METI
from channel.mbie import MBIE
from channel.mpi import MPI
from channel.nhtsa import NHTSA
from channel.nihn import NIHN
from channel.nite import NITE
# from channel.nsw import NSW
from channel.nvwa import NVWA
from channel.opss import OPSS
from channel.rasff import RASFF
from channel.rappelConsommateur import RappelConsommateur
from channel.safetyGate import SAFETYGATE
from channel.taiwanFDA import TAIWANFDA
from channel.tga import TGA
from channel.transportCanada import TransportCanada
from channel.usda import USDA
# from channel.톈진시시장감독관리위원회 import 톈진시시장감독관리위원회
# from channel.필리핀FDA import 필리핀FDA
from common.utils import Utils
from channel.recall_china import RECALL_CHINA
from database.api import API
from datetime import datetime, timedelta

import configparser
import logging
import os
import socket
import sys
import time
import os

# 설정 파일 로드
config = configparser.ConfigParser()
config.read('common/config.ini')

if __name__=='__main__':
    while(True):
        try:
            now = datetime.now()

            # 로그 파일 설정
            now_date = datetime.strftime(now, '%Y-%m-%d')
            log_filename = f'log/{now_date}.log'

            # log 디렉토리 확인 및 생성
            log_dir = 'log'
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            # 로거 설정
            logger = logging.getLogger("CrawlerLogger")
            logger.setLevel(logging.INFO)

            # 기존 핸들러 제거
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)

            # 새로운 파일 핸들러 설정
            file_handler = logging.FileHandler(log_filename, encoding="utf-8")
            file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

            # 핸들러 추가
            logger.addHandler(file_handler)
            api = API(logger)
            utils = Utils(logger, api)

            schedule = {"idx": -1, "chnnlCd": -1, "chnnlNm": '',
                    "colctBgngDt": "", "colctEndDt": "", "url": "", "jobStat": ""} 
             
            now = datetime.now()

            colct_bgng_date = '2023-12-01 00:00:00'
            # colct_end_date = '2025-02-18 23:59:59'
            # colct_bgng_date = datetime.strftime(now - timedelta(3), '%Y-%m-%d 00:00:00')
            colct_end_date = datetime.strftime(now, '%Y-%m-%d 23:59:59')
            schedule['colctBgngDt'] = colct_bgng_date
            schedule['colctEndDt'] = colct_end_date

            colct_bgng_dt = utils.erase_timezone_info(schedule['colctBgngDt'])
            colct_end_dt = utils.erase_timezone_info(schedule['colctEndDt'])  

            start = datetime.now()
            logger.info(schedule['chnnlNm'] + '  ::  수집')
            logger.info(f'수집시작시간  ::  {start}')
            job_stats = ''
            cntanr_nm = socket.gethostname()

            schedule['chnnlCd'] = 110
            schedule['chnnlNm'] = '중국 제품 안전 및 리콜 정보 네트워크 - 기타 제품'
            schedule['url'] = 'https://www.recall.org.cn/search.html?type=12'
            # chnnl = NVWA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                      
            # chnnl.crawl()

            chnnl = BAUA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
            chnnl.crawl()

            # chnnl = FSA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
            # chnnl.crawl()

            # chnnl = AFSCA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
            # chnnl.crawl()

            # chnnl = MBIE(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
            # chnnl.crawl()

            # chnnl = CPSC_NEWS(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
            # chnnl.crawl()

            # chnnl = CPSC_RECALL(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
            # chnnl.crawl()

            # chnnl = ACCC(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
            # chnnl.crawl()

            # chnnl = CTSI(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
            # chnnl.crawl6()

            # chnnl = RAPPELCONSOMMATEUR(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
            # chnnl.crawl()
    
            # chnnl = NIHN(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
            # chnnl.crawl()
    
            # chnnl = BVL(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
            # chnnl.crawl()

            chnnl = RECALL_CHINA(schedule['chnnlCd'], schedule['chnnlNm'], schedule['url'], colct_bgng_dt, colct_end_dt, logger, api)
            chnnl.crawl()

            if chnnl.error_cnt > 0 and chnnl.colct_cnt > 0:
                job_stats = 'L'
                chnnl.prdt_dtl_err_url = set(chnnl.prdt_dtl_err_url)
                err_res = f"총 {chnnl.total_cnt}건 중 {chnnl.colct_cnt}건 수집 성공 | {chnnl.error_cnt}건 수집 오류"
                err_str = ", " .join(chnnl.prdt_dtl_err_url) if chnnl.prdt_dtl_err_url else ""
                if err_str:
                    err_res += f" > {err_str}"

                utils.save_colct_log(err_res, '', schedule['chnnlCd'], schedule['chnnlNm'], 1)
            elif chnnl.error_cnt > 0 and chnnl.colct_cnt == 0:
                job_stats = 'E'                    
            elif chnnl.colct_cnt > 0:
                job_stats = 'Y'
            elif chnnl.duplicate_cnt > 0:
                job_stats = 'D'
            elif chnnl.total_cnt == 0:
                job_stats = 'X'
            else:
                job_stats = 'E'

            end = datetime.now()
            logger.info(f'수집종료시간  ::  {end}')                    
            # api.updateEndSchedule(schedule['idx'], job_stats, chnnl.colct_cnt, end.isoformat())
            diff = end - start
            logger.info(f'Crawl Time : {diff.seconds} seconds')

        except Exception as e:
            logger.error(f'수집기 종료  ::  {e}')
            exc_type, exc_obj, tb = sys.exc_info()
            utils.save_colct_log(exc_obj, tb, schedule['chnnlCd'], schedule['chnnlNm'])

