from channel.accc import ACCC
from channel.bvl import BVL
from channel.caa import CAA
from channel.ccpc import CCPC
from channel.cpsc_recall import CPSC_RECALL
from channel.ctsi import CTSI
from channel.healthCanada_medicine import HC_MEDICINE
from channel.healthCanada_industrialProducts import HC_IP
from channel.healthCanada_food import HC_FOOD
from channel.healthCanada_vihicle import HC_VIHICLE
from channel.nhtsa import NHTSA
from channel.nihn import NIHN
from channel.opss import OPSS
from channel.rappelConsommateur import RAPPELCONSOMMATEUR
from channel.safetyGate import SafetyGate
from channel.taiwanFDA import taiwanFDA
from common.utils import Utils

import configparser
from database.api import API
from datetime import datetime, timedelta
import logging
import socket
import sys
import time

# 설정 파일 로드
config = configparser.ConfigParser()
config.read('common/config.ini')

if __name__=='__main__':
    while(True):
        try:
            now = datetime.now()

            # 로그 파일 설정
            now_date = datetime.strftime(now, '%Y-%m-%d')
            log_filename = f'{now_date}.log'

            # 로거 설정
            logger = logging.getLogger("CrawlerLogger")
            logger.setLevel(logging.INFO)

            # 파일 핸들러 추가
            file_handler = logging.FileHandler(log_filename, encoding="utf-8")
            file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            logger.addHandler(file_handler)

            # 콘솔에도 로그 출력
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            logger.addHandler(console_handler)

            api = API(logger)
            utils = Utils(logger, api)
            schedule = api.getIndividualSchedule()
            if schedule['idx'] != -1:    
                start = datetime.now()
                logger.info(schedule['chnnlNm'] + '  ::  수집')
                logger.info(f'수집시작시간  ::  {start}')
                job_stats = ''
                cntanr_nm = socket.gethostname()

                colct_bgng_dt = utils.erase_timezone_info(schedule['colctBgngDt'])
                colct_end_dt = utils.erase_timezone_info(schedule['colctEndDt'])
                
                api.updateStartSchedule(schedule['idx'], start.isoformat(), cntanr_nm)
                if schedule['chnnlCd'] == 67: # 'Heath Canada_자동차'
                    chnnl = HC_VIHICLE(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 68: # 'Heath Canada_의약품'
                    chnnl = HC_MEDICINE(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 69: # 'Heath Canada_공산품'
                    chnnl = HC_IP(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 70: # 'Heath Canada_식품'
                    chnnl = HC_FOOD(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 71: # NHTSA
                    chnnl = NHTSA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 72: # CAA
                    chnnl = CAA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 73: # 대만 FDA
                    chnnl = taiwanFDA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 74: # Safety Gate
                    chnnl = SafetyGate(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 75: # CTSI
                    chnnl = CTSI(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 76: # CCPC
                    chnnl = CCPC(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 77: # OPSS
                    chnnl = OPSS(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 78: # RASFF
                    chnnl = OPSS(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 79: # Rappel Consommateur
                    chnnl = RAPPELCONSOMMATEUR(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 80: # NIHN
                    chnnl = NIHN(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 81: # BVL
                    chnnl = BVL(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
                    chnnl.crawl()                    
                elif schedule['chnnlCd'] == 82: # ACCC
                    chnnl = ACCC(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
                    chnnl.crawl()               
                elif schedule['chnnlCd'] == 83: # CPSC-리콜
                    chnnl = CPSC_RECALL(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
                    chnnl.crawl()                          
                elif schedule['chnnlCd'] == 84: # CPSC-주의보
                    chnnl = CPSC_(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)                       
                    chnnl.crawl()                                                            
                else:
                    logger.info(f"개별 수집기 개발 필요 - {schedule['idx']}, {schedule['chnnlCd']}, {schedule['chnnlNm']}")
                    end = datetime.now()
                    logger.info(f'수집종료시간  ::  {end}')                    
                    api.updateEndSchedule(schedule['idx'], 'E', 0, end.isoformat())
                    continue

                if chnnl.error_cnt > 0:
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
                api.updateEndSchedule(schedule['idx'], job_stats, chnnl.colct_cnt, end.isoformat())
                diff = end - start
                logger.info(f'Crawl Time : {diff.seconds} seconds')
            else:
                logger.info('수집 스케쥴 미존재 60초 대기')
                time.sleep(60)
        except Exception as e:
            logger.error(f'수집기 종료  ::  {e}')
            exc_type, exc_obj, tb = sys.exc_info()
            utils.save_colct_log(exc_obj, tb, schedule['chnnl_cd'], schedule['chnnl_nm'])
        # finally:
        #     # 메일보내기?