from channel.accc import ACCC
from channel.afsca import AFSCA
from channel.baua import BAUA
from channel.bvl import BVL
from channel.caa import CAA
from channel.ccpc import CCPC
from channel.cpsc_alert import CPSCAlert
from channel.cpsc_recall import CPSCRecall
from channel.ctsi import CTSI
from channel.healthCanada_medicine import HCMedicine
from channel.healthCanada_industrialProducts import HCIP
from channel.healthCanada_food import HCFood
from channel.healthCanada_vehicle import HCVehicle
from channel.mbie import MBIE
from channel.nhtsa import NHTSA
from channel.nihn import NIHN
from channel.opss import OPSS
from channel.rasff import RASFF
from channel.rappelConsommateur import RappelConsommateur
from channel.safetyGate import SAFETYGATE
from channel.taiwanFDA import TAIWANFDA

from common.utils import Utils
import configparser
from database.api import API
from datetime import datetime
import logging
import os
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
            schedule = api.getIndividualSchedule()
            if schedule['idx'] != -1:    
                start = datetime.now()
                logger.info(schedule['chnnlNm'] + '  ::  수집')
                logger.info(f'수집시작시간  ::  {start}')
                job_stats = ''
                cntanr_nm = socket.gethostname()

                colct_bgng_dt = utils.erase_timezone_info(schedule['colctBgngDt'])
                colct_end_dt = utils.erase_timezone_info(schedule['colctEndDt'])
                
                api.updateStartSchedule(schedule['idx'], cntanr_nm)
                if schedule['chnnlCd'] == 1:  # Health Canada - 의약품
                    chnnl = HC_MEDICINE(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 2:  # Health Canada - 공산품
                    chnnl = HC_IP(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 3:  # Health Canada - 식품
                    chnnl = HC_FOOD(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 4:  # Health Canada - 자동차
                    chnnl = HC_VEHICLE(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 5:  # NHTSA
                    chnnl = NHTSA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 6:  # CAA
                    chnnl = CAA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 7:  # 대만 FDA
                    chnnl = TAIWANFDA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 8:  # Safety Gate
                    chnnl = SAFETYGATE(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 9:  # CTSI
                    chnnl = CTSI(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 10:  # OPSS
                    chnnl = OPSS(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 11:  # FSA
                    chnnl = FSA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 12:  # Rappel Consommateur
                    chnnl = RAPPELCONSOMMATEUR(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                # elif schedule['chnnlCd'] == 13:  # BAuA
                #     chnnl = BAUA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                elif schedule['chnnlCd'] == 14:  # BVL
                    chnnl = BVL(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 15:  # AFSCA
                    chnnl = AFSCA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                # elif schedule['chnnlCd'] == 16:  # FSAI - Food Alerts
                #     chnnl = FSAI_FOOD_ALERTS(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                # elif schedule['chnnlCd'] == 17:  # FSAI - Food Allergen Alerts
                #     chnnl = FSAI_FOOD_ALLERGEN_ALERTS(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                # elif schedule['chnnlCd'] == 18:  # BLV - Offentliche Warnungen
                #     chnnl = BLV_PUBLIC_WARNINGS(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                # elif schedule['chnnlCd'] == 19:  # BLV - Ruckrufe
                #     chnnl = BLV_RECALLS(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                # elif schedule['chnnlCd'] == 20:  # Transport Canada
                #     chnnl = TRANSPORT_CANADA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                elif schedule['chnnlCd'] == 21:  # CPSC - 리콜
                    chnnl = CPSC_RECALL(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                # elif schedule['chnnlCd'] == 22:  # 미국 FDA - 리콜
                #     chnnl = US_FDA_RECALL(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                elif schedule['chnnlCd'] == 23:  # NIHN
                    chnnl = NIHN(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                # elif schedule['chnnlCd'] == 24:  # CFS
                #     chnnl = CFS(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                # elif schedule['chnnlCd'] == 25:  # ACCP
                #     chnnl = ACCP(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                elif schedule['chnnlCd'] == 26:  # ACCC
                    chnnl = ACCC(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                # elif schedule['chnnlCd'] == 27:  # TGA
                #     chnnl = TGA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                # elif schedule['chnnlCd'] == 28:  # FSANZ
                #     chnnl = FSANZ(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                elif schedule['chnnlCd'] == 29:  # MBIE
                    chnnl = MBIE(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 30:  # CPSC - 주의보
                    chnnl = CPSC_ALERT(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 31:  # RASFF
                    chnnl = RASFF(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                                                          
                else:
                    logger.info(f"개별 수집기 개발 필요 - {schedule['idx']}, {schedule['chnnlCd']}, {schedule['chnnlNm']}")
                    end = datetime.now()
                    logger.info(f'수집종료시간  ::  {end}')                    
                    api.updateEndSchedule(schedule['idx'], 'E', 0)
                    continue

                if chnnl.error_cnt > 0 and chnnl.colct_cnt > 0:
                    job_stats = 'L'
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
                api.updateEndSchedule(schedule['idx'], job_stats, chnnl.colct_cnt)
                diff = end - start
                logger.info(f'Crawl Time : {diff.seconds} seconds')
            else:
                logger.info('수집 스케쥴 미존재 60초 대기')
                time.sleep(60)
        except Exception as e:
            logger.error(f'수집기 종료  ::  {e}')
            exc_type, exc_obj, tb = sys.exc_info()
            utils.save_colct_log(exc_obj, tb, schedule['chnnlCd'], schedule['chnnlNm'])
        # finally:
        #     # 메일보내기?