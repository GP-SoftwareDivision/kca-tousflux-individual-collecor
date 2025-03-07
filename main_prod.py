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
# from channel.meti import METI
from channel.mbie import MBIE
# from channel.mpi import MPI
from channel.nhtsa import NHTSA
from channel.nihn import NIHN
# from channel.nite import NITE
from channel.nsw import NSW
# from channel.nvwa import NVWA
from channel.opss import OPSS
from channel.rasff import RASFF
from channel.rappelConsommateur import RappelConsommateur
from channel.safetyGate import SAFETYGATE
from channel.taiwanFDA import TAIWANFDA
from channel.tga import TGA
from channel.transportCanada import TransportCanada
# from channel.usda import USDA
# from channel.톈진시시장감독관리위원회 import 톈진시시장감독관리위원회
from channel.philippinesFDA import PhilippinesFDA

from common.utils import Utils
import configparser
from database.api import API
from datetime import datetime
from common.utils import Utils
from channel.recall_china import RECALL_CHINA

import configparser
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
                if schedule['chnnlCd'] == 64:  # Safety Gate (위험 제품 신속주의보시스템)
                    chnnl = SAFETYGATE(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 65:  # 대만 FDA (식품의약국)
                    chnnl = TAIWANFDA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 66:  # CTSI (거래기준연구소)
                    chnnl = CTSI(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 67:  # NHTSA (도로교통안전국)
                    chnnl = NHTSA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 68:  # CAA (소비자청)
                    chnnl = CAA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 69:  # CCPC (경쟁소비자보호위원회)
                    chnnl = CCPC(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 70:  # "Health Canada (캐나다 보건부) - 자동차"
                    chnnl = HCVehicle(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 71:  # "Health Canada (캐나다 보건부) - 의약품"
                    chnnl = HCMedicine(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 72:  # "Health Canada (캐나다 보건부) - 공산품"
                    chnnl = HCIP(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 73:  # "Health Canada (캐나다 보건부) - 식품"
                    chnnl = HCFood(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 74:  # OPSS (제품안전표준사무소)
                    chnnl = OPSS(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 75:  # FSA (식품표준원)
                    chnnl = FSA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 76:  # Rappel Consommateur (소비자 리콜 통합 포털)
                    chnnl = RappelConsommateur(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 77:  # BAuA (연방산업안전보건연구소)
                    chnnl = BAUA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 78:  # BVL (연방소비자보호식품안전청)
                    chnnl = BVL(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 79:  # AFSCA (연방식품안전청)
                    chnnl = AFSCA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 80:  # FSAI (식품안전청) - Food Alerts
                    chnnl = FSAIFoodAlerts(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 81:  # FSAI (식품안전청) - Food Allergen Alerts
                    chnnl = FSAIFoodAllergenAlerts(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 82:  # BLV (식품안전수의약청) - Offentliche Warnungen
                    chnnl = BVL(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 83:  # BLV (식품안전수의약청) - Ruckrufe
                    chnnl = BVL(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                # elif schedule['chnnlCd'] == 84:  # NVWA (식품안전청)
                #     chnnl = NVWA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                elif schedule['chnnlCd'] == 89:  # Transport Canada (캐나다 교통국)
                    chnnl = TransportCanada(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 90:  # CPSC (소비자제품안전위원회) - 리콜
                    chnnl = CPSCRecall(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 91:  # FDA (식품의약국) - 리콜
                    chnnl = FDARecall(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                # elif schedule['chnnlCd'] == 92:  # USDA (농무부)
                #     chnnl = USDA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                # elif schedule['chnnlCd'] == 94:  # NITE (제품평가기술기반기구)
                #     chnnl = NITE(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                # elif schedule['chnnlCd'] == 95:  # METI (경제산업성)
                #     chnnl = METI(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                elif schedule['chnnlCd'] == 96:  # NIHN (국립보건영양원)
                    chnnl = NIHN(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 98:  # CFS (홍콩 식품안전센터)
                    chnnl = CFS(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 99:  # Consumer Council (홍콩 소비자위원회)
                    chnnl = ConsumerCouncil(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                # elif schedule['chnnlCd'] == 100:  # 톈진시 시장감독관리위원회
                #     chnnl = 톈진시시장감독관리위원회(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                # elif schedule['chnnlCd'] == 102:  # DTI (필리핀 무역산업부)
                #     chnnl = DTI(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                elif schedule['chnnlCd'] == 103:  # 필리핀 FDA (식품의약청) 
                    chnnl = PhilippinesFDA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 104:  # ACCP(ASEAN 소비자보호 위원회)
                    chnnl = ACCP(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 105:  # ACCC (호주경쟁소비자위원회)
                    chnnl = ACCC(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 106:  # NSW (호주 뉴사우스웨일즈식품청)
                    chnnl = NSW(schedule['chnnlCd'], schedule['chnnlNm'], schedule['url'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 107:  # TGA (호주 식약처)
                    chnnl = TGA(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 108:  # FSANZ (호주뉴질랜드 식품기준청)
                    chnnl = FSANZ(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 109:  # MBIE(뉴질랜드 기업혁신고용부)
                    chnnl = MBIE(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                # elif schedule['chnnlCd'] == 110:  # MPI (1차산업부)
                #     chnnl = MPI(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                elif schedule['chnnlCd'] == 114:  # CPSC (소비자제품안전위원회) - 주의보
                    chnnl = CPSCAlert(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 116:  # RASFF (식품사료안전주의보)
                    chnnl = RASFF(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] >= 124 and schedule['chnnlCd'] <= 134:  # 중국 제품 안전 및 리콜 정보 네트워크
                    chnnl = RECALL_CHINA(schedule['chnnlCd'], schedule['chnnlNm'], schedule['url'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()
                elif schedule['chnnlCd'] == 113 or schedule['chnnlCd'] == 135 or schedule['chnnlCd'] == 136:  # FDA (식품의약국) - 주의보
                    chnnl = FDAAlert(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                    chnnl.crawl()                
                # elif schedule['chnnlCd'] == 135:  # FDA (식품의약국) - 주의보2
                #     chnnl = FDAAlert(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()
                # elif schedule['chnnlCd'] == 136:  # FDA (식품의약국) - 주의보3
                #     chnnl = FDAAlert(schedule['chnnlCd'], schedule['chnnlNm'], colct_bgng_dt, colct_end_dt, logger, api)
                #     chnnl.crawl()                                                                                                    

                else:
                    logger.info(f"개별 수집기 개발 필요 - {schedule['idx']}, {schedule['chnnlCd']}, {schedule['chnnlNm']}")
                    end = datetime.now()
                    logger.info(f'수집종료시간  ::  {end}')                    
                    api.updateEndSchedule(schedule['idx'], 'E', 0, 0)
                    continue

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
                api.updateEndSchedule(schedule['idx'], job_stats, chnnl.total_cnt, chnnl.colct_cnt, chnnl.duplicate_cnt, chnnl.error_cnt)
                diff = end - start
                logger.info(f'Crawl Time : {diff.seconds} seconds')
            else:
                logger.info('수집 스케쥴 미존재 60초 대기')
                time.sleep(60)
        except Exception as e:
            logger.error(f'수집기 종료  ::  {e}')
            exc_type, exc_obj, tb = sys.exc_info()
            utils.save_colct_log(exc_obj, tb, schedule['chnnlCd'], schedule['chnnlNm'])