import base64
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time
import urllib
from zoneinfo import ZoneInfo

class RASFF():
    def __init__(self, chnnl_cd, chnnl_nm, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_cd = chnnl_cd
        self.chnnl_nm = chnnl_nm        
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 1
        self.header = {
            'Accept':'application/json, text/plain, */*',
            'Accept-Encoding':'gzip, deflate, br, zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'Content-Type':'application/json',
            'Host':'webgate.ec.europa.eu',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }

        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0
        self.prdt_dtl_err_url = []

        self.utils = Utils(logger, api)        

    def crawl(self):
        try: 
            temp_start_date = datetime.strptime(self.start_date,'%Y-%m-%d %H:%M:%S')
            temp_end_date = datetime.strptime(self.end_date,'%Y-%m-%d %H:%M:%S')

            header_start_date = datetime.strftime(temp_start_date, '%d-%m-%Y 00:00:00')
            header_end_date = datetime.strftime(temp_end_date, '%d-%m-%Y 00:00:00')

            url = 'https://webgate.ec.europa.eu/rasff-window/backend/public/notification/search/consolidated/'
            data = {
                "parameters": {"pageNumber": self.page_num, "itemsPerPage": 100, "sorting": "desc", "ordering": "notificationECValidationDate"},
                "notificationReference": None,
                "subject": None,
                "ecValidDateFrom":header_start_date,
                "ecValidDateTo":header_end_date,
                "notifyingCountry": None,
                "originCountry": [5131],
                "distributionCountry": None,
                "notificationType": None,
                "notificationStatus": None,
                "notificationClassification": None,
                "notificationBasis": None,
                "productCategory": None,
                "actionTaken": None,
                "hazardCategory": None,
                "riskDecision": None
            }

            headers = self.header
            referer = self.make_header_referer(temp_start_date, temp_end_date)
            if referer == '': raise Exception('referer 만드는 중 에러 발생')
            headers['Referer'] = referer
            headers['Origin'] = 'https://webgate.ec.europa.eu'
            
            res = requests.post(url=url, headers=self.header, json=data, verify=False, timeout=600)                
            if res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)     

                res_json = json.loads(res.text)
                datas = res_json['notifications']
                if len(datas) == 0:
                    self.logger.info('데이터가 없습니다.')

                for data in datas:
                    try:
                        product_id = data['notifId']
                        product_url = f'https://webgate.ec.europa.eu/rasff-window/backend/public/notification/view/id/{product_id}/'

                        dup_flag, colct_data = self.crawl_detail(product_id)
                        if dup_flag == 0:
                            insert_res = self.utils.insert_data(colct_data)
                            if insert_res == 0:
                                self.colct_cnt += 1
                            elif insert_res == 1:
                                self.error_cnt += 1
                                self.logger.error(f'게시글 수집 오류 > {product_url}')
                                self.prdt_dtl_err_url.append(product_url)
                        elif dup_flag == 2:
                            self.duplicate_cnt += 1
                        else: self.logger.error(f"IDX 확인 필요  >> {colct_data['idx']} ( {product_url} )")
                    except Exception as e:
                        self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                self.page_num += 1
            else: raise Exception(f'통신 차단 : {url}')
        except Exception as e:
            self.logger.error(f'crawl 통신 중 에러 >> {e}')
            self.error_cnt += 1
            exc_type, exc_obj, tb = sys.exc_info()
            self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)            
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
            self.logger.info('수집종료')

    def crawl_detail(self, product_id):
        dup_flag = -1
        result = {'recallNo':'', 'wrtDt':'', 'flwActn':'', 'hrmflCuz':'', 'recallNtn':'',
                  'prdtNm':'', 'prdtDtlPgUrl':'', 'chnnlCd':0, 'idx':''}
        try:
            product_url = f'https://webgate.ec.europa.eu/rasff-window/backend/public/notification/view/id/{product_id}/'
            custom_header = self.header
            custom_header['Referer'] = product_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                 
                product_res_json = json.loads(product_res.text)

                try: result['prdtNm'] = product_res_json['product']['description'].strip()
                except Exception as e: self.logger.error(f'상품명 추출 실패  >>  {e}')

                try:
                    date_text = self.utils.parse_date(product_res_json['ecValidationDate'].strip().split(' ')[0], self.chnnl_nm)
                    date_day = product_res_json['ecValidationDate'].strip().split(' ')[1]
                    wrt_dt = f'{date_text} {date_day}'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat()
                except Exception as e: self.logger.error(f'작성일 추출 실패  >>  {e}')

                result['prdtDtlPgUrl'] = f'https://webgate.ec.europa.eu/rasff-window/screen/notification/{product_id}'
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)   

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:            
                    try: result['recallNo'] = product_res_json['reference'].strip()
                    except Exception as e: self.logger.error(f'리콜넘버 추출 실패  >>  {e}')

                    try: result['flwActn'] = product_res_json['notificationBasis']['description'].strip()
                    except Exception as e: self.logger.error(f'후속조치 추출 실패  >>  {e}')

                    try: 
                        rist_decision = product_res_json['risk']['riskDecision'].strip()
                        try: hazard_observed = product_res_json['risk']['hazardObserved'].strip()
                        except: hazard_observed = 'Not defined'
                        result['hrmflCuz'] = f'{rist_decision}  |  {hazard_observed}'
                    except Exception as e: self.logger.error(f'위해원인 추출 실패  >>  {e}')

                    try: result['recallNtn'] = product_res_json['product']['measures'][0]['takenBy']['organizationName'].strip()
                    except Exception as e: self.logger.error(f'리콜국 추출 실패  >>  {e}')

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')

        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')
            self.prdt_dtl_err_url.append(product_url)
        return dup_flag, result

    def make_header_referer(self, temp_start_date, temp_end_date):
        result = ''
        try:
                kst_start_date = temp_start_date.replace(tzinfo=ZoneInfo("Asia/Seoul")) # KST (UTC+9) 설정
                utc_time = kst_start_date.astimezone(ZoneInfo("UTC")) # UTC 변환                
                utc_start_date = utc_time.strftime("%Y-%m-%dT%H:%M:%S.000Z") # 원하는 형식으로 출력

                kst_end_date = temp_end_date.replace(tzinfo=ZoneInfo("Asia/Seoul")) # KST (UTC+9) 설정
                utc_time = kst_end_date.astimezone(ZoneInfo("UTC")) # UTC 변환                
                utc_end_date = utc_time.strftime("%Y-%m-%dT%H:%M:%S.000Z") # 원하는 형식으로 출력

                data = {"date": {"startRange": utc_start_date,"endRange": utc_end_date},
                    "countries": {"originCountry": [[5131]]},"type": {},"notificationStatus": {},
                    "product": {},"risk": {},"reference": "","subject": ""}

                json_str = json.dumps(data, separators=(',', ':')) # JSON 문자열 변환 (불필요한 공백 제거)

                encoded_bytes = base64.urlsafe_b64encode(json_str.encode('utf-8')) # Base64 URL-safe 인코딩
                encoded_str = encoded_bytes.decode('utf-8')
                
                final_encoded = urllib.parse.quote(encoded_str) # URL 인코딩 (Base64의 '=' → '%3D' 변환)
                result = final_encoded
        except Exception as e:
            self.logger.error(f'{e}')

        return result