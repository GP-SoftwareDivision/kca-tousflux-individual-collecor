from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time

class ACCC():
    def __init__(self, chnnl_cd, chnnl_nm, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_cd = chnnl_cd
        self.chnnl_nm = chnnl_nm
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.header = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding':'zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        }

        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0

        self.utils = Utils(logger, api)


    def crawl(self):
            try:
                crawl_flag = True     
                while(crawl_flag):
                    try:
                        headers = self.header
                        if self.page_num == 0: url = 'https://www.productsafety.gov.au/recalls?source=recalls'
                        else:
                            headers['Referer'] = url
                            url = f'https://www.productsafety.gov.au/recalls?source=recalls&&page={self.page_num}'
                        self.logger.info('수집 시작')
                        res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')

                            datas = html.find('div', {'class':'view-content'}).find_all('div', {'class':'card-wrapper contextual-region h-100 col-12 psa-recall'})
                            for data in datas:
                                try:
                                    date_day = self.utils.parse_date_with_locale(data.find('time')['datetime'].split('T')[0], self.chnnl_nm)
                                    date_time = data.find('time')['datetime'].split('T')[1].replace('Z','')
                                    wrt_dt = date_day + ' ' + date_time                                    
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        product_url = 'https://www.productsafety.gov.au' + data.find('a')['href']
                                        colct_data = self.crawl_detail(product_url)
                                        req_data = json.dumps(colct_data)
                                        insert_res = self.api.insertData2Depth(req_data)
                                        if insert_res == 0:
                                            self.colct_cnt += 1
                                        elif insert_res == 1:
                                            self.error_cnt += 1
                                        elif insert_res == 2 :
                                            self.duplicate_cnt += 1
                                    elif wrt_dt < self.start_date: 
                                        crawl_flag = False
                                        self.logger.info(f'수집기간 내 데이터 수집 완료')
                                        break
                                except Exception as e:
                                    self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                            self.page_num += 1
                            if crawl_flag: self.logger.info(f'{self.page_num}페이지로 이동 중..')
                        else:
                            crawl_flag = False
                            raise Exception('통신 차단')                            
                    except Exception as e:
                        self.logger.error(f'crawl 통신 중 에러 >> {e}')
                        crawl_flag = False
                        self.error_cnt += 1
                        exc_type, exc_obj, tb = sys.exc_info()
                        self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)
            except Exception as e:
                self.logger.error(f'{e}')
            finally:
                self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt}')
                self.logger.info('수집종료')
                
    def crawl_detail(self, product_url):
        result = { 'prdtNm':'', 'wrtDt':'', 'prdtDtlCtn':'', 'hrmflCuz':'', 'hrmflCuz2':'', 
                   'flwActn':'', 'url':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        # 게시일, 위해원인 hrmfl_cuz, 제품 상세내용 prdt_dtl_ctn, 제품명 prdt_nm, 위해/사고?, 정보출처 recall_srce?
        try:
            custom_header = self.header
            if self.page_num == 0: referer_url = 'https://www.productsafety.gov.au/recalls?source=recalls'
            else: referer_url = f'https://www.productsafety.gov.au/recalls?source=recalls&&page={self.page_num}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try: result['prdtNm'] = self.utils.get_clean_string(html.find('div',{'class':'backdrop header-wrapper'}).find('h1').text.strip())
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  ')

                try: 
                    date_day = self.utils.parse_date_with_locale(html.find('div',{'class':'accc-field__section--metadata'}).find('time')['datetime'].split('T')[0], self.chnnl_nm)
                    date_time = html.find('div',{'class':'accc-field__section--metadata'}).find('time')['datetime'].split('T')[1].replace('Z','')
                    wrt_dt = date_day + ' ' + date_time
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  ')

                items = html.find('main').find_all('h2', {'class':'field__label'})
                for item in items:
                    title = item.text.strip()
                    content = item.find_next_siblings()[0].text.strip()                    
                    try:
                        if title == 'Product description':
                            try: result['prdtDtlCtn'] = content
                            except Exception as e: raise Exception(f'')
                        elif title == 'Reason the product is recalled':
                            try: result['hrmflCuz'] = content
                            except Exception as e: raise Exception(f'')
                        elif title == 'The hazards to consumers':
                            try: result['hrmflCuz2'] = content
                            except Exception as e: raise Exception(f'')
                        elif title == 'What consumers should do':
                            try: result['flwActn'] = content
                            except Exception as e: raise Exception(f'')                                                                                                                                                                                               
                    except Exception as e:
                        self.logger.error(f'{e}')
            
                result['url'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result['url'], self.chnnl_nm, result['prdtNm'])                            
            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {product_res.status_code}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result