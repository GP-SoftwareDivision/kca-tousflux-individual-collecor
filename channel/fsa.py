from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time

class FSA():
    def __init__(self, chnnl_cd, chnnl_nm, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_cd = chnnl_cd
        self.chnnl_nm = chnnl_nm
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 1
        self.header = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding':'zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        }

        self.prdt_dtl_err_url = []

        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0

        self.utils = Utils(logger, api)

    def crawl(self):
            try:
                crawl_flag = True     
                retry_num = 0
                headers = self.header
                while(crawl_flag):
                    try:
                        if self.page_num == 1:
                            headers['Referer'] = 'https://www.food.gov.uk/search?filter_type%5BFood%20alert%5D=Food%20alert&filter_type%5BAllergy%20alert%5D=Allergy%20alert'
                            url = 'https://www.food.gov.uk/search-api?filter_type%5BFood%20alert%5D=Food%20alert&filter_type%5BAllergy%20alert%5D=Allergy%20alert'
                        else: 
                            headers['Referer'] = f'https://www.food.gov.uk/search?filter_type%5BFood+alert%5D=Food+alert&filter_type%5BAllergy+alert%5D=Allergy+alert&page={self.page_num}'
                            url = f'https://www.food.gov.uk/search-api?filter_type%5BFood+alert%5D=Food+alert&filter_type%5BAllergy+alert%5D=Allergy+alert&page={self.page_num}'   
                        self.logger.info('수집 시작')
                        res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)  
                            
                            res_json = json.loads(res.text)
                            datas = res_json['#data']['items']

                            if datas == []: 
                                if retry_num >= 10: 
                                    crawl_flag = False
                                    self.logger.info('데이터가 없습니다.')
                                else:
                                    retry_num += 1
                                    continue

                            for data in datas:
                                try:
                                    try: 
                                        wrt_dt_day = datas[0]['updated'].split('T')[0]
                                        wrt_dt_time = datas[0]['updated'].split('T')[1]
                                    except: 
                                        wrt_dt_day = datas[0]['created'].split('T')[0]
                                        wrt_dt_time = datas[0]['created'].split('T')[1]                                    
                                    finally:
                                        wrt_dt = self.utils.parse_date(wrt_dt_day, self.chnnl_nm) + ' ' + wrt_dt_time

                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        product_url = data['url']
                                        dup_flag, colct_data = self.crawl_detail(product_url)
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
                                            # crawl_flag = False
                                            # break
                                        else: self.logger.error(f"IDX 확인 필요  >> {colct_data['idx']} ( {product_url} )")                                                
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
                            raise Exception(f'통신 차단 : {url}')
                    except Exception as e:
                        self.logger.error(f'crawl 통신 중 에러 >> {e}')
                        crawl_flag = False
                        self.error_cnt += 1
                        exc_type, exc_obj, tb = sys.exc_info()
                        self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)
            except Exception as e:
                self.logger.error(f'{e}')
            finally:
                self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
                self.logger.info('수집종료')
                
    def crawl_detail(self, product_url):
        result = { 'wrtDt':'', 'prdtNm':'', 'prdtDtlCtn':'', 'hrmflCuz':'', 'flwActn':'', 
                   'atchFlNm':'', 'atchFlPath':'', 'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        try:
            custom_header = self.header
            if self.page_num == 1: referer_url = 'https://www.food.gov.uk/search?filter_type%5BFood%20alert%5D=Food%20alert&filter_type%5BAllergy%20alert%5D=Allergy%20alert'
            else: referer_url = f'https://www.food.gov.uk/search?filter_type%5BFood+alert%5D=Food+alert&filter_type%5BAllergy+alert%5D=Allergy+alert&page={self.page_num}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')
                try:
                    wrt_dt = self.utils.parse_date(html.find('div', {'class':'article-hero__toolbar__last-updated'}).text.strip(), self.chnnl_nm) + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: raise Exception(f'게시일 수집 중 에러  >>  ')

                products = html.find('div', {'class':'product-details'}).find_all('table')

                try:
                    prdt_nm = ', '.join([product.find('caption').text.strip() for product in products])
                    result['prdtNm'] = self.utils.get_clean_string(prdt_nm)
                except Exception as e: raise Exception(f'제품명 수집 중 에러  >>  ')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    prdt_dtl_ctn = ''
                    for product in products:
                        try:
                            prdt_dtl_ctn += ' | '.join([f"{row.find('th').text} : {row.find('td').text}" for row in product.find_all('tr')])
                            if product != products[-1]: prdt_dtl_ctn += '\n'
                        except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >>  {e}')
                    result['prdtDtlCtn'] = prdt_dtl_ctn

                    infos = html.find('div', {'class':'content-layout__right-content'}).find_all('h2')
                    for info in infos:
                        title = info.text.strip()
                        content = info.find_next_sibling()
                        try:
                            if title == 'Risk statement':
                                try:
                                    result['hrmflCuz'] = content.text.strip()
                                except Exception as e: raise Exception(f'위해원인 수집 중 에러  >>  {e}')
                            elif title == 'Our advice to consumers':
                                try:
                                    result['flwActn'] = content.text.strip()
                                except Exception as e: raise Exception(f'후속조치 수집 중 에러  >>  {e}')
                        except Exception as e: self.logger.error(f'{e}')

                    try: 
                        atchl_url = html.find('div', {'class':'document-download__content document-download__content--'}).find('a')['href']
                        atchl_res = self.utils.download_upload_atchl(self.chnnl_nm, atchl_url)
                        if atchl_res['status'] == 200:
                            result['atchFlPath'] = atchl_res['path']
                            result['atchFlNm'] = atchl_res['fileNm']
                        else:
                            self.logger.info(f"첨부파일 이미 존재 : {atchl_res['fileNm']}")
                    except Exception as e: self.logger.error(f'첨부파일 추출 실패  >>  {e}')

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')

        return dup_flag, result