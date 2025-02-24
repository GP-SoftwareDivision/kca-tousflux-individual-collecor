from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import os
import random
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class HC_FOOD():
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
            'Host':'recalls-rappels.canada.ca',
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
                        if self.page_num == 0: url = 'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A144'
                        else: url = f'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A144&page=%2C1%2C{self.page_num}'
                        self.logger.info('수집 시작')
                        res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')
                            datas = html.find_all('div', {'class':'search-result views-row'})

                            for data in datas:
                                try:
                                    wrt_dt = data.find('span', {'class':'ar-type'}).text.split('|')[1].strip() + ' 00:00:00'
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        product_url = 'https://recalls-rappels.canada.ca' + data.find('a')['href']
                                        colct_data = self.crawl_detail(product_url)
                                        insert_res = self.utils.insert_data(colct_data)
                                        if insert_res == 0:
                                            self.colct_cnt += 1
                                        elif insert_res == 1:
                                            self.error_cnt += 1
                                            self.utils.save_colct_log(f'게시글 수집 오류 > {product_url}', '', self.chnnl_cd, self.chnnl_nm, 1)
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
        result = { 'wrtDt':'', 'prdtNm':'', 'prdtImg':'', 'prdtDtlCtn':'', 'hrmflCuz':'', 'flwActn':'', 'ntslCrst':'', 
                   'ntslPerd':'', 'plor':'', 'distbBzenty':'', 'url':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        try:
            custom_header = self.header
            if self.page_num == 0: referer_url = 'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A144'
            else: referer_url = f'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A144&page=%2C1%2C{self.page_num}'            
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try: result['prdtNm'] = html.find('h1', {'id':'wb-cont'}).text.strip()
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  ') 

                try: 
                    wrt_dt = html.find('time').text.strip() + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  ')

                affected_products = html.find('div', {'class':'ar-affected-products ar-section'}).find('table').find('tbody').find_all('tr')
                for product in affected_products:
                    try:
                        prdt_ctl = ''
                        descriptions = product.find_all('td')
                        for description in descriptions:
                            try:
                                title = description['data-label']
                                text = description.text.strip()
                                prdt_ctl += f'{title} = {text} | ' if description != descriptions[-1] else f'{title} = {text}'
                            except Exception as e:
                                self.logger.error(f'{e}')
                    except Exception as e:
                        self.logger.error(f'affected_products  >>  {e}')

                try: result['prdtDtlCtn'] = prdt_ctl
                except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >>  ')

                try:
                    images = html.find('div',{'class':'product-images'}).find_all('a')
                    image_paths = []
                    for idx, image in enumerate(images):
                        try:
                            img_url = 'https://recalls-rappels.canada.ca'+image['href']
                            img_nm = json.loads(image['data-media'])['token']
                            res = self.utils.download_upload_image(self.chnnl_nm, img_nm, img_url)
                            if res == 1: image_paths.append(res)
                        except Exception as e:
                            self.logger.error(f'{idx}번째 이미지 수집 중 에러  >>  ')
                    result['prdtImg'] = self.utils.get_clean_string(' : '.join(image_paths))
                except Exception as e: self.logger.error(f'제품 이미지 수집 중 에러  >>  '); extract_error = True

                try: 
                    issue_texts = html.find('div', {'class':'ar-issue-long ar-section'}).find('h2')
                    if issue_texts != None: issue_texts = issue_texts.find_next_siblings()
                    else: issue_texts = []
                    issue = ' '.join([issue_text.text.strip() for issue_text in issue_texts])
                    result['hrmflCuz'] = self.utils.get_clean_string(issue)
                except Exception as e: self.logger.error(f'위해원인 수집 중 에러  >>  ') 

                try:
                    flw_actn_texts = html.find('div', {'class':'ar-action-long ar-section'}).find('h2')
                    if flw_actn_texts != None: flw_actn_texts = flw_actn_texts.find_next_siblings()
                    else: flw_actn_texts= []
                    flw_actn = ' '.join([flw_actn_text.text.strip() for flw_actn_text in flw_actn_texts])
                    result['flwActn'] = self.utils.get_clean_string(flw_actn)
                except Exception as e: self.logger.error(f'후속조치 수집 중 에러  >>  ')    

                additional_information = html.find('div',{'class':'ar-additional-info ar-section'}).find_all('details')
                background = [info for info in additional_information if info.summary and 'Background' in info.summary.text]
                if background != []:
                    items = background[0].find_all('p')
                    for i in range(0, len(items), 2):
                        try:
                            if 'Number Sold' in items[i].text:
                                try: result['ntslCrst'] = self.utils.get_clean_string(items[i+1].text.strip())
                                except Exception as e: raise Exception(f'판매현황 수집 중 에러  >>  ')
                            elif 'Time Period Sold' in items[i].text:
                                try: result['ntslPerd'] = self.utils.get_clean_string(items[i+1].text.strip())
                                except Exception as e: raise Exception(f'판매기간 수집 중 에러  >>  ')
                            elif 'Place of Origin' in items[i].text:
                                try: result['plor'] = self.utils.get_clean_string(items[i+1].text.strip())
                                except Exception as e: raise Exception(f'원산지 수집 중 에러  >>  ')
                        except Exception as e: self.logger.error(f'판매현황/판매기간/원산지 수집 중 에러  >>  {e}')

                additional_information = html.find('div',{'class':'ar-additional-info ar-section'}).find_all('details')
                background = [info for info in additional_information if info.summary and 'Background' in info.summary.text]
                if background != []:
                    items = background[0].find_all('p')
                    for i in range(0, len(items), 2):
                        try:
                            if 'Number Sold' in items[i].text:
                                try: result['ntslCrst'] = self.utils.get_clean_string(items[i+1].text.strip())
                                except Exception as e: raise Exception(f'판매현황 수집 중 에러  >>  ')
                            elif 'Time Period Sold' in items[i].text:
                                try: result['ntslPerd'] = self.utils.get_clean_string(items[i+1].text.strip())
                                except Exception as e: raise Exception(f'판매기간 수집 중 에러  >>  ')
                            elif 'Place of Origin' in items[i].text:
                                try: result['plor'] = self.utils.get_clean_string(items[i+1].text.strip())
                                except Exception as e: raise Exception(f'원산지 수집 중 에러  >>  ')
                        except Exception as e: self.logger.error(f'판매현황/판매기간/원산지 수집 중 에러  >>  {e}')

                result['url'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result['url'], self.chnnl_nm, result['prdtNm'])                            
            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {product_res.status_code}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result
