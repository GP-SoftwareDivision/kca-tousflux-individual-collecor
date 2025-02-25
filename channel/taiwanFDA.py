from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import os
import random
import requests
import urllib3
import sys
import time
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class TAIWANFDA():
    def __init__(self, chnnl_cd, chnnl_nm, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_cd = chnnl_cd
        self.chnnl_nm = chnnl_nm
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.headers = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding':'gzip, deflate, br, zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'Host':'www.fda.gov.tw',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
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
                    if self.page_num==0: url = 'https://www.fda.gov.tw/UnsafeFood/UnsafeFood.aspx'
                    else: url = f'https://www.fda.gov.tw/UnsafeFood/UnsafeFood.aspx?idx={self.page_num}'
                    self.logger.info('수집 시작')
                    res = requests.get(url=url, headers=self.headers, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)     

                        html = BeautifulSoup(res.text, 'html.parser')
                        recall_list = html.find('table',{'class':'rwd-table'}).find('tbody').find_all('tr')
                        for recall in recall_list:
                            try:
                                dt = recall.find_all('td')[-1].text
                                wrt_dt = dt+' 00:00:00'
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    self.total_cnt += 1
                                    product_url = 'https://www.fda.gov.tw' + recall.find('a')['href']
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
                                    break
                            except Exception as e:
                                self.logger.error(f'상품 디테일 수집 중 에러  >>  {e}')
                    else: 
                        crawl_flag = False
                        raise Exception(f'통신 차단 : {url}')
                    self.page_num += 1  
                    self.logger.info(f'{self.page_num} 페이지로 이동..')  
                except Exception as e:
                    self.logger.error({f'상품리스트 통신 중 에러  >>  {e}'})
                    crawl_flag = False
                    self.error_cnt += 1
                    exc_type, exc_obj, tb = sys.exc_info()
                    self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)
        except Exception as e:
            self.logger.error(f'crawl 에러 >> {e}')
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt}')
            self.logger.info('수집 종료')

    def crawl_detail(self, product_url):
        result = { 'plor':'', 'prdtNm':'', 'prdtImg':'', 'distbBzenty':'', 'hrmflCuz':'', 'bsnmNm':'', 'brand':'', 
                   'flwActn':'', 'wrtDt':'', 'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            custom_headers = self.headers
            if self.page_num==0: referer_url = 'https://www.fda.gov.tw/UnsafeFood/UnsafeFood.aspx'
            else: referer_url = f'https://www.fda.gov.tw/UnsafeFood/UnsafeFood.aspx?idx={self.page_num}'
            custom_headers['Referer']= referer_url

            product_res = requests.get(url=product_url, headers=custom_headers, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)     

                html = BeautifulSoup(product_res.text, 'html.parser')

                title = html.find('div',{'class':'innerBlock'}).find('h3').text #원산지 + 제품명

                try: result['plor'] = title.split('「')[0].strip()
                except Exception as e: self.logger.error(f'원산지 수집 중 에러  >>  ')

                try: result['prdtNm'] = title.split('「')[1].split('」')[0].strip()
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  ')

                try: 
                    img_url = 'https://www.fda.gov.tw'+html.find('ul', {'class':'morePhotoList'}).find('a')['href'].strip()
                    img_nm = img_url.split('id=')[1]
                    img_res = self.utils.download_upload_image('taiwanFDA', img_nm, img_url)
                    if img_res['status'] == 200:
                        result['prdtImgFlPath'] = img_res['path']
                        result['prdtImgFlNm'] = img_res['fileNm']
                    else:
                        self.logger.info(f"이미지 이미 존재 : {img_res['fileNm']}")
                except Exception as e: self.logger.error(f'이미지 수집 중 에러  >>  {e}')

                info_list = html.find('ul', {'class':'resultList'}).find_all('li')
                for info in info_list:
                    try:
                        division = info.find('p',{'class':'RL-th'}).text
                        if division == '進口商(公司名稱)':
                            try: result['distbBzenty'] = info.find('p',{'class':'RL-td'}).text.strip()
                            except Exception as e: raise Exception(f'수입업체 수집 중 에러  >>  ')
                        elif division == '檢驗方法':
                            try: result['hrmflCuz'] = info.find('p',{'class':'RL-td'}).text.strip()
                            except Exception as e: raise Exception(f'위해원인 수집 중 에러  >>  ')              
                        elif division == '不合格原因暨檢出量詳細說明':
                            try: result['hrmflCuz2'] = info.find('p',{'class':'RL-td'}).text.strip()
                            except Exception as e: raise Exception(f'위해원인2 수집 중 에러  >>  ')
                        elif division == '法規限量標準':
                            try: result['hrmflCuz3'] = info.find('p',{'class':'RL-td'}).text.strip()
                            except Exception as e: raise Exception(f'위해원인3 수집 중 에러  >>  ')
                        elif division == '製造廠或國外負責廠商名稱':
                            try: result['bsnmNm'] = info.find('p',{'class':'RL-td'}).text.strip()
                            except Exception as e: raise Exception(f'제조업체 수집 중 에러  >>  ')          
                        elif division == '牌名':
                            try: result['brand'] = info.find('p',{'class':'RL-td'}).text.strip()
                            except Exception as e: raise Exception(f'브랜드 수집 중 에러  >>  ')
                        elif division == '處置情形':
                            try: result['flwActn'] = info.find('p',{'class':'RL-td'}).text.strip()
                            except Exception as e: raise Exception(f'후속조치 수집 중 에러  >>  ')
                        elif division == '發布日期':
                            try: 
                                wrt_dt = info.find('p',{'class':'RL-td'}).text.strip() + ' 00:00:00'
                                result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                            except Exception as e: raise Exception(f'게시일 수집 중 에러  >>  ')
                    except Exception as e:
                        extract_error = False
                        self.logger.error(f'항목 수집 중 에러{e}')
                time.sleep(random.uniform(3,5))
                    
                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result['prdtDtlPgUrl'], result['chnnlNm'], result['prdtNm'], result['wrtDt'])

            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {product_res.status_code}')
            
        except Exception as e:
            self.logger.error(f'crawl_detail 에러 >> {e}')

        return result