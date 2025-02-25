import re
from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time

class CFS():
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
            'Accept-Encoding':'gzip, deflate, br',
            'Accept-Language':'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        }

        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0

        self.utils = Utils(logger, api)


    def crawl(self):
            try:
                start_dt = datetime.strptime(self.start_date, "%Y-%m-%d %H:%M:%S")
                end_dt = datetime.strptime(self.end_date, "%Y-%m-%d %H:%M:%S")
                years = list(set(range(start_dt.year, end_dt.year + 1)))[::-1]
                org_url = 'https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/whatsnew_fa_<%year%>.html'
                for year in years:
                    try:
                        headers = self.header
                        self.logger.info('수집 시작')
                        url = org_url.replace('<%year%>', str(year))

                        res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                        self.logger.info(f'{url}페이지로 이동 중..')
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')

                            datas = html.find_all('tr', {'class': 'datarow'})
                            for data in datas:
                                try:
                                    product_url = 'https://www.cfs.gov.hk' + data.find('a')['href']
                                    date_day = datetime.strptime(data.find('td', {'class': 'subHeader'}).text.strip(), '%d.%m.%Y').strftime('%Y-%m-%d')
                                    wrt_dt = date_day + ' 00:00:00'
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
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
                                        self.logger.info(f'수집기간 내 데이터 수집 완료')
                                        break
                                except Exception as e:
                                    self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                        else:
                            raise Exception('통신 차단')                            
                    except Exception as e:
                        self.logger.error(f'crawl 통신 중 에러 >> {e}')
                        self.error_cnt += 1
                        exc_type, exc_obj, tb = sys.exc_info()
                        self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)
                        break
            except Exception as e:
                self.logger.error(f'{e}')
            finally:
                self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt}')
                self.logger.info('수집종료')
                
    def crawl_detail(self, product_url):
        extract_error = True
        result = {'prdtNm':'', 'wrtDt':'', 'prdtDtlCtn':'', 'brand':'', 'distbBzenty': '',
                  'hrmflCuz':'', 'hrmflCuz2':'', 'flwActn':'', 'flwActn2':'', 
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        # 위해원인1, 후속조치1, 제품명, 브랜드, 제품상세내용, 유통업체, 위해원인2, 후속조치2, 게시일
        try:
            custom_header = self.header

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try:
                    tags = html.find('table').find_all('th')
                    for tag in tags:
                        header = tag.text.strip()
                        td = tag.find_next_sibling('td')
                        td_text = self.utils.get_clean_string(td.text.strip()) if td else ''
                        if 'Issue Date' in header:
                            date_day = datetime.strptime(td_text, '%d.%m.%Y').strftime('%Y-%m-%d')
                            wrt_dt = date_day + ' 00:00:00'
                            result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                        elif 'Product Name' in header:
                            result['prdtNm'] = td_text
                            result['prdtDtlCtn'] = td_text
                        elif 'Reason For Issuing Alert' in header:
                            result['hrmflCuz'] = td_text
                        elif 'Action Taken by the Centre for Food Safety' in header:
                            result['flwActn'] = td_text
                        elif 'Advice to Consumers' in header:
                            result['flwActn2'] = td_text
                except Exception as e: self.logger.error(f'제품 정보 수집 중 에러  >>  {e}')

                try:
                    image_list = []
                    images = html.find('div', {'id': 'content'}).find_all('img')
                    for idx, image in enumerate(images):
                        try:
                            img_url = 'https://www.cfs.gov.hk' + image['src']
                            file_name = img_url.split('/')[-1]
                            res = self.utils.download_upload_image(self.chnnl_nm, file_name, img_url) #  chnnl_nm, prdt_nm, idx, url
                            if res != '': image_list.append(res)
                        except Exception as e: self.logger.error(f'{idx}번째 이미지 추출 중 에러')
                    result['prdtImg'] = ' : '.join(image_list)
                except Exception as e: self.logger.error(f'제품 이미지 수집 중 에러  >>  {e}')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)                            
            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {product_res.status_code}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result