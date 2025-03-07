from bs4 import BeautifulSoup
from common.utils import Utils
import copy
from datetime import datetime
import random
import re
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class FDAAlert():
    def __init__(self, chnnl_cd, chnnl_nm, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_cd = chnnl_cd
        self.chnnl_nm = chnnl_nm
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.header = {
            'Accept':'application/json, text/javascript, */*; q=0.01',
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
                while(crawl_flag):
                    try:
                        url = f'https://www.fda.gov/food/recalls-outbreaks-emergencies/alerts-advisories-safety-information'
                        self.logger.info('수집 시작')
                        res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                       

                            html = BeautifulSoup(res.text, features='html.parser')
                            
                            items = html.find_all('div', {'class':'panel-group'})
                            if self.chnnl_cd == 113: datas = [item.find('ul').find_all('li') for item in items if item.find('div')['title'] == 'Food, Beverages, and Dietary Supplements'][0]
                            elif self.chnnl_cd == 135: datas = [item.find('ul').find_all('li') for item in items if item.find('div')['title'] == 'Infant Formula & Other Infant/Baby Products'][0]
                            elif self.chnnl_cd == 136: datas = [item.find('ul').find_all('li') for item in items if item.find('div')['title'] == 'Shellfish'][0]
                            
                            for data in datas:
                                try:
                                    product_url = 'https://www.fda.gov' + data.find('a')['href']
                                    date_flag, dup_flag, colct_data = self.crawl_detail(product_url)
                                    if date_flag:
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
                                    else:
                                        crawl_flag = False
                                        self.logger.info(f'수집기간 내 데이터 수집 완료')
                                        break          
                                except Exception as e:
                                    self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                        else: 
                            crawl_flag = False
                            raise Exception(f'통신 차단 :{url}')
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
        date_flag = True
        result = { 'wrtDt':'', 'prdtNm':'', 'prdtDtlCtn':'', 'prdtImgFlPath':'', 'prdtImgFlNm':'',
                    'flwActn':'', 'hrmflCuz':'', 'flwActn2':'',  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        try:
            custom_header = self.header
            custom_header['Referer'] = 'https://www.fda.gov/food/recalls-outbreaks-emergencies/alerts-advisories-safety-information'

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)

                html = BeautifulSoup(product_res.text, 'html.parser')

                main = html.find('main')
                wrt_dt_date = self.utils.parse_date(html.find('aside', {'role':'complementary'}).find('time')['datetime'].replace('Z','').split('T')[0], self.chnnl_nm)
                wrt_dt_time = html.find('aside', {'role':'complementary'}).find('time')['datetime'].replace('Z','').split('T')[1]
                wrt_dt = wrt_dt_date + ' ' + wrt_dt_time
                # try:
                #     date = main.find('div', {'role':'main'}).find('h2').find_next_sibling().text.strip()
                #     wrt_dt = self.utils.parse_date(date, self.chnnl_nm) + ' 00:00:00'
                # except:
                #     date_yn = main.find('div', {'role':'main'}).find('p').find('a')
                #     if date_yn: date = main.find('div', {'role':'main'}).find_all('p')[1].text.strip()
                #     else: date = main.find('div', {'role':'main'}).find('p').text.strip()
                #     wrt_dt = self.utils.parse_date(date, self.chnnl_nm) + ' 00:00:00'
                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                    self.total_cnt += 1

                    try: result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                    except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  ')

                    try: result['prdtNm'] = main.find('h1', {'class':'content-title text-center'}).text.strip()
                    except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}')
                    
                    result['prdtDtlPgUrl'] = product_url
                    result['chnnlNm'] = self.chnnl_nm
                    result['chnnlCd'] = self.chnnl_cd
                    result['idx'] = self.utils.generate_uuid(result)

                    dup_flag = self.api.check_dup(result['idx'])
                    if dup_flag == 0:
                        items = main.find('div', {'role':'main'}).find_all('h2')
                        org_result = copy.copy(result)
                        if len(items) >= 2:
                            for item in items:
                                title = item.text.strip()
                                try:
                                    if title == 'Products' or title == 'Product' or title == 'Tested Products':
                                        try: 
                                            table_yn = main.find('table')
                                            if table_yn: 
                                                prdt_nm, prdt_dtl_ctn = self.extraction_text_from_table(table_yn)
                                                # result['prdtNm'] = item.find_next_sibling().text.strip() + '\n' + prdt_nm
                                                result['prdtDtlCtn'] =  item.find_next_sibling().text.strip() + '\n' + prdt_dtl_ctn
                                            else:
                                                # result['prdtNm'] = item.find_next_sibling().text.strip()
                                                result['prdtDtlCtn'] =  item.find_next_sibling().text.strip()
                                        except Exception as e: raise Exception(f'제품명/제품상세내용 수집 중 에러  >>  {e}')
                                    elif title == 'Summary of Problem and Scope' or title == 'Summary and Scope of the Problem':
                                        try: result['flwActn'] = item.find_next_sibling().text.strip()
                                        except Exception as e: raise Exception(f'후속조치 수집 중 에러  >>  {e}')
                                    elif title == 'Further Information for Consumers':
                                        try: result['hrmflCuz'] = item.find_next_sibling().text.strip()
                                        except Exception as e: raise Exception(f'위해내용 수집 중 에러  >>  {e}')
                                    elif title == 'Recommendation for Consumers' or title == 'Recommendations for Consumers':
                                        try: result['flwActn2'] = item.find_next_sibling().text.strip()
                                        except Exception as e: raise Exception(f'후속조치2 수집 중 에러  >>  {e}')
                                except Exception as e: self.logger.error(f'{e}')

                            if org_result == result:
                                # try: result['prdtNm'] = main.find('h1', {'class':'content-title text-center'}).text.strip()
                                # except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}')

                                try:
                                    prdt_dtl_ctns = main.find('div', {'role':'main'}).find_all('p')
                                    result['prdtDtlCtn'] = ' \n'.join([prdt_dtl_ctn.text.strip() for prdt_dtl_ctn in prdt_dtl_ctns]).strip()
                                except Exception as e: self.logger.error(f'제품상세내용 수집 중 에러  >>  {e}')

                        else:
                            try:
                                prdt_dtl_ctns = main.find('div', {'role':'main'}).find_all('p')
                                result['prdtDtlCtn'] = ' \n'.join([prdt_dtl_ctn.text.strip() for prdt_dtl_ctn in prdt_dtl_ctns])
                            except Exception as e: self.logger.error(f'제품상세내용 수집 중 에러  >>  {e}')


                        try:                        
                            images = main.find('div', {'role':'main'}).find_all('img')
                            images_paths = []
                            images_files = []
                            for idx, image in enumerate(images):
                                try:
                                    img_url = f"https://www.fda.gov{image['src']}"
                                    img_res = self.utils.download_upload_image(self.chnnl_nm, img_url)
                                    if img_res['status'] == 200:
                                        images_paths.append(img_res['path'])
                                        images_files.append(img_res['fileNm'])
                                    else:
                                        self.logger.info(f"{img_res['message']} : {img_res['fileNm']}")                                
                                except Exception as e:
                                    self.logger.error(f'{idx}번째 이+미지 수집 중 에러  >>  {img_url}')
                            result['prdtImgFlPath'] = ' , '.join(set(images_paths))
                            result['prdtImgFlNm'] = ' , '.join(images_files)
                        except Exception as e: self.logger.error(f'제품 이미지 수집 중 에러  >>  {e}')    
                else: 
                    date_flag = False
                    return date_flag, 0, result
            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')

        return date_flag, dup_flag, result

    def extraction_text_from_table(self, info):
        prdt_nm = ''
        prdt_dtl_ctn = ''
        try:
            table_contents = []
            thead = [title.text.replace('\\x0','').strip() for title in info.find('thead').find_all('th')]
            tbodies = [rows for rows in info.find('tbody').find_all('tr')]
            for tbody in tbodies:
                try:
                    items = tbody.find_all('td')
                    row_content = ''
                    for idx, row in enumerate(items):
                        try:
                            row_content += f'{thead[idx]} = {row.text} | ' if row != items[-1] else f'{thead[idx]} = {row.text}'
                        except Exception as e: self.logger.error(f'{idx}번째 항목 추출 중 에러  >>  {e}')
                    table_contents.append(row_content)
                except Exception as e: self.logger.error(f'{e}')
            prdt_dtl_ctn = ' \n ' + ' \n '.join(table_contents) + ' \n '
        except Exception as e:
            self.logger.error(f'제품 상세내용 테이블 태그에서 텍스트 추출 중 에러  >>  {e}')

        return prdt_nm, prdt_dtl_ctn