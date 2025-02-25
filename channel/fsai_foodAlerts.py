import re
from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time

class FSAIFoodAlerts():
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
                crawl_flag = True
                org_url = 'https://www.fsai.ie/news-alerts/food?page=<%pageNum%>'
                while(crawl_flag):
                    try:
                        headers = self.header
                        self.logger.info('수집 시작')
                        url = org_url.replace('<%pageNum%>', str(self.page_num+1))

                        if self.page_num != 0: 
                            headers['Referer'] = org_url.replace('<%pageNum%>', str(self.page_num))
                        res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')

                            datas = html.find_all('a', class_=['feature-card'])
                            for data in datas:
                                try:
                                    product_url = 'https://www.fsai.ie' + data['href']
                                    colct_data = self.crawl_detail(product_url)
                                    wrt_dt = datetime.strptime(colct_data['wrtDt'], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
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
        extract_error = True
        result = {'prdtNm':'', 'wrtDt':'', 'prdtDtlCtn':'', 'hrmflCuz':'', 
                  'flwActn':'', 'prdtImgFlNm':'', 'prdtImgFlPath': '', 'lotNo': '', 
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        # 제품명, 배치번호, 위해원인, 제품 상세내용, 후속조치, 제품이미지
        try:
            custom_header = self.header
            referer_url = f'https://www.fsai.ie/news-alerts/food?page={self.page_num+1}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try: 
                    result['prdtNm'] = html.find('h2').text.strip()
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}')

                try: 
                    date_text = html.find('p', {'class': 'date'}).text.strip()
                    date_day = datetime.strptime(date_text, '%A, %d %B %Y').strftime("%Y-%m-%d")
                    wrt_dt = date_day + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                try:
                    tags = html.find('article').find_all('p')
                    hrmfl_cuz = []
                    flw_actn = []
                    for tag in tags:
                        title_tag = tag.find('strong')
                        if not title_tag: continue
                        title = title_tag.text.strip()
                        next_p = title_tag.find_next_sibling('p')
                        if 'Message:' in title or 'Nature Of Danger:' in title:
                            hrmfl_cuz.append(f"{title} {self.utils.get_clean_content_string(next_p.text.strip()) if next_p else ''}")
                        elif 'Manufacturers, wholesalers, distributors, caterers & retailers:' in title or 'Consumers:' in title:
                            if next_p:
                                flw_actn.append(f"{title} {self.utils.get_clean_string(next_p.text.strip())}")
                    result['hrmflCuz'] = '\n'.join(hrmfl_cuz)
                    result['flwActn'] = '\n'.join(flw_actn)
                except Exception as e: self.logger.error(f'위해원인 및 후속조치 수집 중 에러  >>  {e}')

                try:
                    tables = html.find_all('table')
                    for table in tables:
                        if not table.find('thead'):
                            tags = html.find('table').find_all('td')
                            for tag in tags:
                                if 'Batch Code' in tag.text:
                                    lot_no = tag.find_next_sibling('td')
                                    result['lotNo'] = lot_no.text.strip() if lot_no else ''
                                    break
                        else:
                            try:
                                headers = [th.get_text(strip=True) for th in table.find("thead").find_all("th")]
                                rows = table.find("tbody").find_all("tr")
                                rowspan_data = [None] * len(headers)
                                extracted_data = [','.join(headers)]

                                for row in rows:
                                    cols = row.find_all("td")
                                    row_values = []

                                    col_idx = 0
                                    for col in cols:
                                        while col_idx < len(headers) and rowspan_data[col_idx] is not None:
                                            row_values.append(rowspan_data[col_idx])
                                            rowspan_data[col_idx] = None
                                            col_idx += 1

                                        text = ", ".join(col.stripped_strings)
                                        rowspan = int(col.attrs.get("rowspan", 1))
                                        row_values.append(text)
                                        if rowspan > 1:
                                            rowspan_data[col_idx] = text

                                        col_idx += 1

                                    while col_idx < len(headers) and rowspan_data[col_idx] is not None:
                                        row_values.append(rowspan_data[col_idx])
                                        rowspan_data[col_idx] = None
                                        col_idx += 1

                                    extracted_data.append(','.join(row_values))
                                result['prdtDtlCtn'] += '\n'.join(extracted_data)
                            except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >>  {e}')
                except Exception as e: self.logger.error(f'제품 정보 수집 중 에러  >>  {e}')

                try:
                    images = html.find('article').find_all('img')
                    images_paths = []
                    images_files = []
                    for idx, image in enumerate(images):
                        try:
                            img_url = f'https://www.fsai.ie{image["src"]}'
                            img_res = self.utils.download_upload_image(self.chnnl_nm, img_url)
                            if img_res['status'] == 200:
                                images_paths.append(img_res['path'])
                                images_files.append(img_res['fileNm'])
                            else:
                                self.logger.info(f"이미지 이미 존재 : {img_res['fileNm']}")                                
                        except Exception as e:
                            self.logger.error(f'{idx}번째 이미지 수집 중 에러  >>  {e}')
                    result['prdtImgFlPath'] = ' , '.join(set(images_paths))
                    result['prdtImgFlNm'] = ' , '.join(images_files)
                except Exception as e: self.logger.error(f'제품 이미지 수집 중 에러  >>  {e}'); extract_error = True
            
                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)                            
            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {product_res.status_code}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result