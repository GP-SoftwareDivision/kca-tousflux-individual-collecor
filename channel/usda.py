import re
from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import sys
import time
import urllib.parse as urlparse
from urllib.parse import parse_qs

class USDA():
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
        self.prdt_dtl_err_url = []

        self.utils = Utils(logger, api)


    def crawl(self):
        try:
            retry_num = 0
            crawl_flag = True
            org_url = 'https://www.fsis.usda.gov/recalls?page=<%pageNum%>'
            while(crawl_flag):
                try:
                    headers = self.header
                    self.logger.info('수집 시작')
                    url = org_url.replace('<%pageNum%>', str(self.page_num))
                    headers['Cookie'] = 'ak_bmsc=E34FCED40148FFB3CABCEB1B3AD05E3A~000000000000000000000000000000~YAAQbLxBFzAof0aVAQAAmdimbxtgjqUrm3aUFQDg8+cGKiZgl65KAMUQzpY2Zw2/Xw3FxyROWZNo0TPFGQBq1nhImufIDQlDBd6Z0X7NLnyV0NqSsZFLYu+pfXGiL/AwRwyEZcec3nivaf5QNJTCIuLwl4myvjEU3DZ5bJ+j0b83mgo9znEMrpzB+z/dox3TdUQEZbQXFzz1Tchga+hV8Ve8kPl61GovI0fJDFD5b/hSy3AGL1coD/49OaiaYlGstjhv9jz9Gs0N0fPhYYmezGBZbkklKy1y7a8l9ESx5XHwI+PNnjz/db3XYqkgt+L2r3QiVGf1j+M6Gi99lkKrqpTlNG0CQt3r+R8taLoANzeYPBZmgnHqShwGa2+CpWqB2JjKZW3hEB06L75qeBm4'

                    if self.page_num != 0: 
                        headers['Referer'] = org_url.replace('<%pageNum%>', str(self.page_num-1))
                    res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)                            
                        html = BeautifulSoup(res.text, features='html.parser')

                        datas = html.find_all('section', {'class': 'recall-teaser'})
                        if len(datas) == 0:
                            if retry_num >= 10:
                                crawl_flag = False
                                self.logger.info('데이터가 없습니다.')
                            else:
                                retry_num += 1
                                continue
                        for data in datas:
                            try:
                                product_url = 'https://www.fsis.usda.gov' + data.find('h3').find('a')['href']
                                date_text = data.find('div', {'class': 'recall-teaser__date'}).text.strip()
                                match = re.search(r"\d{2}\/\d{2}\/\d{4}", date_text)

                                if match:
                                    match_date = match.group()
                                    wrt_dt = datetime.strptime(match_date, "%m/%d/%Y").strftime("%Y-%m-%d") + ' 00:00:00'
                                else:
                                    crawl_flag = False
                                    break

                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    self.total_cnt += 1
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
                                        crawl_flag = False
                                        break
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
        dup_flag = -1
        result = {'prdtNm':'', 'wrtDt':'', 'prdtDtlCtn':'', 
                  'atchFlPath':'', 'atchFlNm':'', 'recallBzenty':'', 
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            custom_header = self.header
            referer_url = f'https://www.fsis.usda.gov/recalls?page={self.page_num}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            
            
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try: 
                    date_text = html.find('div', class_=['recall-header__date']).text.strip()
                    match = re.search(r"\d{2}\/\d{2}\/\d{4}", date_text)

                    if match:
                        match_date = match.group()
                        wrt_dt = datetime.strptime(match_date, "%m/%d/%Y").strftime("%Y-%m-%d") + ' 00:00:00'
                        result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                try: 
                    li_list = html.find('section', {'class': 'wysiwyg'}).find_all('li')
                    if len(li_list) == 0:
                        result['prdtNm'] = html.find('h1', {'class': 'recall-header__title'}).text.strip()
                    else:
                        prdt_nm = [li.get_text(strip=True) for li in li_list]
                        result['prdtNm'] = ', '.join(prdt_nm).strip()
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    try: 
                        result['recallBzenty'] = html.find('span', {'class': 'recall-header__establishment'}).text.strip()
                    except Exception as e: self.logger.error(f'리콜 업체 수집 중 에러  >>  {e}')

                    try: 
                        result['prdtDtlCtn'] = html.find('section',{'class':'wysiwyg'}).get_text(separator="\n", strip=True).replace('\n', ' ')
                    except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >>  {e}')
                    
                    try:
                        a_list = [a['href'] for a in html.find('section', {'class': 'wysiwyg'}).find_all('a') if 'view label' in a.text]
                        try:
                            parsed = urlparse.urlparse(a_list[0])
                            atchl_url = parse_qs(parsed.query)['url'][0]
                        except:
                            atchl_url = a_list[0]
                        atchl_res = self.utils.download_upload_atchl(self.chnnl_nm, atchl_url, headers=custom_header)
                        if atchl_res['status'] == 200:
                            result['atchFlPath'] = atchl_res['path']
                            result['atchFlNm'] = atchl_res['fileNm']
                        else:
                            self.logger.info(f"첨부파일 이미 존재 : {atchl_res['fileNm']}")
                    except Exception as e: self.logger.error(f'첨부파일 추출 실패  >>  {e}')

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')
            self.prdt_dtl_err_url.append(product_url)

        return dup_flag, result