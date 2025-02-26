import re
from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time

class BLV():
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
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'Referer': 'https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/rueckrufe-und-oeffentliche-warnungen.html'
        }

        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0

        self.utils = Utils(logger, api)


    def crawl(self):
            try:
                crawl_flag = True
                # chnnl_nm에 따라 링크 다름
                if 'Offentliche Warnungen' in self.chnnl_nm:
                    org_url = 'https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/rueckrufe-und-oeffentliche-warnungen/_jcr_content/par/downloadlist_1976929218.content.paging-<%pageNum%>.html'
                else: 
                    org_url = 'https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/rueckrufe-und-oeffentliche-warnungen/_jcr_content/par/downloadlist.content.paging-<%pageNum%>.html'
                while(crawl_flag):
                    try:
                        headers = self.header
                        self.logger.info('수집 시작')
                        url = org_url.replace('<%pageNum%>', str(self.page_num + 1))

                        res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')

                            datas = html.find('ul', {'class': 'list-unstyled'}).find_all('li')
                            for data in datas:
                                try:
                                    date_text = data.find('span', {'class': 'text-dimmed'}).text.strip()
                                    pattern = r'\d{1,2}\.\d{1,2}\.\d{4}'
                                    matches = re.findall(pattern, date_text)
                                    date_day = datetime.strptime(matches[0], "%d.%m.%Y").strftime("%Y-%m-%d")
                                    wrt_dt = date_day + ' 00:00:00'                                   
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        product_url = 'https://www.blv.admin.ch/' + data.find('a')['href']
                                        colct_data = self.crawl_detail(product_url, wrt_dt)
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
                self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
                self.logger.info('수집종료')
                
    def crawl_detail(self, product_url, wrt_dt):
        extract_error = False
        result = {'prdtNm':'', 'wrtDt':'', 'atchFlPath':'', 'atchFlNm':'',  
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        # 제품명, 게시일, 첨부파일
        try:
            sleep_time = random.uniform(3,5)
            self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
            time.sleep(sleep_time)                
            result['wrtDt'] = datetime.strptime(wrt_dt, '%Y-%m-%d %H:%M:%S').isoformat()
            result['prdtNm'] = product_url.split('/')[-1].replace('.pdf', '')

            try: 
                atchl_res = self.utils.download_upload_atchl(self.chnnl_nm, product_url)
                if atchl_res['status'] == 200:
                    result['atchFlPath'] = atchl_res['path']
                    result['atchFlNm'] = atchl_res['fileNm']
                else:
                    self.logger.info(f"첨부파일 이미 존재 : {atchl_res['fileNm']}")
            except Exception as e: self.logger.error(f'첨부파일 추출 실패  >>  {e}'); extract_error = True
        
            if extract_error: self.logger.info(product_url)

            result['prdtDtlPgUrl'] = product_url
            result['chnnlNm'] = self.chnnl_nm
            result['chnnlCd'] = self.chnnl_cd
            result['idx'] = self.utils.generate_uuid(result)
        except Exception as e:
            self.logger.error(f'{e}')

        return result