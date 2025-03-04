from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time

class PhilippinesDTI():
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
                org_url = 'https://www.dti.gov.ph/advisories/page/<%pageNum%>/'
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

                            datas = html.find_all('div', {'class': 'grid-items'})
                            for data in datas:
                                try:
                                    product_url = data.find('a')['href']
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
                self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
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
                    result['prdtNm'] = html.find('h1',{'class':'entry-title'}).text.strip() 
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}')

                try: 
                    date_text = html.find('meta',{'property':'article:modified_time'})['content'].strip()
                    result['wrtDt'] = datetime.fromisoformat(date_text).replace(tzinfo=None).isoformat()
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                try:
                    contents = html.find('div', {'class': 'entry-content'}).contents
                    prdt_dtl_ctn = []
                    for content in contents:
                        if content.name is None or content.name == 'div': continue
                        elif content.find('object'): continue
                        else:
                            prdt_dtl_ctn.append(content.get_text(separator="\n", strip=True).replace('\n', ' ').replace('\xa0', ' '))
                            
                    result['prdtDtlCtn'] = '\n'.join(prdt_dtl_ctn)
                except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >>  {e}')
                
                try: 
                    atchl_url = html.find('object')['data']
                    atchl_res = self.utils.download_upload_atchl(self.chnnl_nm, atchl_url)
                    if atchl_res['status'] == 200:
                        result['atchFlPath'] = atchl_res['path']
                        result['atchFlNm'] = atchl_res['fileNm']
                    else:
                        self.logger.info(f"첨부파일 이미 존재 : {atchl_res['fileNm']}")
                except Exception as e: self.logger.error(f'첨부파일 추출 실패  >>  {e}'); extract_error = True

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)                            
            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result