from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import re
import requests
import sys
import time

class TransportCanada():
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
                body_data = {
                    '__VIEWSTATE': '',
                    '__VIEWSTATEGENERATOR': '',
                    '__EVENTVALIDATION': '',
                    'ctl00$ctl00$MainContent$BodyContent$RecallsTopDataPager$ctl00$ctl02': 'Next',
                    'ctl00$ctl00$MainContent$BodyContent$ddlPage': 1
                }
                pass_cnt = 0    # 게시글 정렬이 최신순이 아님
                MAX_PASS_CNT = 25
                while(crawl_flag):
                    try:
                        headers = self.header
                        url = 'https://wwwapps.tc.gc.ca/Saf-Sec-Sur/7/VRDB-BDRV/search-recherche/results-resultats.aspx?lang=eng&mk=0&mkName=All%2520makes&md=0&fy=0&ty=9999&ft=&ls=0&sy=0&syName=All%2520Systems&all=0'
                        self.logger.info('수집 시작')

                        if self.page_num != 0:
                            headers['Referer'] = url
                            res = requests.post(url=url, headers=headers, data=body_data, verify=False, timeout=600)
                        else:
                            res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')

                            datas = html.find_all('tr')
                            for data in datas:
                                try:
                                    if len(data.get('class', [])) > 0: continue
                                    date_day = data.find_all('td')[1].text.strip()
                                    wrt_dt = date_day + ' 00:00:00'                                 
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        product_url = 'https://wwwapps.tc.gc.ca/Saf-Sec-Sur/7/VRDB-BDRV/search-recherche/' + data.find('a')['href']
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
                                        pass_cnt += 1 
                                        if pass_cnt > MAX_PASS_CNT: 
                                            crawl_flag = False
                                            self.logger.info(f'수집기간 내 데이터 수집 완료')
                                            break
                                except Exception as e:
                                    self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                            self.page_num += 1
                            try:
                                body_data.update({
                                    '__VIEWSTATE': html.find('input', {'id': '__VIEWSTATE'})['value'],
                                    '__VIEWSTATEGENERATOR': html.find('input', {'id': '__VIEWSTATEGENERATOR'})['value'],
                                    '__EVENTVALIDATION': html.find('input', {'id': '__EVENTVALIDATION'})['value'],
                                    'ctl00$ctl00$MainContent$BodyContent$ddlPage': self.page_num + 1
                                    })
                            except:
                                self.logger.error(f'다음 페이지 호출 데이터 추출 중 에러 >> {e}')
                                break
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
        result = {'prdtNm':'', 'wrtDt':'', 'prdtDtlCtn':'', 'prdtDtlCtn2':'', 'hrmflCuz':'',
                  'ntslCrst': '', 'flwActn':'', 'brand': '', 'mnfctrBzenty': '',
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        # 제품 상세내용, 게시일, 제품 상세내용2, 판매현황, 위해원인, 후속조치, 브랜드, 제품명, 제조업체
        try:
            custom_header = self.header
            referer_url = 'https://wwwapps.tc.gc.ca/Saf-Sec-Sur/7/VRDB-BDRV/search-recherche/results-resultats.aspx?lang=eng&mk=0&mkName=All%2520makes&md=0&fy=0&ty=9999&ft=&ls=0&sy=0&syName=All%2520Systems&all=0'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try: 
                    result['prdtDtlCtn'] = html.find('span', {'id': 'MainContent_BodyContent_LB_Recall_Number'}).text.strip()
                    result['prdtDtlCtn'] += ', Manufacturer Recall Number: ' + html.find('span', {'id': 'MainContent_BodyContent_LB_ManufacturerRecallNumber_d'}).text.strip()
                except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >>  {e}')

                try: 
                    result['ntslCrst'] = html.find('span',{'id':'MainContent_BodyContent_LB_UnitAffected_d'}).text.strip()
                except Exception as e: self.logger.error(f'판매현황 수집 중 에러  >>  {e}')

                try: 
                    tmp_text = self.utils.get_clean_string(html.find('span',{'id':'MainContent_BodyContent_LB_RecallDetail_d'}).text.strip())
                    recall_details = re.split(r'(Corrective Actions:)', tmp_text)
                    result['hrmflCuz'] = recall_details[0].strip()
                    result['flwActn'] = f'{recall_details[1]}{recall_details[2]}' if len(recall_details) > 2 else result['hrmflCuz']
                except Exception as e: self.logger.error(f'위해원인 및 후속조치 수집 중 에러  >>  {e}')

                try: 
                    date_day = html.find('span', {'id': 'MainContent_BodyContent_LB_RecallDate_d'}).text.strip()
                    wrt_dt = date_day + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                try: 
                    table = html.find('table', {'id': 'MainContent_BodyContent_DG_RecallDetail'})
                    rows = table.find_all("tr") if table else []
                    table_data = []
                    for row in rows:
                        cols = row.find_all(["td", "th"])
                        col_texts = [col.get_text(strip=True) for col in cols]
                        table_data.append(",".join(col_texts))  

                    result['prdtDtlCtn'] = result['prdtDtlCtn'] + '\n' + '\n'.join(table_data)
                except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >>  {e}')

                try: 
                    spans = html.find_all('span', id=re.compile(r'MainContent_BodyContent_DG_RecallDetail_LB_Make_'))
                    result['brand'] = ', '.join(span.text for span in spans)
                except Exception as e: self.logger.error(f'브랜드 수집 중 에러  >>  {e}')

                try: 
                    spans = html.find_all('span', id=re.compile(r'MainContent_BodyContent_DG_RecallDetail_LB_Model_'))
                    result['prdtNm'] = ', '.join(span.text for span in spans)
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}')

                try: 
                    spans = html.find_all('span', id=re.compile(r'MainContent_BodyContent_DG_Manufacturer_LB_ManufacturerName_'))
                    if len(spans) == 0:
                        spans = html.find_all('span', id=re.compile(r'MainContent_BodyContent_DG_Make_LB_MakeName_'))
                    result['mnfctrBzenty'] = ', '.join(span.text for span in spans)
                except Exception as e: self.logger.error(f'제조업체 수집 중 에러  >>  {e}')
            
                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)                            
            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result