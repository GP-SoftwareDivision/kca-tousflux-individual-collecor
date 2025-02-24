from common.utils import Utils
import json
import os
import random
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class NHTSA():
    def __init__(self, chnnl_cd, chnnl_nm, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_cd = chnnl_cd
        self.chnnl_nm = chnnl_nm
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.header = {
            'Accept':'*/*',
            'Accept-Encoding':'gzip, deflate, br, zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'Content-Type':'application/json',
            'Origin':'https://www.nhtsa.gov',
            'Referer':'https://www.nhtsa.gov/',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }

        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0

        self.utils = Utils(logger, api)

    def crawl(self):
        try:
            url = f'https://api.nhtsa.gov/safetyIssues/byDate?dateStart={self.start_date}&dateEnd={self.end_date}&max=100&issueType=recall&name='
            res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
            if res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                                
                res_json = json.loads(res.text)
                datas = res_json['results'][0]['recalls']
                for data in datas:
                    try:
                        wrt_dt = data['reportReceivedDate'].replace('T',' ').replace('Z','')
                        if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                            self.total_cnt += 1
                            colct_data = self.crawl_detail(data)
                            insert_res = self.utils.insert_data(colct_data)
                            if insert_res == 0:
                                self.colct_cnt += 1
                            elif insert_res == 1:
                                self.error_cnt += 1
                                product_url = f"https://www.nhtsa.gov/?nhtsaId={data['nhtsaCampaignNumber']}"
                                self.utils.save_colct_log(f'게시글 수집 오류 > {product_url}', '', self.chnnl_cd, self.chnnl_nm, 1)
                            elif insert_res == 2 :
                                self.duplicate_cnt += 1
                        elif wrt_dt < self.start_date: 
                            self.logger.info(f'수집기간 내 데이터 수집 완료')
                            break
                    except Exception as e:
                        self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                self.page_num += 1
            else: raise Exception(f'통신 차단 : {url}')
        except Exception as e:
            self.logger.error(f'crawl 통신 중 에러 >> {e}')
            self.error_cnt += 1
            exc_type, exc_obj, tb = sys.exc_info()
            self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt}')
            self.logger.info('수집종료')

    def crawl_detail(self, data):
        extract_error = False
        result = {'wrtDt':'', 'hrmflCuz':'', 'prdtDtlCtn':'', 'mnfctrBzenty':'',
                  'ntslCrst':'', 'prdtNm':'', 'hrmflCuz':'', 'actchfl':'', 'url':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            try: result['wrtDt'] = data['reportReceivedDate'].strip()
            except: self.logger.error('작성일 추출 실패  >>  '); extract_error = True;

            try: result['hrmflCuz'] = data['consequence']
            except: self.logger.error('위해원인 추출 실패  >>  '); extract_error = True;

            try: result['prdtDtlCtn'] = data['nhtsaCampaignNumber']
            except: self.logger.error('제품 상세내용 추출 실패  >>  '); extract_error = True;

            try: result['mnfctrBzenty'] = data['manufacturer']
            except: self.logger.error('제조업체 추출 실패  >>  '); extract_error = True;

            try: result['ntslCrst'] = data['potentialNumberOfUnitsAffected']
            except: self.logger.error('판매현황 추출 실패  >>  '); extract_error = True;            

            try: result['prdtNm'] = data['summary']
            except: self.logger.error('제품명 추출 실패  >>  '); extract_error = True;      

            try: result['hrmflCuz'] = data['consequence'] + 'ª' + data['summary']
            except: self.logger.error('위해원인 추출 실패  >>  '); extract_error = True;    
            
            try: result['flwActn'] = data['correctiveAction']
            except: self.logger.error('후속조치 추출 실패  >>  '); extract_error = True;   

            try: 
                atchl_urls = data['associatedDocuments']
                atchl_url = [url['url'] for url in atchl_urls if url['summary']=='Recall 573 Report'][0]
                result['actchfl'] = self.utils.download_upload_atchl('NHTSA', result['prdtDtlCtn'], atchl_url)
            except Exception as e: self.logger.error(f'첨부파일 추출 실패  >>  {e}'); extract_error = True;   
            
            result['url'] = f"https://www.nhtsa.gov/?nhtsaId={data['nhtsaCampaignNumber']}"
            result['chnnlNm'] = self.chnnl_nm
            result['chnnlCd'] = self.chnnl_cd
            result['idx'] = self.utils.generate_uuid(result['url'], self.chnnl_nm, result['prdtNm'])

            if extract_error: self.logger.info(data)

            time.sleep(random.uniform(5,10))

        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')
        return result

