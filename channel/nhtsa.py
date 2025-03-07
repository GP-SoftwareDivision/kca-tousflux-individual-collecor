from common.utils import Utils
import json
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

        self.prdt_dtl_err_url = []

        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0

        self.utils = Utils(logger, api)

    def crawl(self):
        try:
            crawl_flag = True
            total_article_cnt = 0
            article_cnt = 0   
            retry_num = 0  
            while(crawl_flag):
                try:
                    dateStart = self.start_date.split(' ')[0]
                    dateEnd = self.end_date.split(' ')[0]
                    if self.page_num == 0: url = f'https://api.nhtsa.gov/safetyIssues/byDate?dateStart={dateStart}&dateEnd={dateEnd}&max=100&issueType=recall&name='
                    else: url = f'https://api.nhtsa.gov/safetyIssues/byDate?dateStart={dateStart}&dateEnd={dateEnd}&offset={self.page_num}&max=100&issueType=recall&name='
                    res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)
                                        
                        res_json = json.loads(res.text)
                        if self.page_num == 0: total_article_cnt = res_json['meta']['pagination']['total']
                        article_cnt += res_json['meta']['pagination']['count']
                        datas = res_json['results'][0]['recalls']
                        
                        if datas == []: 
                            if retry_num >= 10: 
                                crawl_flag = False
                                self.logger.info('데이터가 없습니다.')
                            else:
                                retry_num += 1
                                continue

                        for data in datas:
                            try:
                                wrt_dt = data['reportReceivedDate'].replace('T',' ').replace('Z','')
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    self.total_cnt += 1
                                    dup_flag, colct_data = self.crawl_detail(data)
                                    if dup_flag == 0:
                                        insert_res = self.utils.insert_data(colct_data)
                                        if insert_res == 0:
                                            self.colct_cnt += 1
                                        elif insert_res == 1:
                                            self.error_cnt += 1
                                            product_url = f"https://www.nhtsa.gov/?nhtsaId={data['nhtsaCampaignNumber']}"
                                            self.logger.error(f'게시글 수집 오류 > {product_url}')
                                            self.prdt_dtl_err_url.append(product_url)
                                    elif dup_flag == 2:
                                        self.duplicate_cnt += 1
                                        crawl_flag = False
                                        break
                                    else: self.logger.error(f"IDX 확인 필요  >> {colct_data['idx']} ( {product_url} )")                                                                                            
                                elif wrt_dt < self.start_date: 
                                    self.logger.info(f'수집기간 내 데이터 수집 완료')
                                    break
                            except Exception as e:
                                self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                        if total_article_cnt != article_cnt: self.page_num += 50
                        elif total_article_cnt == article_cnt: crawl_flag == False

                        if crawl_flag: self.logger.info(f'{int(self.page_num/50)}페이지로 이동 중..')
                    else: raise Exception(f'통신 차단 : {url}')
                except Exception as e:
                    self.logger.error(f'crawl 통신 중 에러 >> {e}')
                    self.error_cnt += 1
                    exc_type, exc_obj, tb = sys.exc_info()
                    self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)
        except Exception as e: self.logger.error(f'{e}')
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
            self.logger.info('수집종료')

    def crawl_detail(self, data):
        result = {'wrtDt':'', 'hrmflCuz':'', 'prdtDtlCtn':'', 'mnfctrBzenty':'',
                  'ntslCrst':'', 'prdtNm':'', 'hrmflCuz':'', 'atchFlPath':'', 'atchFlNm':'',
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            try: result['wrtDt'] = data['reportReceivedDate'].strip()
            except: self.logger.error('작성일 추출 실패  >>  ')

            try: result['prdtNm'] = data['summary']
            except: self.logger.error('제품명 추출 실패  >>  ')

            result['prdtDtlPgUrl'] = f"https://www.nhtsa.gov/?nhtsaId={data['nhtsaCampaignNumber']}"
            result['chnnlNm'] = self.chnnl_nm
            result['chnnlCd'] = self.chnnl_cd
            result['idx'] = self.utils.generate_uuid(result)

            dup_flag = self.api.check_dup(result['idx'])
            if dup_flag == 0:
                try: result['hrmflCuz'] = data['consequence']
                except: self.logger.error('위해원인 추출 실패  >>  ')

                try: result['prdtDtlCtn'] = f"NHTSA Campaign Number : {data['nhtsaCampaignNumber']}"
                except: self.logger.error('제품 상세내용 추출 실패  >>  ')

                try: result['mnfctrBzenty'] = data['manufacturer']
                except: self.logger.error('제조업체 추출 실패  >>  ')

                try: result['ntslCrst'] = str(data['potentialNumberOfUnitsAffected'])
                except: self.logger.error('판매현황 추출 실패  >>  ')

                try: result['hrmflCuz'] = data['consequence'] + 'ª' + data['summary']
                except: self.logger.error('위해원인 추출 실패  >>  ')
                
                try: result['flwActn'] = data['correctiveAction']
                except: self.logger.error('후속조치 추출 실패  >>  ')

                try: 
                    atchl_urls = data['associatedDocuments']
                    atchl_url = [url['url'] for url in atchl_urls if url['summary']=='Recall 573 Report'][0]
                    atchl_res = self.utils.download_upload_atchl(self.chnnl_nm, atchl_url)
                    if atchl_res['status'] == 200:
                        result['atchFlPath'] = atchl_res['path']
                        result['atchFlNm'] = atchl_res['fileNm']
                    else:
                        self.logger.info(f"이미지 이미 존재 : {atchl_res['fileNm']}")
                except Exception as e: self.logger.error(f'첨부파일 추출 실패  >>  {e}')
                time.sleep(random.uniform(5,10))

        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')
        return dup_flag, result

