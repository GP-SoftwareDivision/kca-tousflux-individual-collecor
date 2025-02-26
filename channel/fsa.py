from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time

class FSA():
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
            'Accept-Encoding':'zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        }

        self.locale_str = 'en'

        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0

        self.utils = Utils(logger, api)

    def crawl(self):
            try:
                crawl_flag = True     
                headers = self.header
                while(crawl_flag):
                    try:
                        if self.page_num == 0:
                            headers['Referer'] = 'https://www.food.gov.uk/search?filter_type%5BFood%20alert%5D=Food%20alert&filter_type%5BAllergy%20alert%5D=Allergy%20alert'
                            url = 'https://www.food.gov.uk/search-api?filter_type%5BFood%20alert%5D=Food%20alert&filter_type%5BAllergy%20alert%5D=Allergy%20alert'
                        else: 
                            headers['Referer'] = f'https://www.food.gov.uk/search?filter_type%5BFood+alert%5D=Food+alert&filter_type%5BAllergy+alert%5D=Allergy+alert&page={self.page_num}'
                            url = f'https://www.food.gov.uk/search-api?filter_type%5BFood+alert%5D=Food+alert&filter_type%5BAllergy+alert%5D=Allergy+alert&page={self.page_num}'   
                        self.logger.info('수집 시작')
                        res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)  
                            
                            res_json = json.loads(res.text)
                            datas = res_json['#data']['items']

                            for data in datas:
                                try:
                                    try: 
                                        wrt_dt_day = datas[0]['updated'].split('T')[0]
                                        wrt_dt_time = datas[0]['updated'].split('T')[1]
                                    except: 
                                        wrt_dt_day = datas[0]['created'].split('T')[0]
                                        wrt_dt_time = datas[0]['created'].split('T')[1]                                    
                                    finally:
                                        wrt_dt = self.utils.parse_date(wrt_dt_day, self.chnnl_nm) + ' ' + wrt_dt_time
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        product_url = data['url']
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
                                        self.logger.info(f'수집기간 내 데이터 수집 완료')
                                        break
                                except Exception as e:
                                    self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                            self.page_num += 12
                            if crawl_flag: self.logger.info(f'{self.page_num/12}페이지로 이동 중..')
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
        result = { 'wrtDt':'', 'prdtNm':'', 'atchFlPath':'', 'atchFlNm':'', 
                  'prdtDtlCtn':'', 'hrmflCuz':'', 'flwActn':'', 
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        # 게시일, 위해내용, pdf, 후속조치, 제품명, 제품 상세내용
        try:
            custom_header = self.header
            if self.page_num == 0: referer_url = 'https://www.food.gov.uk/search?filter_type%5BFood%20alert%5D=Food%20alert&filter_type%5BAllergy%20alert%5D=Allergy%20alert'
            else: referer_url = f'https://www.food.gov.uk/search?filter_type%5BFood+alert%5D=Food+alert&filter_type%5BAllergy+alert%5D=Allergy+alert&page={self.page_num}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                main = html.find('main')

                try:
                    date_text = self.utils.parse_date(html.find('div', {'class':'article-hero__toolbar__last-updated'}).text.strip(), self.chnnl_nm)
                    date_day = date_text + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(date_day, "%Y-%m-%d %H:%M:%S").isoformat()
                except Exception as e: raise Exception(f'게시일 수집 중 에러  >>  {e}')

                try:
                    prdt_nm_list = main.find_all('caption')
                    result['prdtNm'] = ', '.join(prdt_nm.text.strip() for prdt_nm in prdt_nm_list)
                except Exception as e: raise Exception(f'제품명 수집 중 에러  >>  {e}')
                
                try: 
                    atchl_url = main.find('section', {'class': 'document-download'}).find('a')['href']
                    atchl_res = self.utils.download_upload_atchl(self.chnnl_nm, atchl_url)
                    if atchl_res['status'] == 200:
                        result['atchFlPath'] = atchl_res['path']
                        result['atchFlNm'] = atchl_res['fileNm']
                    else:
                        self.logger.info(f"첨부파일 이미 존재 : {atchl_res['fileNm']}")
                except Exception as e: self.logger.error(f'첨부파일 추출 실패  >>  {e}'); extract_error = True
                
                try:
                    titles = main.find_all('h2', {'class': 'form-title'})
                    for title in titles:
                        try:
                            label = title.text.strip().lower()
                            description = title.find_next_sibling('div').get_text(separator="\n", strip=True).replace('\n', ' ')
                            if label == 'risk statement':
                                result['hrmflCuz'] = description
                            elif label == 'our advice to consumers':
                                result['flwActn'] = description
                        except:
                            pass 
                except Exception as e: raise Exception(f'위해원인 수집 중 에러  >>  {e}')

                tbody_list = main.find_all('tbody')
                prdt_dtl_ctn = []
                for tbody in tbody_list:
                    try:
                        ths = tbody.find_all('th')
                        tds = tbody.find_all('td')
                        ctn = []
                        for i in range(len(ths)):
                            label = ths[i].text.strip()
                            description = tds[i].text.strip()
                            ctn.append(f"{label} = {description}")
                        prdt_dtl_ctn.append(' | '.join(ctn))
                    except Exception as e: raise Exception(f'제품 상세내용 수집 중 에러  >>  {e}')
                result['prdtDtlCtn'] = '\n'.join(prdt_dtl_ctn)

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)                            
            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result