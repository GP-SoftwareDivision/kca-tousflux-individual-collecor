from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import  os 
import random
import re
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CCPC():
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
            'Accept-Encoding':'gzip, deflate, br, zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'Cache-Control':'max-age=0',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }

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
                    url = 'https://www.ccpc.ie/consumers/product-safety/product-recalls/'
                    self.logger.info('수집 시작')
                    res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공 {sleep_time}초 대기')
                        time.sleep(sleep_time)                        
                        html = BeautifulSoup(res.text, features='html.parser')
                        recall_list = html.find('table').find_all('tr')
                        for recall in recall_list[1:]:
                            try:
                                unformatted_date = recall.find('td').text
                                wrt_dt = self.utils.match_date_format(unformatted_date)
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    self.total_cnt += 1
                                    product_url = recall.find('a')['href']
                                    colct_data = self.crawl_detail(url, product_url)
                                    req_data = json.dumps(colct_data)
                                    insert_res = self.api.insertData2Depth(req_data)
                                    if insert_res == 0:
                                        self.colct_cnt += 1
                                    elif insert_res == 1:
                                        self.error_cnt += 1
                                    elif insert_res == 2 :
                                        self.duplicate_cnt += 1
                                    else:
                                        self.error_cnt += 1
                                        raise Exception('api 통신 중 에러')
                                elif wrt_dt < self.start_date:                    
                                    crawl_flag = False
                                    self.logger.info(f'수집기간 내 데이터 수집 완료')
                                    break
                            except Exception as e:
                                self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                    else:
                        crawl_flag = False 
                        raise Exception('통신 차단')
                except Exception as e:
                    self.logger.error(f'crawl 통신 중 에러 >> {e}')
        except Exception as e:
            self.logger.error(f'crawl 통신 중 에러 >> {e}')
            crawl_flag = False
            self.error_cnt += 1
            exc_type, exc_obj, tb = sys.exc_info()
            self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt}')
            self.logger.info('수집 종료')

    def crawl_detail(self, url, product_url):
        extract_error = False
        result = {'prdtNm':'', 'recallBzenty':'', 'wrtDt':'', 'prdtImg':'', 'prdtDtlCtn':'',
                  'url':'', 'chnnlNm':'','chnnlCd':0, 'idx':''}
        try:
            custom_header = self.header
            custom_header['Referer'] = url
            res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                html = BeautifulSoup(res.text, features='html.parser')

                try: result['prdtNm'] = html.find('div', {'class':'inner-content'}).find('h1').text
                except: self.logger.error('제품명 추출 중 에러  >> '); extract_error = True;

                try: result['recallBzenty'] = html.find('div', {'class':'inner-content'}).find('h1').text
                except: self.logger.error('리콜업체 추출 중 에러  >> '); extract_error = True;

                try: 
                    date = html.find('div', {'class':'inner-content'}).find('p', {'class':'date'}).text
                    wrt_dt = self.utils.match_date_format(date)
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat()
                except: self.logger.error('작성일 추출 중 에러  >> '); extract_error = True;

                try:
                    image_paths = []
                    image_urls = html.find('div', {'class':'inner-content'}).find_all('img') 
                    for idx, url in enumerate(image_urls):
                        try:
                            img_url = url['src']
                            img_nm = self.utils.normalize_image_filename(img_url.split('/')[-1])
                            res = self.utils.download_upload_image(self.chnnl_nm, img_nm, img_url)
                            if res != '': image_paths.append(res)
                        except Exception as e: self.logger.error(f'{idx}번째 이미지 추출 중 실패')
                    result['prdtImg'] = ' : '.join(image_paths)
                except: self.logger.error('이미지 추출 중 에러  >> '); extract_error = True;

                try: 
                    start_tag = html.find('h1', string=result['prdtNm'])  # 시작점 찾기
                    # end_tag = html.find('h2').find('span', string="What to do:")  # 끝점 찾기
                    end_tag = html.find(lambda tag: tag.name == "h2" and (tag.text.strip() == "What to do:" or tag.find("span", string="What to do:")))
                    if end_tag == None: 
                        end_tag = html.find('a', string='Return to Product Recalls')  
                    #     content = self.utils.extract_content(start_tag, end_tag)
                    # else: 
                    #     end_tag = html.find('h1', string='Return to Product Recalls')
                    content = self.utils.extract_content(start_tag, end_tag)
                    if content != []: result['prdtDtlCtn'] = self.utils.get_clean_string(' '.join(content))
                except: self.logger.error('제품 상세내용 추출 중 에러  >> '); extract_error = True;

                print('===================================================================================================')
                print('===================================================================================================')
                print('===================================================================================================')
                print(product_url)
                print(result['prdtDtlCtn'])
                result['url'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result['url'], self.chnnl_nm, result['prdtNm'])
                if extract_error: self.logger.info(f'url :: {product_url}')
            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {res.status_code}')
            
        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')

        return result