from bs4 import BeautifulSoup
from common.utils import Utils
import json
import random
import requests
import time

class OPSS():
    def __init__(self, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.header = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding':'zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }

        self.utils = Utils(logger, api)

    def crawl(self):
        crawl_flag = True
        while(crawl_flag):
            try:
                if self.page_num == 0: url = 'https://www.gov.uk/product-safety-alerts-reports-recalls'
                else: url = f'https://www.gov.uk/product-safety-alerts-reports-recalls?page={self.page_num}'
                res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                if res.status_code == 200:
                    sleep_time = random.uniform(3,5)
                    self.logger.info(f'통신 성공, {sleep_time}초 대기')
                    time.sleep(sleep_time)                

                    html = BeautifulSoup(res.text, features='html.parser')
                    recall_list = html.find('div',{'id':'js-results'}).find('ul').find_all('li', {'class':'gem-c-document-list__item'})
                    for recall in recall_list:
                        try:
                            recall_date = recall.find('time')['datetime'] + ' 00:00:00'
                            if recall_date >= self.start_date and recall_date <= self.end_date:
                                product_url = 'https://www.gov.uk' + recall.find('a')['href']
                                colct_data = self.crawl_detail(product_url)
                                req_data = json.dumps(colct_data)
                                insert_res = self.api.insertData2Depth(req_data)
                                if insert_res == 0:
                                    self.colct_cnt += 1
                                elif insert_res == 1:
                                    self.error_cnt += 1
                                elif insert_res == 2 :
                                    self.duplicate_cnt += 1                                
                            else: 
                                crawl_flag = False
                                break
                        except Exception as e:
                            self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                    url_list = [recall.find('a')['href'] for recall in recall_list[1:]]

                    self.page_num += 1
                    if crawl_flag: self.logger.info(f'{self.page_num} 페이지로 이동 중 ..')
                else: 
                    crawl_flag = False
                    raise Exception('통신 차단')

            except Exception as e:
                self.logger.error(f'crawl 통신 중 에러 >> {e}')

    def crawl_detail(self, product_url):
        result = {'prdtNm':'', 'wrtDt':'', 'brand':'', 'prdtDtlCtn':'', 'prdtDtlCtn2':'', 'plor':'', 
                  'prdtImg':'', 'hrmflCuz':'', 'flwActn':'', 'recallNo':'', 'url':'', 'chnnlCd':0, 'idx':''}
        try:
            if self.page_num == 0: referer_url = 'https://www.gov.uk/product-safety-alerts-reports-recalls'
            else: referer_url = f'https://www.gov.uk/product-safety-alerts-reports-recalls?page={self.page_num}'
            custom_header = self.header
            custom_header['Referer'] = referer_url
            res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)            
            if res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)     

                html = BeautifulSoup(res.text, features='html.parser')

                try: result['prdtNm'] = html.find('h1', {'class':'gem-c-title__text govuk-heading-l'}).text.strip()
                except: self.logger.error('제품명 추출 중 에러  >> ')

                try: 
                    date = html.find('dl', {'class':'gem-c-metadata__list'}).find('dt', string='Published').find_next_siblings('dd')[0].text
                    result['wrtDt'] = self.utils.match_date_format(date)
                except: self.logger.error('작성일 추출 중 에러  >> ')  

                product_information = html.find('h2',{'id':'product-information'}).find_next_sibling('table').find('tbody').find_all('tr')

                prdt_dtl_ctn = ''
                for info in product_information:
                    try:
                        title = info.find_all('td')[0].text.strip()
                        if title == 'Brand':
                            try: result['Brand'] = info.find_all('td')[1].text.strip()
                            except: self.logger.error('브랜드 추출 중 에러  >> ')  
                        elif title == 'Country of Origin':
                            try: result['plor'] = info.find_all('td')[1].text.strip()
                            except: self.logger.error('원산지 추출 중 에러  >> ')  
                        elif title == 'Product Description':
                            try: result['prdtDtlCtn2'] = info.find_all('td')[1].text.strip()
                            except: self.logger.error('원산지 추출 중 에러  >> ')  
                        elif title == 'Product Report and Image':
                            try: 
                                pdf_url = info.find_all('td')[1].find('a')['href']
                                res = self.utils.extract_img_from_pdf('opss', result['prdtNm'], pdf_url)
                                if res != '': result['prdtDtlCtn2']
                                else: 
                                    if res is not None and res == '':
                                        raise Exception('pdf에서 이미지 추출 중 에러')
                            except: self.logger.error(f'원산지 추출 중 에러  >> {e}')  
                        else:
                            try: 
                                content = info.find_all('td')[1].text.strip()
                                prdt_dtl_ctn += f'{title} : {content} | ' if info != product_information else f'{title} : {content}'
                            except: self.logger.error('원산지 추출 중 에러  >> ')  
                        
                    except Exception as e:
                        self.logger.error(f'{e}')

                result['prdtDtlCtn'] = prdt_dtl_ctn

                try: 
                    start_tag = html.find('h2', {'id':'hazard'})  # 시작점 찾기
                    end_tag = html.find('h2', {'id':'hazard'}).find_next_siblings('h2')  # 끝점 찾기

                    content = self.utils.extract_content(start_tag, end_tag)

                    if content != []: 
                        result['hrmflCuz'] = content
                    elif content == []: raise Exception('위해원인 추출 중 에러  >> ')
                except: self.logger.error('위해원인 추출 중 에러  >> ')

                try: 
                    start_tag = html.find('h2', {'id':'corrective-action'})  # 시작점 찾기
                    end_tag = html.find('h2', {'id':'corrective-action'}).find_next_siblings('h2')  # 끝점 찾기

                    content = self.utils.extract_content(start_tag, end_tag)

                    if content != []: 
                        result['flwActn'] = content
                    elif content ==[] : raise Exception('후속조치 추출 중 에러  >> ')
                except: self.logger.error('후속조치 추출 중 에러  >> ')

                try:
                    start_tag = html.find('h2', {'id':'corrective-action'})  # 시작점 찾기
                    end_tag = html.find('h2', {'id':'corrective-action'}).find_next_siblings('h2')  # 끝점 찾기

                    content = self.utils.extract_content(start_tag, end_tag)
                    if content != []:
                        recall_no = [text for text in content if 'PSD case number' in text][0].replace('PSD case number:','').strip()
                        result['recallNo'] = html.find('h1', {'class':'gem-c-title__text govuk-heading-l'}).text.strip()
                    elif content ==[] : raise Exception('리콜번호 추출 중 에러  >> ')
                except: self.logger.error('리콜번호 추출 중 에러  >> ')

            else: raise Exception('통신 차단')
            
        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')