from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import sys
import time

class OPSS():
    def __init__(self, chnnl_cd, chnnl_name, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_nm = chnnl_name
        self.chnnl_cd = chnnl_cd
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 1
        self.header = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding':'zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
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
            retry_num = 0
            while(crawl_flag):
                try:
                    if self.page_num == 1: url = 'https://www.gov.uk/product-safety-alerts-reports-recalls'
                    else: url = f'https://www.gov.uk/product-safety-alerts-reports-recalls?page={self.page_num}'
                    res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)                

                        html = BeautifulSoup(res.text, features='html.parser')
                        datas = html.find('div',{'id':'js-results'}).find('ul').find_all('li', {'class':'gem-c-document-list__item'})

                        if datas == []: 
                            if retry_num >= 10: 
                                crawl_flag = False
                                self.logger.info('데이터가 없습니다.')
                            else:
                                retry_num += 1
                                continue

                        for data in datas:
                            try:
                                wrt_dt = data.find('time')['datetime'] + ' 00:00:00'
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    product_url = 'https://www.gov.uk' + data.find('a')['href']
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
                                        # crawl_flag = False
                                        # break
                                    else: self.logger.error(f"IDX 확인 필요  >> {colct_data['idx']} ( {product_url} )")                                                                        
                                elif wrt_dt < self.start_date:
                                    if self.duplicate_cnt >= 50: 
                                        crawl_flag = False
                                        self.logger.info(f'수집기간 내 데이터 수집 완료')
                                        break
                            except Exception as e:
                                self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                        self.page_num += 1
                        if crawl_flag: self.logger.info(f'{self.page_num} 페이지로 이동 중 ..')
                    else: 
                        crawl_flag = False
                        raise Exception(f'통신 차단 :{url}')
                except Exception as e:
                    self.logger.error(f'crawl 통신 중 에러 >> {e}')
                    crawl_flag = False
                    self.error_cnt += 1
                    exc_type, exc_obj, tb = sys.exc_info()
                    self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)
        except Exception as e: self.logger.error(f'{e}')
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
            self.logger.info('수집종료')

    def crawl_detail(self, product_url):
        result = {'prdtNm':'', 'wrtDt':'', 'brand':'', 'prdtDtlCtn':'', 'prdtDtlCtn2':'', 'plor':'', 
                  'atchFlNm':'', 'atchFlPath':'', 'hrmflCuz':'', 'flwActn':'', 'recallNo':'',
                  'prdtDtlPgUrl':'', 'chnnlCd':0, 'idx':''}
        try:
            if self.page_num == 1: referer_url = 'https://www.gov.uk/product-safety-alerts-reports-recalls'
            else: referer_url = f'https://www.gov.uk/product-safety-alerts-reports-recalls?page={self.page_num}'
            custom_header = self.header
            custom_header['Referer'] = referer_url
            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)            
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)     

                html = BeautifulSoup(product_res.text, features='html.parser')

                try: result['prdtNm'] = html.find('h1', {'class':'gem-c-heading__text govuk-heading-l'}).text.strip()
                except: self.logger.error('제품명 추출 중 에러  >> ')

                try: 
                    wrt_dt = self.utils.parse_date(html.find('dl', {'class':'gem-c-metadata__list'}).find('dt', string='Published').find_next_siblings('dd')[0].text, self.chnnl_nm) + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except: self.logger.error('작성일 추출 중 에러  >> ')  
                
                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    product_information = html.find('h2',{'id':'product-information'}).find_next_sibling('table').find('tbody').find_all('tr')

                    prdt_dtl_ctn = ''
                    for info in product_information:
                        try:
                            title = info.find_all('td')[0].text.strip()
                            if title == 'Brand':
                                try: result['brand'] = info.find_all('td')[1].text.strip()
                                except: self.logger.error('브랜드 추출 중 에러  >> ')  
                            elif title == 'Country of Origin':
                                try: result['plor'] = info.find_all('td')[1].text.strip()
                                except: self.logger.error('원산지 추출 중 에러  >> ')  
                            elif title == 'Product Description':
                                try: result['prdtDtlCtn2'] = info.find_all('td')[1].text.strip()
                                except: self.logger.error('제품 상세설명2 추출 중 에러  >> ')  
                            elif title == 'Product Report and Image':

                                try: 
                                    atchl_url = info.find_all('td')[1].find('a')['href']
                                    atchl_res = self.utils.download_upload_atchl(self.chnnl_nm, atchl_url)
                                    if atchl_res['status'] == 200:
                                        result['atchFlPath'] = atchl_res['path']
                                        result['atchFlNm'] = atchl_res['fileNm']
                                    else:
                                        self.logger.info(f"첨부파일 이미 존재 : {atchl_res['fileNm']}")
                                except Exception as e: self.logger.error(f'첨부파일 추출 실패  >>  {e}')
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
                            result['hrmflCuz'] = '\n'.join(content)
                        elif content == []: raise Exception('위해원인 추출 중 에러  >> ')
                    except: self.logger.error('위해원인 추출 중 에러  >> ')

                    try: 
                        start_tag = html.find('h2', {'id':'corrective-action'})  # 시작점 찾기
                        end_tag = html.find('h2', {'id':'corrective-action'}).find_next_siblings('h2')  # 끝점 찾기
                        if end_tag == []: end_tag = html.find('h2', {'id':'corrective-action'}).find_next_siblings('h3')

                        content = self.utils.extract_content(start_tag, end_tag)

                        if content != []: 
                            result['flwActn'] = '\n'.join(content)
                        elif content ==[] : raise Exception('후속조치 추출 중 에러  >> ')
                    except: self.logger.error('후속조치 추출 중 에러  >> ')

                    try:
                        add_infos = html.find('h3', {'id':'additional-information'}).find_next_siblings()
                        recall_no = [info.text.replace('PSD case number:','').strip() for info in add_infos if 'PSD case number' in info.text][0]
                        result['recallNo'] = recall_no
                    except: self.logger.error('리콜번호 추출 중 에러  >> ')

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >> {product_url}')
            
        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')

        return dup_flag, result