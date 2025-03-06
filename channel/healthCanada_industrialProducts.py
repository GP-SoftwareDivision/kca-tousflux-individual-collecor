from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class HCIP():
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
            'Host':'recalls-rappels.canada.ca',
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
                while(crawl_flag):
                    try:
                        if self.page_num == 0: url = 'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A101'
                        else: url = f'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A101&page=%2C1%2C{self.page_num}'
                        self.logger.info('수집 시작')
                        res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')
                            datas = html.find_all('div', {'class':'search-result views-row'})

                            for data in datas:
                                try:
                                    wrt_dt = data.find('span', {'class':'ar-type'}).text.split('|')[1].strip() + ' 00:00:00'
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        product_url = 'https://recalls-rappels.canada.ca' + data.find('a')['href']
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
        extract_error = False
        result = { 'wrtDt':'', 'prdtNm':'', 'prdtImgFlNm':'', 'prdtImgFlPath': '', 
                  'prdtDtlCtn':'', 'hrmflCuz':'', 'flwActn':'', 'ntslCrst':'', 
                   'ntslPerd':'', 'plor':'', 'distbBzenty':'', 
                   'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        try:
            custom_header = self.header
            if self.page_num == 0: referer_url = 'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A101'
            else: referer_url = f'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A101&page=%2C1%2C{self.page_num}'            
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)

                html = BeautifulSoup(product_res.text, 'html.parser')

                try: 
                    wrt_dt = html.find('time').text.strip() + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                try: result['prdtNm'] = html.find('div',{'class':'field field--name-field-product field--label-inline'}).text.replace('\n',' ').replace('Product ','').strip()                
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}')

                try:
                    images = html.find('div',{'class':'product-images'}).find_all('a')
                    images_paths = []
                    images_files = []
                    for idx, image in enumerate(images):
                        try:
                            img_url = 'https://recalls-rappels.canada.ca'+image['href']
                            # img_nm = json.loads(image['data-media'])['token']
                            img_res = self.utils.download_upload_image(self.chnnl_nm, img_url)
                            if img_res['status'] == 200:
                                images_paths.append(img_res['path'])
                                images_files.append(img_res['fileNm'])
                            else:
                                self.logger.info(f"이미지 이미 존재 : {img_res['fileNm']}")                                
                        except Exception as e:
                            self.logger.error(f'{idx}번째 이미지 수집 중 에러  >>  {e}')
                    result['prdtImgFlPath'] = ' , '.join(set(images_paths))
                    result['prdtImgFlNm'] = ' , '.join(images_files)
                except Exception as e: self.logger.error(f'제품 이미지 수집 중 에러  >>  {e}'); extract_error = True

                try: result['prdtDtlCtn'] = self.utils.get_clean_string(html.find('div',{'class':'ar-affected-products ar-section'}).text.replace('\n',' ').replace('Affected products','').strip())
                except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >>  {e}')

                try: result['hrmflCuz'] = self.utils.get_clean_string(html.find('div',{'class':'ar-issue-long ar-section'}).text.replace('\n',' ').replace('Issue','').strip())
                except Exception as e: self.logger.error(f'위해원인 수집 중 에러  >>  {e}') 

                try: result['flwActn'] = self.utils.get_clean_string(html.find('div',{'class':'ar-action-long ar-section'}).find('strong').text.replace('\n',' ').strip())
                except Exception as e: self.logger.error(f'후속조치 수집 중 에러  >>  {e}')    

                additional_information = html.find('div',{'class':'ar-additional-info ar-section'}).find_all('details')
                background = [info for info in additional_information if info.summary and 'Background' in info.summary.text]
                if background != []:
                    items = background[0].find_all('p')
                    for i in range(0, len(items), 2):
                        try:
                            if 'Number Sold' in items[i].text:
                                try: result['ntslCrst'] = self.utils.get_clean_string(items[i+1].text.strip())
                                except Exception as e: raise Exception(f'판매현황 수집 중 에러  >>  {e}')
                            elif 'Time Period Sold' in items[i].text:
                                try: result['ntslPerd'] = self.utils.get_clean_string(items[i+1].text.strip())
                                except Exception as e: raise Exception(f'판매기간 수집 중 에러  >>  {e}')
                            elif 'Place of Origin' in items[i].text:
                                try: result['plor'] = self.utils.get_clean_string(items[i+1].text.strip())
                                except Exception as e: raise Exception(f'원산지 수집 중 에러  >>  {e}')
                        except Exception as e: self.logger.error(f'판매현황/판매기간/원산지 수집 중 에러  >>  {e}')

                details = [info for info in additional_information if info.summary and 'Details' in info.summary.text][0].find('div', {'class':'field field--name-companies'})
                distb_bzenty = details.find('div',{'class':'field--item'}).text.replace('\n',' ').strip().split(' ')
                try: result['distbBzenty'] = self.utils.get_clean_string(' '.join(distb_bzenty) if distb_bzenty[0]=='Retailer' else '')
                except Exception as e: self.logger.error(f'유통업체 수집 중 에러  >>  {e}')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)
            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result
