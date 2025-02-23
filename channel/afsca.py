from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time

class AFSCA():
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
            'Host':'favv-afsca.be',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        }

        self.locale_str = ''

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
                        if self.page_num == 0: url = 'https://favv-afsca.be/fr/produits'
                        else: url = f'https://favv-afsca.be/fr/produits?page={self.page_num}'
                        self.logger.info('수집 시작')
                        res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')

                            datas = html.find('div', {'class':'view--products--page'}).find('div', {'class':'view__content'}).find_all('li')
                            for data in datas:
                                try:
                                    try: self.locale_str = html.find('html')['lang']
                                    except: self.locale_str = ''

                                    wrt_dt = self.utils.parse_date(data.find('time')['datetime'], self.chnnl_nm)
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        product_url = 'https://favv-afsca.be' + data.find('a')['href']
                                        colct_data = self.crawl_detail(product_url)
                                        req_data = json.dumps(colct_data)
                                        insert_res = self.api.insertData2Depth(req_data)
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
                self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt}')
                self.logger.info('수집종료')
                
    def crawl_detail(self, product_url):
        result = { 'wrtDt':'','prdtImg':'', 'hrmflCuz':'', 'hrmflCuz2':'', 'flwActn':'',
                   'prdtNm':'', 'brand':'', 'bsnmNm':'', 'prdtDtlCtn':'', 'url':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        # 게시일, 위해원인 hrmfl_cuz, 제품 상세내용 prdt_dtl_ctn, 제품명 prdt_nm, 위해/사고?, 정보출처 recall_srce?
        try:
            custom_header = self.header
            if self.page_num == 0: referer_url = 'https://favv-afsca.be/fr/produits'
            else: referer_url = f'https://favv-afsca.be/fr/produits?page={self.page_num}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                main = html.find('div', {'class':'node__wrapper node__wrapper--main'})
                try:
                    wrt_dt = main.find('time')['datetime'].strip()
                    result['wrtDt'] = self.utils.parse_date(wrt_dt, self.chnnl_nm)
                except Exception as e: raise Exception(f'게시일 수집 중 에러  >>  ')

                # try:
                #     imgs = main.find('div', {'class':'node__product-images'}).find_all('img')
                #     for idx, img in enumerate(imgs):
                #         try:
                #             img_url = 'https://favv-afsca.be' + img['src']
                #         except Exception as e: raise Exception (f'{idx}벉째 이미지 추출 중 에러  >>  ')
                #     result['wrtDt'] = self.utils.parse_date(wrt_dt, self.chnnl_nm)
                # except Exception as e: raise Exception(f'제품 이미지 수집 중 에러  >>  ')      

                
                 
                          



                try:
                    prdt_nm = self.utils.get_clean_string(main.find('div', {'class':'row'}).find('h1').text.strip())
                    result['prdtNm'] = prdt_nm if len(prdt_nm) < 1000 else prdt_nm[:1000]
                except Exception as e: raise Exception(f'제품명 수집 중 에러  >>  ')

                imgs = main.find('div', {'class':'glide__nav'}).find_all('img')
                img_list = []
                for img in imgs:
                    try:
                        img_url = 'https://www.productsafety.govt.nz' + img['src']
                    except Exception as e: self.logger.error(f'{e}')
                
                try:
                    hrmfl_cuz = self.utils.get_clean_string(main.find('div',{'class':'recall__info recall__info--hazard'}).text.replace('The Hazard!', '').strip())
                    result['hrmflCuz'] = hrmfl_cuz
                except Exception as e: raise Exception(f'위해원인 수집 중 에러  >>  ')

                try:
                    flw_actn = self.utils.get_clean_string(main.find('div',{'class':'recall__info recall__info--whattodo'}).text.replace('What to do...', '').strip())
                    result['flwActn'] = flw_actn if len(flw_actn) < 2000 else flw_actn[:2000]
                except Exception as e: raise Exception(f'후속조치 수집 중 에러  >>  ')
                
                

                infos = main.find('div', {'class':'typography recall__content recall__content--mobile'}).find_all('div', {'class':'recall__content-block'})
                for info in infos:
                    try:
                        title = info.find('h4').text.strip()
                        content = infos[0].text.strip()
                        if title == 'Product Identifiers':
                            try:
                                prdt_dtl_cnt = content.replace(title, '')
                                result['prdtDtlCtn'] = prdt_dtl_cnt
                            except Exception as e: raise Exception(f'제품상세내용 수집 중 에러  >>  ')
                        elif title == 'Supplier Contact':
                            try:
                                distb_bzenty = content.replace(title, '')
                                result['distbBzenty'] = distb_bzenty if len(distb_bzenty) < 300 else distb_bzenty[:300]
                            except Exception as e: raise Exception(f'공급업체 수집 중 에러  >>  ')
                    except Exception as e: self.logger.error(f'{e}')

                result['url'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result['url'], self.chnnl_nm, result['prdtNm'])                            
            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {product_res.status_code}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result