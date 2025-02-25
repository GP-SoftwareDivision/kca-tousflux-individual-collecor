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
                prdt_dtl_ctn = ''
                products = [info for info in main.find_all('p') if 'Description du produit' in info.text or 'Description des produits' in info.text][0].find_next_siblings('ul')
                infos = [product.find_all('li', recursive=False) for product in products]
                if infos == []:
                    try:
                        prdouct_description = [info for info in main.find_all('p') if 'Description du produit' in info.text or 'Description des produits' in info.text][0]
                        if 'Marque' not in prdouct_description.text:
                            infos = [info for info in main.find_all('p') if 'Description du produit' in info.text or 'Description des produits' in info.text][0].find_next_sibling('p').find_all('span', recursive=False)

                            for content in infos:
                                try:
                                    text = content.text.replace('\xa0','').strip()
                                    prdt_dtl_ctn += f'{text} \n ' if content != infos[-1] else text
                                except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >> {e}')     
                        else:
                            prdt_dtl_ctn += prdouct_description.text.replace('Description du produit :', '').replace('\xa0', ' ')
                    except: 
                        infos = [info.text.strip() for info in main.find_all('p') if 'Marque : ' in info.text]
                        prdt_dtl_ctn += ' ª '.join(infos)
                else:
                    for idx, info in enumerate(infos):
                        try:
                            for content in info:
                                try:
                                    text = content.text.replace('\xa0','').strip()
                                    prdt_dtl_ctn += f'{text} | ' if content != info[-1] else text                                        
                                except Exception as e: self.logger.error(f'{idx}번째 제품 상세내용 수집 중 에러  >> {e}')

                            if info != infos[-1]: prdt_dtl_ctn += ' ª '
                        except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >> {e}')



                # for info in infos:
                #     content = info.text
                #     try:
                #         if 'Nom' in content or 'Nom des produits' in content or 'Marque' in content:
                #             detail_infos = content.split('\n')
                #             for detail_info in detail_infos:
                #                 try:
                #                     if 'Nom' in detail_info or 'Nom des produits' in detail_info:
                #                         try: result['prdtNm'] = detail_info.replace('Nom des produits : ', '').replace('Nom : ', '').replace('Nom des produits\xa0: ', '').replace('Nom\xa0: ', '').replace('Nom du produit\xa0: ', '')
                #                         except Exception as e: raise Exception(f'제품명 수집 중 에러  >>  {e}')
                #                     elif 'Marque' in content:
                #                         try: result['brand'] = detail_info.replace('Marque : ', '').replace('Marque\xa0: ', '')
                #                         except Exception as e: raise Exception(f'브랜드 수집 중 에러  >>  {e}')
                #                 except Exception as e: self.logger.error(f'{e}')
                #         else:
                #             try: result['prdtDtlCtn'] += content if info == infos[-1] else f'{content} | '
                #             except Exception as e: raise Exception(f'제품 상세내용 수집 중 에러  >>  {e}')
                #     except Exception as e: self.logger.error(f'{e}')  

                # infos = [info for info in main.find_all('p') if 'Description du produit' in info.text][0].find_next_siblings('ul')
                # brand = ''
                # prdt_nm = ''
                # prdt_dtl_ctn = ''
                # for info in infos:
                #     try:
                #         self.crawl_infos(infos, brand, prdt_nm, prdt_dtl_ctn)
                #         if info != infos[-1]:
                #             brand += ' | '
                #             prdt_nm += ' | '
                #             prdt_dtl_ctn += ' | '
                #     except Exception as e: self.logger.error(f'{e}')


                bsnm_nm = [info for info in main.find_all('p') if 'Le produit a été distribué par ' in info.text]
                if bsnm_nm != []:
                    try: result['bsnmNm'] = bsnm_nm[0].text.strip()
                    except Exception as e: self.logger.error(f'업체 수집 중 에러  >>  {e}')
                    
                result['prdtDtlCtn'] = prdt_dtl_ctn
                result['url'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result['url'], self.chnnl_nm, result['prdtNm'])                            
            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {product_res.status_code}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result
    
    # def crawl_infos(self, infos, brand, prdt_nm, prdt_dtl_ctn):
    #     try:
    #         for info in infos:
    #             content = info.text
    #             try:
    #                 if 'Nom' in content or 'Nom des produits' in content or 'Marque' in content:
    #                     detail_infos = content.split('\n')
    #                     for detail_info in detail_infos:
    #                         try:
    #                             if 'Nom' in detail_info or 'Nom des produits' in detail_info:
    #                                 try: result['prdtNm'] = detail_info.replace('Nom des produits : ', '').replace('Nom : ', '')
    #                                 except Exception as e: raise Exception(f'제품명 수집 중 에러  >>  {e}')
    #                             elif 'Marque' in content:
    #                                 try: result['brand'] = detail_info.replace('Marque\xa0: ', '')
    #                                 except Exception as e: raise Exception(f'브랜드 수집 중 에러  >>  {e}')
    #                         except Exception as e: self.logger.error(f'{e}')
    #                 else:
    #                     try: result['prdtDtlCtn'] += content if info == infos[-1] else f'{content} | '
    #                     except Exception as e: raise Exception(f'제품 상세내용 수집 중 에러  >>  {e}')
    #             except Exception as e: self.logger.error(f'{e}')        
    #     except Exception as e:
    #         self.logger.error(f'{e}')