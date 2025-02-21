from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import time

class RAPPELCONSOMMATEUR():
    def __init__(self, chnnl_cd, chnnl_nm, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_nm = chnnl_nm
        self.chnnl_cd = chnnl_cd        
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.header = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding':'gzip, deflate, br, zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'Host':'rappel.conso.gouv.fr',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }

        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0

        self.utils = Utils(logger, api)

    def crawl(self):
        crawl_flag = True
        while(crawl_flag):
            try:
                headers = self.header
                if self.page_num == 0: url = 'https://rappel.conso.gouv.fr/'
                else:
                    headers['Referer'] = url 
                    url = f'https://rappel.conso.gouv.fr/categorie/0/{self.page_num}'
                res = requests.get(url=url, headers=headers, verify=False, timeout=600)        
                if res.status_code == 200:
                    sleep_time = random.uniform(3,5)
                    self.logger.info(f'통신 성공, {sleep_time}초 대기')
                    time.sleep(sleep_time)                            
                    html = BeautifulSoup(res.text, features='html.parser')
                    datas = html.find('div',{'class':'products'}).find_all('li', {'class':'product-item'})
                    for data in datas:
                        try:
                            # test = self.utils.parse_date_from_text(recall.find('time')['datetime'], self.chnnl_nm)
                            date_day = self.utils.parse_date_from_text(data.find('time')['datetime'].split(' ')[0], self.chnnl_nm)
                            date_time = data.find('time')['datetime'].split(' ')[1]
                            wrt_dt = date_day + ' ' + date_time
                            if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                product_url = 'https://rappel.conso.gouv.fr' + data.find('a')['href']
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
                            else: 
                                crawl_flag = False
                                break
                        except Exception as e:
                            self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')

                    self.page_num += 1
                    if crawl_flag: self.logger.info(f'{self.page_num+1} 페이지로 이동 중 ..')
                else: 
                    crawl_flag = False
                    raise Exception('통신 차단')

            except Exception as e:
                self.logger.error(f'crawl 통신 중 에러 >> {e}')

    def crawl_detail(self, product_url):
        result = {'wrtDt':'', 'prdtNm':'', 'brand':'', 'recallBzenty':'',
                  'item':'', 'prdtDtlCtn':'', 'ntslPerd':'', 'prdtDtlCtn2':'', 'ntslCrst':'', 'distbBzenty':'',
                  'hrmflCuz':'', 'flwActn':'', 'flwActn2':'', 'url':'', 'chnnlCd':0, 'idx':''}
        try:
            if self.page_num == 0: referer_url = 'https://rappel.conso.gouv.fr/'
            else: referer_url = f'https://rappel.conso.gouv.fr/categorie/0/{self.page_num}'
            custom_header = self.header
            custom_header['Referer'] = referer_url
            res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)          
            if res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)       
                
                html = BeautifulSoup(res.text, features='html.parser')

                main = html.find('div', {'class':'card product-main'})

                try: 
                    wrt_dt_day = self.utils.parse_date_from_text(main.find('time')['datetime'].split(' ')[0], self.chnnl_nm)
                    wrt_dt_time = main.find('time')['datetime'].split(' ')[1]
                    wrt_dt = wrt_dt_day + ' ' + wrt_dt_time
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except: self.logger.error('작성일 추출 중 에러  >> ')  

                try:
                    prdt_nm = main.find('p' ,{'class':'h5 product-main-title'}).text.strip()
                    result['prdtNm'] = self.utils.get_clean_string(prdt_nm)
                except: self.logger.error('제품명 추출 중 에러  >> ')

                try: result['Brand'] = main.find('p', {'class':'text-muted product-main-brand'}).find('strong').text.strip()
                except Exception as e: self.logger.error('브랜드 추출 중 에러  >> ')  

                product_main_info_list = main.find('ul', {'class':'product-desc-list'}).find_all('li', {'class':'product-desc-item'})

                for info in product_main_info_list:
                    try:
                        title = info.find('span', {'class':'carac'}).text.strip().replace('\xa0',' ')
                        print(title)
                        content = info.find('span', {'class':'val'}).text.strip()
                        print(content)
                        if title == 'Origine de la fiche :':
                            try: result['recallBzenty'] = self.utils.get_clean_string(content)
                            except: raise Exception('리콜업체 추출 중 에러  >> ')
                    except Exception as e:
                        self.logger.error(f'메인 정보 수집 중 에러  >>  {e}')

                product_info_list1 = html.find('div', {'class':'card product-ident'}).find_all('li', {'class':'product-desc-item'})
                prdt_dtl_ctn = ''
                for info in product_info_list1:
                    try:
                        title = info.find('span', {'class':'carac'}).text.strip().replace('\xa0',' ')
                        print(title)
                        if title == 'Identification des produits': self.extract_info_from_table(info)
                        else: content = info.find('span', {'class':'val'}).text.strip()
                        print(content)
                        if title == 'Catégorie de produit':
                            try: result['item'] = self.utils.get_clean_string(content)
                            except: raise Exception('제품 카테고리 추출 중 에러  >> ')  
                        elif title == 'Noms des modèles ou références':
                            try: prdt_dtl_ctn += self.utils.get_clean_string(content)
                            except: raise Exception('제품 상세내용 추출 중 에러  >> ')
                        # elif title == 'Identification des produits':
                        #     try: prdt_dtl_ctn += content
                        #     except: raise Exception('제품 상세내용 추출 중 에러  >> ')            
                        elif title == 'Date début/Fin de commercialisation':
                            try: result['ntslPerd'] = self.utils.get_clean_content_string(content.replace(' ',''))
                            except: raise Exception('판매 기간 추출 중 에러  >> ')
                        elif title == 'Température de conservation':
                            try: result['prdtDtlCtn2'] = self.utils.get_clean_string(content)
                            except: raise Exception('제품 상세내용2 추출 중 에러  >> ')
                        elif title == 'Zone géographique de vente':
                            try: result['ntslCrst'] = self.utils.get_clean_string(content)
                            except: raise Exception('판매 현황 추출 중 에러  >> ')
                        elif title == 'Distributeurs':
                            try: result['distbBzenty'] = self.utils.get_clean_string(content)
                            except: raise Exception('유통업체 추출 중 에러  >> ')
                    except Exception as e:
                        self.logger.error(f'서브 정보1 수집 중 에러  >>  {e}')

                result['prdtDtlCtn'] = prdt_dtl_ctn

                product_info_list2 = html.find('div', {'class':'card product-practical'}).find_all('li', {'class':'product-desc-item'})
                flw_actn = ''
                for info in product_info_list2:
                    try:
                        title = info.find('span', {'class':'carac'}).text.strip().replace('\xa0',' ')
                        print(title)
                        content = info.find('span', {'class':'val'}).text.strip()
                        print(content)
                        if title == 'Motif du rappel':
                            try: result['hrmflCuz'] = self.utils.get_clean_string(content)
                            except: raise Exception('위해원인 추출 중 에러  >> ')  
                        elif title == 'Risques encourus par le consommateur':
                            try: result['hrmflCuz'] = self.utils.get_clean_string(content)   
                            except: raise Exception('위해원인 추출 중 에러  >> ')                           
                        elif title == 'Conduite à tenir par le consommateur':
                            try: result['flwActn'] = self.utils.get_clean_string(content)
                            except: raise Exception('후속조치 추출 중 에러  >> ')
                        elif title == 'Modalités de compensation':
                            try: result['flwActn2'] = self.utils.get_clean_string(content)
                            except: raise Exception('후속조치2 추출 중 에러  >> ')                            
                    except Exception as e:
                        self.logger.error(f'서브 정보2 수집 중 에러  >>  {e}')

            else: raise Exception('통신 차단')
            
        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')
        
        return result
    
    def extract_info_from_table(self, info):
        try:
            print()
        except Exception as e:
            self.logger.error('제품 상세내용 표에서 데이터 추출 중 에러')