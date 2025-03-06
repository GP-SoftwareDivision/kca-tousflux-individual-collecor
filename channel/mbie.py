from datetime import datetime
from bs4 import BeautifulSoup
from common.utils import Utils
import random
import requests
import sys
import time

class MBIE():
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
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        }

        self.locale_str = ''

        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0
        self.prdt_dtl_err_url = []

        self.utils = Utils(logger, api)

    def crawl(self):
        try:
            retry_num = 0
            crawl_flag = True     
            while(crawl_flag):
                try:
                    if self.page_num == 0: url = 'https://www.productsafety.govt.nz/recalls'
                    else: url = f'https://www.productsafety.govt.nz/recalls?start={self.page_num}'
                    self.logger.info('수집 시작')
                    res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)                            
                        html = BeautifulSoup(res.text, features='html.parser')

                        datas = html.find('div', {'class':'recalls__grid'}).find_all('article')
                        if len(datas) == 0:
                            if retry_num >= 10:
                                crawl_flag = False
                                self.logger.info('데이터가 없습니다.')
                            else:
                                retry_num += 1
                                continue
                            
                        for data in datas:
                            try:
                                try: self.locale_str = html.find('html')['lang']
                                except: self.locale_str = ''

                                wrt_dt = self.utils.parse_date(data.find('time')['datetime'], self.chnnl_nm) + ' 00:00:00'
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    self.total_cnt += 1
                                    product_url = 'https://www.productsafety.govt.nz' + data.find('a')['href']
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
                                        crawl_flag = False
                                        break
                                    else: self.logger.error(f"IDX 확인 필요  >> {colct_data['idx']} ( {product_url} )")
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
        dup_flag = -1
        result = { 'wrtDt':'', 'prdtNm':'', 'prdtImgFlPath':'', 'prdtImgFlNm':'', 
                  'prdtDtlCtn':'', 'distbBzenty':'', 'hrmflCuz':'', 'flwActn':'', 
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        # 게시일, 위해원인 hrmfl_cuz, 제품 상세내용 prdt_dtl_ctn, 제품명 prdt_nm, 위해/사고?, 정보출처 recall_srce?
        try:
            custom_header = self.header
            if self.page_num == 0: referer_url = 'https://www.productsafety.govt.nz/recalls'
            else: referer_url = f'https://www.productsafety.govt.nz/recalls?start={self.page_num}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                main = html.find('div', {'id':'main'})

                try:
                    wrt_dt = main.find('div', {'class':'date recall__date'}).text.strip()
                    date_text = self.utils.parse_date(wrt_dt, self.chnnl_nm) + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(date_text, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: raise Exception(f'게시일 수집 중 에러  >>  {e}')

                try:
                    prdt_nm = self.utils.get_clean_string(main.find('div', {'class':'row'}).find('h1').text.strip())
                    result['prdtNm'] = prdt_nm
                except Exception as e: raise Exception(f'제품명 수집 중 에러  >>  {e}')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    try:
                        images = main.find('div', {'class':'glide__nav'}).find_all('img')
                        images_paths = []
                        images_files = []
                        for idx, image in enumerate(images):
                            try:
                                img_url = 'https://www.productsafety.govt.nz' + image['src']
                                img_res = self.utils.download_upload_image(self.chnnl_nm, img_url)
                                if img_res['status'] == 200:
                                    images_paths.append(img_res['path'])
                                    images_files.append(img_res['fileNm'])
                                else:
                                    self.logger.info(f"{img_res['message']} : {img_res['fileNm']}")                                
                            except Exception as e:
                                self.logger.error(f'{idx}번째 이미지 수집 중 에러  >>  {img_url}')
                        result['prdtImgFlPath'] = ' , '.join(set(images_paths))
                        result['prdtImgFlNm'] = ' , '.join(images_files)
                    except Exception as e: self.logger.error(f'제품 이미지 수집 중 에러  >>  {e}')
                    
                    try:
                        hrmfl_cuz = self.utils.get_clean_string(main.find('div',{'class':'recall__info recall__info--hazard'}).text.replace('The Hazard!', '').strip())
                        result['hrmflCuz'] = hrmfl_cuz
                    except Exception as e: raise Exception(f'위해원인 수집 중 에러  >>  {e}')

                    try:
                        flw_actn = self.utils.get_clean_string(main.find('div',{'class':'recall__info recall__info--whattodo'}).text.replace('What to do...', '').strip())
                        result['flwActn'] = flw_actn
                    except Exception as e: raise Exception(f'후속조치 수집 중 에러  >>  {e}')
                    
                    infos = main.find('div', {'class':'typography recall__content recall__content--mobile'}).find_all('div', {'class':'recall__content-block'})
                    for info in infos:
                        try:
                            title = info.find('h4').text.strip()
                            content = infos[0].text.strip()
                            if title == 'Product Identifiers':
                                try:
                                    prdt_dtl_cnt = content.replace(title, '')
                                    result['prdtDtlCtn'] = prdt_dtl_cnt
                                except Exception as e: raise Exception(f'제품상세내용 수집 중 에러  >>  {e}')
                            elif title == 'Supplier Contact':
                                try:
                                    bsnm_nm = content.replace(title, '')
                                    result['bsnmNm'] = bsnm_nm
                                except Exception as e: raise Exception(f'공급업체 수집 중 에러  >>  {e}')
                        except Exception as e: self.logger.error(f'{e}')
            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')

        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')
            self.prdt_dtl_err_url.append(product_url)
        return dup_flag, result