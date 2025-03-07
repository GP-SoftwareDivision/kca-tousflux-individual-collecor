from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import sys
import time

class BVL():
    def __init__(self, chnnl_cd, chnnl_nm, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_cd = chnnl_cd
        self.chnnl_nm = chnnl_nm
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 1
        self.header = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding':'zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
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
                    headers = self.header
                    if self.page_num == 1: url = 'https://www.lebensmittelwarnung.de/DE/Home/home_node.html'
                    else:
                        headers['Referer'] = url
                        url = f'https://www.lebensmittelwarnung.de/DE/Home/home_node.html?gtp=310780_list1%253D{self.page_num}'
                    self.logger.info('수집 시작')
                    res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)                            
                        html = BeautifulSoup(res.text, features='html.parser')

                        datas = html.find_all('li',{'class':'lmw-search__results-element'})

                        if datas == []: 
                            if retry_num >= 10: 
                                crawl_flag = False
                                self.logger.info('데이터가 없습니다.')
                            else:
                                retry_num += 1
                                continue

                        for data in datas:
                            try:
                                wrt_dt = self.utils.parse_date(data.find('time').text.strip(), self.chnnl_nm) + ' 00:00:00'
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    self.total_cnt += 1
                                    product_url = data.find('a')['href']
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
        except Exception as e: self.logger.error(f'{e}')
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
            self.logger.info('수집종료')
                
    def crawl_detail(self, product_url):
        result = { 'wrtDt':'', 'prdtImgFlPath':'', 'prdtImgFlNm':'', 'prdtNm':'', 'prdtDtlCtn':'', 'bsnmNm':'',
                   'hrmflCuz':'', 'ntslCrst':'', 'flwActn':'', 'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        try:
            custom_header = self.header
            if self.page_num == 1: referer_url = 'https://www.lebensmittelwarnung.de/DE/Home/home_node.html'
            else: referer_url = f'https://www.lebensmittelwarnung.de/DE/Home/home_node.html?gtp=310780_list1%253D{self.page_num}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try: 
                    wrt_dt = self.utils.parse_date(html.find('time')['datetime'].strip(), self.chnnl_nm) + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  ')

                main = html.find('main', {'id':'lmw-main'})

                section1 = main.find('div', {'class':'lmw-section'})
                infos1 = section1.find('div', {'class':'lmw-section__content'}).find_all('dt', {'class':'lmw-description-list__term'})
                bsnm_nm = ''
                prdt_dtl_ctn = ''
                for info in infos1:
                    try:
                        title = info.text.strip()
                        if title == 'Produktbezeichnung/ -beschreibung:':
                            try:
                                prdt_nm = info.find_next_sibling().text.strip()
                                result['prdtNm'] = prdt_nm
                            except Exception as e: raise Exception(f'제품명 수집 중 에러  >>  {e}')
                        elif title == 'Haltbarkeit:':
                            try:
                                prdt_dtl_ctn += info.find_next_sibling().text.strip()
                            except Exception as e: raise Exception(f'제품상세내용 수집 중 에러  >>  {e}')
                        elif title == 'Verpackungseinheit:':
                            try:
                                prdt_dtl_ctn += info.find_next_sibling().text.strip()
                            except Exception as e: raise Exception(f'제품상세내용 수집 중 에러  >>  {e}')
                        elif title == 'Hersteller / Inverkehrbringer:':
                            try:
                                bsnm_nm += info.find_next_sibling().text.strip()
                                result['bsnmNm'] = bsnm_nm
                            except Exception as e: raise Exception(f'업체 수집 중 에러  >>  {e}')                            
                    except Exception as e: self.logger.error(f'{e}')

                result['prdtDtlCtn'] = prdt_dtl_ctn
                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    image_info = section1.find('div', {'class':'lmw-section__head'})
                    if image_info != None:
                        images_paths = []
                        images_files = []
                        images = image_info.find_all('img')
                        for idx, image in enumerate(images):
                            try:
                                img_url = 'https://www.lebensmittelwarnung.de' + image['src']
                                img_res = self.utils.download_upload_image(self.chnnl_nm, img_url)
                                if img_res['status'] == 200:
                                    images_paths.append(img_res['path'])
                                    images_files.append(img_res['fileNm'])
                                else:
                                    self.logger.info(f"이미지 이미 존재 : {img_res['fileNm']}")                                
                            except Exception as e:
                                self.logger.error(f'{idx}번째 이미지 수집 중 에러  >>  ')
                        result['prdtImgFlPath'] = ' , '.join(set(images_paths))
                        result['prdtImgFlNm'] = ' , '.join(images_files)
                                        
                    section2 = main.find_all('section', {'class':'lmw-section lmw-toggle'})

                    for section in section2:
                        title = section.find('h2').text.strip()
                        try:
                            if 'Was ist der Grund der Meldung?' in title:
                                try:
                                    hrmflCuz = section2[0].find('div', {'class':'lmw-section__block'}).text.strip()
                                    result['hrmflCuz'] = self.utils.get_clean_string(hrmflCuz)
                                except Exception as e: raise Exception(f'위해원인 수집 중 에러  >>  {e}')
                            elif 'Wo war das Produkt auf dem Markt?' in title:
                                try:
                                    ntsl_crst = section2[0].find('div', {'class':'lmw-section__block'}).text.strip()
                                    result['ntslCrst'] = self.utils.get_clean_string(ntsl_crst)
                                except Exception as e: raise Exception(f'판매현황 수집 중 에러  >>  {e}')
                            elif 'Was kann ich tun, wenn ich das Produkt zu Hause habe?' in title:
                                try:
                                    flw_actn = section2[0].find('div', {'class':'lmw-section__block'}).text.strip()
                                    result['flwActn'] = self.utils.get_clean_string(flw_actn)
                                except Exception as e: raise Exception(f'후속조치 수집 중 에러  >>  {e}')
                        except Exception as e: self.logger.error(f'{e}')

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')

        return dup_flag, result