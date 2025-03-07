from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class FDARecall():
    def __init__(self, chnnl_cd, chnnl_nm, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_cd = chnnl_cd
        self.chnnl_nm = chnnl_nm
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.header = {
            'Accept':'application/json, text/javascript, */*; q=0.01',
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
                headers = self.header
                while(crawl_flag):
                    try:
                        url = f'https://www.fda.gov/datatables/views/ajax?search_api_fulltext=&field_regulated_product_field=All&field_terminated_recall=All&draw=8&columns%5B0%5D%5Bdata%5D=0&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=1&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=2&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=3&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=4&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=5&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=6&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=false&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=7&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=false&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&start={self.page_num}&length=10&search%5Bvalue%5D=&search%5Bregex%5D=false&_drupal_ajax=1&_wrapper_format=drupal_ajax&pager_element=0&view_args=&view_base_path=safety%2Frecalls-market-withdrawals-safety-alerts%2Fdatatables-data&view_display_id=recall_datatable_block_1&view_dom_id=ec0c513a490ccd84f8e30ddac152a7c25c282a044f947c3f53b5db8dcbba15b6&view_name=recall_solr_index&view_path=%2Fsafety%2Frecalls-market-withdrawals-safety-alerts&total_items=919'
                        self.logger.info('수집 시작')
                        headers['Referer'] = 'https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts'
                        res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                       

                            datas = json.loads(res.text)['data']

                            if datas == []: 
                                if retry_num >= 10: 
                                    crawl_flag = False
                                    self.logger.info('데이터가 없습니다.')
                                else:
                                    retry_num += 1
                                    continue

                            for data in datas:
                                try:
                                    wrt_dt_date = self.utils.parse_date(BeautifulSoup(data[0], 'html.parser').find('time')['datetime'].split('T')[0], self.chnnl_nm)
                                    wrt_dt_time = BeautifulSoup(data[0], 'html.parser').find('time')['datetime'].split('T')[1].replace('Z','')
                                    wrt_dt = wrt_dt_date + ' ' + wrt_dt_time
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        product_url = 'https://www.fda.gov' + BeautifulSoup(data[1], 'html.parser').find('a')['href']
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

                            self.page_num += 10
                            if crawl_flag: self.logger.info(f'{int(self.page_num/10)}페이지로 이동 중..')
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
        result = { 'wrtDt':'', 'recallBzenty':'', 'brand':'', 'prdtNm':'', 'prdtImgFlPath':'', 'prdtImgFlNm':'',
                   'prdtDtlCtn':'', 'flwActn':'', 'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        try:
            custom_header = self.header
            custom_header['Host'] = 'www.fda.gov'
            # if self.page_num == 0: referer_url = 'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A101'
            # else: referer_url = f'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A101&page=%2C1%2C{self.page_num}'            
            # custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)

                html = BeautifulSoup(product_res.text, 'html.parser')

                main = html.find('div', {'role':'main'})

                infos = main.find('div', {'class':'inset-column'}).find_all('dt')
                for info in infos:
                    title = info.text.strip()
                    content = info.find_next_sibling()
                    try:
                        if title == 'FDA Publish Date:':
                            try: 
                                wrt_dt_date = self.utils.parse_date(content.find('time')['datetime'].split('T')[0], self.chnnl_nm)
                                wrt_dt_time = content.find('time')['datetime'].split('T')[1].replace('Z','')
                                wrt_dt = wrt_dt_date + ' ' + wrt_dt_time
                                result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                            except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  ')
                        elif title == 'Company Name:':
                            try: result['recallBzenty'] = content.text.strip()
                            except Exception as e: self.logger.error(f'리콜업체체 수집 중 에러  >>  ')
                        elif title == 'Brand Name:':
                            try: result['brand'] = content.text.replace('Brand Name(s)','').strip()
                            except Exception as e: self.logger.error(f'브랜드드 수집 중 에러  >>  ')
                        elif title == 'Product Description:':
                            try: result['prdtNm'] = content.text.replace('Product Description','').strip()
                            except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  ')

                    except Exception as e: self.logger.error(f'{e}')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    infos2 = main.find('h2', {'id':'recall-announcement'}).find_next_siblings()
                    prdt_dtl_ctn = ''
                    flw_actn = ''
                    for info in infos2:
                        try:
                            if info.name == 'p': prdt_dtl_ctn += info.text.strip()
                            elif info.name == 'div':
                                if info.find('table'): prdt_dtl_ctn += self.extraction_text_from_table(info)
                            elif info.name == 'ul':
                                tags = info.find_all('li')
                                for idx, tag in enumerate(tags):
                                    try:
                                        if tag.find('a'): prdt_dtl_ctn +=  f"{tag.text.strip()} ( {tag.find('a')['href']} )"
                                        else: prdt_dtl_ctn += tag.text.strip()
                                    except Exception as e: self.logger.error(f'{idx}번째 li태그 수집 중 에러  >>  {e}')
                        except Exception as e: self.logger.error(f'{e}')

                    result['prdtDtlCtn'] = prdt_dtl_ctn
                    result['flwActn'] = prdt_dtl_ctn
                                    
                    image_info = html.find('div', {'id':'recall-photos'})
                    if image_info != None:
                        images_paths = []
                        images_files = []
                        images = image_info.find_all('img')
                        for idx, image in enumerate(images):
                            try:
                                img_url = 'https://www.fda.gov' + image['src']
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

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')

        return dup_flag, result

    def extraction_text_from_table(self, info):
        result = ''
        try:
            table_contents = []
            thead = [title.text.replace('\\x0','').strip() for title in info.find('thead').find_all('th')]
            tbodies = [rows for rows in info.find('tbody').find_all('tr')]
            for tbody in tbodies:
                try:
                    items = tbody.find_all('td')
                    row_content = ''
                    for idx, row in enumerate(items):
                        try:
                            row_content += f'{thead[idx]} = {row.text} | ' if row != items[-1] else f'{thead[idx]} = {row.text}'
                        except Exception as e: self.logger.error(f'{idx}번째 항목 추출 중 에러  >>  {e}')
                    table_contents.append(row_content)
                except Exception as e: self.logger.error(f'{e}')
            result = ' \n ' + ' \n '.join(table_contents) + ' \n '
        except Exception as e:
            self.logger.error(f'제품 상세내용 테이블 태그에서 텍스트 추출 중 에러  >>  {e}')

        return result