from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import sys
import time

class ConsumerCouncil():
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
            'Accept-Encoding':'gzip, deflate, br',
            'Accept-Language':'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
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
                org_url = 'https://www.consumer.org.hk/en/consumer-alert?page=<%pageNum%>'
                while(crawl_flag):
                    try:
                        headers = self.header
                        self.logger.info('수집 시작')
                        url = org_url.replace('<%pageNum%>', str(self.page_num+1))

                        if self.page_num != 0: 
                            headers['Referer'] = org_url.replace('<%pageNum%>', str(self.page_num))
                        res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')

                            datas = html.find_all('li', {'class': 'v-shadow-blk-list__blk'})

                            if datas == []: 
                                if retry_num >= 10: 
                                    crawl_flag = False
                                    self.logger.info('데이터가 없습니다.')
                                else:
                                    retry_num += 1
                                    continue

                            for data in datas:
                                try:
                                    product_url = 'https://www.consumer.org.hk' + data.find('a')['href']
                                    date_day = datetime.strptime(data.find('li').text.strip(), '%Y.%m.%d').strftime('%Y-%m-%d')
                                    wrt_dt = date_day + ' 00:00:00'
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
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
        result = {'prdtNm':'', 'wrtDt':'', 'brand': '', 'plor': '', 'prdtDtlCtn':'', 
                  'hrmflCuz':'', 'flwActn':'', 'flwActn2':'', 'prdtImgFlNm':'', 'prdtImgFlPath': '', 
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            custom_header = self.header
            referer_url = f'https://www.consumer.org.hk/en/consumer-alert?page={self.page_num+1}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try: 
                    date_text = html.find('div', {'class': 'cover-article__main'}).find('ul').text.strip()
                    date_day = datetime.strptime(date_text, '%Y.%m.%d').strftime('%Y-%m-%d')
                    wrt_dt = date_day + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                table = html.find('table')
                if table:
                    try:
                        tags = html.find('table').find_all('th')
                        if tags and ':' in tags[0].text:
                            dtl_ctns = []
                            for tag in tags:
                                header = tag.text.strip()
                                td = tag.find_next_sibling('td')
                                td_text = self.utils.get_clean_string(td.text.strip()) if td else ''
                                if 'Product name' in header:
                                    result['prdtNm'] = td_text
                                elif 'Brand' in header:
                                    result['brand'] = td_text
                                elif 'Place of origin' in header:
                                    result['plor'] = td_text
                                else:
                                    dtl_ctns.append(f'{header} {td_text}')
                            result['prdtDtlCtn'] = '\n'.join(dtl_ctns)
                        else:
                            for tag in tags:
                                header = tag.text.strip()
                                td = tag.find_next_sibling('td')
                                td_text = self.utils.get_clean_string(td.text.strip().replace('\xa0', '\n')) if td else ''
                                if 'Product Name' in header:
                                    result['prdtNm'] = td_text
                                    result['prdtDtlCtn'] = td_text
                                elif 'Reason For Issuing Alert' in header:
                                    result['hrmflCuz'] = td_text
                                elif 'Action Taken by the Centre for Food Safety' in header:
                                    result['flwActn'] = td_text
                                elif 'Advice to Consumers' in header:
                                    result['flwActn2'] = td_text
                    except Exception as e: self.logger.error(f'제품 정보 수집 중 에러  >>  {e}')
                else:
                    try: 
                        result['prdtNm'] = html.find('h1').text.strip()
                    except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}')

                    try: 
                        result['prdtDtlCtn'] = self.utils.get_clean_string(html.find('div', class_=['cover-article__content']).text.strip())
                    except Exception as e: self.logger.error(f'제품 상세 내용 수집 중 에러  >>  {e}')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)    

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    try:
                        image_info = html.find('div', class_=['cover-article__content'])
                        if image_info != None:                          
                            images = image_info.find_all('img')
                            images_paths = []
                            images_files = []
                            for idx, image in enumerate(images):
                                try:
                                    img_url = image['src']
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
                    except Exception as e: self.logger.error(f'제품 이미지 수집 중 에러  >>  {e}')
                        
            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')

        return dup_flag, result