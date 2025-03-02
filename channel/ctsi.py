from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CTSI():
    def __init__(self, chnnl_cd, chnnl_name, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.header = {
            'Accept':'*/*',
            'Accept-Encoding':'identity',
            'Accept-Language':'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control':'max-age=0',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }
        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0

        self.utils = Utils(logger, api)
        self.chnnl_nm = chnnl_name
        self.chnnl_cd = chnnl_cd

    def crawl(self):
        try:
            crawl_flag = True
            while(crawl_flag):
                try:
                    url = 'https://apps.tradingstandards.uk/navless/recall/listing2.asp'
                    self.logger.info('수집시작')
                    res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)

                        html = res.text
                        soup = BeautifulSoup(html, "html.parser")

                        datas = soup.find('tbody').find_all('tr')
                        
                        for data in datas:
                            try:
                                product_url = data.find('a').get('href')
                                wrt_dt = data.find('td').text 
                                wrt_dt = datetime.strptime(wrt_dt, '%Y-%m-%d').strftime('%Y-%m-%d 00:00:00')
                                
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    self.total_cnt += 1
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
                        if crawl_flag: self.logger.info(f'{self.page_num+1}페이지로 이동')
                    else: raise Exception(f'통신 차단 : {url}')
                except Exception as ex:
                    self.logger.error(f'crawl 통신 중 에러 >> {ex}')
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
        extract_error = False
        product_url = f'https://apps.tradingstandards.uk/navless/recall/' + product_url
        result = {'wrtDt':'', 'prdtNm':'', 'hrmflCuz':'', 'prdtDtlCtn':'', 'brand': '',
                  'plor': '', 'flwActn': '', 'prdtImgFlPath':'', 'prdtImgFlNm':'', 'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            custom_header = self.header

            res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                                
                html = res.text
                soup = BeautifulSoup(html, "html.parser")

                title = soup.find('title').text
                try:
                    split_txt = 'Recall: '
                    if 'Safety Report: ' in title:
                        split_txt = 'Safety Report: '
                    elif 'Safety Notice: ' in title:
                        split_txt = 'Safety Notice: '
                    result['wrtDt'] = title.split(split_txt)[0].strip()
                    result['wrtDt'] = datetime.strptime(result['wrtDt'], '%Y.%m.%d').isoformat()
                except:
                    self.logger.error('작성시간 추출 실패  >>  '); extract_error = True;
                    
                content_html = soup.find('contenthtml')
                
                try:
                    images = content_html.find_all('img')
                    images_paths = []
                    images_files = []
                    for idx, image in enumerate(images):
                        try:
                            img_url = image['src'].strip()
                            img_name = img_url.split('assets/')[1].strip()
                            img_res = self.utils.download_upload_image(self.chnnl_nm, img_name, img_url)
                            if img_res['status'] == 200:
                                images_paths.append(img_res['path'])
                                images_files.append(img_res['fileNm'])
                            else:
                                self.logger.info(f"이미지 이미 존재 : {img_res['fileNm']}")
                        except Exception as e: self.logger.error(f'{idx}번째 이미지 추출 중 에러')

                    result['prdtImgFlPath'] = ' , '.join(set(images_paths))
                    result['prdtImgFlNm'] = ' , '.join(images_files)
                except: self.logger.error('상품이미지 추출 실패  >>  '); extract_error = True

                for p in soup.find_all("p"):
                    try:
                        strong = p.find("strong")
                        if strong:
                            key = strong.text.strip().replace(":", "")  # 키에서 콜론 제거
                            value = p.text.replace(strong.text, "").strip()

                            if key == "Product": 
                                result['prdtNm'] = value
                            elif key == "Brand": 
                                result['brand'] = value
                            elif key == "Product details"or key == "Product description":
                                result['prdtDtlCtn'] = value
                            elif key == "Risk statement" or key == "Hazard":
                                result['hrmflCuz'] = value
                            elif key == "Country of origin":
                                result['plor'] = value
                            elif key == "Corrective action" or key == "Our advice to consumers":
                                result['flwActn'] = value
                        elif key:  # 이전 strong이 있으면 해당 키에 내용 추가
                            value = " " + p.text.strip()
                            if key == "Product": 
                                result['prdtNm'] += value
                            elif key == "Brand": 
                                result['brand'] += value
                            elif key == "Product details"or key == "Product description":
                                result['prdtDtlCtn'] += value
                            elif key == "Risk statement" or key == "Hazard":
                                result['hrmflCuz'] += value
                            elif key == "Country of origin":
                                result['plor'] += value
                            elif key == "Corrective action" or key == "Our advice to consumers":
                                result['flwActn'] += value
                    except: pass
                
                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                if extract_error: self.logger.info(f'url :: {product_url}')

            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {res.status_code}')

        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')

        return result
