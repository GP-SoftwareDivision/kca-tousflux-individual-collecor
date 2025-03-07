from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import sys
import time

class FSANZ():
    def __init__(self, chnnl_cd, chnnl_nm, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_cd = chnnl_cd
        self.chnnl_nm = chnnl_nm
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.header = {
            'Accept':'*/*',
            'Accept-Encoding':'gzip, deflate, br',
            'Accept-Language':'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        }

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
                    headers = self.header
                    self.logger.info('수집 시작')
                    url = 'https://www.foodstandards.gov.au/food-recalls/alerts'

                    if self.page_num != 0: 
                        headers['Referer'] = url
                        url = f'https://www.foodstandards.gov.au/food-recalls/alerts?page={self.page_num}'
                    res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)                            
                        html = BeautifulSoup(res.text, features='html.parser')

                        datas = html.find_all('div', {'class': 'views-row'})
                        if len(datas) == 0:
                            if retry_num >= 10:
                                crawl_flag = False
                                self.logger.info('데이터가 없습니다.')
                            else:
                                retry_num += 1
                                continue
                            
                        for data in datas:
                            try:
                                date_text = data.find('p', {'class': 'published-date'}).text.strip().replace('Published ', '').strip()
                                date_day = datetime.strptime(date_text, "%d %B %Y").strftime("%Y-%m-%d")
                                wrt_dt = date_day + ' 00:00:00'                                 
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    self.total_cnt += 1
                                    product_url = 'https://www.foodstandards.gov.au/' + data.find('a')['href']
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
        except Exception as e:
            self.logger.error(f'{e}')
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
            self.logger.info('수집종료')
                
    def crawl_detail(self, product_url):
        dup_flag = -1
        result = {'prdtNm':'', 'wrtDt':'', 'ntslCrst': '', 'hrmflCuz':'', 
                  'flwActn':'', 'bsnmNm': '', 'prdtImgFlNm':'', 'prdtImgFlPath': '', 
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            custom_header = self.header
            referer_url = 'https://www.foodstandards.gov.au/food-recalls/alerts'
            if self.page_num != 0: 
                referer_url = f'https://www.foodstandards.gov.au/food-recalls/alerts?page={self.page_num}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)

            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try: 
                    result['prdtNm'] = html.find('h1').text.strip()
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}')

                try: 
                    date_text = html.find('time').text.strip().replace('Published ', '').strip()
                    date_day = datetime.strptime(date_text, "%d %B %Y").strftime("%Y-%m-%d")
                    wrt_dt = date_day + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    try:
                        images = html.find('div', class_=['field-food-recall-image']).find_all('img')
                        images_paths = []
                        images_files = []
                        for idx, image in enumerate(images):
                            try:
                                img_url = f'https://www.foodstandards.gov.au{image["src"]}'
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

                    recall_desc = html.find('article').find('div', {'class': 'field-bare'})

                    try:
                        h3_tag = recall_desc.find('h3')
                        if h3_tag:
                            description = h3_tag.find_previous_sibling('p')
                            result['ntslCrst'] = description.text.strip() if description else ""
                        else:
                            result['ntslCrst'] = ""
                    except Exception as e: self.logger.error(f'판매현황 수집 중 에러  >>  {e}')

                    try: 
                        problem_section = recall_desc.find_all('h2')
                        problem_text = []
                        for h2 in problem_section:
                            text = h2.get_text(strip=True)
                            next_p = h2.find_next_sibling('p')
                            if "Problem:" in text or "Food safety hazard:" in text:
                                if next_p:
                                    problem_text.append(f"{text} {next_p.text.strip()}")
                            elif "What to do:" in text:
                                result['flwActn'] = next_p.text.strip() if next_p else ""
                            elif "For further information please contact:" in text:
                                result['bsnmNm'] = next_p.get_text(separator=" ").strip() if next_p else ""
                        result['hrmflCuz'] = " ".join(problem_text)
                    except Exception as e: self.logger.error(f'위해원인 및 후속조치, 업체 수집 중 에러  >>  {e}')

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')

        except Exception as e:
            self.logger.error(f'{e}')
            self.prdt_dtl_err_url.append(product_url)

        return dup_flag, result