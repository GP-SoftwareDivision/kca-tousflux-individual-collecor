from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MPI():
    def __init__(self, chnnl_cd, chnnl_name, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_nm = chnnl_name
        self.chnnl_cd = chnnl_cd
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.header = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding':'gzip, deflate, br, zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }
        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0

        self.utils = Utils(logger, api)

    def crawl(self):
        try:
            url = 'https://www.mpi.govt.nz/food-safety-home/food-recalls-and-complaints/recalled-food-products/'
            self.logger.info('수집시작')
            res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
            if res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                res.encoding = res.apparent_encoding
                html = BeautifulSoup(res.text, "html.parser")

                # datas = soup.find('tbody').find_all('tr')
                datas = [item for item in html.find_all('div', {'class':'richtext'}) if '2025 recalls' in item.text][0].find_all('li')
                for data in datas:
                    try:
                        product_url = data.find('a')['href']
                        date_flag, dup_flag, colct_data = self.crawl_detail(product_url)
                        if date_flag:
                            if dup_flag == 0:
                                insert_res = self.utils.insert_data(colct_data)
                                if insert_res == 0:
                                    self.colct_cnt += 1
                                elif insert_res == 1:
                                    self.error_cnt += 1
                                    self.utils.save_colct_log(f'게시글 수집 오류 > {product_url}', '', self.chnnl_cd, self.chnnl_nm, 1)
                                # elif insert_res == 2 :
                                #     self.duplicate_cnt += 1
                        else:
                            crawl_flag = False
                            self.logger.info(f'수집기간 내 데이터 수집 완료')
                            break          
                    except Exception as e:
                        self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                        
            else:raise Exception(f'통신 차단 : {url}')
        except Exception as e:
            self.logger.error(f'{e}')
            self.error_cnt += 1
            exc_type, exc_obj, tb = sys.exc_info()
            self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)            
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
            self.logger.info('수집종료')

    def crawl_detail(self, product_url):
        date_flag = True
        result = {'prdtNm':'', 'wrtDt':'', 'hrmflCuz':'', 'prdtImgFlPath':'', 'prdtImgFlNm':'', 'prdtDtlCtn': '',
                  'flwActn': '', 'recallBzenty':'', 'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            custom_header = self.header
            custom_header['Referer'] = 'https://www.mpi.govt.nz/food-safety-home/food-recalls-and-complaints/recalled-food-products/'
            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                product_res.encoding = product_res.apparent_encoding
                html = BeautifulSoup(product_res.text, "html.parser")

                main = html.find('div', {'id':'main-content-link'})
                # wrapper intro

                wrt_dt = self.utils.parse_date(main.find('div', {'class':'wrapper intro'}).text.strip().split(':')[0], self.chnnl_nm) + ' 00:00:00'
                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                    self.total_cnt += 1

                    try: result['prdtNm'] = main.find('h1').text.strip()
                    except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  ')

                    try: result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat()
                    except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  ')

                    result['prdtDtlPgUrl'] = product_url
                    result['chnnlNm'] = self.chnnl_nm
                    result['chnnlCd'] = self.chnnl_cd
                    result['idx'] = self.utils.generate_uuid(result)

                    dup_flag = self.api.check_dup(result['idx'])
                    if dup_flag == 0:
                        try: result['hrmflCuz'] = main.find('div', {'class':'wrapper intro'}).text.strip().split(':')[1].strip()
                        except Exception as e: self.logger.error(f'위해원인 수집 중 에러  >>  ')


                        try:
                            images = main.find('div', {'id':'main-article-content-link'}).find_all('img')
                            images_paths = []
                            images_files = []
                            for idx, image in enumerate(images):
                                try:
                                    img_url = f"https://www.mpi.govt.nz{image['src']}" if image['src'][0] == '/' else f"https://www.mpi.govt.nz/{image['src']}"
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

                        prdt_dtl_ctn = ''

                        try: 
                            rows = main.find('div', {'id':'main-article-content-link'}).find('table').find_all('tr')
                            for row in rows[1:]:
                                prdt_dtl_ctn += ' : '.join(rows[1].text.strip().split('\n\n\n')) if row != rows[-1] else ' : '.join(rows[1].text.strip().split('\n\n\n'))
                                prdt_dtl_ctn += '\n'
                            result['prdtDtlCtn'] = prdt_dtl_ctn
                        except Exception as e: self.logger.error(f'제품 상세설명 수집 중 에러  >>  ')

                        try:
                            start_tag = html.find('h2', string = 'Consumer advice')  # 시작점 찾기
                            end_tag = html.find('h2', string = 'Who to contact') # 끝점 찾기
                            if end_tag == None: 
                                end_tag = html.find('a', string='Return to Product Recalls')  # 끝점 찾기
                            flw_actn = self.utils.extract_content(start_tag, end_tag)
                            if flw_actn != []: result['flwActn'] = self.utils.get_clean_string(' '.join(flw_actn))
                        except Exception as e: self.logger.error(f'후속조치 수집 중 에러  >>  ')

                        try: 
                            start_tag = html.find('h2', string = 'Who to contact')  # 시작점 찾기
                            end_tag = html.find('h2', string = 'Subscribe to food recalls') # 끝점 찾기
                            if end_tag == None: 
                                end_tag = html.find('a', string='Return to Product Recalls')  # 끝점 찾기
                            recall_bzenty = self.utils.extract_content(start_tag, end_tag)
                            if flw_actn != []: result['recallBzenty'] = self.utils.get_clean_string(' '.join(recall_bzenty))
                        except Exception as e: self.logger.error(f'리콜업체 수집 중 에러  >>  ')        
                    elif dup_flag == 2:
                        self.duplicate_cnt += 1
                    else: self.logger.error(f"IDX 확인 필요  >> {result['idx']} ( {product_url} )")

                else:
                    date_flag = False
                    return date_flag, result

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')

        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')

        return date_flag, dup_flag, result
