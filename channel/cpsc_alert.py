from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import sys
import time

class CPSCAlert():
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
            'Cookie':'_ga=GA1.1.312510556.1739947867; _hjSession_3799316=eyJpZCI6IjU5OWVlNjM4LWNiY2ItNDg1Ni04YzUyLWNiNmZiOWQyNTE2ZSIsImMiOjE3Mzk5NDc4NjgwOTAsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=; _hjSessionUser_3799316=eyJpZCI6ImE0NjViMTk4LTljOGEtNTQ2ZS05ZmIyLTk5MWQ3ZGU2NGRmOCIsImNyZWF0ZWQiOjE3Mzk5NDc4NjgwODgsImV4aXN0aW5nIjp0cnVlfQ==; AWSALB=tst8I3fWKDeHdH+J24TvA5AAokkBf+M+RZD109qzLcTR9py8q64cA7jcZucNU49Z6khUdBj7VD7TqMguv6C7HkgjtsWa4hy79qATxoEkFz2O4NS995yglniJyRlE; AWSALBCORS=tst8I3fWKDeHdH+J24TvA5AAokkBf+M+RZD109qzLcTR9py8q64cA7jcZucNU49Z6khUdBj7VD7TqMguv6C7HkgjtsWa4hy79qATxoEkFz2O4NS995yglniJyRlE; _ga_XM65T6P0GR=GS1.1.1739947868.1.1.1739947904.0.0.0; _ga_CSLL4ZEK4L=GS1.1.1739947868.1.1.1739947906.0.0.0; _ga_55LVJ78J3V=GS1.1.1739947867.1.1.1739949509.0.0.0',
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
                        headers = self.header
                        if self.page_num == 0: url = 'https://www.cpsc.gov/Newsroom/News-Releases'
                        else: 
                            headers['Referer'] = url
                            url = f'https://www.cpsc.gov/Newsroom/News-Releases?page={self.page_num}'
                        self.logger.info('수집 시작')
                        res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')

                            datas = html.find('div', {'id':'block-cpsc-content'}).find_all('div', {'class':'views-row'})
                            for data in datas:
                                try:
                                    try: self.locale_str = html.find('html')['lang']
                                    except: self.locale_str = ''

                                    wrt_dt = self.utils.parse_date(data.find('div', {'class':'list-date date'}).text.strip(), self.chnnl_nm) + ' 00:00:00'
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        product_url = 'https://www.cpsc.gov' + data.find('a')['href']
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
        result = { 'prdtImg':'', 'prdtNm':'', 'prdtDtlCtn':'', 
                   'hrmflCuz':'', 'wrtDt':'', 'ntslCrst':'', 'flwActn':'', 'acdntYn':'',
                   'distbBzenty':'', 'plor':'', 'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        try:
            custom_header = self.header
            if self.page_num == 0: referer_url = 'https://www.cpsc.gov/Newsroom/News-Releases'
            else: referer_url = f'https://www.cpsc.gov/Newsroom/News-Releases?page={self.page_num}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try: 
                    wrt_dt = self.utils.parse_date(html.find('div', {'class':'node-news__release-date'}).text.replace('Release Date:','').strip(), self.chnnl_nm) + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except: self.logger.error('게시일 수집 중 에러  >>  ') 

                try: result['prdtNm'] = html.find('h1', {'class':'margin-0 page-title'}).text.strip()
                except: self.logger.error('제품명 수집 중 에러  >>  ')

                border = html.find('div', {'class':'node-news__releases grid-row'})

                try:
                    images = border.find_all('img')
                    images_paths = []
                    images_files = []
                    for idx, image in enumerate(images):
                        try:
                            img_url = 'https://www.cpsc.gov'+image['src']
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

                contents = border.find_next_sibling()
                try:
                    prdt_dtl_ctn = contents.text.strip()
                    result['prdtDtlCtn'] = self.utils.get_clean_content_string(prdt_dtl_ctn)
                except: self.logger.error('제품상세내용 수집 중 에러  >>  ')       
                
                bold_contents = contents.find_all('strong')
                flw_actns = [content for content in bold_contents if content.find('u')]
                if flw_actns != []:
                    try:
                        flw_actn = [content.text.strip() for content in flw_actns] 
                        result['flwActn'] = ' \n'.join(flw_actn.text.strip())
                    except: self.logger.error(f'후속조치 수집 중 에러  >>  {e}')
            
                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)                            
            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result