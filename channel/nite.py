from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class NITE():
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
            'Host':'www.nite.go.jp',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }
        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0
        self.prdt_dtl_err_url = []

        self.utils = Utils(logger, api)

    def crawl(self):
        try:
            url = 'https://www.nite.go.jp/jiko/jikojohou/recall_new/2024/index.html'
            self.logger.info('수집시작')
            res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
            if res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                res.encoding = res.apparent_encoding
                html = BeautifulSoup(res.text, "html.parser")

                datas = html.find('div', {'class':'main'}).find('ul').find_all('li')
                if len(datas) == 0:
                    self.logger.info('데이터가 없습니다.')

                for data in datas:
                    try:
                        wrt_dt = self.utils.parse_date(data.find('a').text.split('\u3000')[0].strip(), self.chnnl_nm) + ' 00:00:00'
                        if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                            self.total_cnt += 1
                            product_url = f"https://www.nite.go.jp{data.find('a')['href']}"
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
                            else: self.logger.error(f"IDX 확인 필요  >> {colct_data['idx']} ( {product_url} )")
                        elif wrt_dt < self.start_date:
                            self.logger.info(f'수집기간 내 데이터 수집 완료')
                            break
                    except Exception as e:
                        self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                        
            else:raise Exception(f'통신 차단 : {url}')
        except Exception as e:
            self.logger.error(f'crawl 통신 중 에러 >> {e}')
            self.error_cnt += 1
            exc_type, exc_obj, tb = sys.exc_info()
            self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)            
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
            self.logger.info('수집종료')

    def crawl_detail(self, product_url):
        dup_flag = -1
        result = {'wrtDt':'', 'bsnmNm':'', 'prdtDtlCtn': '', 'prdtNm':'', 'hrmflCuz':'', 'flwActn': '',
                  'prdtImgFlPath':'', 'prdtImgFlNm':'', 'recallSrce':'', 'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            custom_header = self.header
            custom_header['Referer'] = 'https://www.nite.go.jp/jiko/jikojohou/recall_new/2024/index.html'

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                product_res.encoding = product_res.apparent_encoding
                html = BeautifulSoup(product_res.text, "html.parser")

                main = html.find('div', {'class':'main'})

                try:
                    info = main.find('table').find('tbody').find_all('tr')
                    prdt_nm_idx = [idx for idx, column in enumerate(info[0].find_all('th')) if column.text == '商品名'][0]
                    prdt_nm = ', '.join([row.find_all('td')[prdt_nm_idx].text.strip() for row in info[1:]])
                    result['prdtNm'] = prdt_nm
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}') 

                infos1 = main.find_all('h2')
                for info in infos1:
                    title = info.text.strip()
                    try:
                        if title == 'リコール実施日':
                            try: 
                                wrt_dt = self.utils.parse_date(info.find_next_sibling('p').text.strip(), self.chnnl_nm) + ' 00:00:00'
                                result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat()
                            except Exception as e: raise Exception(f'게시일 수집 중 에러  >>  {e}')
                        elif title == '事業者名':
                            try: result['bsnmNm'] = info.find_next_sibling('p').text.strip()
                            except Exception as e: raise Exception(f'업체 수집 중 에러  >>  {e}')
                    except Exception as e:
                        self.logger.error(f'{e}')   

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    infos2 = main.find_all('h3')
                    for info in infos2:
                        title = info.text.strip()
                        try:
                            if title == '製品名及び型式':
                                tags = info.find_next_siblings()
                                prdt_dtl_ctn = self.extract_prdt_dtl_ctn(tags)
                                try: result['prdtDtlCtn'] = prdt_dtl_ctn.strip()
                                except Exception as e: raise Exception(f'제품상세내용 수집 중 에러  >>  {e}')
                            elif title == 'リコールの内容':
                                try: result['hrmflCuz'] = info.find_next_sibling('p').text.strip()
                                except Exception as e: raise Exception(f'위해원인 수집 중 에러  >>  {e}')
                            elif title == '対処方法':
                                try: result['flwActn'] = info.find_next_sibling('p').text.strip()
                                except Exception as e: raise Exception(f'후속조치 수집 중 에러  >>  {e}')
                            elif title == '問い合わせ先等':
                                try:
                                    images = [img for tag in info.find_next_siblings() if tag.find('img') for img in tag.find_all('img')]
                                    images_paths = []
                                    images_files = []
                                    for idx, image in enumerate(images):
                                        try:
                                            img_url = 'https://www.nite.go.jp' + image['src']
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
                        except Exception as e:
                            self.logger.error(f'{e}')
            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')

        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')
            self.prdt_dtl_err_url.append(product_url)

        return dup_flag, result

    def extract_prdt_dtl_ctn(self, tags):
        result = ''
        try:
            for tag in tags:
                try:
                    if tag.name == 'ul': result += '\n'.join([li.text.strip() for li in tag.find_all('li')]) + '\n'
                    elif tag.name == 'table': 
                        ths = [title.text.strip() for title in tag.find_all('tr')[0].find_all('th')]
                        rows = tag.find_all('tr')[1:]
                        for idx, row in enumerate(rows):
                            try:
                                columns = row.find_all('td')
                                result += ' | '.join([f'{ths[idx]} : {self.utils.get_clean_string(column.text.strip())}' for idx, column in enumerate(row.find_all('td'))]) + '\n'
                            except Exception as e: self.logger.error(f'제품 상세내용 테이블 수집 중 에러  >>  {idx}번째 줄')
                    elif tag.name == 'p': 
                        if tag.find('img'):
                            print()
                        else:
                            result += tag.text.strip() + '\n'
                    elif tag.name == 'h3':break
                except Exception as e: self.logger.error(f'{e}')
        except Exception as e:
            self.logger.error(f'{e}')
        return result