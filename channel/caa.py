from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import os
import random
import re
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CAA():
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
            'Host':'www.recall.caa.go.jp',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }
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
                    url = 'https://www.recall.caa.go.jp/result/index.php?screenkbn=03'
                    self.logger.info('수집시작')
                    data = {
                        'search': '',
                        'viewCount': 15,
                        'screenkbn': '03',
                        'category': '',
                        'viewCountdden':15,
                        'portarorder': 2,
                        'actionorder': 0,
                        'pagingHidden': self.page_num
                    }
                    res = requests.post(url=url, headers=self.header, data=data, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)
                        html = res.text
                        soup = BeautifulSoup(html, "html.parser")
                        datas = soup.find('div',{'class':'search_result_main'}).find('tbody').find_all('tr')
                        for data in datas:
                            try:
                                product_url = data.find('td', {'class': 'new_window'}).find('a').get('href')
                                product_url = f'https://www.recall.caa.go.jp' + product_url
                                wrt_dt = data.find('span', {'class': 'result_list_post_date'}).text # 2025/01/24
                                wrt_dt = datetime.strptime(wrt_dt, '%Y/%m/%d').strftime('%Y-%m-%d 00:00:00')
                                
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
                        if crawl_flag: self.logger.info(f'{self.page_num}페이지로 이동 중..')
                    else: 
                        crawl_flag = False
                        raise Exception('통신 차단')
                except Exception as e:
                    self.logger.error(f'crawl 통신 중 에러 >> {e}')
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
        result = {'wrtDt':'', 'prdtNm':'', 'hrmflCuz':'', 'prdtDtlCtn':'', 
                  'flwActn': '', 'recallBzenty':'', 'prdtImg':'', 'url':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            custom_header = self.header

            res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                html = res.text
                soup = BeautifulSoup(html, "html.parser")

                details = soup.find_all('li')
                for detail in details:
                    try:
                        val = None
                        name = detail.find('span', {'class': 'detail_cap'})
                        detail_txt = detail.find('span', {'class': 'detail_text'})
                        script = detail.find('script')

                        if name is None:
                            continue
                        
                        name = name.text.strip()
                        if detail_txt is not None and detail_txt.text != '':
                            val = detail_txt.text.strip()
                        elif script is not None:
                            try:
                                script_txt = script.text.strip()
                                tmp_txts = re.findall("contentsText = '(.*)'", script_txt)
                                script_txt = tmp_txts[0] if len(tmp_txts) > 0 else ''
                                json_data = json.loads(script_txt.replace('\\', ''))
                                val = ''.join('\n' if op['insert'] == 'n' else op['insert'].replace('\u3000', ' ') for op in json_data['ops'] if 'insert' in op)
                            except Exception as e: self.logger.error(f'scipt 추출 중 에러')
                        else:
                            continue
                        
                        if name == '商品名': # 제품명
                            result['prdtNm'] = val
                        elif name == '連絡先': # 리콜업체
                            result['recallBzenty'] = val
                        elif name == '対応方法': # 후속조치
                            result['flwActn'] = val
                        elif name == '対応開始日': # 게시일
                            val = datetime.strptime(val, '%Y年%m月%d日').isoformat()
                            result['wrtDt'] = val
                        elif name == '対象の特定情報': # 제품상세내용
                            result['prdtDtlCtn'] = val
                        elif name == '備考': # 위해원인
                            result['hrmflCuz'] = val 
                    except:
                        pass

                try:
                    images = soup.find('ul', {'class': 'detail_main_img'}).find_all('img')
                    image_paths = []
                    for idx, image in enumerate(images):
                        try:
                            src = image['src'].strip()
                            img_url = f'https://www.recall.caa.go.jp{src}'
                            img_nm = src.split('product/')[1].strip()
                            res = self.utils.download_upload_image(self.chnnl_nm, img_nm, img_url)
                            if res != '': image_paths.append(res)
                        except Exception as e: self.logger.error(f'{idx}번째 이미지 추출 중 에러')
                    result['prdtImg'] = ' : '.join(image_paths)
                except: self.logger.error('제품 이미지 추출 실패  >>  '); extract_error = True

                result['url'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result['url'], self.chnnl_nm, result['prdtNm'])
                if extract_error: self.logger.info(f'url :: {product_url}')
            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {res.status_code}')
        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')

        return result