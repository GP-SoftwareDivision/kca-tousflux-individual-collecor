import re
from bs4 import BeautifulSoup
import numpy as np
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time

class ACCP():
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
            'Accept-Encoding':'gzip, deflate, br',
            'Accept-Language':'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'Referer': 'https://aseanconsumer.org/product-alert'
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
                        headers = self.header
                        self.logger.info('수집 시작')
                        url = 'https://aseanconsumer.org/product-alert-datatable'
                        body_data = {
                            "draw":2,
                            "columns[0][data]": "recall_date",
                            "columns[0][name]": "recall_date",
                            "columns[0][searchable]": "true",
                            "columns[0][orderable]": "true",
                            "columns[0][search][value]": "",
                            "columns[0][search][regex]": "false",
                            "columns[1][data]": "picture",
                            "columns[1][name]": "picture",
                            "columns[1][searchable]": "false",
                            "columns[1][orderable]": "false",
                            "columns[1][search][value]": "",
                            "columns[1][search][regex]": "false",
                            "columns[2][data]": "name",
                            "columns[2][name]": "name",
                            "columns[2][searchable]": "true",
                            "columns[2][orderable]": "true",
                            "columns[2][search][value]": "",
                            "columns[2][search][regex]": "false",
                            "columns[3][data]": "type",
                            "columns[3][name]": "type",
                            "columns[3][searchable]": "true",
                            "columns[3][orderable]": "true",
                            "columns[3][search][value]": "",
                            "columns[3][search][regex]": "false",
                            "columns[4][data]": "model_product",
                            "columns[4][name]": "model_product",
                            "columns[4][searchable]": "true",
                            "columns[4][orderable]": "true",
                            "columns[4][search][value]": "",
                            "columns[4][search][regex]": "false",
                            "columns[5][data]": "country",
                            "columns[5][name]": "country",
                            "columns[5][searchable]": "true",
                            "columns[5][orderable]": "true",
                            "columns[5][search][value]": "",
                            "columns[5][search][regex]": "false",
                            "columns[6][data]": "jurisdiction_of_recall",
                            "columns[6][name]": "jurisdiction_of_recall",
                            "columns[6][searchable]": "true",
                            "columns[6][orderable]": "true",
                            "columns[6][search][value]": "",
                            "columns[6][search][regex]": "false",
                            "columns[7][data]": "original_alert",
                            "columns[7][name]": "original_alert",
                            "columns[7][searchable]": "true",
                            "columns[7][orderable]": "true",
                            "columns[7][search][value]": "",
                            "columns[7][search][regex]": "false",
                            "order[0][column]": 0,
                            "order[0][dir]": "desc",
                            "start": 25 * self.page_num,
                            "length": 25,
                            "search[value]": "",
                            "search[regex]": "false"
                        }
                        headers.update({
                            'Origin': 'https://aseanconsumer.org'
                        })

                        res = requests.post(url=url, headers=headers, data=body_data, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            

                            res_json = json.loads(res.text)
                            datas = res_json['data']
                            for data in datas:
                                try:
                                    wrt_dt = data['recall_date'] + ' 00:00:00'                                    
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        a_tag = data['name']
                                        soup = BeautifulSoup(a_tag, 'html.parser')
                                        product_url = soup.find('a')['href']
                                        colct_data = self.crawl_detail(product_url)
                                        req_data = json.dumps(colct_data)
                                        insert_res = self.api.insertData2Depth(req_data)
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
        result = {'prdtNm':'', 'wrtDt':'', 'recallNtn': '', 'hrmflCuz':'', 'plor':'', 
                  'mnfctrBzenty': '', 'recallSrce': '', 'prdtImg': '', 'prdtDtlCtn':'', 
                  'prdtDtlCtn2':'', 'url':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        # 게시일, 리콜국, 원산지, 제조업체, 제품명, 제품 상세내용, 위해원인, 제품 이미지, 정보출처, 제품 상세내용2
        try:
            custom_header = self.header

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try:
                    tr_tags = html.select("div.col-product-alert > table > tbody > tr")
                    prdt_dtl = []
                    hrmfl_cuz = []
                    for tr_tag in tr_tags:
                        td = tr_tag.find('td')
                        td_text = td.text.strip()
                        td_next = td.find_next_sibling('td')
                        td_next_text = td_next.text.strip() if td_next else ''
                        if 'Date' in td_text:
                            date_day = datetime.strptime(td_next_text, "%d-%m-%Y").strftime("%Y-%m-%d")
                            wrt_dt = date_day + ' 00:00:00'
                            result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat()
                        elif 'Jurisdiction Of Recall' in td_text:
                            result['recallNtn'] = td_next_text
                        elif 'Country' in td_text:
                            result['plor'] = td_next_text
                        elif 'Manufacturer Name' in td_text:
                            result['mnfctrBzenty'] = td_next_text
                        elif 'Product Name' in td_text:
                            result['prdtNm'] = td_next_text
                        elif 'Code' in td_text or 'Type' in td_text or 'Model' in td_text or 'Volume' in td_text:
                            prdt_dtl.append(f'{td_text}: {td_next_text}')
                        elif 'Risk Level' in td_text or 'Hazard' in td_text:
                            hrmfl_cuz.append(f'{td_text}: {td_next_text}')
                        elif 'Provider Link' in td_text:
                            result['recallSrce'] = td_next_text
                        elif 'Description' in td_text:
                            desc_table = td_next.find('table')
                            if desc_table:
                                p_text = ''.join([t.text.strip() for t in td_next.find_all('p', recursive=False)])
                                rows = desc_table.find_all("tr") if desc_table else []
                                table_data = []
                                for row in rows:
                                    cols = row.find_all(["td", "th"])
                                    col_texts = [col.get_text(strip=True) for col in cols]
                                    table_data.append(",".join(col_texts))  

                                result['prdtDtlCtn2'] = p_text + '\n' + '\n'.join(table_data)
                            else:
                                result['prdtDtlCtn2'] = td_next_text
                    result['prdtDtlCtn'] = '\n'.join(prdt_dtl)
                    result['hrmflCuz'] = '\n'.join(hrmfl_cuz)
                except Exception as e: self.logger.error(f'제품 정보 수집 중 에러  >>  ')

                try:
                    image_list = []
                    images = html.select("table img")
                    for idx, image in enumerate(images):
                        try:
                            img_url = image['src']
                            file_name = img_url.split('/')[-1]
                            res = self.utils.download_upload_image(self.chnnl_nm, file_name, img_url) #  chnnl_nm, prdt_nm, idx, url
                            if res != '': image_list.append(res)
                        except Exception as e: self.logger.error(f'{idx}번째 이미지 추출 중 에러')
                    result['prdtImg'] = ' : '.join(image_list)
                except Exception as e: self.logger.error(f'제품 이미지 수집 중 에러  >>  ')
            
                result['url'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result['url'], self.chnnl_nm, result['prdtNm'])                            
            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {product_res.status_code}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result