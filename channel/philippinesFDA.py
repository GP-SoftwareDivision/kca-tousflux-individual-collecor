from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time

class PhilippinesFDA():
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
            'Referer': 'https://www.fda.gov.ph/advisories/'
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
                    url = 'https://www.fda.gov.ph/wp-admin/admin-ajax.php'
                    body_data = {
                        "draw":1,
                        "columns[0][data]": "date",
                        "columns[0][name]": "date",
                        "columns[0][searchable]": "true",
                        "columns[0][orderable]": "true",
                        "columns[0][search][value]": "",
                        "columns[0][search][regex]": "false",
                        "columns[1][data]": "image",
                        "columns[1][name]": "image",
                        "columns[1][searchable]": "false",
                        "columns[1][orderable]": "false",
                        "columns[1][search][value]": "",
                        "columns[1][search][regex]": "false",
                        "columns[2][data]": "title",
                        "columns[2][name]": "title",
                        "columns[2][searchable]": "true",
                        "columns[2][orderable]": "true",
                        "columns[2][search][value]": "",
                        "columns[2][search][regex]": "false",
                        "columns[3][data]": "categories",
                        "columns[3][name]": "categories",
                        "columns[3][searchable]": "true",
                        "columns[3][orderable]": "false",
                        "columns[3][search][value]": "",
                        "columns[3][search][regex]": "false",
                        "columns[4][data]": "categories_hfilter",
                        "columns[4][name]": "categories_hfilter",
                        "columns[4][searchable]": "true",
                        "columns[4][orderable]": "true",
                        "columns[4][search][value]": "",
                        "columns[4][search][regex]": "false",
                        "order[0][column]": 0,
                        "order[0][dir]": "desc",
                        "start": 25 * self.page_num,
                        "length": 25,
                        "search[value]": "",
                        "search[regex]": "false",
                        "table_id": "ptp_0430655160c1850c_2",
                        "action": "ptp_load_posts",
                        "_ajax_nonce": "abde797de6"
                    }
                    headers.update({
                        'Origin': 'https://www.fda.gov.ph'
                    })

                    res = requests.post(url=url, headers=headers, data=body_data, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)                            

                        res_json = json.loads(res.text)
                        datas = res_json['data']
                        if not datas: 
                            if retry_num >= 10:
                                crawl_flag = False
                                self.logger.info('데이터가 없습니다.')
                            else:
                                retry_num += 1
                                continue

                        for data in datas:
                            try:
                                date_day = datetime.strptime(data['date'].strip(), "%d %B %Y").strftime("%Y-%m-%d")
                                wrt_dt = date_day + ' 00:00:00'                                
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    a_tag = data['title']
                                    soup = BeautifulSoup(a_tag, 'html.parser')
                                    product_url = soup.find('a')['href']
                                    title = str.lower(soup.find('a').text)
                                    if 'unregistered' in title or 'unnotified' in title or 'unauthorized' in title: continue  # Unregistered, unnotified, unauthorized 제품은 미확인 
                                    self.total_cnt += 1
                                    dup_flag, colct_data = self.crawl_detail(product_url)
                                    if dup_flag == 0:
                                        insert_res = self.utils.insert_data(colct_data)
                                        if insert_res == 0:
                                            self.colct_cnt += 1
                                        elif insert_res == 1:
                                            self.error_cnt += 1
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
        result = {'prdtNm':'', 'wrtDt':'', 
                  'atchFlPath':'', 'atchFlNm':'', 'prdtDtlCtn':'', 
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        
        try:
            custom_header = self.header

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)

            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                html = BeautifulSoup(product_res.text, "html.parser")

                try: 
                    result['prdtNm'] = html.find('h1',{'class':'entry-title'}).text.strip() 
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}')

                try: 
                    date_text = html.find('meta',{'property':'article:modified_time'})['content'].strip()
                    result['wrtDt'] = datetime.fromisoformat(date_text).replace(tzinfo=None).isoformat()
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    try:
                        contents = html.find('div', {'class': 'page-content'}).contents
                        prdt_dtl_ctn = []
                        for content in contents:
                            if content.name is None or content.name == 'div': continue
                            elif content.find('table'):
                                for inner in content.contents:
                                    if inner.name == 'table':
                                        rows = content.find_all("tr")
                                        table_data = []
                                        for row in rows:
                                            cols = row.find_all(["td", "th"])
                                            col_texts = [col.get_text(strip=True) for col in cols]
                                            table_data.append(",".join(col_texts))
                                        prdt_dtl_ctn.append('\n'.join(table_data))
                                    else:
                                        prdt_dtl_ctn.append(inner.get_text(separator="\n", strip=True).replace('\n', ' '))
                            else:
                                prdt_dtl_ctn.append(content.get_text(separator="\n", strip=True).replace('\n', ' '))
                                
                        result['prdtDtlCtn'] = '\n'.join(prdt_dtl_ctn)
                    except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >>  {e}')
                    
                    try: 
                        atchl_url = html.find('li', class_=['mime-application-pdf']).find('a')['href']
                        custom_header.update({
                            'Refere': product_url
                        })
                        atchl_res = self.utils.download_upload_atchl(self.chnnl_nm, atchl_url, custom_header)
                        if atchl_res['status'] == 200:
                            result['atchFlPath'] = atchl_res['path']
                            result['atchFlNm'] = atchl_res['fileNm']
                        else:
                            self.logger.info(f"첨부파일 이미 존재 : {atchl_res['fileNm']}")
                    except Exception as e: self.logger.error(f'첨부파일 추출 실패  >>  {e}')

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')
            self.prdt_dtl_err_url.append(product_url)

        return dup_flag, result