import re
from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class TianjinMarketSupervision():
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
        self.prdt_dtl_err_url = []

        self.utils = Utils(logger, api)

    def crawl(self):
        try:
            retry_num = 0
            crawl_flag = True
            url = 'https://scjg.tj.gov.cn/tjsscjdglwyh_52651/xwdt/cpzh/xfpzh/'
            while(crawl_flag):
                try:
                    headers = self.header
                    self.logger.info('수집 시작')

                    if self.page_num != 0:
                        headers['Referer'] = url
                        url = f'https://scjg.tj.gov.cn/tjsscjdglwyh_52651/xwdt/cpzh/xfpzh/index_{self.page_num}.html'
                    res = requests.get(url=url, headers=headers, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)                            
                        html = BeautifulSoup(res.text, features='html.parser')

                        datas = html.find('ul', {'class': 'news_list'}).find_all('li')
                        if len(datas) == 0:
                            if retry_num >= 10:
                                crawl_flag = False
                                self.logger.info('데이터가 없습니다.')
                            else:
                                retry_num += 1
                                continue

                        for data in datas:
                            try:
                                date_day = data.find('span', {'class': 'time'}).text.strip()
                                wrt_dt = date_day + ' 00:00:00'
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    self.total_cnt += 1
                                    product_url = 'https://scjg.tj.gov.cn/tjsscjdglwyh_52651/xwdt/cpzh/xfpzh/' + data.find('a')['href'].replace('./', '')
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
        result = {'prdtNm':'', 'wrtDt':'', 'prdtDtlCtn': '',
                  'hrmflCuz':'', 'flwActn': '', 'bsnmNm': '',
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            custom_header = self.header
            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if self.page_num == 0:
                custom_header['Referer'] = 'https://scjg.tj.gov.cn/tjsscjdglwyh_52651/xwdt/cpzh/xfpzh/'
            else:
                custom_header['Referer'] = f'https://scjg.tj.gov.cn/tjsscjdglwyh_52651/xwdt/cpzh/xfpzh/index_{self.page_num}.html'
            
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                product_res.encoding = product_res.apparent_encoding
                html = BeautifulSoup(product_res.text, "html.parser")

                try:
                    result['prdtNm'] = html.find('div', {'class': 'news_title'}).text.strip()
                    result['bsnmNm'] = result['prdtNm']
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}')

                try:
                    date_text = html.find('span', {'class':'date'}).text.strip()
                    match = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", date_text)

                    if match:
                        wrt_dt = match.group()
                        result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M").isoformat()
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    try:
                        p_tags = html.find('div', class_=['trs_editor_view']).find_all('p')
                        p_tags = [p_tag for p_tag in p_tags if p_tag.get_text().replace('&nbsp;', '').replace('&ensp;', '').replace('&emsp;', '').strip() != '']
                        prdt_dtl_ctn = []
                        hrmfl_cuz = []
                        flw_actn = []

                        if len(p_tags) < 3:
                            prdt_dtl_ctn = [p_tag.get_text(separator="\n", strip=True).replace('\n', ' ') for p_tag in p_tags]
                        else:
                            for i in range(len(p_tags)):
                                text = p_tags[i].get_text(separator="\n", strip=True).replace('\n', ' ')

                                if i == 0:
                                    prdt_dtl_ctn.append(text)
                                elif len(p_tags) == 3 and i == 2:
                                    flw_actn.append(text)
                                elif len(p_tags) > 3 and i > len(p_tags) -1 - 2:
                                    flw_actn.append(text)
                                else:
                                    hrmfl_cuz.append(text)

                        result['flwActn'] = '\n'.join(flw_actn).strip()
                        result['hrmflCuz'] = '\n'.join(hrmfl_cuz).strip()
                        result['prdtDtlCtn'] = '\n'.join(prdt_dtl_ctn).strip()
                    except Exception as e:
                        self.logger.error(f'제품 정보 수집 중 에러  >>  {e}')

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')

        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')
            self.prdt_dtl_err_url.append(product_url)
            
        return dup_flag, result
