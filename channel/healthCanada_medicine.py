from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class HCMedicine():
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
            'Accept-Encoding':'zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'Host':'recalls-rappels.canada.ca',
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
                    if self.page_num == 0: url = 'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A180'
                    else: url = f'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A180&page=%2C1%2C{self.page_num}'
                    self.logger.info('수집 시작')
                    res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)                       

                        html = BeautifulSoup(res.text, features='html.parser')
                        datas = html.find_all('div', {'class':'search-result views-row'})
                        if len(datas) == 0:
                            if retry_num >= 10:
                                crawl_flag = False
                                self.logger.info('데이터가 없습니다.')
                            else:
                                retry_num += 1
                                continue

                        for data in datas:
                            try:
                                wrt_dt = data.find('span', {'class':'ar-type'}).text.split('|')[1].strip() + ' 00:00:00'
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    self.total_cnt += 1
                                    product_url = 'https://recalls-rappels.canada.ca' + data.find('a')['href']
                                    colct_data = self.crawl_detail(product_url)
                                    insert_res = self.utils.insert_data(colct_data)
                                    if insert_res == 0:
                                        self.colct_cnt += 1
                                    elif insert_res == 1:
                                        self.error_cnt += 1
                                        self.logger.error(f'게시글 수집 오류 > {product_url}')
                                        self.prdt_dtl_err_url.append(product_url)
                                    elif insert_res == 2 :
                                        self.duplicate_cnt += 1
                                        crawl_flag = False
                                        break
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
        result = { 'wrtDt':'', 'brand':'', 'prdtNm':'', 'prdtDtlCtn':'', 
                  'hrmflCuz':'', 'flwActn':'', 'recallBzenty':'', 
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        try:
            custom_header = self.header
            if self.page_num == 0: referer_url = 'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A180'
            else: referer_url = f'https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=category%3A180&page=%2C1%2C{self.page_num}'            
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)

                html = BeautifulSoup(product_res.text, 'html.parser')

                try: 
                    wrt_dt = html.find('time').text.strip() + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat()                     
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                affected_products = html.find('div', {'class':'ar-affected-products ar-section'}).find('table').find('tbody').find_all('tr')
                brand_list = []
                prdt_nm_list = []
                prdt_dtl_ctn_list = []
                if html.find('table', class_ = ['provisional']):
                    for product in affected_products:
                        try:
                            title_list = []
                            prdt_ctl = ''
                            descriptions = product.find_all('td')
                            for description in descriptions:
                                title_list.append(description['data-label'])
                                label = description['data-label'].lower()
                                try:
                                    if label == 'photo': continue
                                    if label == 'brand': brand_list.append(description.text.strip())
                                    elif label == 'product name' or label == 'affected products' or label == 'product': prdt_nm_list.append(description.text.strip())
                                    else:
                                        title = description['data-label']
                                        text = description.get_text(separator="\n", strip=True).replace('\n', ',')
                                        text = " ".join(text.split())
                                        if description == descriptions[-1]: 
                                            prdt_ctl += f'{title} = {text}'
                                            prdt_dtl_ctn_list.append(prdt_ctl)
                                        else: prdt_ctl += f'{title} = {text}  |  '                
                                except Exception as e:
                                    self.logger.error(f'{e}')
                        except Exception as e:
                            self.logger.error(f'affected_products  >>  {e}')
                else:
                    try:
                        ths = html.find('div', {'class':'ar-affected-products ar-section'}).find('table').find('tbody').find_all('th')
                        title_list = [th.text for th in ths]
                        for product in affected_products:
                            tds = product.find_all('td')
                            prdt_dtl = []
                            if len(tds) == 0: continue
                            for i in range(len(title_list)):
                                try:
                                    title = title_list[i].lower()
                                    text = tds[i].get_text(separator="\n", strip=True).replace('\n', ',')
                                    text = " ".join(text.split())
                                    if title == 'photo':
                                        continue
                                    elif title == 'product':
                                        prdt_nm_list.append(text)
                                    elif title == 'brand':
                                        brand_list.append(text)
                                    else:
                                        prdt_dtl.append(f"{title_list[i]} = {text}")
                                except:
                                    pass
                            prdt_dtl_ctn_list.append(' | '.join(prdt_dtl))
                    except Exception as e:
                        self.logger.error(f'제품 정보 추출 에러  >>  {e}')

                try: result['brand'] = ', '.join(brand_list)
                except Exception as e: self.logger.error(f'브랜드 수집 중 에러  >>  {e}')

                try: result['prdtNm'] = ', '.join(prdt_nm_list)
                except Exception as e: self.logger.error(f'제품명 수집 중 에러  >>  {e}') 

                try: result['prdtDtlCtn'] = '\n'.join(prdt_dtl_ctn_list)
                except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >>  {e}') 
                
                try: 
                    issues = html.find('div',{'class':'ar-issue-long ar-section'}).find_all('p')
                    issue_list = [issue.text.strip() for issue in issues]                    
                    result['hrmflCuz'] += ', '.join(issue_list)
                except Exception as e: self.logger.error(f'위해원인 수집 중 에러  >>  {e}') 

                try: 
                    flw_actns = html.find('div', {'class':'ar-action-long ar-section'}).find_all('li')
                    flw_actn_list = [flw_actn.text.strip() for flw_actn in flw_actns]   
                    result['flwActn'] = ' '.join(flw_actn_list)
                except Exception as e: self.logger.error(f'후속조치 수집 중 에러  >>  {e}')    
                
                additional_information = html.find('div',{'class':'ar-additional-info ar-section'}).find_all('details')
                recall_bzenty = [info for info in additional_information if info.summary and 'Details' in info.summary.text][0].find('div', {'class':'field field--name-companies'})
                try: result['recallBzenty'] = self.utils.get_clean_string(recall_bzenty.find('div',{'class':'field--item'}).text.strip()).replace('  ','')
                except Exception as e: self.logger.error(f'리콜업체 수집 중 에러  >>  {e}')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)                            
            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')
            self.prdt_dtl_err_url.append(product_url)

        return result
