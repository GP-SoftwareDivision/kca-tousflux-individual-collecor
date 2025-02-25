from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import requests
import sys
import time

class BVL():
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
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
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
                        if self.page_num == 0: url = 'https://www.lebensmittelwarnung.de/DE/Home/home_node.html'
                        else: url = f'https://www.lebensmittelwarnung.de/DE/Home/home_node.html?gtp=310780_list1%253D{self.page_num}'
                        self.logger.info('수집 시작')
                        res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')

                            datas = html.find_all('li',{'class':'lmw-search__results-element'})
                            for data in datas:
                                try:
                                    wrt_dt = self.utils.parse_date_with_locale(data.find('time').text.strip(), self.chnnl_nm) + ' 00:00:00'
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        title = data.find('h3').text.strip()
                                        if '【機能性表示食品】' not in title:
                                            self.total_cnt += 1
                                            product_url = data.find('h3').find('a')['href']
                                            colct_data = self.crawl_detail(product_url)
                                            insert_res = self.api.insertData2Depth(colct_data)
                                            if insert_res == 0:
                                                self.colct_cnt += 1
                                            elif insert_res == 1:
                                                self.error_cnt += 1
                                                self.utils.save_colct_log(f'게시글 수집 오류 > {product_url}', '', self.chnnl_cd, self.chnnl_nm, 1)
                                            elif insert_res == 2 :
                                                self.duplicate_cnt += 1
                                        else:
                                            self.logger.info('"기능성표시식품"이므로 수집 제외')
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
        result = { 'wrtDt':'', 'hrmflCuz':'', 'prdtDtlCtn':'', 'prdtNm':'', '위해/사고?':'', '정보출처 recall_srce?':'',
                   'url':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        # 게시일, 위해원인 hrmfl_cuz, 제품 상세내용 prdt_dtl_ctn, 제품명 prdt_nm, 위해/사고?, 정보출처 recall_srce?
        try:
            custom_header = self.header
            if self.page_num == 0: referer_url = 'https://www.lebensmittelwarnung.de/DE/Home/home_node.html'
            else: referer_url = f'https://www.lebensmittelwarnung.de/DE/Home/home_node.html?gtp=310780_list1%253D{self.page_num}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')

                try: 
                    wrt_dt = self.utils.parse_date_with_locale(html.find('time').text.strip(), self.chnnl_nm) + ' 00:00:00'
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: self.logger.error(f'작성일 수집 중 에러  >>  ')

                cluster_list = html.find('div', {'data-elementor-type':'single-post'}).find_all('div', {'class':'elementor-widget-container'})
                main = [content for content in cluster_list if content.find_parent('div')['data-widget_type'] == 'theme-post-content.default'][0].find_all('p')

                prdt_dtl_ctn = ''
                test = ''
                for item in main:
                    try:
                        title = item.find('strong').text.strip()
                        if title == '■製品の概要': 
                            content = item.find_next_sibling('table').find_all('tr')
                        else: 
                            content = item.text.strip()
                        
                        if title == '■注意喚起の内容':
                            try: result['hrmflCuz'] = self.utils.get_clean_content_string(content.replace(title, ''))
                            except Exception as e:
                                raise Exception (f'위해원인 수집 중 에러  >>  ')
                        elif title == '■健康被害の状況':
                            try: test += self.utils.get_clean_content_string(content.replace(title, ''))
                            except Exception as e:
                                raise Exception (f'위해/사고 수집 중 에러  >>  ')                            
                        elif title == '■当該製品に関する国内の状況':
                            try: test += self.utils.get_clean_content_string(content.replace(title, ''))
                            except Exception as e:
                                raise Exception (f'위해/사고 수집 중 에러  >>  ')                            
                        elif title == '■引用元':
                            try: result['정보출처 recall_srce?'] = self.utils.get_clean_content_string(content.replace(title, ''))
                            except Exception as e:
                                raise Exception (f'정보출처 수집 중 에러  >>  ')                            
                        elif title == '■製品の概要':
                            title_list = [title.text.strip() for title in content[0].find_all('td')]
                            # for td in content[1:]:
                            #     try:

                            #     except Exception as e:
                            #         self.logger.error(f'{e}')
                            # try: 
                            #     result['prdtDtlCtn'] = self.utils.get_clean_content_string(content.replace(title, ''))
                            # except Exception as e:
                            #     raise Exception (f'제품 상세내용 수집 중 에러  >>  ')                    

                    except Exception as e:
                        self.logger.error(f'항목 수집 중 에러  >>  ')

                result['위해/사고?'] = test
                result['prdtDtlCtn'] = prdt_dtl_ctn

                result['url'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result['url'], self.chnnl_nm, result['prdtNm'])                            
            else: raise Exception(f'상세페이지 접속 중 통신 에러  >> {product_res.status_code}')
        except Exception as e:
            self.logger.error(f'{e}')

        return result