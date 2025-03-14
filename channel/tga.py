from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime, timedelta
import random
import requests
import sys
import time

class TGA():
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
            'Accept-Language':'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'Connection': 'keep-alive',
            'Referer': 'https://apps.tga.gov.au/Prod/DRAC/arn-entry.aspx'
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
            body_data = {
                '__VIEWSTATE': '',
                '__VIEWSTATEGENERATOR': '',
                'ctl00$body$PageNext': 'Next >',
            }
            url = 'https://apps.tga.gov.au/Prod/DRAC/arn-report.aspx'
            search_end_date = datetime.now() - timedelta(days=2)
            paging_cookie = ''

            while(crawl_flag):
                try:
                    headers = self.header
                    self.logger.info('수집 시작')
                    headers.update({
                        'Host': 'apps.tga.gov.au',
                        'Origin': 'https://apps.tga.gov.au'
                    })

                    if self.page_num != 0:
                        cookie = f"DRAC-Web=OriginalText=&AgreedToDisclaimer=True&SortField=&ProductKeys=&EndDate={datetime.strftime(search_end_date, '%#d/%m/%Y')} 12:00:00 AM&StartDate=1/07/2012 12:00:00 AM&ProductType=all"
                        paging_cookie = paging_cookie.replace('<%Cookie%>', cookie)
                        headers.update({
                            'Referer': url,
                            'Cookie': paging_cookie
                        })
                        res = requests.post(url=url, headers=headers, data=body_data, timeout=600)
                    else:
                        first_body_data = {
                            "cmbProductType": "all",
                            "recall-name": "",
                            "start-year": 2012,
                            "start-month": 6,
                            "start-day": 1,
                            "start-month-text": "6",
                            "start-day-text": "1",
                            "end-year": search_end_date.year,
                            "end-month": search_end_date.month-1,
                            "end-day": search_end_date.day,
                            "end-month-text": str(search_end_date.month-1),
                            "end-day-text": str(search_end_date.day),
                            "action-id": "",
                            "sponsor-name": "", 
                        }
                        cookie = f"DRAC-Web=OriginalText=&AgreedToDisclaimer=True&SortField=&ProductKeys=&EndDate={datetime.strftime(search_end_date, '%#d/%m/%Y')} 12:00:00 AM&StartDate=1/07/2012 12:00:00 AM&ProductType=all&ExportReport=&ActionId=&ActionLevel=&ActionType=&HazardClassification=&SponsorSearchText=&SponsorKeys=; DRAC-Web2=ProductKeys2=;"
                        headers.update({'Cookie': cookie})
                        res = requests.post(url=url, headers=headers, data=first_body_data, timeout=600)
                        paging_cookie = f"ASP.NET_SessionId={res.cookies.get('ASP.NET_SessionId')}; <%Cookie%>; DRAC-Web2={res.cookies.get('DRAC-Web2')}; apps.tga.gov.au={res.cookies.get('apps.tga.gov.au')}"
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)                            
                        html = BeautifulSoup(res.text, features='html.parser')

                        datas = html.find('tbody').find_all('tr')
                        if len(datas) == 0:
                            if retry_num >= 10:
                                crawl_flag = False
                                self.logger.info('데이터가 없습니다.')
                            else:
                                retry_num += 1
                                continue

                        for data in datas:
                            try:
                                date_text = data.find_all('td')[0].text.strip()
                                date_day = datetime.strptime(date_text, "%d/%m/%Y").strftime("%Y-%m-%d")
                                wrt_dt = date_day + ' 00:00:00'
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    recall_action_level = data.find_all('td')[-1].text.strip()
                                    if recall_action_level == 'Hospital': continue  # 병원/의료인 내에서 해결하는 정보 제외
                                    self.total_cnt += 1
                                    product_url = f"https://apps.tga.gov.au/Prod/DRAC/{data.find('a')['href']}"
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
                        try:
                            body_data.update({
                                '__VIEWSTATE': html.find('input', {'name': '__VIEWSTATE'})['value'],
                                '__VIEWSTATEGENERATOR': html.find('input', {'name': '__VIEWSTATEGENERATOR'})['value'],
                                'ctl00$body$PageNext': html.find('input', {'name': 'ctl00$body$PageNext'})['value']
                            })
                        except Exception as e:
                            self.logger.error(f'다음 페이지 호출 데이터 추출 중 에러 >> {e}')
                            crawl_flag = False
                            break
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
        result = {'prdtNm':'', 'wrtDt':'', 'prdtDtlCtn':'', 'hrmflCuz':'', 'flwActn':'', 
                  'atchFlPath':'', 'atchFlNm':'', 'bsnmNm': '', 
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            custom_header = self.header 
            referer_url = 'https://apps.tga.gov.au/Prod/DRAC/arn-report.aspx'
            custom_header['Referer'] = referer_url
            pdf_url = 'https://apps.tga.gov.au/Prod/DRAC/pdf/arn-pdf.ashx'

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                html = BeautifulSoup(product_res.text, "html.parser")

                try:
                    tmp_html = html.find('span', {'id': 'lblProductName'}).prettify()
                    recall_datas = tmp_html.split('<br/>')
                    result['prdtNm'] = self.utils.get_clean_string(recall_datas[0])
                    if len(recall_datas) > 1:
                        result['prdtDtlCtn'] = self.utils.get_clean_string(''.join(recall_datas[1:]))
                    else:
                        result['prdtDtlCtn'] = self.utils.get_clean_string(''.join(recall_datas))
                except Exception as e: self.logger.error(f'제품명 및 상세내용 수집 중 에러  >>  {e}')

                try: 
                    date_text = html.find('span',{'id':'lblRecallDate'}).text.strip()
                    date_day = datetime.strptime(date_text, "%d/%m/%Y").strftime("%Y-%m-%d")
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
                        result['bsnmNm'] = html.find('span',{'id':'lblSponsor'}).text.strip()
                    except Exception as e: self.logger.error(f'업체 수집 중 에러  >>  {e}')

                    try: 
                        result['hrmflCuz'] = html.find('span',{'id':'lblInformation'}).text.strip()
                    except Exception as e: self.logger.error(f'위해원인 수집 중 에러  >>  {e}')

                    try: 
                        result['flwActn'] = html.find('span',{'id':'lblReason'}).text.strip()
                    except Exception as e: self.logger.error(f'후속조치 수집 중 에러  >>  {e}')
                    
                    try: 
                        atchl_url = pdf_url
                        custom_header.update({
                            'Cookie': f'DRAC-Web={product_res.cookies.get("DRAC-Web")}'
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