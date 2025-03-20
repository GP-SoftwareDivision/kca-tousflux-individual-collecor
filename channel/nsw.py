from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
from urllib3.exceptions import MaxRetryError, NameResolutionError

import random
import requests
import urllib3
import sys
import time
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class NSW():
    def __init__(self, chnnl_cd, chnnl_name, url, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_nm = chnnl_name
        self.chnnl_cd = chnnl_cd
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date

        self.page_num = 0
        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0
        self.prdt_dtl_err_url = []

        # 게시판 통신 관련 정보
        self.nb_url = 'https://www.foodauthority.nsw.gov.au/news/recalls?page=<%pageNum%>'
        self.nb_header = ''
        self.nb_refer_url = url

        # 통신에 사용되는 공통 부모 url 정의
        self.parent_url = 'https://www.foodauthority.nsw.gov.au'

        # 이미지 통신 관련 정보
        self.img_header = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding':'gzip, deflate, br, zstd',
            'Accept-Language':'ko,en-US;q=0.9,en;q=0.8,el;q=0.7',
            'User-Agent':'PostmanRuntime/7.43.0',
            'Host': 'www.foodauthority.nsw.gov.au',
            'Connection': 'keep-alive'
        }

        self.utils = Utils(logger, api)

        # insertData시 오류 났을 경우 파악용
        self.save_log_cnt = 0

    def crawl(self):
        try:
            self.nb_header = {
                'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding':'gzip, deflate, br, zstd',
                'Accept-Language':'ko,en-US;q=0.9,en;q=0.8,el;q=0.7',
                'User-Agent':'PostmanRuntime/7.43.0',
                'Referer' : self.nb_refer_url,
                'Host': 'www.foodauthority.nsw.gov.au',
                'Connection': 'keep-alive'
            }

            self.logger.info('수집시작')
            nb_flag = True;
            retry_num = 0
            while(nb_flag):
                try:
                    if(retry_num >= 10):
                        nb_flag = False
                        break

                    # url에 pageNum replace
                    tmp_nb_url = self.nb_url.replace('<%pageNum%>', str(self.page_num))
                    res = requests.get(url=tmp_nb_url, headers=self.nb_header, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)
                        res.encoding = res.apparent_encoding
                        try:
                            soup = BeautifulSoup(res.text, 'html.parser')
                            data_list = soup.find_all('div', class_=re.compile('views-layout__item'))
                            if(len(data_list)) == 0:
                                retry_num += 1
                                if(retry_num >= 10):
                                    nb_flag = False
                                else:
                                    continue

                            for data in data_list:
                                try:
                                    date = data.find('time').text.strip()
                                    tmp_dtl_url = data.find('a')['href'].strip()
                                    
                                    dtl_url = self.parent_url + tmp_dtl_url
                                    date_flag, dup_flag, colct_data = self.crawl_detail(dtl_url, date)
                                    if date_flag:
                                        if dup_flag == 0:
                                            insert_res = self.utils.insert_data(colct_data)
                                            if insert_res == 0:
                                                self.colct_cnt += 1
                                            elif insert_res == 1:
                                                self.error_cnt += 1
                                                self.logger.error(f'게시글 수집 오류 > {dtl_url}')
                                                self.prdt_dtl_err_url.append(dtl_url)
                                        else:
                                            nb_flag = False
                                            self.logger.info(f'수집기간 내 데이터 수집 완료')
                                            break
                                    else:
                                        nb_flag = False
                                        break

                                except Exception as e:
                                    self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                            
                        except Exception as e:
                            self.logger.error(f'게시판 데이터 json 변환 중 에러 발생 >> {e}')

                    else:
                        nb_flag = False
                        raise Exception(f'통신 차단 : {self.tmp_nb_url}')
                    
                except Exception as e:
                    if isinstance(e, MaxRetryError) or isinstance(e, NameResolutionError):
                        # maxRetry, nameResolution 관련 에러가 발생한 거면 최대 두 번 더 시도하도록 구성
                        if self.max_retries_num < 2:
                            self.max_retries_num += 1
                            self.page_num -= 1
                            continue

                    self.logger.error(f'게시판 페이지 통신 중 에러 발생 >> {e}')
                    nb_flag = False
                    exc_type, exc_obj, tb = sys.exc_info()
                    self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)
                    self.save_log_cnt += 1
                    self.error_cnt += 1

                finally:
                    # 다음 페이지 수집 시도를 위한 pageNum 증가
                    self.page_num += 1

        except Exception as e:
            self.logger.error(f'nsw 수집 중 에러 발생 >> {e}')
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
            self.logger.info('수집종료')

    def crawl_detail(self, prdt_url, date):
        date_flag = True
        dup_flag = 2
        result = {
            'prdtNm':'',
            'wrtDt':'',
            'ntslCrst': '',
            'prdtDtlCtn': '',
            'hrmflCuz' : '',
            'flwActn' : '',
            'bsnmNm': '',
            'prdtImgFlPath':'',
            'prdtImgFlNm':'',
            'idx': '',
            'chnnlNm': '',
            'chnnlCd': self.chnnl_cd
        }
        try:
            self.dtl_header = {
                'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding':'gzip, deflate, br, zstd',
                'Accept-Language':'ko,en-US;q=0.9,en;q=0.8,el;q=0.7',
                'User-Agent':'PostmanRuntime/7.43.0',
                'Host': 'www.foodauthority.nsw.gov.au',
                'Connection': 'keep-alive'
            }

            dtl_res = requests.get(url=prdt_url, headers=self.dtl_header, verify=False, timeout=600)
            if dtl_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                dtl_res.encoding = dtl_res.apparent_encoding
                soup = BeautifulSoup(dtl_res.text, "html.parser")

                wrt_dt = self.utils.parse_date(date, self.chnnl_nm) + ' 00:00:00'
                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                    self.total_cnt += 1

                    # 제품명 수집
                    try: 
                        tmp_prdt_nm = soup.find('h1').text.strip()
                        if(tmp_prdt_nm != None):
                            result['prdtNm'] = tmp_prdt_nm
                    except Exception as e:
                        self.logger.error(f'제품명 수집 중 에러  >>  {e}')


                    ### 판매현황, 제품 상세내용, 위해원인, 후속조치, 업체 파싱하기 위한 사전 작업 ###
                    full_text = soup.get_text()

                    ntsl_crst_pattern = r'Product information:\s*([\s\S]*?)(?=(?:\n\s*)*(Date marking:|Date markings:|Products and Date markings:|$))'
                    dtl_pattern = r'(Date marking:|Date markings:|Products and Date markings:)\s*([\s\S]*?)(?=\s*Problem:|$)'  
                    hrmfl_pattern = r'Problem:\s*([\s\S]*?)(?=\s*What to do:|$)'
                    flw_actn_pattern = r'What to do:\s*([\s\S]*?)(?=\s*For further information please contact:|$)'  
                    bsnm_pattern = r'For further information please contact:\s*(?:[\n\s]*)([^\n]+)'

                    ntsl_crst_match = re.search(ntsl_crst_pattern, full_text)
                    if ntsl_crst_match:
                        tmp_ntsl_crst = ntsl_crst_match.group(1).strip()
                        soup_ntsl_crst = BeautifulSoup(tmp_ntsl_crst, 'html.parser')
                        ntsl_crst_clean_text = soup_ntsl_crst.get_text(separator=' ').strip()
                        result['ntslCrst'] = re.sub(r'\s+', ' ', ntsl_crst_clean_text) if ntsl_crst_clean_text else '-'

                    dtl_match = re.search(dtl_pattern, full_text)
                    if dtl_match:
                        tmp_prdt_dtl_ctn = dtl_match.group(2).strip()
                        soup_prdt_dtl_ctn = BeautifulSoup(tmp_prdt_dtl_ctn, 'html.parser')
                        prdt_dtl_ctn_clean_text = soup_prdt_dtl_ctn.get_text(separator=' ').strip()
                        result['prdtDtlCtn'] = re.sub(r'\s+', ' ', prdt_dtl_ctn_clean_text) if prdt_dtl_ctn_clean_text else '-'

                    hrmfl_match = re.search(hrmfl_pattern, full_text)
                    if hrmfl_match:
                        tmp_hrmfl = hrmfl_match.group(1).strip()
                        soup_hrmfl = BeautifulSoup(tmp_hrmfl, 'html.parser')
                        hrmfl_clean_text = soup_hrmfl.get_text(separator=' ').strip()
                        result['hrmflCuz'] = re.sub(r'\s+', ' ', hrmfl_clean_text) if hrmfl_clean_text else '-'

                    flw_actn_match = re.search(flw_actn_pattern, full_text)
                    if flw_actn_match:
                        tmp_flw_actn = flw_actn_match.group(1).strip()
                        soup_flw_actn = BeautifulSoup(tmp_flw_actn, 'html.parser')
                        flw_actn_clean_text = soup_flw_actn.get_text(separator=' ').strip()
                        result['flwActn'] = re.sub(r'\s+', ' ', flw_actn_clean_text) if flw_actn_clean_text else '-'
                    
                    bsnm_match = re.search(bsnm_pattern, full_text)
                    if bsnm_match:
                        tmp_bsnm = bsnm_match.group(1).strip()
                        soup_bsnm = BeautifulSoup(tmp_bsnm, 'html.parser')
                        bsnm_clean_text = soup_bsnm.get_text(separator=' ').strip()
                        bsnm_clean_text = bsnm_clean_text.split('\xa0')[0].strip()
                        result['bsnmNm'] = re.sub(r'\s+', ' ', bsnm_clean_text) if bsnm_clean_text else '-'

                    ### 판매현황, 제품 상세내용, 위해원인, 후속조치, 업체 파싱 및 적재 ###

                    # 작성일자 dateTime format으로 전환
                    try:
                        result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat()
                    except Exception as e:
                        self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                    result['prdtDtlPgUrl'] = prdt_url
                    result['chnnlNm'] = self.chnnl_nm
                    result['chnnlCd'] = self.chnnl_cd
                    result['idx'] = self.utils.generate_uuid(result)

                    dup_flag = self.api.check_dup(result['idx'])
                    if dup_flag == 0:
                        try:
                            images = list(dict.fromkeys(article.img['src'] for article in soup.find_all('article', class_='media-entity--type-image') if article.img))
                            images_paths = []
                            images_files = []

                            for idx, image in enumerate(images):
                                try:
                                    img_url = self.parent_url + image
                                    img_res = self.utils.download_upload_image(self.chnnl_nm, img_url, headers=self.img_header)
                                    if img_res['status'] == 200:
                                        images_paths.append(img_res['path'])
                                        images_files.append(img_res['fileNm'])
                                    else:
                                        self.logger.info(f"{img_res['message']} : {img_res['fileNm']}")                                
                                except Exception as e:
                                    self.logger.error(f'{idx}번째 이미지 수집 중 에러  >>  {img_url}')
                            result['prdtImgFlPath'] = ' , '.join(set(images_paths))
                            result['prdtImgFlNm'] = ' , '.join(images_files)

                        except Exception as e:
                            # 에전 글 수집하는 경우에만 soup의 type이 dict, dict에서는 이미지 따로 가져올 수 없으므로 다음과 같이 처리
                            if type(soup) != dict:
                                self.logger.error(f'제품 이미지 수집 중 에러  >>  {e}')

                        try: 
                            prdt_dtl_ctn = soup.find('div', class_=re.compile('TRS_Editor')).text.strip()
                            result['prdtDtlCtn'] = prdt_dtl_ctn
                        except Exception as e:
                            try:
                                prdt_dtl_ctn = soup['data']['docContent']
                                prdt_dtl_ctn = prdt_dtl_ctn.strip()
                                result['prdtDtlCtn'] = prdt_dtl_ctn
                            except Exception as e:
                                self.logger.error(f'제품 상세설명 수집 중 에러  >>  {e}')

                    elif dup_flag == 2:
                        self.duplicate_cnt += 1

                    else: self.logger.error(f"IDX 확인 필요  >> {result['idx']} ( {prdt_url} )")

                else:
                    date_flag = False
                    return date_flag, dup_flag, result

            else: 
                raise Exception(f'[{dtl_res.status_code}]상세페이지 접속 중 통신 에러  >>  {prdt_url}')

        except Exception as e:
            self.error_cnt += 1
            self.prdt_dtl_err_url.append(prdt_url)
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')

        return date_flag, dup_flag, result