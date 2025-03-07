from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime

import random
import requests
import urllib3
import sys
import time
import json
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class RECALL_CHINA():
    def __init__(self, chnnl_cd, chnnl_name, url, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_nm = chnnl_name
        self.chnnl_cd = chnnl_cd
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.header = ''
        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0
        self.prdt_dtl_err_url = []
        
        # 게시판 통신 관련 정보
        self.nb_url = 'https://www.recall.org.cn/dpac_back/document/search?consumerClass=<%typeId%>&docChannelType=1&pageSize=20&current=<%pageNum%>&within=1'
        self.nb_header = ''
        self.nb_refer_url = url

        # 상세 페이지 url 가져오기 위한 관련 정보
        self.pre_url = 'https://www.recall.org.cn/dpac_back/document/get/<%prdtId%>'
        self.pre_refer_url = 'https://www.recall.org.cn/info.html?id=<%prdtId%>'

        # 상세페이지 통신 관련 정보
        self.dtl_header = ''
        self.dtl_refer_url = 'https://www.recall.org.cn/info.html?id=<%prdtId%>'
        
        self.utils = Utils(logger, api)

    def crawl(self):
        try:
            # 채널마다 typeId 가 달라지므로 url에서 split
            type_id = self.nb_refer_url.split('type=')[1]
            self.nb_header = {
                'Accept':'application/json, text/plain, */*',
                'Accept-Encoding':'gzip, deflate, br, zstd',
                'Accept-Language':'ko,en-US;q=0.9,en;q=0.8,el;q=0.7',
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
                'Referer' : self.nb_refer_url,
                'Host': 'www.recall.org.cn'
            }

            self.logger.info('수집시작')
            nb_flag = True;
            retry_num = 0
            while(nb_flag):
                try:
                    if(retry_num >= 10):
                        nb_flag = False
                        break

                    # 다음 페이지 수집 시도를 위한 pageNum 증가
                    self.page_num += 1

                    # url에 typeId replace
                    tmp_nb_url = self.nb_url.replace('<%typeId%>', type_id).replace('<%pageNum%>', str(self.page_num))
                    res = requests.get(url=tmp_nb_url, headers=self.nb_header, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)
                        res.encoding = res.apparent_encoding
                        try:
                            tmp_data_list = json.loads(res.text)
                            data_list = tmp_data_list.get('data', {}).get('searchData', {}).get('records', [])
                            if not data_list:
                                retry_num += 1
                                continue

                            for data in data_list:
                                try:
                                    doc_id = data['docId']
                                    date = data['docRelTime']
                                    tmp_dtl_url = self.crawl_dtl_url(doc_id)
                                    if(tmp_dtl_url != ''):
                                        dtl_url = 'https://www.recall.org.cn' + tmp_dtl_url
                                        date_flag, dup_flag, colct_data = self.crawl_detail(dtl_url, doc_id, date)
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
                                    else:
                                        retry_num += 1

                                except Exception as e:
                                    self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                            
                        except Exception as e:
                            self.logger.error(f'게시판 데이터 json 변환 중 에러 발생 >> {e}')

                    else:
                        nb_flag = False
                        raise Exception(f'통신 차단 : {self.tmp_nb_url}')
                    
                except Exception as e:
                    nb_flag = False
                    self.logger.error(f'게시판 페이지 통신 중 에러 발생 >> {e}')

        except Exception as e:
            self.logger.error(f'{e}')
            self.error_cnt += 1
            exc_type, exc_obj, tb = sys.exc_info()
            if '차단' in str(e):
                self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)            
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
            self.logger.info('수집종료')

    def crawl_detail(self, prdt_url, doc_id, date):
        date_flag = True
        dup_flag = 2
        result = {
            'prdtNm':'',
            'wrtDt':'',
            'prdtImgFlPath':'',
            'prdtImgFlNm':'',
            'prdtDtlCtn': '',
            'recallBzenty':'',
            'idx': '',
            'chnnlNm': '',
            'chnnlCd': self.chnnl_cd
        }
        try:
            tmp_dtl_refer_url = self.dtl_refer_url.replace('<%prdtId%>', str(doc_id))
            self.dtl_header = {
                'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding':'gzip, deflate, br, zstd',
                'Accept-Language':'ko,en-US;q=0.9,en;q=0.8,el;q=0.7',
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
                'Referer' : tmp_dtl_refer_url,
                'Host': 'www.recall.org.cn'
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

                    if 'document' in prdt_url:
                        try:
                            soup = json.loads(dtl_res.text)
                        except Exception as e:
                            self.logger.error(f'예전 게시글 json 변환 중 에러 >> {e}')
                            raise Exception('예전 게시글 json 변환 중 에러 발생')

                    # 제품명, 리콜업체 동시에 수집
                    try: 
                        tmp_prdt_nm = soup.find('div', class_=re.compile('show_tit')).text.strip()
                        if(tmp_prdt_nm != None):
                            result['prdtNm'] = tmp_prdt_nm
                            result['recallBzenty'] = tmp_prdt_nm
                    except Exception as e:
                        try:
                            tmp_prdt_nm = soup['data']['docTitle']
                            if(tmp_prdt_nm != None):
                                result['prdtNm'] = tmp_prdt_nm
                                result['recallBzenty'] = tmp_prdt_nm
                        except Exception as e:
                            self.logger.error(f'제품명 수집 중 에러  >>  {e}')

                    # 작성일자 dateTime format으로 전환
                    try:
                        result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat()
                    except Exception as e:
                        self.logger.error(f'작성일 수집 중 에러  >>  {e}')

                    result['prdtDtlPgUrl'] = self.dtl_refer_url.replace('<%prdtId%>', str(doc_id)) if 'dpac_back/document' in prdt_url else prdt_url
                    result['chnnlNm'] = self.chnnl_nm
                    result['chnnlCd'] = self.chnnl_cd
                    result['idx'] = self.utils.generate_uuid(result)

                    dup_flag = self.api.check_dup(result['idx'])
                    if dup_flag == 0:
                        try:
                            images = list(dict.fromkeys(div.img['src'] for div in soup.find_all('div', class_='TRS_Editor') if div.img))
                            images_paths = []
                            images_files = []

                            tmp_img_part = prdt_url.split("/")
                            tmp_img_url = "/".join(tmp_img_part[:-1]) + "/"
                            for idx, image in enumerate(images):
                                try:
                                    image = image.split('./')[1]
                                    img_url = tmp_img_url + image
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

    # 상세 페이지 URL을 가져오기 위한 통신
    def crawl_dtl_url(self, prdt_id):
        res = ''
        try:
            tmp_pre_url = self.pre_url.replace('<%prdtId%>', str(prdt_id))
            tmp_pre_refer_url = self.pre_refer_url.replace('<%prdtId%>', str(prdt_id))
            tmp_pre_dtl_header = {
                'Accept':'application/json, text/plain, */*',
                'Accept-Encoding':'gzip, deflate, br, zstd',
                'Accept-Language':'ko,en-US;q=0.9,en;q=0.8,el;q=0.7',
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
                'Referer' : tmp_pre_refer_url,
                'Host': 'www.recall.org.cn'
            }

            tmp_data = requests.get(url=tmp_pre_url, headers=tmp_pre_dtl_header, verify=False, timeout=600)
            data = json.loads(tmp_data.text)

            try:
                tmp_res = data['data']['docPubUrl']
                res = tmp_res
            except Exception as e:
                tmp_res = '/dpac_back/document/get/' + str(prdt_id)
                res = tmp_res

        except Exception as e:
            self.logger.error(f'crawl_dtl_url 통신 중 에러  >>  {e}')

        finally:
            return res
