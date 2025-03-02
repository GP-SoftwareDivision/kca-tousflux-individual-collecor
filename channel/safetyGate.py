from common.utils import Utils
import json
import random
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class SAFETYGATE():
    def __init__(self, chnnl_cd, chnnl_nm, colct_bgng_date, colct_end_date, logger, api):
        self.api = api
        self.logger = logger
        self.chnnl_cd = chnnl_cd
        self.chnnl_nm = chnnl_nm
        self.start_date = colct_bgng_date
        self.end_date = colct_end_date
        self.page_num = 0
        self.header = {
            'Accept':'application/json, text/plain, */*',
            'Accept-Encoding':'gzip, deflate, br, zstd',
            'Accept-Language':'ko-KR,ko;q=0.9',
            'Content-Type':'application/json',
            'Host':'ec.europa.eu',
            'Origin':'https://ec.europa.eu',
            'Referer':'https://ec.europa.eu/safety-gate-alerts/screen/search',
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
                    url = 'https://ec.europa.eu/safety-gate-alerts/public/api/notification/carousel/?'
                    data = {'language': "en", 'page': self.page_num}
                    self.logger.info('수집시작')
                    res = requests.post(url=url, headers=self.header, json=data, verify=False, timeout=600)
                    if res.status_code == 200:
                        sleep_time = random.uniform(3,5)
                        self.logger.info(f'통신 성공, {sleep_time}초 대기')
                        time.sleep(sleep_time)     
                                                
                        res_json = json.loads(res.text)
                        datas = res_json['content']
                        for data in datas:
                            try:
                                wrt_dt = data['publicationDate'].replace('T',' ').split('.')[0]
                                if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                    self.total_cnt += 1
                                    product_id = data['id']
                                    colct_data = self.crawl_detail(product_id)
                                    insert_res = self.utils.insert_data(colct_data)
                                    if insert_res == 0:
                                        self.colct_cnt += 1
                                    elif insert_res == 1:
                                        self.error_cnt += 1
                                        product_url = f'https://ec.europa.eu/safety-gate-alerts/public/api/notification/{product_id}?language=en'
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
                        self.logger.info(f'{self.page_num+1}페이지로 이동')
                    else: raise Exception(f'통신 차단 : {url}')
                except Exception as ex:
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

    def crawl_detail(self, product_id):
        extract_error = False
        result = {'recallNo':'', 'wrtDt':'', 'recallNtn':'', 
                  'prdtNm':'', 'brand':'', 'prdtDtlCtn':'', 'plor':'', 
                  'hrmflCuz':'', 'prdtImgFlNm':'', 'prdtImgFlPath': '', 
                  'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            product_url = f'https://ec.europa.eu/safety-gate-alerts/public/api/notification/{product_id}?language=en'
            custom_header = self.header
            custom_header['Referer'] = f'https://ec.europa.eu/safety-gate-alerts/screen/search/webReport/alertDetail/{product_id}?lang=en'

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)     

                product_res_json = json.loads(product_res.text)

                try: result['recallNo'] = product_res_json['reference'].strip()
                except Exception as e: self.logger.error(f'리콜넘버 추출 실패  >>  {e}'); extract_error = True;

                try: result['wrtDt'] = product_res_json['publicationDate'].strip() # creationDate / publicationDate / modificationDate
                except Exception as e: self.logger.error(f'작성일 추출 실패  >>  {e}'); extract_error = True;

                try: result['recallNtn'] = product_res_json['country']['name'].strip()
                except Exception as e: self.logger.error(f'리콜국 추출 실패  >>  {e}'); extract_error = True;

                try: result['prdtNm'] = product_res_json['product']['versions'][0]['name'].strip()
                except Exception as e: self.logger.error(f'상품명 추출 실패  >>  {e}'); extract_error = True;

                try:
                    brands = product_res_json['product']['brands'] 
                    result['brand'] = ', '.join(brand.get('brand') for brand in brands)
                except Exception as e: self.logger.error(f'브랜드 추출 실패  >>  {e}'); extract_error = True;

                try:
                    prdt_dtl_ctn = []
                    model_types = product_res_json['product']['modelTypes']
                    barcodes = product_res_json['product']['barcodes']
                    if len(model_types) > 0:
                        prdt_dtl_ctn.append(f"Model type : {', '.join(model_type.get('modelType') for model_type in model_types)}")
                    if len(barcodes) > 0:
                        prdt_dtl_ctn.append(f"Barcode : {', '.join(barcode.get('barcode') for barcode in barcodes)}")
                    if product_res_json['product']['versions'][0]['description']:
                        prdt_dtl_ctn.append(f"Description : {product_res_json['product']['versions'][0]['description'].strip()}")
                    if product_res_json['product']['versions'][0]['packageDescription']:
                        prdt_dtl_ctn.append(f"Packaging description : {product_res_json['product']['versions'][0]['packageDescription'].strip()}")
                    result['prdtDtlCtn'] = '\n'.join(prdt_dtl_ctn)
                except Exception as e: self.logger.error(f'제품 상세내용 추출 실패  >>  {e}'); extract_error = True;

                try: result['plor'] = product_res_json['traceability']['countryOrigin']['name'].strip()
                except Exception as e: self.logger.error(f'원산지 추출 실패  >>  {e}'); extract_error = True;

                try:
                    hrmfl_cuz = []
                    if product_res_json['risk']['versions'][0]['riskDescription']:
                        hrmfl_cuz.append(f"Risk description : {product_res_json['risk']['versions'][0]['riskDescription'].strip()}")
                    if product_res_json['risk']['versions'][0]['legalProvision']:
                        hrmfl_cuz.append(f"Legal provision : {product_res_json['risk']['versions'][0]['legalProvision'].strip()}")
                    result['hrmflCuz'] = '\n'.join(hrmfl_cuz)
                except Exception as e: self.logger.error(f'위해원인 추출 실패  >>  {e}'); extract_error = True;

                try:
                    images = product_res_json['product']['photos']
                    images_paths = []
                    images_files = []
                    for idx, image in enumerate(images):
                        try:
                            id = image['id']
                            img_url = f'https://ec.europa.eu/safety-gate-alerts/public/api/notification/image/{id}'
                            img_res = self.utils.download_upload_image(self.chnnl_nm, img_url)
                            if img_res['status'] == 200:
                                images_paths.append(img_res['path'])
                                images_files.append(img_res['fileNm'])
                            else:
                                self.logger.info(f"이미지 이미 존재 : {img_res['fileNm']}")                                
                        except Exception as e:
                            self.logger.error(f'{idx}번째 이미지 수집 중 에러  >>  {e}')
                    result['prdtImgFlPath'] = ' , '.join(set(images_paths))
                    result['prdtImgFlNm'] = ' , '.join(images_files)
                except Exception as e: self.logger.error(f'제품 이미지 수집 중 에러  >>  {e}'); extract_error = True

                result['prdtDtlPgUrl'] = f"https://ec.europa.eu/safety-gate-alerts/screen/webReport/alertDetail/{product_res_json['id']}?lang=en"
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                if extract_error: 
                    url = f'https://ec.europa.eu/safety-gate-alerts/screen/webReport/alertDetail/{id}?lang=en'
                    self.logger.info(f'url :: {url}')
                    self.logger.info(f'json :: {product_res_json}') 

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')

        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')

        return result

