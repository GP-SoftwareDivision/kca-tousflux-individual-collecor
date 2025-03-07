from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import random
import requests
import urllib3
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class METI():
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
            'Host':'www.meti.go.jp',
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
            url = 'https://www.meti.go.jp/product_safety/recall/index.html'
            self.logger.info('수집시작')
            # headers = self.header
            # headers['Cookie'] = 'myview=M%7C; __ulfpc=202503021818268243; bm_mi=71B97D221EA644FCEE40AA01D8813027~YAAQVnpGaFxFwECVAQAAulUmVhrM9jL7feA0tQ9ByAzISl5MNEQtxjjIvnW8Vnq6cOwKdMTO9DL5nYud1glkXFe7fNPwx6PG7zZXFOG2VwkyftVvo0aKVk0MVutaL4yd4y1TjbQTxuvP4n+Ax0EQgF9gHP+J/A37rC3+oxNvgWKXIwmqc+0fyfeqf0nQ7VaLMkToV4QDFRPGMmL0KRoLoY36LaprPxES3Nh8MtujK3pLRWpK5oZyrYCISblBLvVXiR3n7lBnhANkcSFJE4YfMPia+PutcJ1mgXLqTW7mjjrTccTZGJiAvopXuCDDo30TtBbXOTcTVMbmHjsSUOY=~1; ak_bmsc=2A04EE671A675D8033944A089D693D7B~000000000000000000000000000000~YAAQVnpGaHJFwECVAQAAslkmVhqBoTWxLXz2HC6qb+sMTjnrkxG9StMf/JzlXA1uq3eiOySuCPUTJkzTZ6FwYFiVBRCWIL9srnLTkdKB2aECpCRDOhav6wHwSFvQNsn1+wObOv5QSlMalpC2Jc07LsJ6ToWZZnlmA6XvRggmJfuSlxZOe5rnwKRurmVX3ezb+YMCdfquHTpvqkpq4gkMBBJtccLR/6i6+GURkxsjMWfj+VGL94VOsYtRrVHIqMTpoaL55BVGD/F40afTceWOli2uj0Ag+YveHaoqIk0jF9a2a64BL5ecn57HSLtaeBj+GNiWcQyj47MQngNNMmh4VyAAXiIJ2lLEAaNpkTtHIN2W+eH/9OJW6mEhfNID9DFz7IE7aC7rpopt4lN7EqcKXZvt82qaxf0HGRxhHW2RC1G0E1Mpz67rWbAwFglIKPTBFh69S9kA7t78sxWoN+3uKoekXxk8/q0UPS0=; bm_sv=34283EB1563ECC28E411F16576EA833E~YAAQlaUrF84qpECVAQAARmksVhpAmvQdFNk1mSkNO7KSUjUArgo0NLskJTLx4ILefRwN7GQUynTK6zW3xtCYv0h5pAKgb/XG+929E2nKsbw1URCVj8NPROu2FTt+bLqI1EvSYoxg6Zd/3vDyo0PxSJn3elh4cc6mQJzBaPm7KwLbEa/6DHq8ea5jg27y/TOR3VAB7B28PVRnn8pGGHoOysjP41sQ19verOGs5K3F8w7rpBOWW0IBlk7OSUbBERC4FQ==~1'
            res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
            if res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                res.encoding = res.apparent_encoding
                html = BeautifulSoup(res.text, "html.parser")

                # datas = soup.find('tbody').find_all('tr')
                datas = [item for item in html.find_all('div', {'class':'h22011 r10'}) if item.text.strip() == '日付順リコール製品情報'][0].find_next_sibling('table').find_all('tr')
                if len(datas) == 0:
                    self.logger.info('데이터가 없습니다.')

                for data in datas[1:]:
                    try:
                        wrt_dt = self.utils.parse_date(data.find('th').text.strip(), self.chnnl_nm) + ' 00:00:00'
                        if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                            self.total_cnt += 1
                            product_url = 'https://www.meti.go.jp/product_safety/recall/' + data.find('a')['href']
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
                            else: self.logger.error(f"IDX 확인 필요  >> {colct_data['idx']} ( {product_url} )")
                        elif wrt_dt < self.start_date:
                            self.logger.info(f'수집기간 내 데이터 수집 완료')
                            break
                    except Exception as e:
                        self.logger.error(f'데이터 항목 추출 중 에러 >> {e}')
                        
            else: raise Exception(f'통신 차단 : {url}')
        except Exception as e:
            self.logger.error(f'crawl 통신 중 에러 >> {e}')
            self.error_cnt += 1
            exc_type, exc_obj, tb = sys.exc_info()
            self.utils.save_colct_log(exc_obj, tb, self.chnnl_cd, self.chnnl_nm)            
        finally:
            self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
            self.logger.info('수집종료')

    def crawl_detail(self, product_url):
        dup_flag = -1
        result = {'wrtDt':'', 'prdtNm':'', 'recallBzenty':'', 'hrmflCuz':'', 'flwActn': '', 'prdtDtlCtn': '',
                  'prdtImgFlPath':'', 'prdtImgFlNm':'', 'recallSrce':'', 'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}
        try:
            custom_header = self.header
            custom_header['Referer'] = 'https://www.meti.go.jp/product_safety/recall/index.html'
            # custom_header['Cookie'] = 'myview=M%7C; __ulfpc=202503021844543183; bm_mi=874BE3C94EF00949D654CAEF840020EE~YAAQlaUrF7NopECVAQAATZM+VhrnDmIBoY+XpeBrrmPMMYDG71/+ly6HehOfuQBAJYuV5FzBssOpbI1jLzcFmbzes/2fpEutzcl/5DFKkmvwYEZ7bRmkcbFGl36rXL3KPyv4VnaO5csYI5bkDGySJLZBC42Tpn5D/RAqs1bifX3o2zUxv1Xo9B0ElMst7iV+simFhBhylU7o/xictftg0Pp3/eoT82+2iB77keu7CPrnu5wxl/60O+cK0lGhifskj7C55QhGaXnR63gYOUrR+2c8ODXu1pEbDspCrC77Td3B6nfjhmlTeCroCygVBSVO7rg/Zzc5gO2YJZWbXkjtg0qHuOXaFd9VbPo=~1; ak_bmsc=A45D12660ED03E686AE278D1E37CBFDA~000000000000000000000000000000~YAAQlaUrFwVqpECVAQAAQRY/VhqdNEOERlfqmlgYYXr/fZ8zVd2QKGbX6m04yeu9yPXWXZcbhFC5gS+rHpDFMirBkR517JzBRioCIogD82mvijtLU7QRuOdEpRx4VPn8XXIvC6UnAinNLPvDOV2LgaFgVx8JAMt5Pdj3qFuMC50LKmoJgL+WHeoxjjgQ70KdxjRe/Y0j3IimAvrN89jwEgnO+h8WC80L9QJpfouCDL13UGLJ0dM+1I6/RGRmxb6/o0176LrOfQ2AkVqkCKLui8A1sPT/49EedYardd151EYrStM50WjpDX9Zah/W+7pIGdaOeYFXvxeuKVu6GKlbwKCizdPoxERJBrCDH3dEK0JTGdhlGx9pmJGORxG6k5iOWors6dIgEOkSi67rm1tZxUrMZbV8FZpEHAC7lZxid9hLZYqI0wVx+M0PkD9ZndvLlt3v676DGiOF6iUzcUoX/2lvOimTvegCz/dc/A/EcqXpTVyxhyE=; UqZBpD3n3iPIDwJU9AfooHmcWqNMpNYaZuac69nY=v1JMPBg8Sc9qj; bm_sv=83DA40EBFF375EAD3FA77331B074E980~YAAQPzVDFxfFQ1CVAQAAOZNNVhr7Fu0V4PX3RkHrQA75Mg521f2DKNdWVs9ggTL8M8lR19M+wO3QafHYwjLvLqgO8sSL5yQk3GF821yseLubANuE7k8vLfMseOeO5yQRERUkOmkTRFeS3TbVP0zxmVZJKLOdt/pAduu9MfG+TYvW8wHLNPX7Po9FTW+SoVEpO3dgpXIc60AJQ+MjzQ4xQkoKc3F3DgSrVpsin/pgU4ZKWtwW8UVYBF/LImafRpCtyw==~1'

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)
                product_res.encoding = product_res.apparent_encoding
                html = BeautifulSoup(product_res.text, "html.parser")

                datas = html.find('div', {'class':'wrapper2011'}).find_all('div', {'class':'h22011 r10'})
                for data in datas:
                    title = data.find('h2').text.strip()
                    try:
                        if title == 'リコール実施日':
                            try: 
                                wrt_dt = self.utils.parse_date(data.find_next_sibling('p').text.strip(), self.chnnl_nm) + ' 00:00:00'
                                result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat()
                            except Exception as e: raise Exception(f'게시일 수집 중 에러  >>  {e}')
                        elif title == '製品名':
                            try: result['prdtNm'] = data.find_next_sibling('p').text.strip()
                            except Exception as e: raise Exception(f'제품명 수집 중 에러  >>  {e}')
                        elif title == 'リコール事業者名':
                            try: result['recallBzenty'] = data.find_next_sibling('p').text.strip()
                            except Exception as e: raise Exception(f'리콜업체 수집 중 에러  >>  {e}')
                        elif title == 'リコール実施の理由':
                            try: result['hrmflCuz'] = data.find_next_sibling('p').text.strip()
                            except Exception as e: raise Exception(f'위해원인 수집 중 에러  >>  {e}')
                        elif title == 'リコール対策内容':
                            try: result['flwActn'] = data.find_next_sibling('p').text.strip()
                            except Exception as e: raise Exception(f'후속조치 수집 중 에러  >>  {e}')
                        elif title == 'リコール製品の概要':
                            try: 
                                tags = data.find_next_siblings()
                                prdt_dtl_ctn = self.extract_prdt_dtl_ctn(tags)
                                if prdt_dtl_ctn == '': self.logger.error(f'제품 상세내용 확인 필요  >>  {product_url}')
                                result['prdtDtlCtn'] = prdt_dtl_ctn
                            except Exception as e: raise Exception(f'제품 상세내용 수집 중 에러  >>  {e}')
                       
                            # try:
                            #     images = [img for tag in data.find_next_siblings() if tag.find('img') for img in tag.find_all('img')]
                            #     images_paths = []
                            #     images_files = []
                            #     for idx, image in enumerate(images):
                            #         try:
                            #             img_url = 'https://www.meti.go.jp/product_safety/recall/file/' + image['src']
                            #             img_res = self.utils.download_upload_image(self.chnnl_nm, img_url)
                            #             if img_res['status'] == 200:
                            #                 images_paths.append(img_res['path'])
                            #                 images_files.append(img_res['fileNm'])
                            #             else:
                            #                 self.logger.info(f"{img_res['message']} : {img_res['fileNm']}")                                
                            #         except Exception as e:
                            #             self.logger.error(f'{idx}번째 이미지 수집 중 에러  >>  {img_url}')
                            #     result['prdtImgFlPath'] = ' , '.join(set(images_paths))
                            #     result['prdtImgFlNm'] = ' , '.join(images_files)
                            # except Exception as e: self.logger.error(f'제품 이미지 수집 중 에러  >>  {e}')
                        elif title == '事業者リコール情報URL':
                            try: result['recallSrce'] = data.find_next_sibling('p').text.strip()
                            except Exception as e: raise Exception(f'정보출처 수집 중 에러  >>  {e}')
                    except Exception as e:
                        self.logger.error(f'{e}')

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    # todo 이미지 수집
                    print(dup_flag)

            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >>  {product_url}')

        except Exception as e:
            self.logger.error(f'crawl_detail 통신 중 에러  >>  {e}')
            self.prdt_dtl_err_url.append(product_url)

        return result