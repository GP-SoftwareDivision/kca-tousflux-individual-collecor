from bs4 import BeautifulSoup
from common.utils import Utils
from datetime import datetime
import json
import random
import re
import requests
import sys
import time

class AFSCA():
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
            'Accept-Language':'ko-KR,ko;q=0.9',
            'Host':'favv-afsca.be',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        }

        self.prdt_dtl_err_url = []

        self.total_cnt = 0
        self.colct_cnt = 0
        self.error_cnt = 0
        self.duplicate_cnt = 0

        self.utils = Utils(logger, api)

    def crawl(self):
            try:
                crawl_flag = True
                retry_num = 0
                while(crawl_flag):
                    try:
                        if self.page_num == 0: url = 'https://favv-afsca.be/fr/produits'
                        else: url = f'https://favv-afsca.be/fr/produits?page={self.page_num}'
                        self.logger.info('수집 시작')
                        res = requests.get(url=url, headers=self.header, verify=False, timeout=600)
                        if res.status_code == 200:
                            sleep_time = random.uniform(3,5)
                            self.logger.info(f'통신 성공, {sleep_time}초 대기')
                            time.sleep(sleep_time)                            
                            html = BeautifulSoup(res.text, features='html.parser')

                            datas = html.find('div', {'class':'view--products--page'}).find('div', {'class':'view__content'}).find_all('li')

                            if datas == []: 
                                if retry_num >= 10: 
                                    crawl_flag = False
                                    self.logger.info('데이터가 없습니다.')
                                else:
                                    retry_num += 1
                                    continue

                            for data in datas:
                                try:
                                    try: self.locale_str = html.find('html')['lang']
                                    except: self.locale_str = ''

                                    wrt_dt = self.utils.parse_date(data.find('time')['datetime'], self.chnnl_nm)
                                    if wrt_dt >= self.start_date and wrt_dt <= self.end_date:
                                        self.total_cnt += 1
                                        product_url = 'https://favv-afsca.be' + data.find('a')['href']
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
            except Exception as e: self.logger.error(f'{e}')
            finally:
                self.logger.info(f'전체 개수 : {self.total_cnt} | 수집 개수 : {self.colct_cnt} | 에러 개수 : {self.error_cnt} | 중복 개수 : {self.duplicate_cnt}')
                self.logger.info('수집종료')
                
    def crawl_detail(self, product_url):
        result = { 'wrtDt':'','prdtImgFlPath':'', 'prdtImgFlNm':'', 'hrmflCuz':'', 'hrmflCuz2':'', 'flwActn':'',
                   'prdtNm':'', 'brand':'', 'bsnmNm':'', 'prdtDtlCtn':'', 
                   'prdtDtlPgUrl':'', 'idx': '', 'chnnlNm': '', 'chnnlCd': 0}        
        try:
            custom_header = self.header
            if self.page_num == 0: referer_url = 'https://favv-afsca.be/fr/produits'
            else: referer_url = f'https://favv-afsca.be/fr/produits?page={self.page_num}'
            custom_header['Referer'] = referer_url

            product_res = requests.get(url=product_url, headers=custom_header, verify=False, timeout=600)
            if product_res.status_code == 200:
                sleep_time = random.uniform(3,5)
                self.logger.info(f'상세 페이지 통신 성공, {sleep_time}초 대기')
                time.sleep(sleep_time)                
                
                html = BeautifulSoup(product_res.text, 'html.parser')
                main = html.find('div', {'class':'node__wrapper node__wrapper--main'})
                try:
                    wrt_dt_date = self.utils.parse_date(main.find('time')['datetime'].strip().split('T')[0], self.chnnl_nm)
                    wrt_dt_time = main.find('time')['datetime'].split('T')[1].replace('Z','')
                    wrt_dt = wrt_dt_date + ' ' + wrt_dt_time
                    result['wrtDt'] = datetime.strptime(wrt_dt, "%Y-%m-%d %H:%M:%S").isoformat() 
                except Exception as e: raise Exception(f'게시일 수집 중 에러  >>  ')

                prdt_nm_list = []
                brand_list = []
                prdt_dtl_ctn = ''
                products = [info for info in main.find_all('p') if 'Description du produit' in info.text or 'Description des produits' in info.text][0].find_next_siblings('ul')
                infos = [product.find_all('li', recursive=False) for product in products]
                if infos == []:
                    try:
                        prdouct_description = [info for info in main.find_all('p') if 'Description du produit' in info.text or 'Description des produits' in info.text][0]
                        if 'Marque' not in prdouct_description.text:
                            infos = [info for info in main.find_all('p') if 'Description du produit' in info.text or 'Description des produits' in info.text][0].find_next_sibling('p').find_all('span', recursive=False)
                            if infos != []:
                                for content in infos:
                                    try:
                                        text = content.text.replace('\xa0','').strip()
                                        if 'Nom du produit' in text or 'Nom' in text or 'Nom des produit' in text or 'Produit' in text: 
                                            if 'Marque' in text:
                                                prdt_nm = text.split('Marque')[0].replace('Nom des produits : ', '').replace('Nom : ', '').replace('Nom des produits\xa0: ', '').replace('Nom\xa0: ', '').replace('Nom du produit\xa0: ', '').replace('Nom du produit : ', '').replace('Nom du produit: ', '').strip()
                                                prdt_nm_list.append(prdt_nm)
                                                brand = text.split('Marque')[1].replace(':', '').replace('\xa0', '').strip()
                                                brand_list.append(brand)                                                    
                                            else:
                                                prdt_nm = text.replace('Nom des produits : ', '').replace('Nom : ', '').replace('Nom des produits\xa0: ', '').replace('Nom\xa0: ', '').replace('Nom du produit\xa0: ', '').replace('Nom du produit : ', '').replace('Nom du produit: ', '').strip()
                                                prdt_nm_list.append(prdt_nm)
                                        elif 'Marque' in text: 
                                            brand = text.replace('Marque : ', '').replace('Marque\xa0: ', '').replace('Marque: ', '')
                                            brand_list.append(brand)                            
                                        prdt_dtl_ctn += f'{text} \n ' if content != infos[-1] else text
                                    except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >> {e}')     
                            else:
                                product_text = [info for info in main.find_all('p') if 'Description du produit' in info.text or 'Description des produits' in info.text][0].find_next_sibling('p').text.strip()

                                prdt_nm_match = re.search(r"^(.*?(Nom|Produit).*?)$", product_text, re.MULTILINE)
                                prdt_nm = prdt_nm_match.group(1).replace('Nom des produits : ', '').replace('Nom : ', '').replace('Nom des produits\xa0: ', '').replace('Nom\xa0: ', '').replace('Nom du produit\xa0: ', '').replace('Nom du produit : ', '').replace('Nom du produit: ', '').replace('-','').strip() if prdt_nm_match else ""
                                prdt_nm_list.append(prdt_nm)

                                brand_match = re.search(r"Marque : (.+)", product_text)
                                brand = brand_match.group(1).replace('Marque : ', '').replace('Marque\xa0: ', '').replace('Marque: ', '').replace('-','').strip() if brand_match else ""
                                brand_list.append(brand)
                                
                                prdt_dtl_ctn =  ' | '.join([text.strip() for text in product_text.split('\n')]) if [text.strip() for text in product_text.split('-')][0] != '' else ' | '.join([text.strip() for text in product_text.split('\n')[1:]])  
                        else:
                            product_text = prdouct_description.text.replace('Description du produit :', '').replace('\xa0', ' ').strip()

                            prdt_nm_match = re.search(r"^(.*?(Nom|Produit).*?)$", product_text, re.MULTILINE)
                            prdt_nm = prdt_nm_match.group(1).replace('Nom des produits : ', '').replace('Nom : ', '').replace('Nom des produits\xa0: ', '').replace('Nom\xa0: ', '').replace('Nom du produit\xa0: ', '').replace('Nom du produit : ', '').replace('Nom du produit: ', '').replace('-','').strip() if prdt_nm_match else ""
                            prdt_nm_list.append(prdt_nm)

                            brand_match = re.search(r"Marque : (.+)", product_text)
                            brand = brand_match.group(1).replace('Marque : ', '').replace('Marque\xa0: ', '').replace('Marque: ', '').replace('-','').strip() if brand_match else ""
                            brand_list.append(brand)
                            
                            prdt_dtl_ctn =  ' | '.join([text.strip() for text in product_text.split('-')]) if [text.strip() for text in product_text.split('-')][0] != '' else ' | '.join([text.strip() for text in product_text.split('-')[1:]]) 
                    except: 
                        infos = [info.text.strip() for info in main.find_all('p') if 'Marque : ' in info.text]
                        prdt_dtl_ctn += ' ª '.join(infos)
                else:
                    for idx, info in enumerate(infos):
                        try:
                            for content in info:
                                try:
                                    text = content.text.replace('\xa0','').strip()
                                    if 'Nom du produit' in text or 'Nom' in text or 'Nom des produit' in text or 'Produit' in text: 
                                        prdt_nm = text.replace('Nom des produits : ', '').replace('Nom : ', '').replace('Nom des produits\xa0: ', '').replace('Nom\xa0: ', '').replace('Nom du produit\xa0: ', '').replace('Nom du produit : ', '').replace('Nom du produit: ', '')
                                        prdt_nm_list.append(prdt_nm)
                                    elif 'Marque' in text: 
                                        brand = text.replace('Marque : ', '').replace('Marque\xa0: ', '').replace('Marque: ', '')
                                        brand_list.append(brand)
                                    prdt_dtl_ctn += f'{text} | ' if content != info[-1] else text                                        
                                
                                except Exception as e: self.logger.error(f'{idx}번째 제품 상세내용 수집 중 에러  >> {e}')

                            if info != infos[-1]: prdt_dtl_ctn += ' ª '
                        except Exception as e: self.logger.error(f'제품 상세내용 수집 중 에러  >> {e}')

                side_info = html.find('div', {'class':'node__column node__column--side-content'})
                if prdt_nm_list == [] or  brand_list == []:
                    items = side_info.find_all('div', {'class':'field__label'})
                    produit = [item for item in items if item.text.strip() == 'Produit(s)']
                    text = produit[0].find_next_sibling().text

                    if prdt_nm_list == []: result['prdtNm'] = text
                    if brand_list == []: result['brand'] = text

                result['prdtNm'] = ', '.join(prdt_nm_list)
                result['brand'] = ', '.join(brand_list)
                result['prdtDtlCtn'] = prdt_dtl_ctn

                result['prdtDtlPgUrl'] = product_url
                result['chnnlNm'] = self.chnnl_nm
                result['chnnlCd'] = self.chnnl_cd
                result['idx'] = self.utils.generate_uuid(result)

                dup_flag = self.api.check_dup(result['idx'])
                if dup_flag == 0:
                    try:
                        images = main.find('div', {'class':'node__product-images'}).find_all('img')
                        images_paths = []
                        images_files = []                    
                        for idx, image in enumerate(images):
                            try:
                                img_url = 'https://favv-afsca.be' + image['src']
                                img_res = self.utils.download_upload_image(self.chnnl_nm, img_url)
                                if img_res['status'] == 200:
                                    images_paths.append(img_res['path'])
                                    images_files.append(img_res['fileNm'])
                                else:
                                    self.logger.info(f"이미지 이미 존재 : {img_res['fileNm']}")                                                            
                            except Exception as e: raise Exception (f'{idx}벉째 이미지 추출 중 에러  >>  ')
                        result['prdtImgFlPath'] = ' , '.join(set(images_paths))
                        result['prdtImgFlNm'] = ' , '.join(images_files)
                    except Exception as e: raise Exception(f'제품 이미지 수집 중 에러  >>  ')     

                    bsnm_nm = [info for info in main.find_all('p') if 'Le produit a été distribué par ' in info.text]
                    if bsnm_nm != []:
                        try: result['bsnmNm'] = bsnm_nm[0].text.strip()
                        except Exception as e: self.logger.error(f'업체 수집 중 에러  >>  {e}')   

                    try:
                        hrmfl_cuz = [title.find_next_sibling() for title in side_info.find_all('div', {'class':'field__label'}) if title.text.strip() == 'Problématique'][0].text.strip()
                        result['hrmflCuz2'] = hrmfl_cuz
                    except Exception as e: self.logger.error(f'위해원인2 수집 중 에러  >>  {e}')
                    
                    tags = html.find('div', {'class':'clearfix field--text-formatted field field--name-body field--type-text-with-summary field--label-hidden field__item'})
                    tag_yn = [idx for idx, tag in enumerate(tags) if tag != '\n' if 'Description du produit' in tag.text or 'Description des produits' in tag.text]
                    if len(tag_yn) >= 1:
                        contents = [tag for tag in tags if tag != '\n'][:tag_yn[0]]
                        result['hrmflCuz'] = [content.text for content in contents][0].strip()
                        flw_actn = [content.text for content in contents if content.find('strong')][0].strip() 
                        result['flwActn'] = flw_actn.replace('Description du produit','').replace('Description des produits','').replace(':','')
                    else:
                        result['hrmflCuz'] = '\n'.join([tag.text.strip() for tag in tags])
                        result['flwActn'] = '\n'.join([tag.text.strip() for tag in tags])                               
                                     
            else: raise Exception(f'[{product_res.status_code}]상세페이지 접속 중 통신 에러  >> {product_url}')
        except Exception as e:
            self.logger.error(f'{e}')

        return dup_flag, result