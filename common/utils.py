import base64
from urllib.parse import urlparse
from datetime import datetime, timedelta
from dateutil import parser
from pathlib import Path
from PIL import Image

import base64
import json
import linecache
import os
import random
import re
import requests
import socket
import time
import uuid
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from pypdf import PdfReader, PdfWriter
import zipfile

DATE_PATTERNS = {
    "dd MMM yyyy": "%d %b %Y",
    "yyyy-MM-dd": "%Y-%m-%d",
    "d MMM yyyy": "%d %b %Y",
    "dd.MM.yyyy": "%d.%m.%Y",
    "EEEE, dd MMM yyyy": "%A, %d %b %Y",
    "dd-MM-yyyy": "%d-%m-%Y",
    "MMMM d, yyyy": "%B %d, %Y",
    "MM/dd/yyyy": "%m/%d/%Y",
    "EEE, MM/dd/yyyy": "%a, %m/%d/%Y",
    "yyyy/MM/dd": "%Y/%m/%d",
    "yyyy年M月d日": "%Y年%m月%d日",  # Needs locale handling if needed
    "d.M.yyyy": "%d.%m.%Y",
    "d MMMM, yyyy": "%d %B, %Y",
    "'Published date: 'd MMM yyyy": "'Published date: '%d %b %Y",
    "'Date of release: 'd MMM yyyy": "'Date of release: '%d %b %Y",
    "EEE, d MMM yyyy": "%a, %d %b %Y",
    "'Last updated: 'd MMMM yyyy": "'Last updated: '%d %B %Y",
    "'Waarschuwing | 'dd-MM-yyyy": "'Waarschuwing | '%d-%m-%Y",
    "yyyy.MM.dd": "%Y.%m.%d",
    "d MMMM yyyy": "%d %B %Y",
    "EEEE d MMMM yyyy": "%A %d %B %Y",  # Needs locale handling if needed
    "EEE, MM/dd/yyyy - 'Current'": "%a, %m/%d/%Y - 'Current'",
    "[A-Za-z ]+ \\| yyyy-MM-dd": "[A-Za-z ]+ | %Y-%m-%d",
    "MM-dd yyyy": "%m-%d %Y",
    "MMMM YYYY": "%B %Y",
    "[A-Za-z]+ YYYY" : "%B %Y"
}


class Utils():
    def __init__(self, logger, api):
        self.logger = logger
        self.api = api

    def get_latest_downloaded_file_name(self, directory):
        """
        주어진 디렉터리에서 가장 최근에 생성된 파일의 이름을 반환
        """
        try:
            files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
            if not files:
                return None
            latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(directory, f)))
            return latest_file
        except Exception as e:
            print(f"Error getting latest file: {e}")
            return None

    def rename_file(self, directory):
        file_name = ""
        try:
            last_file_name = self.get_latest_downloaded_file_name(directory)
            if not last_file_name:
                return ""
            
            last_file_path = Path(directory) / last_file_name
            
            # 특수문자 제거 (한글, 영어, 숫자, 일부 유니코드 문자 및 마침표 허용)
            file_name = re.sub(r"[^a-zA-Z0-9\u3131-\uD79D\u4E00-\u9FFF\u3040-\u309F\u30A0-\u30FF\.\u00C0-\u017F]", "", last_file_name)
            new_file_path = Path(directory) / file_name
            
            last_file_path.rename(new_file_path)
        except Exception as e:
            print(f"Error renaming file: {e}")
        finally:
            return file_name

    def download_upload_atchl(self, chnnl_nm, url, headers=None):
        result = ''
        time.sleep(random.uniform(3,5))
        try:    
            save_path = self.download_atchl(chnnl_nm, url, headers)
            if os.path.getsize(save_path) / (1024 * 1024) > 1:
                save_path = self.reduce_atchl(save_path)
            res = self.upload_atchl(save_path, chnnl_nm)
            if res != '': result = res
        except Exception as e:
            self.logger.error(f'{e}')
        finally:
            # 파일 삭제
            if os.path.exists(save_path):
                os.remove(save_path)
                self.logger.info(f'파일 삭제 완료: {save_path}')
        return result         

    def download_atchl(self, chnnl_nm, url, headers=None):
        result = ''
        now = datetime.strftime(datetime.now(), '%Y-%m-%d')
        try:
            file_name = str(int(time.time() * 1000))
            save_path = f'/app/files/atchl/{chnnl_nm}/{now}/{file_name}.pdf'
            os.makedirs(os.path.dirname(save_path), exist_ok=True) # 디렉토리 생성
            with requests.get(url, headers=headers, stream=True) as response:
                if response.status_code != 200:
                    raise Exception(f'파일 다운로드 실패, HTTP status code: {response.status_code}')

                with open(save_path, 'wb') as f:
                    f.write(response.content)

            self.logger.info(f'파일 다운로드 성공: {save_path}')
            result = save_path
        except Exception as e:
            self.logger.error(f'{e}')
        return result

    def upload_atchl(self, save_path, chnnl_nm):
        result = ''
        try:
            # API 업로드
            with open(save_path, 'rb') as file:
                files = {'file': (os.path.basename(save_path), file, 'application/pdf')}
                data = {'chnnlNm': chnnl_nm}
                try:
                    res_file = self.api.uploadNas(files, data)
                    result = json.loads(res_file.text)
                    if result['status'] == 200: self.logger.info(f'파일서버에 이미지 업로드 성공: {result}')
                    else: raise Exception(f"파일서버에 이미지 업로드 중 에러  >>  status : {result['status']} | message : {result['message']}")
                except Exception as e: raise Exception(f'파일서버에 첨부파일 업로드 중 에러 {e}')                
        except Exception as e:
            self.logger.error(f'{e}')     
        return result
    
    def download_upload_image(self, chnnl_nm, url, timeout=600):
        result = ''
        time.sleep(random.uniform(3,5))
        try:
            save_path = self.download_image(chnnl_nm, url, timeout)
            if save_path != '':
                res = self.upload_image(save_path, chnnl_nm)
                result = res
        except Exception as e:
            self.logger.error(f'{e}')
        finally:
            # 파일 삭제
            if os.path.exists(save_path):
                os.remove(save_path)
                self.logger.info(f'파일 삭제 완료: {save_path}')        
        return result
    
    def download_image(self, chnnl_nm, url, timeout):  # timeout=600(10분)
        result = ''
        now = datetime.strftime(datetime.now(), '%Y-%m-%d')
        try:
            file_name = str(int(time.time() * 1000))
            save_path = f'/app/files/image/{chnnl_nm}/{now}/{file_name}.jpeg'
            os.makedirs(os.path.dirname(save_path), exist_ok=True) # 디렉토리 생성
            # if chnnl_nm == 'METI - 개별':
            #     headers = {
            #         'Cookie':'bm_mi=874BE3C94EF00949D654CAEF840020EE~YAAQlaUrF7NopECVAQAATZM+VhrnDmIBoY+XpeBrrmPMMYDG71/+ly6HehOfuQBAJYuV5FzBssOpbI1jLzcFmbzes/2fpEutzcl/5DFKkmvwYEZ7bRmkcbFGl36rXL3KPyv4VnaO5csYI5bkDGySJLZBC42Tpn5D/RAqs1bifX3o2zUxv1Xo9B0ElMst7iV+simFhBhylU7o/xictftg0Pp3/eoT82+2iB77keu7CPrnu5wxl/60O+cK0lGhifskj7C55QhGaXnR63gYOUrR+2c8ODXu1pEbDspCrC77Td3B6nfjhmlTeCroCygVBSVO7rg/Zzc5gO2YJZWbXkjtg0qHuOXaFd9VbPo=~1; ak_bmsc=A45D12660ED03E686AE278D1E37CBFDA~000000000000000000000000000000~YAAQlaUrFwVqpECVAQAAQRY/VhqdNEOERlfqmlgYYXr/fZ8zVd2QKGbX6m04yeu9yPXWXZcbhFC5gS+rHpDFMirBkR517JzBRioCIogD82mvijtLU7QRuOdEpRx4VPn8XXIvC6UnAinNLPvDOV2LgaFgVx8JAMt5Pdj3qFuMC50LKmoJgL+WHeoxjjgQ70KdxjRe/Y0j3IimAvrN89jwEgnO+h8WC80L9QJpfouCDL13UGLJ0dM+1I6/RGRmxb6/o0176LrOfQ2AkVqkCKLui8A1sPT/49EedYardd151EYrStM50WjpDX9Zah/W+7pIGdaOeYFXvxeuKVu6GKlbwKCizdPoxERJBrCDH3dEK0JTGdhlGx9pmJGORxG6k5iOWors6dIgEOkSi67rm1tZxUrMZbV8FZpEHAC7lZxid9hLZYqI0wVx+M0PkD9ZndvLlt3v676DGiOF6iUzcUoX/2lvOimTvegCz/dc/A/EcqXpTVyxhyE=; bm_sv=83DA40EBFF375EAD3FA77331B074E980~YAAQlaUrF6/SpECVAQAAf5FjVhpv1iitoFQ7Zupue2geLkwxTB7icyA4fzAjX9px/Ov83fnYhbmhLU7sGUg48asVwKvXI51LONNMYtoP33N4U5eqVyKFqq8F4fQP0/3rRAh2JNtquay1MQINIP4awMQSDnQ6UzCfYuWd4OmEAg+7OcgRqbojmvR7I6BLChDYR7Fi8+zmNwjkAUpxNk+2ErVAfasYILYousRfxgkd6ReEb/wmkhUzbMU1/yCC9SHdRA==~1',
            #         'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            #         'Accept-Encoding':'gzip, deflate, br, zstd',
            #         'Accept-Language':'ko-KR,ko;q=0.9',
            #         'Host':'www.meti.go.jp',
            #     }
            #     # 데이터 손실 방지를 위해 stream=True 사용, timeout=10분(600초) 설정
            #     response = requests.get(url, headers=headers, stream=True, timeout=timeout)
            if '중국 제품 안전 및 리콜 정보 네트워크' in chnnl_nm:
                china_recall_headers = {
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
                    'Accept':'*/*',
                    'Accept-Encoding':'gzip, deflate, br',
                    'Host':'www.recall.org.cn',
                    'Connection': 'keep-alive'
                }
                response = requests.get(url, headers=china_recall_headers, stream=True, timeout=timeout, verify=False)
            else:
                response = requests.get(url, stream=True, timeout=timeout)

            if response.status_code != 200:
                raise Exception(f'파일 다운로드 실패, HTTP status code: {response.status_code}')
                        
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            self.logger.info(f'파일이 성공적으로 다운로드되었습니다: {save_path}')
            result = save_path
        except Exception as e:
            self.logger.error(f'첨부파일 다운로드 중 오류 발생: {e}')
        return result

    def upload_image(self, save_path, chnnl_nm):
        result = {'path':'', 'fileNm':''}
        try:
            self.resize_image(save_path)

            # API 업로드
            with open(save_path, 'rb') as file:
                files = {'file': (os.path.basename(save_path), file, 'image/jpeg')}
                data = {'chnnlNm': chnnl_nm}
                try:
                    res_file = self.api.uploadNas(files, data)
                    result = json.loads(res_file.text)
                    if result['status'] == 200: self.logger.info(f'파일서버에 이미지 업로드 성공: {result}')
                    else: raise Exception(f"파일서버에 이미지 업로드 중 에러  >>  status : {result['status']} | message : {result['message']}")
                except Exception as e: self.logger.error(f'{e}')     
        except Exception as e:
            self.logger.error(f'{e}')
        return result
    
    def resize_image(self, image_path, target_size_kb=1024):  # target_size_kb를 기본 1MB로 설정
        try:
            img = Image.open(image_path)

            # RGBA 모드라면 RGB로 변환 (먼저 수행)
            if img.mode == 'RGBA':
                img = img.convert('RGB')

            # RGBA 모드라면 RGB로 변환 (먼저 수행)
            if img.mode == 'RGBA':
                img = img.convert('RGB')

            # 이미지 크기 확인
            current_size = os.path.getsize(image_path) / 1024  # KB 단위로 변환

            if current_size <= target_size_kb:
                self.logger.info(f"이미지 크기가 이미 {target_size_kb}KB 이하입니다.")
                return image_path

            # 리사이징된 이미지 저장 경로 설정
            resized_image_path = f"{image_path}"
            img_resized = img.resize((1024, 1024))
        

            # 품질을 조절하며 목표 크기 이하로 만들기
            quality = 70
            img_resized.save(resized_image_path, format="JPEG", quality=quality)
            resized_size = os.path.getsize(resized_image_path) / 1024

            while resized_size > target_size_kb and quality > 10:
                quality -= 10
                img_resized.save(resized_image_path, format="JPEG", quality=quality)
                resized_size = os.path.getsize(resized_image_path) / 1024
                self.logger.info(f"품질 {quality}로 재시도 후 크기: {resized_size} KB")

            if resized_size > target_size_kb:
                self.logger.warning(f"최소 품질로도 목표 용량({target_size_kb}KB)을 맞추지 못했습니다.")
            else:
                self.logger.info(f"이미지가 리사이징되었습니다: {resized_image_path}")

            return resized_image_path

        except Exception as e:
            self.logger.error(f'resize_image  >>  {e}')
            return image_path  # 에러 발생 시 원본 경로 반환
    
    def normalize_image_filename(self, file_name):
        # 이미지 확장자 목록
        image_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp"}
        
        # 파일명에서 확장자 추출
        name, ext = os.path.splitext(file_name)
        
        # 확장자가 이미지 확장자 목록에 포함되어 있으면 제거
        if ext.lower() in image_extensions:
            return name  # 확장자를 제거한 파일명 반환
        
        # 확장자가 없거나 이미지 확장자가 아니면 기본 확장자 추가
        return file_name

    def extract_content(self, start_tag, end_tag):
        content = []
        seen_texts = set()  # 중복 방지

        try:
            for tag in start_tag.find_all_next():
                if tag == end_tag: break # tag가 마지막태그인 경우 종료

                if tag.name == 'p' and tag.get('class') == ['date']:  continue # tag 자체가 <p class="date">일 경우

                text = tag.get_text(strip=True)
                if text and text not in seen_texts:  # 중복된 텍스트 방지
                    seen_texts.add(text)
                    content.append(text)
        except Exception as e:
            self.logger.error(f"Error in extract_content: {e}")

        return content

    def generate_uuid(self, result):
        generated_uuid = ''
        try:
            tmp_str = result['prdtDtlPgUrl'] + result['chnnlNm'] + result['prdtNm'] + result['wrtDt']
            base64_encoded_str = base64.b64encode(tmp_str.encode('utf-8')).decode('utf-8')
            generated_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, base64_encoded_str)

        except Exception as e:
            self.logger.error(f'uuid 생성 중 에러 >> {e}')

        return str(generated_uuid)

    def get_clean_string(self, input_str: str) -> str:
        return self.remove_line_break(self.remove_quote(self.replace_regex(input_str))).strip()

    def replace_regex(self, input_str: str) -> str:
        clean_str = input_str
        try:
            if re.search(r"[\u4e00-\u9fff]", input_str):
                return clean_str
            
            # Replace script and style tags
            clean_str = re.sub(r"(?s)<script.*?</script>|<style.*?</style>", "", clean_str)
            # Remove HTML comments
            clean_str = re.sub(r"<!--.*?-->", "", clean_str)
            # Remove img tags
            clean_str = re.sub(r"<img[^>]*>", "", clean_str)
            # Replace HTML tags with space
            clean_str = re.sub(r"<[^>]+>", " ", clean_str)
            # Replace HTML entities
            clean_str = re.sub(r"&\S*?;", " ", clean_str)
            # Remove email addresses
            clean_str = re.sub(r"\b[\w.-]+@[\w.-]+\.\w{2,}\b", "", clean_str)
            # Remove reporter names
            clean_str = re.sub(r"[가-힣]{2,4}(\.|)기자", "", clean_str)
            # Remove non-BMP characters
            clean_str = re.sub(r"[^\u0000-\uFFFF]", "", clean_str)
            # Remove phone numbers
            clean_str = re.sub(r"\d{2,3}-\d{4}-\d{4}", "", clean_str)
            # Remove Chinese characters
            clean_str = re.sub(r"[\u4e00-\u9fff]+", "", clean_str)
            # Remove � characters
            clean_str = re.sub(r"�", "", clean_str)
            # Remove weird whitespace characters
            clean_str = re.sub(r"[\u00A0\u200B\u200C\u200D\u200E\u200F\u202F\u205F\u2060\uFEFF]", "", clean_str)
        except Exception as e:
            print(f"Error: {e}")
        return clean_str

    def get_clean_content_string(self, input_str: str) -> str:
        return self.remove_line_break(self.remove_quote(self.replace_regex(input_str))).strip()

    def replace_content_regex(self, input_str: str) -> str:
        clean_str = input_str
        try:
            if re.search(r"[\u4e00-\u9fff]", input_str):
                return clean_str
            
            # Replace script and style tags
            clean_str = re.sub(r"(?s)<script.*?</script>|<style.*?</style>", "", clean_str)
            # Remove HTML comments
            clean_str = re.sub(r"<!--.*?-->", "", clean_str)
            # Remove img tags
            clean_str = re.sub(r"<img[^>]*>", "", clean_str)
            # Replace HTML tags with space
            clean_str = re.sub(r"<[^>]+>", " ", clean_str)
            # Replace HTML entities
            clean_str = re.sub(r"&\S*?;", " ", clean_str)
            # Remove email addresses
            clean_str = re.sub(r"\b[\w.-]+@[\w.-]+\.\w{2,}\b", "", clean_str)
            # Remove reporter names
            clean_str = re.sub(r"[가-힣]{2,4}(\.|)기자", "", clean_str)
            # Remove non-BMP characters
            clean_str = re.sub(r"[^\u0000-\uFFFF]", "", clean_str)
            # Remove phone numbers
            clean_str = re.sub(r"\d{2,3}-\d{4}-\d{4}", "", clean_str)
            # Remove � characters
            clean_str = re.sub(r"�", "", clean_str)
            # Remove weird whitespace characters
            clean_str = re.sub(r"[\u00A0\u200B\u200C\u200D\u200E\u200F\u202F\u205F\u2060\uFEFF]", "", clean_str)
        except Exception as e:
            print(f"Error: {e}")
        return clean_str

    def remove_line_break(self,input_str: str) -> str:
        # 줄바꿈, br 태그, 공백 제거
        return re.sub(r"(\r\n|\n|\r|<br>|</br>)", " ", input_str).replace("\s+", " ").strip()

    def remove_quote(self,input_str: str) -> str:
        # 인용구랑 백슬래시 제거
        return re.sub(r"[\\\"']", "", input_str)
    
    def save_colct_log(self, exc_obj, tb, channel_cd, channel_nm, error_flag=0):
        ip = ''
        try:
            if error_flag == 1:
                err_lc = tb
                err_dtl = f'{exc_obj}'
            else:
                err_lc = self.get_error_location(tb)
                err_dtl = exc_obj.args[0] if exc_obj.args and len(exc_obj.args) > 0 else ''

            if '통신 차단 :' in err_dtl:
                ip = self.get_ip(err_dtl.replace('통신 차단 :', ''))
                err_dtl = f"통신 오류, IP 차단 및  방화벽(outbound) 확인 필요 대상 ip 정보 : {ip}"
            
            data = {
                "errDtl": err_dtl,
                "errLc": err_lc,
                "chnnlCd": channel_cd, 
                "chnnlNm": channel_nm,
                "errcnt": 1, 
                "newIp": ip if ip else None
            }
            self.api.saveLog(data)
        except Exception as e: self.logger.error(f'error message 생성 중 에러 >> {e}')

    def get_error_location(self, tb):
        err_lc = ''
        try:
            lineno = tb.tb_lineno  # 예외 발생 라인 번호
            f = tb.tb_frame
            filename = f.f_code.co_filename  # 예외 발생 함수명
            linecache.checkcache(filename)
            # line = linecache.getline(filename, lineno, f.f_globals)  # 예외 발생 라인 정보
            err_lc = f'EXCEPTION IN {filename} (LINE {lineno})'
        except Exception as e: self.logger.error(f'error 발생 라인 생성 중 에러 >> {e}')
        return err_lc
    
    def get_ip(self, url):
        ip = ''
        try:
            domain = urlparse(url.strip()).netloc
            ip = socket.gethostbyname(domain)
        except Exception as e: self.logger.error(f'ip 출력 중 에러 >> {e}')
        return ip

    def erase_timezone_info(self, timestamp):
        result = timestamp
        try:
            dt = datetime.fromisoformat(timestamp) # 문자열을 datetime 객체로 변환
            dt_without_tz = dt.replace(tzinfo=None) # 타임존 정보 제거
            formatted_timestamp = dt_without_tz.strftime('%Y-%m-%d %H:%M:%S') # 원하는 형식으로 변환
        except Exception as e: self.logger.error(f'타임존 제거 중 에러')

        return formatted_timestamp

    def parse_date(self, date_string, channel_name):
        result = ''
        date_patterns = [
            r"\d{1,2} [A-Za-z]{3,} \d{4}",
            r"\d{1,2} [A-Za-z]{3} \d{4}",
            r"\d{4}-\d{2}-\d{2}",
            r"\d{1,2}\.\d{1,2}\.\d{4}",
            r"\d{4}/\d{2}/\d{2}",
            r"\d{4}年\d{1,2}月\d{1,2}日",
            r"[A-Za-z]{3}, \d{2}/\d{2}/\d{4}",
            r"\d{1,2} [A-Za-z]+,? \d{4}",
            r"\d{1,2}/\d{1,2}/\d{4}",
            r"[A-Za-z]+ \d{1,2}, \d{4}",
            r"\d{1,2} (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC) \d{4}",
            r"\d{1,2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}",
            r"\d{2}/\d{2}/\d{4}",
            r"Last updated: \d{1,2} [A-Za-z]+ \d{4}",
            r"Waarschuwing \| \d{2}-\d{2}-\d{4}",
            r"\d{4}\.\d{2}\.\d{2}",
            r"[^\W\d_]+\s\d{1,2}\s[^\W\d_]+\s\d{4}", 
            r"\d{2}/\d{2}/\d{4}",
            r"\d{1,2}-\d{1,2}-\d{4}",
            r"[A-Za-z]{3}, \d{2}/\d{2}/\d{4} - Current",
            r"[A-Za-z ]+ \| \d{4}-\d{2}-\d{2}",
            r"\d{2}-\d{2} \d{4}",
            r"[A-Za-z]+ \d{4}",
            r"[A-Za-z]+ \d{4}"
        ]

        for pattern in date_patterns:
            match = re.search(pattern, date_string, re.IGNORECASE)
            if match:
                extracted_date = match.group(0)
                for key, fmt in DATE_PATTERNS.items():
                    try:
                        current_fmt = fmt
                        if channel_name in ("AFSCA - 개별", "Safety Gate - 개별", "TGA - 개별"):
                            current_fmt = "%d/%m/%Y"
                        elif channel_name == "CFS - 개별":
                            current_fmt = "%d.%m.%Y"
                        elif channel_name == 'RASFF - 개별':
                            current_fmt = "%d-%m-%Y"

                        try:
                            dt = datetime.strptime(extracted_date, current_fmt)
                            return datetime.strftime(dt, '%Y-%m-%d')
                        except ValueError:
                            try:
                                dt = parser.parse(extracted_date)
                                return datetime.strftime(dt, '%Y-%m-%d')
                            except ValueError:
                                continue
                    except (ValueError, TypeError):  # Handle parsing errors
                        pass

        try:
            return self.parsed_str_to_date(result)
        except Exception:
            pass

        return result


    def parsed_str_to_date(self, time_str):
        current_date = datetime.now()

        try:
            if "시간" in time_str:
                hours = int(re.sub(r"\D", "", time_str))
                result = current_date - timedelta(hours=hours)
            elif "분" in time_str:
                minutes = int(re.sub(r"\D", "", time_str))
                result = current_date - timedelta(minutes=minutes)
            elif "일" in time_str:
                days = int(re.sub(r"\D", "", time_str))
                result = current_date - timedelta(days=days)
            elif "오늘" in time_str:
                result = current_date
            elif "어제" in time_str:
                result = current_date - timedelta(days=1)
            elif "방금" in time_str:
                result = current_date
            elif "주" in time_str:
                weeks = int(re.sub(r"\D", "", time_str))
                result = current_date - timedelta(weeks=weeks)
            elif "개월" in time_str:
                months = int(re.sub(r"\D", "", time_str))
                result = current_date - timedelta(days=months*30) #approximate
            elif "년" in time_str:
                years = int(re.sub(r"\D", "", time_str))
                result = current_date - timedelta(days=years*365) #approximate
            else:
                time_match = re.search(r"\d{2}:\d{2}", time_str)
                if time_match:
                    time = time_match.group(0)
                    date_str = current_date.strftime("%Y-%m-%d")
                    dt_str = f"{date_str} {time}:00"
                    result = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                else:
                    date_match = re.search(r"\d{4}\.\d{2}\.\d{2}", time_str)
                    if date_match:
                        date = date_match.group(0).replace(".", "-")
                        if "오후" in time_str:
                            result = datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S") + timedelta(hours=12)
                        elif "오전" in time_str:
                            result = datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S")
                        else:
                            result = datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S")
                    else:
                        raise ValueError("Invalid time string format")
            return result
        except (ValueError,TypeError):
            return None

    def insert_data(self, colct_data):
        result = 1
        org_data = { 'idx': 'N/A', 'chnnlCd': 0, 'chnnlNm': 'N/A', 'wrtDt': '', 'item': 'N/A', 'brand': 'N/A', 'prdtNm': 'N/A', 'prdtDtlCtn': 'N/A',
                     'prdtDtlCtn2': 'N/A', 'mdlNm': 'N/A', 'mdlNo': 'N/A', 'brcd': 'N/A', 'cnsmExp': 'N/A', 'lotNo': 'N/A', 'wght': 'N/A', 'prdtSize': 'N/A',
                     'prdtImgFlNm': 'N/A','prdtImgFlPath': 'N/A', 'hrmflCuz': 'N/A', 'hrmflCuz2': 'N/A', 'hrmflCuz3': 'N/A', 'plor': 'N/A', 'recallNtn': 'N/A',
                     'bsnmNm': 'N/A', 'ntslPerd': 'N/A', 'ntslCrst': 'N/A', 'acdntYn': 'N/A', 'flwActn': 'N/A', 'flwActn2': 'N/A', 'prdtDtlPgUrl': 'N/A',
                     'recallSrce': 'N/A', 'atchFlNm': 'N/A', 'atchFlPath': 'N/A','recallNo': 'N/A', 'recallBzenty': 'N/A', 'mnfctrBzenty': 'N/A',
                     'distbBzenty': 'N/A', 'capture': 'N/A', 'regDt': ''}
        try:
            data_length_limit = {
                'item': 300,
                'brand': 300,
                'prdtNm': 1000,
                'prdtDtlCtn2': 500,
                'mdlNm': 300,
                'mdlNo': 2000,
                'brcd': 300,
                'cnsmExp': 300,
                'lotNo': 300,
                'wght': 300,
                'prdtSize': 300,
                'prdtImgFlNm': 2000,
                'prdtImgFlPath': 2000,
                'plor': 300,
                'recallNtn': 300,
                'bsnmNm': 2000,
                'ntslPerd': 300,
                'ntslCrst': 4000,
                'acdntYn': 300,
                'flwActn': 2000,
                'flwActn2': 4000,
                'atchFlNm': 2000,
                'atchFlPath': 2000,
                'recallNo': 500,
                'recallBzenty': 1000,
                'mnfctrBzenty': 300,
                'distbBzenty': 300,
            }
            truncate_data = colct_data
            for key, data_length in data_length_limit.items():
                if truncate_data.get(key):
                    truncate_data[key] = self.truncate_utf8(str(truncate_data[key]), data_length)

            # colct_data 값이 존재하면 org_data에 바꿔넣기
            for key in truncate_data:
                if key in org_data:
                    org_data[key] = colct_data[key]
        
            req_data = json.dumps(org_data)
            result =  self.api.insertData2Depth(req_data)   
        except Exception as e:
            self.logger.error(f'{e}')
        return result

    def truncate_utf8(self, text: str, max_bytes: int) -> str:
        if not text:
            return text

        encoded = text.encode('utf-8')
        if len(encoded) <= max_bytes:
            return text

        ellipsis = "..."
        ellipsis_bytes = len(ellipsis.encode('utf-8'))

        truncated = encoded[: max_bytes - ellipsis_bytes]

        attempts = 10  
        while attempts > 0:
            try:
                decoded = truncated.decode('utf-8')
                break
            except UnicodeDecodeError:
                truncated = truncated[:-1]
                attempts -= 1

        return decoded + ellipsis
    
    def reduce_atchl(self, pdf_path, max_size_mb=1):
        try:
            reader = PdfReader(pdf_path)
            writer = PdfWriter()
            for page in reader.pages:
                writer.add_page(page)

            for page in writer.pages:
                page.compress_content_streams()
                for img in page.images:
                    img.replace(img.image, quality=5)
            
            writer.add_metadata(reader.metadata)

            with open(pdf_path, 'wb') as fp:
                writer.write(fp)
        except Exception as e:
            self.logger.error(f'파일 용량 축소 실패: {pdf_path}')

        file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)  # 파일 크기(MB) 계산
    
        if file_size_mb > max_size_mb:  # 지정된 크기보다 크면 압축
            zip_path = self.compress_pdf_to_zip(pdf_path)
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
                self.logger.info(f'파일 삭제 완료: {pdf_path}')
            return zip_path
        return pdf_path

    def compress_pdf_to_zip(self, pdf_path):
        zip_path = os.path.splitext(pdf_path)[0] + ".zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(pdf_path, arcname=pdf_path.split("/")[-1])  # 원본 파일명 유지
        return zip_path

    # def capture(self, html_content):
    #     try:
    #         capture_path = 'test.jpeg'

    #         # Chrome 옵션 설정 (창 최대화)
    #         chrome_options = Options()
    #         chrome_options.add_argument("--headless")  # UI 없이 실행
    #         chrome_options.add_argument("--disable-gpu")
    #         chrome_options.add_argument("--window-size=1920,1080")

    #         # 캐싱된 리소스를 사용하도록 Chrome 설정
    #         chrome_options.add_argument("--disable-web-security")
    #         chrome_options.add_argument("--allow-running-insecure-content")
    #         chrome_options.add_argument("--disk-cache-size=500000000")  # 500MB 캐시

    #         chrome_options.add_argument("User-Agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36")
    #         chrome_options.add_argument("Accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
    #         chrome_options.add_argument("Accept-Encoding=gzip, deflate, br, zstd")
    #         chrome_options.add_argument("Accept-Language=ko-KR,ko;q=0.9")
    #         chrome_options.add_argument("Cookie=mf_user=4356e22a84c70c01b5174881c95869c6|; OptanonAlertBoxClosed=2025-01-31T09:14:41.747Z; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Jan+31+2025+18%3A14%3A41+GMT%2B0900+(%ED%95%9C%EA%B5%AD+%ED%91%9C%EC%A4%80%EC%8B%9C)&version=202501.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=f1cab516-6334-478b-8abb-313ce36dd126&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A0%2CC0005%3A0%2CC0004%3A0%2CC0002%3A0&hosts=H54%3A1%2CH66%3A0%2CH123%3A0%2CH16%3A0%2CH1%3A0%2CH61%3A0%2CH5%3A0%2CH110%3A0%2CH125%3A0%2CH11%3A0%2CH111%3A0%2CH148%3A0%2CH19%3A0%2CH154%3A0&genVendors=&AwaitingReconsent=false&intType=3; mf_d2891043-3559-44fe-8275-3c4d7d40173d=7beb7f78c623ae25b4e9c87f09990982|01315040819b8d6a29e0273e3f5b1e4f0337e1d7.4426533033.1738308230543$013153079118d20152e8f7f36cbd74296c692027.2096531278.1738308233108$01313944346bbafc3cbb60375e182b903b97ab3c.2096531278.1738314879546|1738314994090||1|||s7sddYZvWUSIx6ylU1Y1MA|0|18.21|73.30405")
            

    #         # WebDriver 실행
    #         # driver = webdriver.Chrome(options=chrome_options)
    #         service = Service(executable_path="/home/kca/chromedriver-linux64/chromedriver")
    #         driver = webdriver.Chrome(service=service, options=chrome_options)


    #         # 빈 페이지 열기
    #         driver.get("about:blank")

    #         # HTML을 동적으로 삽입
    #         driver.execute_script("document.write(arguments[0]);", html_content)

    #         # 페이지 로딩 대기
    #         time.sleep(2)

    #         # 페이지 전체 높이 가져오기
    #         total_height = driver.execute_script("return document.body.scrollHeight")
    #         driver.set_window_size(1920, total_height)  # 창 크기를 전체 페이지 높이로 설정

    #         # 스크린샷 저장
    #         driver.save_screenshot(capture_path)

    #         # WebDriver 종료
    #         driver.quit()
    #     except Exception as e:
    #         self.logger.error(f'화면 캡쳐 중 에러 >> {e}')
