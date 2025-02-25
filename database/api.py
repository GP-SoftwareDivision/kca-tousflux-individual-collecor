import requests
import json

## local
API_SERVER_IP = 'http://34.22.91.88:9100/api/overSeasRecall'
FILE_SERVER_IP = 'http://34.22.91.88:9100/api/file'

## server
# API_SERVER_IP = 'http://210.90.35.209:9100/api/overSeasRecall'
# FILE_SERVER_IP = 'http://192.168.6.146:9100/api/file'

class API():
    def __init__(self, logger):
        self.logger = logger

    def test(self):
        result = ''
        try:
            url = API_SERVER_IP + '/getSchedule'
            res = requests.get(url=url)
            if res.status_code == 200:
                self.logger.info('Success Test')
        except Exception as e:
            result = None
            self.logger.error(f'test 메소드 발생 중 에러 : {e}')
        return result

    def getIndividualSchedule(self):
        result = {"idx": -1, "chnnlCd": -1, "chnnlNm": '',
                  "colctBgngDt": "", "colctEndDt": "", "url": "", "jobStat": ""}        
        try:
            url = API_SERVER_IP + '/getIndividualSchedule'
            res = requests.get(url=url)
            if res.status_code == 200 and res.text != '': 
                result = json.loads(res.text)

        except Exception as e:
            self.logger.error(f'스케쥴 조회 중 에러 >> {e}')
        
        return result
    
    def uploadNas(self, type, files, data):
        res = None
        try:
            if type == 'file': url = FILE_SERVER_IP + '/uploadNas'
            # else: url = API_SERVER_IP + '/uploadNas'
            files = files
            data = data
            res = requests.post(url=url, files=files, data=data)
            # if res.status_code != 200: res = None
            # elif res.status_code == 409: res = 
        except Exception as e:
            self.logger.error(f'첨부파일/이미지 업로드 중 에러  : {e}')
        return res

    def insertData2Depth(self, req_data):
        result = 1
        try:
            url = API_SERVER_IP + '/save2DepthCrawlData'
            headers = {
                'Content-Type':'application/json',
            }
            res = requests.post(url=url, data=req_data, headers=headers)
            if res.status_code == 200: result = int(res.text)

        except Exception as e:
            self.logger.error(f'데이터 적재 중 에러 >> {e}')
            self.logger.error(f'{req_data}')
        
        return result
    
    def updateStartSchedule(self, idx, cntanr_nm):
        data = {
            "idx": idx, 
            "cntanrNm": cntanr_nm
        }
        result = 0
        try:
            url = API_SERVER_IP + '/updateSchedule'
            headers = {
                'Content-Type':'application/json',
            }
            res = requests.put(url=url, data=json.dumps(data), headers=headers)
            if res.status_code == 200: result = 1

        except Exception as e:
            result = -1
            self.logger.error(f'스케줄 시작 업데이트 에러 >> {e}')
            self.logger.error(f'{idx}')
        
        return result
    
    def updateEndSchedule(self, idx, job_stats, colct_cnt):
        data = {
            "idx": idx, 
            "jobStat": job_stats,
            "colctCnt": colct_cnt
        }
        result = 0
        try:
            url = API_SERVER_IP + '/updateEndSchedule'
            headers = {
                'Content-Type':'application/json',
            }
            res = requests.put(url=url, data=json.dumps(data), headers=headers)
            if res.status_code == 200: result = 1

        except Exception as e:
            result = -1
            self.logger.error(f'스케줄 끝 업데이트 에러 >> {e}')
            self.logger.error(f'{idx}')
        
        return result
    
    def saveLog(self, data):
        result = 0
        try:
            url = API_SERVER_IP + '/saveLog'
            headers = {
                'Content-Type':'application/json',
            }
            res = requests.post(url=url, data=json.dumps(data), headers=headers)
            if res.status_code == 200: result = 1

        except Exception as e:
            result = -1
            self.logger.error(f'로그 저장 에러 >> {e}')
            self.logger.error(f'{data}')
        
        return result

