from typing import List, Optional, Union
import requests
from datetime import datetime
from urllib import parse


def get_meal_form_data(
    code: str,
    to_date: datetime,
    end_date: datetime,
) -> bytes:
    return bytes(
        "rows=100&infId=OPEN17320190722180924242823&infSeq=1&downloadType=J&"
        + f"ATPT_OFCDC_SC_CODE={code}&SCHUL_NM=&MMEAL_SC_NM="
        + f"&MLSV_YMD={to_date.strftime('%Y%m%d')}&MLSV_YMD={end_date.strftime('%Y%m%d')}",
        "utf-8",
    )



def download_meal_info():
    NEIS_MEAL_DOWNLOAD_API_URL = "https://open.neis.go.kr/portal/data/sheet/downloadSheetData.do"
    NEIS_API_HEADERS = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        "Host": "open.neis.go.kr",
        "Origin": "https://open.neis.go.kr",
        "Referer": "https://open.neis.go.kr/portal/data/service/selectServicePage.do?page=1&rows=10&sortColumn=&sortDirection=&infId=OPEN17320190722180924242823&infSeq=1",
    }

    NEIS_API_FORM_DATA = get_meal_form_data("G10", datetime.now(), datetime.now())

    r = requests.post(
        NEIS_MEAL_DOWNLOAD_API_URL,
        data=NEIS_API_FORM_DATA,
        headers=NEIS_API_HEADERS,
    )

    if r.status_code == 200:
        return r.json()



if __name__ == "__main__":
    print(download_meal_info())
    print("end")
