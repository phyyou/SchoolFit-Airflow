from typing import List, Union
import httpx


def removesuffix(target: str, suffix: Union[str, List[str]]):
    if isinstance(suffix, list):
        for suf in suffix:
            if target.endswith(suf):
                return target[: -len(suf)]
    else:
        if target.endswith(suffix):
            return target[: -len(suffix)]


def get_meal_info():
    NEIS_KEY = ""
    NEIS_MEAL_API_URL = "https://open.neis.go.kr/hub/mealServiceDietInfo"
    NEIS_API_PARAMS = {"KEY": NEIS_KEY, "Type": "json", "pIndex": 1, "pSize": 1}

    school_info = {
        "ATPT_OFCDC_SC_CODE": "G10",
        "SD_SCHUL_CODE": "7430310",
        "MMEAL_SC_CODE": 1,
        "MLSV_YMD": "20220415",
    }

    NEIS_API_PARAMS.update(school_info)
    r = httpx.get(NEIS_MEAL_API_URL, params=NEIS_API_PARAMS)

    ntr_info = {}
    if r.is_success:
        meal_info = r.json()
        dish: List[str] = meal_info["mealServiceDietInfo"][1]["row"][0]["DDISH_NM"].split("<br/>")
        ntr_info_raw: List[str] = meal_info["mealServiceDietInfo"][1]["row"][0]["NTR_INFO"].split(
            "<br/>"
        )
        for ntr_info_item in ntr_info_raw:
            ntr_info.update(
                {
                    removesuffix(ntr_info_item.split(" : ")[0], ["(g)", "(mg)", "(R.E)"]): float(
                        ntr_info_item.split(" : ")[1]
                    )
                }
            )

        cal_info = float(
            removesuffix(meal_info["mealServiceDietInfo"][1]["row"][0]["CAL_INFO"], " Kcal")
        )

    return {"ntr_info": ntr_info, "cal_info": cal_info}


if __name__ == "__main__":
    print(get_meal_info())
    print("end")
