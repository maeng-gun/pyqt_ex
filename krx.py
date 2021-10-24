import requests as req
import pandas as pd
import functools
import time


def krx_req_post(site, params):
    """
    krx 사이트에서 POST 요청을 통해 얻은 JSON 파일을 데이터프레임으로 반환

    Parameters
    ----------
    site : str
        'data' - data.krx.co.kr 사이트 인 경우
        'open' - open.krx.co.kr 사이트인 경우

    params : dict
        POST 요청 시 세부정보를 담은 딕셔너리

    Returns
    -------
    pd.DataFrame (데이터프레임 형식)
    """

    url = {'data': 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd',
           'open': 'http://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx'}
    res = req.post(url[site], data=params).json()
    key = list(res.keys())[0]
    data = pd.DataFrame(res[key])
    return data


def krx_req_get(otp_params):
    """
    KRX 사이트(open.krx.co.kr)에서 GET요청을 보내 OTP 코드를 얻는 함수

    Parameters
    ----------
    otp_params : dict
        GET 요청시 쿼리스트링 파라미터들을 담은 딕셔너리

    Returns
    -------
    str (문자열 형식)

    """
    url = 'http://open.krx.co.kr/contents/COM/GenerateOTP.jspx'
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/89.0.4389.82 Safari/537.36 "}
    otp_code = req.get(url, params=otp_params, headers=headers).text
    return otp_code


def get_holidays_from_krx(year):
    unix_time = str(int(round(time.time() * 1000)))
    otp_params = {'bld': 'MKD/01/0110/01100305/mkd01100305_01',
                  'name': 'form',
                  '_': unix_time}
    otp_code = krx_req_get(otp_params)

    # 휴장일 표 얻기
    view_params = {'search_bas_yy': str(year),
                   'gridTp': 'KRX',
                   'pagePath': 'contents/MKD/01/0110/01100305/MKD01100305.jsp',
                   'code': otp_code}
    holidays = krx_req_post('open', view_params)['calnd_dd']
    return holidays


def get_krx_sector(date):
    """
    KRX정보데이터시스템(data.krx.co.kr) 기본통계 - 주식 - 세부안내 - [12025]업종분류현황
    """
    print('KRX 산업분류기준 스크래핑 시작', end='...')
    sector_match = {'음식료·담배': '음식료품', '섬유·의류': '섬유의복', '종이·목재': '종이목재',
                    '출판·매체복제': '종이목재', '제약': '의약품', '비금속': '비금속광물', '금속': '철강금속',
                    '기계·장비': '기계', '일반전기전자': '전기전자', '의료·정밀기기': '의료정밀',
                    '운송장비·부품': '운수장비', '기타제조': '기타제조업', '전기·가스·수도': '전기가스업',
                    '건설': '건설업', '유통': '유통업', '숙박·음식': '서비스업', '운송': '운수창고업',
                    '금융': '기타금융', '기타서비스': '서비스업', '오락·문화': '서비스업', '통신서비스': '통신업',
                    '방송서비스': '서비스업', '인터넷': '서비스업', '디지털컨텐츠': '서비스업', '소프트웨어': '서비스업',
                    '컴퓨터서비스': '서비스업', '통신장비': '전기전자', '정보기기': '전기전자', '반도체': '전기전자',
                    'IT부품': '전기전자'}

    def read(mkt):
        params = {'bld': "dbms/MDC/STAT/standard/MDCSTAT03901",
                  'mktId': mkt,
                  'trdDd': date}
        data = krx_req_post('data', params)
        col = ['ISU_SRT_CD', 'ISU_ABBRV', 'MKT_TP_NM', 'IDX_IND_NM', 'MKTCAP']
        col_new = ['sym_cd', 'sym_nm', 'mkt_cd', 'sec_krx', 'mkt_cap']
        data = data[col].set_axis(col_new, axis=1)
        data['sec_krx'] = data.sec_krx.replace(sector_match)
        return data.replace(',', '', regex=True).astype({'mkt_cap': 'int64'})

    df1 = read('STK')
    df2 = read('KSQ')
    print('종료!')
    return pd.concat([df1, df2]).reset_index(drop=True)


def get_symbols_in_index(date):
    """
    KRX정보데이터시스템(data.krx.co.kr) 기본통계 - 주식 - 세부안내 - [11006]지수구성종목
    """
    print('코스피200/코스닥우량 지수 구성종목 정보 스크래핑 시작', end='...')
    col = ['indIdx', 'indIdx2', 'tboxindIdx_finder_equidx0_2', 'codeNmindIdx_finder_equidx0_2']
    idx_list = krx_req_post('data', {'bld': "dbms/comm/finder/finder_equidx", 'mktsel': '1'})
    idx = idx_list[['full_code', 'short_code', 'codeName', 'codeName']].set_axis(col, axis=1)

    index_list = ['코스피 200', '코스닥 우량기업부']
    df_list = []
    col = ['ISU_SRT_CD', 'ISU_ABBRV']
    col_new = ['sym_cd', 'sym_nm']

    for index in index_list:
        try:
            params = idx[idx.tboxindIdx_finder_equidx0_2 == index].iloc[0].to_dict()
            params.update(bld="dbms/MDC/STAT/standard/MDCSTAT00601",
                          trdDd=date)
            df = krx_req_post('data', params)[col].set_axis(col_new, axis=1)
            df[index.replace(' ', '')] = True
            df_list.append(df)
        except Exception:
            cols = col_new + [index.replace(' ', '')]
            df = pd.DataFrame(columns=cols)
            df_list.append(df)

    data = functools.reduce(lambda left, right: pd.merge(left, right, how='outer', on=col_new), df_list)
    data.columns = col_new + ['ksp_200', 'ksq_bc']
    print('종료!')
    return data.fillna(False)


def get_investment_company(date):
    """
    KRX정보데이터시스템(data.krx.co.kr) 기본통계 - 주식 - 기타증권 - [12014],[12015],[12016]
    """
    print('KRX 투자회사(뮤추얼펀드/선박투자/인프라투자) 목록 스크래핑 시작', end='...')
    bld = {"12014": "dbms/MDC/STAT/standard/MDCSTAT02901",
           "12015": "dbms/MDC/STAT/standard/MDCSTAT02801",
           "12016": "dbms/MDC/STAT/standard/MDCSTAT03001"}

    def read(num):
        params = {'bld': bld[num],
                  'trdDd': date}
        data = krx_req_post('data', params)
        col = ['ISU_SRT_CD', 'ISU_NM']
        col_new = ['sym_cd', 'sym_nm']
        data = data[col].set_axis(col_new, axis=1)
        data['inv_com'] = True
        return data

    df1 = read('12014')
    df2 = read('12015')
    df3 = read('12016')
    result = pd.concat([df1, df2, df3]).reset_index(drop=True)
    print('종료!')
    return result.fillna(False)


def get_symbols_data(workdays):
    df = pd.DataFrame()

    for date in workdays:
        print(f'@ {date} KRX 종목 정보 스크래핑 시작')
        df1 = get_krx_sector(date)
        df2 = get_symbols_in_index(date)
        df3 = get_investment_company(date)
        df_list = [df1, df2, df3]
        data = functools.reduce(lambda left, right:
                                pd.merge(left, right, how='left', on=['sym_cd', 'sym_nm']), df_list)
        data[['ksp_200', 'ksq_bc', 'inv_com']] = \
            data[['ksp_200', 'ksq_bc', 'inv_com']].fillna(False)
        data['sym_obj'] = (data.sym_cd.str[-1] == '0') & (data.sym_cd.str[0] != '9') & \
                          (~data.inv_com) & (~data.sym_nm.str.contains('스팩'))
        data.insert(0, 'base_dt', date)
        data.insert(0, 'base_mt', (pd.DatetimeIndex(data.base_dt).to_period('M') + 1).strftime('%Y-%m'))
        df = df.append(data, ignore_index=True)

    return df
