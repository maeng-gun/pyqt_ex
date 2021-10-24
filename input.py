import pandas as pd
import util as ut
import input.krx as krx
import input.fn as fn
import input.raw_data as rt
import input.derive as derive
import input.factor as factor

# HDF5 DB 접근 객체 생성
raw = ut.DbTools('inp_raw')
pre = ut.DbTools('inp_preprocess')


# 원천데이터/파생데이터 관련 함수 (get, update, add)
def get_initial_raw_data(date_range):
    """
    원천 데이터의 최초 적재 및 전처리를 통한 파생 데이터 생성 함수.
    6개 원천데이터 테이블(holidays, month_end, daily, quarterly, annual, returns)와
    2개의 파생데이터 테이블(quarterly_prep, annual_prep)을 생성함

    Parameters
    ----------
    date_range : list [str, str]
        MP적용월 기준으로 적재하려는 기간을 ['시작연월(yyyy-mm)','종료연월(yyyy-mm)'] 형태의 리스트로 입력
    """

    # 1. 2000년 이후 holidays 테이블 생성
    #  - 실제 KRX에서는 2009년 이후 휴장일만 정확히 제공하고 있음
    #  - 2000년~2008년 휴장일은 별도 조사후 수동으로 추가했음...

    df_h = rt.get_holidays_table(2009)
    # df_h_2 = 수동조사한 휴장일
    # df_h = df_h_2.append(df_h)
    raw.put('holidays', df_h)

    # 2. month_end 테이블 생성
    df_me, symbols = rt.get_month_end_table(date_range)
    raw.put('month_end', df_me)

    # 3. daily 테이블 생성
    df_d = rt.get_daily_table(date_range, symbols)
    raw.put('daily', df_d)

    # 4. quarterly 테이블 생성
    df_q = rt.get_quarterly_table(date_range, symbols)
    raw.put('quarterly', df_q)

    # 5. quarterly_prv 테이블 생성
    df_q_prv = rt.get_quarterly_prv_table(date_range, symbols)
    raw.put('quarterly_prv', df_q_prv)

    # 6. annual 테이블 생성
    df_y = rt.get_annual_table(date_range, symbols)
    raw.put('annual', df_y)

    # 4. returns 테이블 생성
    df_r = rt.get_returns_table(date_range, symbols)
    raw.put('returns', df_r)

    # 5. quarterly_prep 테이블 생성 - 분기 데이터 전처리(파생 데이터 생성)
    df_q_prep = derive.get_quarterly_prep_table(df_q, df_q_prv)
    pre.put('quarterly_prep', df_q_prep)

    # 6. annual_prep 테이블 생성 - 연간 데이터 전처리(파생 데이터 생성)
    df_y_prep = derive.get_annual_prep_table(df_y, df_q, df_q_prv)
    pre.put('annual_prep', df_y_prep)

    # 7. filter_factors 테이블 생성 - 유니버스 선별기준 및 팩터 전처리
    df_uni_fac = factor.get_filter_factors_table(None)
    pre.put('filter_factors', df_uni_fac)


def update_raw_data(month=None):
    today = pd.Timestamp.today().strftime('%Y%m%d')
    year = today[:4]
    month = month if month else pd.Timestamp.today().strftime('%Y-%m')

    # 1. 휴장일 정보(매일)
    holidays_current_year = rt.get_holidays_table(year)
    raw.remove('holidays', f'h_day>="{year}"')
    raw.append('holidays', holidays_current_year)

    # 2. 월말, 일간, 분기(확정/잠정), 연간 및 파생데이터 테이블 업데이트(매월 초)
    last_update_month = raw.read('month_end', 'base_mt', start=-1).iloc[0]
    if last_update_month >= month:
        print(month+"까지 원천데이터 업데이트가 이미 완료됐습니다")
        pass

    else:
        # 2-1) month_end 테이블 추가
        month_end_updated, symbols = rt.get_month_end_table(month)
        symbols_saved = raw.read('month_end', ['sym_cd'], 'sym_obj = "True"').sym_cd.drop_duplicates()
        raw.append('month_end', month_end_updated)

        # 2-2) 새롭게 크롤링된 종목들을 유지종목과 신규종목으로 구분
        symbols_remain = symbols[symbols.isin(symbols_saved)]
        symbols_new = symbols[~symbols.isin(symbols_saved)]

        # 2-3) 유지종목(symbols_remain) : 최근 데이터만 업데이트

        #  a) 일간 : 지난달 한달치 (DB에서 삭제할꺼 없음)
        daily_updated = rt.get_daily_table(month, symbols_remain, offset=0)
        raw.append('daily', daily_updated)

        #  b) 분기_확정
        if month[5:] in ['01', '05', '07', '10']:
            if month[5:] == '05':
                month = month[:5] + '04'
            quarterly_updated = rt.get_quarterly_table(month, symbols_remain, offset=0)
            raw.append('quarterly', quarterly_updated)

        #  c) 분기_잠정
        if month[5:] not in ['01', '07', '10']:
            quarterly_prv_updated = rt.get_quarterly_prv_table(month, symbols_remain, offset=0)
            update_q = ut.date_offset(month, True, 'Q')[0]
            raw.remove('quarterly_prv', f'sym_cd in {symbols_remain.to_list()} & base_dt >= "{update_q}"')
            raw.append('quarterly_prv', quarterly_prv_updated)

        #  d) 연간
        if month[5:] == '05':
            annual_updated = rt.get_annual_table(month, symbols_remain, offset=0)
            raw.append('annual', annual_updated)

        # 2-4) 신규종목 : 모든 기간 업데이트
        if symbols_new.shape[0] > 0:

            table_dict = {'daily': rt.get_daily_table, 'quarterly': rt.get_quarterly_table,
                          'quarterly_prv': rt.get_quarterly_prv_table, 'annual': rt.get_annual_table}
            updated_new = {}

            for table in table_dict.keys():
                start = raw.read(table, 'base_dt', stop=1).iloc[0]
                end = raw.read(table, 'base_dt', start=-1).iloc[0]
                date_range = [start, end]
                updated_new[table] = table_dict[table](date_range, symbols_new, offset=0, last=False)
                raw.append(table, updated_new[table])

        # 2-5) 파생데이터 업데이트

        #   a) 분기간 : 전월말 기준으로 전분기 자료(DB에서 삭제), 5월의 경우 전분기(1Q)와 전전분기(4Q)를 다 가져와야함
        update_q_prv = ut.date_offset(month, True, 'Q', 0)[0]
        offset_det = 3 if month[5:] == '04' else 2
        update_q_det = ut.date_offset(month, False, 'Q', offset_det)[0]
        start = ut.date_offset(update_q_det, False, 'Q', '1Y')[0]
        df_q = raw.read('quarterly', query=f'base_dt >= "{start}"').reset_index(drop=True)
        df_q_prv = raw.read('quarterly_prv', query=f'base_dt >= "{start}"').reset_index(drop=True)

        df_q_prep = derive.get_quarterly_prep_table(df_q, df_q_prv)
        if month in ['01', '05', '07', '10']:
            start = update_q_det[:6]
        else:
            start = update_q_prv[:6]
        df_q_prep = df_q_prep[(df_q_prep.quarter >= start)].copy()
        pre.remove('quarterly_prep', f'quarter >= "{start}"')
        pre.append('quarterly_prep', df_q_prep)

        #   b) 연간 : 2~5월(전월말이 1~4월)의 경우에 전년말 자료(DB에서 삭제)
        if '02' <= month[5:] <= '05':
            update_y_1 = ut.date_offset(month, True, 'Y', 0)[0]
            update_y_2 = ut.date_offset(month, True, 'Y', 1)[0]
            update_y_3 = ut.date_offset(month, True, 'Y', 2)[0]
            df_q = raw.read('quarterly', query=f'base_dt = "{update_y_1}" | base_dt = "{update_y_2}"')\
                .reset_index(drop=True)
            df_q_prv = raw.read('quarterly_prv', query=f'base_dt = "{update_y_1}" | base_dt = "{update_y_2}"') \
                .reset_index(drop=True)
            df_y = raw.read('annual', query=f'base_dt >= "{update_y_3}" & base_dt <= "{update_y_1}"')\
                .reset_index(drop=True)

            df_y_prep = derive.get_annual_prep_table(df_y, df_q, df_q_prv)
            df_y_prep = df_y_prep[df_y_prep.year == update_y_1[:4]].copy()
            pre.remove('annual_prep', f'quarter >= "{update_y_1[:4]}"')
            pre.append('annual_prep', df_y_prep)

        # 2-6) 유니버스 선별기준 및 팩터 테이블 업데이트
            df_uni_fac = factor.get_filter_factors_table(month)
            pre.append('filter_factors', df_uni_fac)

    # 3. 종목/지수 수익률 테이블 (월초 원천데이터 업데이트 후 매일)
    first_day = raw.read('returns', 'base_dt', stop=1).iloc[0]
    last_update_day = raw.read('returns', 'base_dt', start=-1).iloc[0]
    last_update_next_day = ut.all_workdays[ut.all_workdays.get_loc(last_update_day) + 1]
    last_workday = ut.all_workdays[ut.all_workdays.get_loc(today) - 1]

    if last_update_next_day.strftime('%Y%m%d') > last_workday.strftime('%Y%m%d'):
        print(last_update_day+"까지 일별수익률 업데이트가 이미 완료됐습니다")

    else:
        sym_saved_r = raw.read('returns', 'sym_cd').drop_duplicates()
        sym_saved_me = raw.read('month_end', ['sym_cd'], 'sym_obj = "True"').sym_cd.drop_duplicates()
        sym_saved = sym_saved_me[sym_saved_me.isin(sym_saved_r)]
        sym_new = sym_saved_me[~sym_saved_me.isin(sym_saved_r)]

        date_range_saved = [last_update_next_day.strftime('%Y%m%d'), last_workday.strftime('%Y%m%d')]
        ret = rt.get_returns_table(date_range_saved, sym_saved, 0)
        raw.append('returns', ret)

        if not sym_new.empty:
            date_range_new = [first_day, last_workday.strftime('%Y%m%d')]
            ret_new = rt.get_returns_table(date_range_new, sym_new, 0, False)
            raw.append('returns', ret_new)


def add_columns_to_raw_data():
    add_item_list = ut.get_table_info(add=True)
    if add_item_list.empty:
        print("추가할 컬럼이 없습니다.")
    else:
        for table in add_item_list.groupby('테이블명_물리'):
            if table[0] == 'month_end':
                month_end_saved = raw.read('month_end').reset_index(drop=True)
                df_dt_sym = month_end_saved[month_end_saved.sym_obj][['base_dt', 'sym_cd']].copy()
                month_end_new = fn.get_cross_section_data(df_dt_sym, add=True)
                raw.put('month_end', month_end_saved.merge(month_end_new, how='left', on=['base_dt', 'sym_cd']))
            else:
                ts_data_saved = raw.read(table[0])
                start = ts_data_saved.base_dt.iloc[0]
                end = ts_data_saved.base_dt.iloc[-1]
                date_range = [start, end]
                symbols = ts_data_saved.sym_cd.drop_duplicates()

                table_dict = {'daily': rt.get_daily_table, 'quarterly': rt.get_quarterly_table,
                              'quarterly_prv': rt.get_quarterly_prv_table, 'annual': rt.get_annual_table}
                ts_data_new = table_dict[table[0]](date_range, symbols, offset=0, last=False)
                ts_data_updated = ts_data_saved.merge(ts_data_new, how='left', on=['base_dt', 'sym_cd'])
                raw.put(table[0], ts_data_updated)

                if table[0] == 'quarterly':
                    quarterly_prep_new = derive.get_quarterly_prep_table(ts_data_new, None, add=True)
                    quarterly_prep_saved = pre.read('quarterly_prep').reset_index(drop=True)
                    quarterly_prep_added = quarterly_prep_saved.merge(quarterly_prep_new,
                                                                      how='left', on=['sym_cd', 'quarter'])
                    pre.put('quarterly_prep', quarterly_prep_added)
