import pandas as pd
import util as ut
import krx
# import input.fn as fn

raw = ut.DbTools('inp_raw')


def get_holidays_table(start_year):
    print(f'{str(start_year)}년 이후 KRX 휴장일 정보 스크래핑 시작', end='...')

    this_year = pd.Timestamp.today().year
    holidays = pd.Series(dtype='str')

    for year in pd.period_range(start_year, this_year, freq='Y'):
        holidays_sr = krx.get_holidays_from_krx(year)
        holidays = holidays.append(holidays_sr)

    data = holidays.to_frame('h_day').reset_index(drop=True)
    print('종료!')

    return data


def get_month_end_table(date_range):

    # 1) kr x크롤링
    last_month_end_workdays = ut.workdays_offset(date_range, freq='ME').strftime('%Y%m%d')
    krx_month_end = krx.get_symbols_data(last_month_end_workdays)
    #
    # # 2) fn가이드 크롤링(cross-sectional)
    # df_dt_sym = krx_month_end[krx_month_end.sym_obj][['base_dt', 'sym_cd']].copy()
    # fn_month_end = fn.get_cross_section_data(df_dt_sym)

    # df_me = krx_month_end.merge(fn_month_end, how='left', on=['base_dt', 'sym_cd'])
    # symbols = df_dt_sym.sym_cd.drop_duplicates()

    return krx_month_end

#
# def get_symbols(date_range):
#     last_month_end_workdays = ut.workdays_offset(date_range, freq='ME').strftime('%Y%m%d')
#     start = last_month_end_workdays[0]
#     end = last_month_end_workdays[-1]
#     symbols = raw.read('month_end', ['sym_cd'],
#                        query=f'sym_obj = "True" & base_dt >= "{start}" & base_dt <= "{end}"')
#     return symbols.sym_cd.drop_duplicates()
#
#
# def get_daily_table(date_range, symbols=None, offset=None, add=False, last=True):
#
#     if isinstance(symbols, list):
#         symbols = pd.Series(symbols)
#
#     if not symbols.empty:
#         symbols = 'A' + symbols
#     else:
#         symbols = 'A' + get_symbols(date_range)
#
#     return fn.get_time_series_data(date_range, 'daily', symbols, offset=offset, add=add, last=last)
#
#
# def get_quarterly_table(date_range, symbols=None, offset=None, add=False, last=True):
#
#     if isinstance(symbols, list):
#         symbols = pd.Series(symbols)
#
#     if not symbols.empty:
#         symbols = 'A' + symbols
#     else:
#         symbols = 'A' + get_symbols(date_range)
#
#     return fn.get_time_series_data(date_range, 'quarterly', symbols, offset=offset, add=add, last=last)
#
#
# def get_quarterly_prv_table(date_range, symbols=None, offset=None, add=False, last=True):
#
#     if isinstance(symbols, list):
#         symbols = pd.Series(symbols)
#
#     if not symbols.empty:
#         symbols = 'A' + symbols
#     else:
#         symbols = 'A' + get_symbols(date_range)
#
#     return fn.get_time_series_data(date_range, 'quarterly_prv', symbols, offset=offset, add=add, last=last)
#
#
# def get_annual_table(date_range, symbols=None, offset=None, add=False, last=True):
#
#     if isinstance(symbols, list):
#         symbols = pd.Series(symbols)
#
#     if not symbols.empty:
#         symbols = 'A' + symbols
#     else:
#         symbols = 'A' + get_symbols(date_range)
#
#     return fn.get_time_series_data(date_range, 'annual', symbols, offset=offset, add=add, last=last)
#
#
# def get_returns_table(date_range, symbols=None, offset=None, index=True):
#
#     if isinstance(symbols, list):
#         symbols = pd.Series(symbols)
#
#     if not symbols.empty:
#         symbols = 'A' + symbols
#     else:
#         symbols = 'A' + get_symbols(date_range)
#
#     r_company = fn.get_time_series_data(date_range, 'returns', symbols, item='ret_stk', offset=offset)\
#         .rename(columns={'ret_stk': 'ret'})
#
#     # 공통사용 변수 지정 : 코스피 / 코스피200 / 코스닥 종목코드
#     if index:
#         sym_index = ['I.001', 'I.101', 'I.201']
#
#         r_index = fn.get_time_series_data(date_range, 'returns', sym_index, item='ret_idx', offset=offset)\
#             .rename(columns={'ret_idx': 'ret'})
#
#         return pd.concat([r_company, r_index]).sort_values(['base_dt', 'sym_cd'], ignore_index=True)
#
#     else:
#         return r_company.sort_values(['base_dt', 'sym_cd'], ignore_index=True)
