import pandas as pd
import os


# 데이터베이스 In / Out 관련

class DbTools:

    def __init__(self, file):
        path = os.path.dirname(__file__) + '/external_files/hdf_data/'
        self.store = pd.HDFStore(path + file + '.h5')
        self.close()

    def open(self):
        self.store.open()

    def close(self):
        self.store.close()

    @property
    def tables(self):
        self.open()
        table = self.store.keys()
        self.close()
        return table

    def put(self, table, data):
        self.open()
        self.store.put(table, data, 't', data_columns=True)
        self.close()

    def append(self, table, data):
        self.open()
        self.store.append(table, data, 't', data_columns=True)
        self.close()

    def remove(self, table, query=None):
        self.open()
        self.store.remove(table, where=query)
        self.close()

    def read(self, table, col=None, query=None, start=None, stop=None):
        self.store.open()
        data = self.store.select_column(table, col, start, stop) if isinstance(col, str) and not query \
            else self.store.select(table, query, start, stop, col)
        self.close()
        return data


# 영업일을 적용한 Time-Series Index 관련

# 1) 휴장일 포함한 판다스 영업일 offset 객체 생성
holidays = DbTools('inp_raw').read('holidays').h_day
BD = pd.offsets.CustomBusinessDay(holidays=holidays)
BM = pd.offsets.CustomBusinessMonthEnd(holidays=holidays)


# 2) 2000년 이후 주기별 영업일 DatetimeIndex 생성
all_workdays = pd.date_range('2000-01-01', str(pd.Timestamp.today().year)+'-12-31', freq=BD)
all_month_end_workdays = pd.date_range('2000-01-01', str(pd.Timestamp.today().year) + '-12-31', freq=BM)
all_quarter_end_workdays = all_month_end_workdays[2::3]
all_year_end_workdays = all_month_end_workdays[11::12]


def workdays_offset(date_range, last=True, freq='D'):
    """입력받은 날짜 구간을 영업일로 변화 후, 구간을 이동시키거나 주기를 변경하여 영업일 인덱스(workdays DatetimeIndex)를 반환"""

    # 입력된 날짜 구간(date_range)을 영업일 기준으로 변환
    if isinstance(date_range, (str, int)):
        date_range = str(date_range)
        if len(date_range) == 7:
            workdays = all_workdays[all_workdays.slice_indexer(date_range, date_range)].strftime('%Y%m%d')
            date_range = [workdays[0], workdays[-1]]
        else:
            last_workday = all_workdays[all_workdays.slice_indexer(end=date_range)][-1].strftime('%Y%m%d')
            date_range = [last_workday, last_workday]

    elif isinstance(date_range, (list, tuple)):
        start, end = (str(date_range[0]), str(date_range[1]))
        workdays = all_workdays[all_workdays.slice_indexer(start, end)].strftime('%Y%m%d')
        date_range = [workdays[0], workdays[-1]]

    else:
        workdays = all_workdays[all_workdays.slice_indexer(start='2000-02')].strftime('%Y%m%d')
        date_range = [workdays[0], workdays[-1]]

    # date_range 구간 내 모든 영업일의 인덱스 산출
    workdays = all_workdays[all_workdays.slice_indexer(date_range[0], date_range[1])]

    # 기간을 전월말 기준으로 이동시키거나(=last) 주기를 변경한(=freq) 영업일 인덱스 산출
    months = workdays.to_period('M').drop_duplicates().strftime('%Y-%m')
    quarters = workdays.to_period('Q').drop_duplicates().strftime('%Y-%m')
    years = workdays.to_period('Y').drop_duplicates().strftime('%Y-%m')
    last_months = (workdays.to_period('M').drop_duplicates() - 1).strftime('%Y-%m')
    last_months_1 = (workdays.to_period('M').drop_duplicates() - 2).strftime('%Y-%m')
    last_quarters = ((workdays.to_period('M').drop_duplicates() - 1).asfreq('Q')
                     .drop_duplicates() - 1).strftime('%Y-%m')
    last_years = ((workdays.to_period('M').drop_duplicates() - 1).asfreq('Y')
                  .drop_duplicates() - 1).strftime('%Y-%m')

    if last:
        if freq == 'D':
            return all_workdays[all_workdays.slice_indexer(last_months[0], last_months[-1])]
        if freq == 'ME':
            return all_month_end_workdays[all_month_end_workdays.slice_indexer(last_months[0], last_months[-1])]
        if freq == 'M':
            return all_month_end_workdays[all_month_end_workdays.slice_indexer(last_months_1[0], last_months_1[-1])]
        if freq == 'Q':
            return all_quarter_end_workdays[all_quarter_end_workdays.slice_indexer(last_quarters[0],
                                                                                   last_quarters[-1])]
        if freq == 'Y':
            return all_year_end_workdays[all_year_end_workdays.slice_indexer(last_years[0],
                                                                             last_years[-1])]
    else:
        if freq == 'D':
            return workdays
        if freq == 'M':
            return all_month_end_workdays[all_month_end_workdays.slice_indexer(months[0], months[-1])]
        if freq == 'Q':
            return all_quarter_end_workdays[all_quarter_end_workdays.slice_indexer(quarters[0], quarters[-1])]
        if freq == 'Y':
            return all_year_end_workdays[all_year_end_workdays.slice_indexer(years[0], years[-1])]


def date_offset(date, last=True, freq='D', start_offset=0):
    """구간을 전월말로 이동시키거나, 주기를 변경하거나, 시작일을 앞당긴 새로운 구간을 생성"""

    base_workdays = workdays_offset(date, last, freq)
    all_list = {'D': all_workdays, 'M': all_month_end_workdays,
                'Q': all_quarter_end_workdays, 'Y': all_year_end_workdays}
    all_days = all_list[freq]

    if freq == 'Q' and start_offset == '1Y':
        start = all_days[all_days.get_loc(str(base_workdays[0].year - 1) + '-03')][0]
    elif freq == 'Q' and start_offset == '2Y':
        start = all_days[all_days.get_loc(str(base_workdays[0].year - 2) + '-03')][0]
    else:
        start = all_days[all_days.get_loc(base_workdays[0]) - int(start_offset)]

    return [start.strftime('%Y%m%d'), base_workdays[-1].strftime('%Y%m%d')]
