import sys
from PyQt5.QtWidgets import *
from PyQt5 import uic
import util as ut
import pandas as pd
import raw_data as rd
from gtabview import view

raw = ut.DbTools('inp_raw')
form_class = uic.loadUiType("./ui_form/ex1.ui")[0]


class Form(QDialog, form_class):

    def __init__(self):
        super().__init__()
        self.setupUi(self)
        self.show()
        self._holidays = None
        self._stock_info = None
        self._returns = None

    def scrap_data(self):
        year = pd.Timestamp.today().year
        month = pd.Timestamp.today().month

        if self.ckb1.isChecked():
            self.tbox.append(f'{year}년 이후 KRX 휴장일 정보 스크래핑')
            self._holidays = rd.get_holidays_table(year)
            self.tbox.append('- 종료!')

        if self.ckb2.isChecked():
            self.tbox.append(f'{year}-{month}월 KRX 종목 정보 스크래핑')
            self._stock_info = rd.get_month_end_table(f'{year}-{month}')
            self.tbox.append('- 종료!')

    def view_data(self):
        view_text = self.vbox.currentText()
        view_dict = {'휴장일': self._holidays,
                     '종목정보': self._stock_info,
                     '수익률': self._returns}
        view(view_dict[view_text], recycle=False, title=view_text)
        # raw.remove('holidays', f'h_day>="{year}"')
        # raw.append('holidays', holidays_current_year)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    w = Form()
    sys.exit(app.exec())
