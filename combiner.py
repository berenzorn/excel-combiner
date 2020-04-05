import argparse
from c9 import C9
import pandas as pd
from table import Table
from sparkle import Sparkle


class Common(Table):
    def table_combine(self, c9, sp) -> list:
        exec = (f"SELECT {sp}.country, {c9}.network, {c9}.tadig, {c9}.mcc, {c9}.mnoid, {c9}.profile, "
                f"{c9}.ws_price, {c9}.ws_inc, {c9}.retail_price, {c9}.rp_inc, {sp}.moc, {sp}.mtc, {sp}.sms_mo, "
                f"{sp}.sms_mt, {sp}.gprs, round((({c9}.ws_price/1.1)/{sp}.gprs)*100, 2) as '(C9/Sparkle) %'  "
                f"FROM {c9} INNER JOIN {sp} ON {c9}.tadig = {sp}.tadig;")
        self.table_execute(exec)
        combined = self.cursor.fetchall()
        return combined


def config() -> tuple:
    mysql_user = 'root'
    mysql_pass = 'balloon'
    mysql_host = 'localhost'
    mysql_base = 'sys'
    return mysql_user, mysql_pass, mysql_host, mysql_base


def read_excel(filename, skiprows) -> list:
    c = pd.read_excel(filename, header=None, skiprows=skiprows)
    return c.values.tolist()


def write_excel(combined: list, filename: str):
    df = pd.DataFrame(
        combined, columns=['country', 'network', 'tadig', 'mcc', 'mnoid', 'profile', 'ws_price',
                           'ws_inc', 'retail_price', 'rp_inc', 'moc', 'mtc', 'sms_mo', 'sms_mt',
                           'gprs', '(C9/Sparkle) %'])
    df.to_excel(filename)


def fill_table(table, name: str, skiprows: int):
    if not table.table_check():
        table.table_make()
    table.table_truncate()

    xlist = read_excel(name, skiprows)
    for row in xlist:
        table.table_append(row=row)
    table.end_table_connect()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("c9", type=str, help="Файл С9")
    parser.add_argument("sparkle", type=str, help="Файл Sparkle")
    parser.add_argument("output", type=str, help="Выходной файл")
    args = parser.parse_args()

    creds = config()
    c9_table = C9(name=str(args.c9).split('.')[0], creds=creds)
    fill_table(c9_table, args.c9, 3)
    sp_table = Sparkle(name=str(args.sparkle).split('.')[0], creds=creds)
    fill_table(sp_table, args.sparkle, 1)
    cmn_table = Common(name=str(args.output), creds=creds)
    combined = cmn_table.table_combine(c9_table, sp_table)
    write_excel(combined, args.output)
