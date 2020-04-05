import argparse
from c9 import C9
import pandas as pd
from table import Table


class Common(Table):
    def table_combine(self, name) -> list:
        exec = (f"SELECT country, network, tadig, mcc, mnoid, profile, ws_price, ws_inc, "
                f"retail_price, rp_inc, 4g, blocking, cheapest FROM {name};")
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
                           'ws_inc', 'retail_price', 'rp_inc', '4g', 'blocking', 'cheapest'])
    df.to_excel(filename)


def fill_table(table, name: str, skiprows: int):
    if not table.table_check():
        table.table_make()
    table.table_truncate()

    xlist = read_excel(name, skiprows)
    for row in xlist:
        table.table_append(row=row)
    table.end_table_connect()


def fill_table_4g(table, name: str, skiprows: int):
    if not table.table_check():
        table.table_make_4g()
    table.table_truncate()

    xlist = read_excel(name, skiprows)
    for row in xlist:
        table.table_append_4g(row=row)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("c9", type=str, help="Файл С9")
    # parser.add_argument("sparkle", type=str, help="Файл Sparkle")
    parser.add_argument("output", type=str, help="Выходной файл")
    args = parser.parse_args()

    creds = config()
    c9_table = C9(name=str(args.c9).split('.')[0], creds=creds)
    fill_table_4g(c9_table, args.c9, 3)
    c9_table.fetch_countries()
    c9_table.end_table_connect()
    cmn_table = Common(name=str(args.output), creds=creds)
    combined = cmn_table.table_combine(c9_table.name)
    write_excel(combined, args.output)
