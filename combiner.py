import argparse
import pandas as pd
from updater import Updater
import datetime
import random


def config() -> tuple:
    mysql_user = 'root'
    mysql_pass = 'funwfats'
    mysql_host = 'localhost'
    mysql_base = 'sys'
    return mysql_user, mysql_pass, mysql_host, mysql_base


def read_excel(filename, skiprows) -> list:
    c = pd.read_excel(filename, header=None, skiprows=skiprows)
    return c.values.tolist()


def write_excel(combined: list, filename: str):
    parts = filename.split('.')
    filename = f"{parts[0]}-{datetime.datetime.now().strftime('%d.%m.%y-%H.%M')}.{parts[1]}"
    df = pd.DataFrame(
        combined,
        columns=['id', 'country', 'network', 'tadig', 'mcc', 'mnoid', 'profile', 'ws_price_prev',
                 'ws_price_curr', 'retail_price_prev', 'retail_price_curr', '4g', 'blocking',
                 'cheapest_prev', 'cheapest_curr', 'differ', 'new_ops'])
    df.to_excel(filename)


def write_csv(combined: list, filename: str):
    with open(filename, 'w') as file:
        for x in combined:
            file.write(x + '\n')


def fill_table(table, name: str, skiprows: int):
    if table.table_check():
        table.table_drop()
    table.table_make()

    xlist = read_excel(name, skiprows)
    for row in xlist:
        table.table_append(row=row)
    table.table_execute(f"UPDATE {table.name} SET `4g` = NULL where `4g` = 'nan';")
    table.table_execute(f"UPDATE {table.name} SET `blocking` = NULL where `blocking` = 'nan';")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("previous", type=str, help="Файл С9 прошлый")
    parser.add_argument("current", type=str, help="Файл С9 текущий")
    parser.add_argument("output", type=str, help="Выходной файл")
    parser.add_argument("-c", "--country-list", dest="clist", action="store_true", help="Сделать список стран")
    parser.add_argument("-p", "--plmn-list", dest="plist", action="store_true", help="Сделать PLMN листы")
    parser.add_argument("-k", "--keep", action="store_true", help="Сохранить SQL таблицы после работы")
    args = parser.parse_args()

    creds = config()
    time = datetime.datetime.now()
    rnd1 = int(random.random()*1357911)
    rnd2 = int(random.random()*24681012)
    c9_table_prev = Updater(name=f"stage1_{rnd1}", creds=creds)
    c9_table_curr = Updater(name=f"stage1_{rnd2}", creds=creds)
    fill_table(c9_table_prev, args.previous, 3)
    fill_table(c9_table_curr, args.current, 3)
    c9_table_prev.fetch_countries(args.clist)
    c9_table_curr.fetch_countries(args.clist)
    c9_table_curr.show_difference(c9_table_prev)
    c9_table_curr.show_new_ops(c9_table_prev)

    if args.plist:
        write_csv(c9_table_curr.plmn_make('G'), 'plmn-G.txt')
        write_csv(c9_table_curr.plmn_make('G+'), 'plmn-Gplus.txt')
        write_csv(c9_table_curr.plmn_make('UL'), 'plmn-UL.txt')

    combined = c9_table_curr.table_combine(c9_table_prev)
    write_excel(combined, f"{str(args.output)}.xlsx")
    if not args.keep:
        c9_table_prev.table_drop()
        c9_table_curr.table_drop()
    c9_table_prev.end_table_connect()
    c9_table_curr.end_table_connect()
    print(datetime.datetime.now() - time)
