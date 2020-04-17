import argparse
import pandas as pd
from table import Table
import datetime


class C9(Table):
    def table_append(self, row: list):
        sid = ""
        for i in row:
            sid = f"{sid}, '{str(i).rstrip()}'"
        insert = f"INSERT INTO {self.name} (id, country, network, tadig, mcc, mnoid, profile, ws_price, ws_inc, " \
            f"retail_price, rp_inc, 4g, blocking, cheapest) VALUES ({sid[2:]});"
        self.table_execute(insert)

    def table_make(self):
        create = (f"CREATE TABLE {self.name} (`id` VARCHAR(20) NOT NULL, `country` VARCHAR(10) NULL, `network` VARCHAR(120) NULL, "
                  f"`tadig` VARCHAR(10) NULL, `mcc` VARCHAR(15) NULL, `mnoid` INT NULL, `profile` VARCHAR(10) NULL, `ws_price` FLOAT NULL, "
                  f"`ws_inc` INT NULL, `retail_price` FLOAT NULL, `rp_inc` INT NULL, `4g` VARCHAR(10) NULL, `blocking` VARCHAR(50) NULL, "
                  f"`cheapest` VARCHAR(10) NULL, PRIMARY KEY (`id`));")
        self.table_execute(create)

    def show_difference(self, prev):
        self.table_execute(f"ALTER TABLE {self.name} ADD COLUMN `differ` INT NULL;")
        self.table_execute(f"UPDATE {self.name} AS C CROSS JOIN (SELECT {prev.name}.id, {prev.name}.cheapest, {self.name}.cheapest AS MARK "
                           f"FROM {prev.name} CROSS JOIN {self.name} on {prev.name}.id = {self.name}.id WHERE {prev.name}.cheapest IS NULL "
                           f"AND {self.name}.cheapest IS NOT NULL) AS F USING (id) SET C.differ = '1' WHERE F.MARK IS NOT NULL;")
        self.table_execute(f"UPDATE {self.name} AS C CROSS JOIN (SELECT {prev.name}.id, {prev.name}.cheapest as MARK, {self.name}.cheapest "
                           f"FROM {prev.name} CROSS JOIN {self.name} ON {prev.name}.id = {self.name}.id WHERE {prev.name}.cheapest IS NOT NULL "
                           f"AND {self.name}.cheapest IS NULL) AS F USING (id) SET C.differ = '0' WHERE F.MARK IS NOT NULL;")

    def show_new_ops(self, prev):
        self.table_execute(f"SELECT id FROM {self.name};")
        curr_set = set(self.cursor.fetchall())
        self.table_execute(f"SELECT id FROM {prev.name};")
        prev_set = set(self.cursor.fetchall())
        added_ops = curr_set - prev_set
        # removed_ops = prev_set - curr_set
        self.table_execute(f"ALTER TABLE {self.name} ADD COLUMN `new_ops` VARCHAR(10) NULL;")
        for i in added_ops:
            self.table_execute(f"UPDATE {self.name} SET {self.name}.new_ops = 'new' WHERE id = '{i[0]}';")
        self.table_execute(f"UPDATE {self.name} SET {self.name}.differ = '1' WHERE cheapest IS NOT NULL AND {self.name}.new_ops = 'new';")
        # for i in removed_ops:
        #     self.table_execute(f"INSERT INTO {self.name} ")

    def plmn_make(self, prof):
        self.cursor.execute(f"SELECT mcc, 4g from {self.name} where cheapest is not null and profile = '{prof}';")
        g_list = self.cursor.fetchall()
        plmn_list = []
        for x in g_list:
            mcc = x[0].split('/')[0]
            mnc = x[0].split('/')[1]
            if x[1] is None:
                plmn_list.append(f"{mcc},{mnc},3G")
            else:
                plmn_list.append(f"{mcc},{mnc},4G")
        return plmn_list


class Common(Table):
    def table_combine(self, prev, curr) -> list:
        self.table_execute(f"SELECT {curr}.id, {curr}.country, {curr}.network, {curr}.tadig, {curr}.mcc, {curr}.mnoid, {curr}.profile, {prev}.ws_price, "
                           f"{curr}.ws_price, {prev}.retail_price, {curr}.retail_price, {curr}.4g, {curr}.blocking, {prev}.cheapest, {curr}.cheapest, "
                           f"{curr}.differ, {curr}.new_ops from {curr} LEFT OUTER JOIN {prev} on {curr}.id = {prev}.id;")
        return self.cursor.fetchall()


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
    df = pd.DataFrame(combined,
                      columns=['id', 'country', 'network', 'tadig', 'mcc', 'mnoid', 'profile',
                               'ws_price_prev', 'ws_price_curr', 'retail_price_prev', 'retail_price_curr',
                               '4g', 'blocking', 'cheapest_prev', 'cheapest_curr', 'differ', 'new_ops'])
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
        # print(row[1:])
        table.table_append(row=row[1:])
    table.table_execute(f"UPDATE {table.name} SET `4g` = NULL where `4g` = 'nan';")
    table.table_execute(f"UPDATE {table.name} SET `blocking` = NULL where `blocking` = 'nan';")
    table.table_execute(f"UPDATE {table.name} SET `cheapest` = NULL where `cheapest` = 'nan';")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("c9", type=str, help="Файл прошлый")
    parser.add_argument("update", type=str, help="Файл текущий")
    parser.add_argument("output", type=str, help="Выходной файл")
    args = parser.parse_args()

    creds = config()
    c9_table = C9(name=str(args.c9).split('.')[0], creds=creds)
    fill_table(c9_table, args.c9, 1)
    c9_update = C9(name=str(args.update).split('.')[0], creds=creds)
    fill_table(c9_update, args.update, 1)
    c9_update.show_difference(c9_table)
    c9_update.show_new_ops(c9_table)
    # write_csv(c9_update.plmn_make('G'), 'plmn_G.txt')
    # write_csv(c9_update.plmn_make('G+'), 'plmn_Gplus.txt')
    # write_csv(c9_update.plmn_make('UL'), 'plmn_UL.txt')
    cmn_table = Common(name=str(args.output), creds=creds)
    combined = cmn_table.table_combine(c9_table.name, c9_update.name)
    write_excel(combined, args.output)
    # c9_table.table_drop()
    # c9_update.table_drop()
    c9_table.end_table_connect()
    c9_update.end_table_connect()
