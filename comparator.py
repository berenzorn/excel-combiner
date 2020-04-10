import argparse
import pandas as pd
from table import Table


class C9(Table):
    def table_append(self, row: list):
        string = ""
        for i in row:
            string = f"{string}, '{str(i).rstrip()}'"
        insert = f"INSERT into {self.name} (country, network, tadig, mcc, mnoid, profile, ws_price, ws_inc, " \
            f"retail_price, rp_inc, 4g, blocking, cheapest) VALUES ({string[2:]});"
        self.table_execute(insert)

    def table_make(self):
        create = (f'CREATE TABLE {self.name} (`id` INT NOT NULL AUTO_INCREMENT, `country` VARCHAR(10) NULL, '
                  '`network` VARCHAR(120) NULL, `tadig` VARCHAR(10) NULL, `mcc` VARCHAR(15) NULL, `mnoid` INT NULL, '
                  '`profile` VARCHAR(10) NULL, `ws_price` FLOAT NULL, `ws_inc` INT NULL, `retail_price` FLOAT NULL, '
                  '`rp_inc` INT NULL, `4g` VARCHAR(10) NULL, `blocking` VARCHAR(50) NULL, `cheapest` INT NULL, '
                  'PRIMARY KEY (`id`));')
        self.table_execute(create)
        self.table_execute(f"UPDATE {self.name} SET `4g` = NULL where `4g` = 'nan';")
        self.table_execute(f"UPDATE {self.name} SET `blocking` = NULL where `blocking` = 'nan';")

    def fetch_countries(self):
        self.cursor.execute(f"SELECT country from {self.name};")
        country_set = list(set(self.cursor.fetchall()))
        country_list = [x[0] for x in country_set]
        for i in country_list:
            self.table_execute(f"UPDATE {self.name} AS C INNER JOIN (SELECT country, MIN(ws_price) as MINI, "
                               f"cheapest from {self.name} where country = '{i}') AS A USING (country) "
                               f"SET C.cheapest = '1' WHERE C.ws_price = A.MINI;")
        self.cnx.commit()


class Common(Table):
    def table_combine(self) -> list:
        exec = (f"SELECT country, network, tadig, mcc, mnoid, profile, ws_price, ws_inc, "
                f"retail_price, rp_inc, 4g, blocking, cheapest FROM c9update;")
        self.table_execute(exec)
        combined = self.cursor.fetchall()
        return combined


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
        table.table_append(row=row[1:])
    # for i in xlist:
    #     print(i)
    # return xlist


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("c9", type=str, help="Файл С9")
    parser.add_argument("update", type=str, help="Файл Update")
    # parser.add_argument("output", type=str, help="Выходной файл")
    args = parser.parse_args()

    creds = config()
    c9_table = C9(name=str(args.c9).split('.')[0], creds=creds)
    fill_table(c9_table, args.c9, 1)
    # table_c9 = fill_table(c9_table, args.c9, 3)
    # print(len(table_c9[0]))
    c9_update = C9(name=str(args.c9).split('.')[0], creds=creds)
    fill_table(c9_update, args.update, 1)
    # update_c9 = fill_table(c9_update, args.update, 3)
    # print(len(update_c9[0]))


    # c9_table.fetch_countries()
    # c9_table.end_table_connect()
    # cmn_table = Common(name=str(args.output), creds=creds)
    # combined = cmn_table.table_combine()
    # write_excel(combined, args.output)

