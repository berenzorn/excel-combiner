import argparse
import pandas as pd
from table import Table


class C9(Table):
    def table_append(self, row: list):
        string = ""
        for i in row:
            string = f"{string}, '{str(i).rstrip()}'"
        sid = f"'{row[3]}{row[2]}{row[4]}', {string[2:]}"
        insert = f"INSERT into {self.name} (id, country, network, tadig, mcc, mnoid, profile, ws_price, ws_inc, " \
            f"retail_price, rp_inc, 4g, blocking) VALUES ({sid});"
        self.table_execute(insert)

    def table_make(self):
        self.table_execute(f"DROP TABLE IF EXISTS {self.name}")
        create = (f"CREATE TABLE {self.name} (`id` VARCHAR(20) NOT NULL, `country` VARCHAR(10) NULL, `network` VARCHAR(120) NULL, "
                  f"`tadig` VARCHAR(10) NULL, `mcc` VARCHAR(15) NULL, `mnoid` INT NULL, `profile` VARCHAR(10) NULL, "
                  f"`ws_price` FLOAT NULL, `ws_inc` INT NULL, `retail_price` FLOAT NULL, `rp_inc` INT NULL, `4g` VARCHAR(10) NULL, "
                  f"`blocking` VARCHAR(50) NULL, PRIMARY KEY (`id`));")
        self.table_execute(create)
        self.table_execute(f"ALTER TABLE {self.name} ADD COLUMN `cheapest` INT NULL AFTER `blocking`;")

    def fetch_countries(self):
        self.cursor.execute(f"SELECT country from {self.name};")
        country_set = list(set(self.cursor.fetchall()))
        country_list = [x[0] for x in country_set]
        for i in country_list:
            self.table_execute(f"UPDATE {self.name} AS C INNER JOIN (SELECT country, MIN(ws_price) as MINI, cheapest from {self.name} "
                               f"where country = '{i}') AS A USING (country) SET C.cheapest = '1' WHERE C.ws_price = A.MINI;")
        self.cnx.commit()


class Common(Table):
    def table_combine(self, name) -> list:
        self.table_execute(f"SELECT * FROM {name};")
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
    df = pd.DataFrame(
        combined, columns=['id', 'country', 'network', 'tadig', 'mcc', 'mnoid', 'profile', 'ws_price',
                           'ws_inc', 'retail_price', 'rp_inc', '4g', 'blocking', 'cheapest'])
    df.to_excel(filename)


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
    parser.add_argument("c9", type=str, help="Файл С9")
    parser.add_argument("output", type=str, help="Выходной файл")
    args = parser.parse_args()

    creds = config()
    c9_table = C9(name=str(args.c9).split('.')[0], creds=creds)
    fill_table(c9_table, args.c9, 3)
    c9_table.fetch_countries()
    cmn_table = Common(name=str(args.output), creds=creds)
    combined = cmn_table.table_combine(c9_table.name)
    write_excel(combined, args.output)
    # c9_table.table_execute(f"DROP TABLE IF EXISTS {c9_table.name}")
    c9_table.end_table_connect()

