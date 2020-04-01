import pandas as pd
import table
import argparse


class C9(table.Table):
    def table_append(self, row: list):
        string = ""
        for i in row:
            string = f"{string}, '{str(i).rstrip()}'"
        insert = f"INSERT into {self.name} (country, network, tadig, mcc, mnoid, profile, ws_price, ws_inc, " \
            f"retail_price, rp_inc) VALUES ({string[2:]});"
        self.table_execute(insert)

    def table_make(self):
        create = (f'CREATE TABLE {self.name} (`id` INT NOT NULL AUTO_INCREMENT, `country` VARCHAR(45) NULL, '
                  '`network` VARCHAR(100) NULL, `tadig` VARCHAR(16) NULL, `mcc` VARCHAR(45) NULL, `mnoid` INT NULL, '
                  '`profile` VARCHAR(45) NULL, `ws_price` FLOAT NULL, `ws_inc` INT NULL, `retail_price` FLOAT NULL, '
                  '`rp_inc` INT NULL, PRIMARY KEY (`id`));')
        self.table_execute(create)


class Sparkle(table.Table):
    def table_append(self, row: list):
        string = ""
        for i in row:
            string = f"{string}, '{str(i).rstrip()}'"
        insert = f"INSERT into {self.name} (area, country, partner_name, tadig, moc, mtc, sms_mo, sms_mt, gprs) " \
            f"VALUES ({string[2:]});"
        self.table_execute(insert)

    def table_make(self):
        create = (f'CREATE TABLE {self.name} (`id` INT NOT NULL AUTO_INCREMENT, `area` VARCHAR(45) NULL, '
                  f'`country` VARCHAR(45) NULL, `partner_name` VARCHAR(100) NULL, `tadig` VARCHAR(16) NULL, '
                  f'`moc` FLOAT NULL, `mtc` INT NULL, `sms_mo` FLOAT NULL, `sms_mt` INT NULL, `gprs` FLOAT NULL, '
                  f'PRIMARY KEY (`id`));')
        self.table_execute(create)


class Common(table.Table):
    def table_combine(self) -> list:
        exec = (f"SELECT sparkle.country, c9.network, c9.tadig, c9.mcc, c9.mnoid, c9.profile, c9.ws_price, "
                f"c9.ws_inc, c9.retail_price, c9.rp_inc, sparkle.moc, sparkle.mtc, sparkle.sms_mo, "
                f"sparkle.sms_mt, sparkle.gprs, round((c9.ws_price/sparkle.gprs)*100, 2) as '(C9/Sparkle) %'  "
                f"FROM c9 INNER JOIN sparkle ON c9.tadig = sparkle.tadig;")
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
    combined = cmn_table.table_combine()
    write_excel(combined, args.output)
