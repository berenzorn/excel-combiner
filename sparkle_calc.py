import pandas as pd
import table
import argparse


class C9(table.Table):
    def table_append(self, row: list):
        string = ""
        for i in row:
            string = f"{string}, '{str(i).rstrip()}'"
        sid = f"'{row[3]}{row[2]}{row[4]}', {string[2:]}"
        insert = f"INSERT into {self.name} VALUES ({sid});"
        self.table_execute(insert)

    def table_make(self):
        self.table_execute(f"DROP TABLE IF EXISTS {self.name}")
        create = (f"CREATE TABLE {self.name} (`id` VARCHAR(20) NOT NULL, `country` VARCHAR(10) NULL, `network` VARCHAR(120) NULL, "
                  f"`tadig` VARCHAR(10) NULL, `mcc` VARCHAR(15) NULL, `mnoid` INT NULL, `profile` VARCHAR(10) NULL, "
                  f"`ws_price` FLOAT NULL, `ws_inc` INT NULL, `retail_price` FLOAT NULL, `rp_inc` INT NULL, `4g` VARCHAR(10) NULL, "
                  f"`blocking` VARCHAR(50) NULL, PRIMARY KEY (`id`));")
        self.table_execute(create)
        print("C9 Table created")


class Sparkle(table.Table):
    def table_append(self, row: list):
        string = ""
        for i in row:
            string = f"{string}, '{str(i).rstrip()}'"
        insert = f"INSERT into {self.name} (area, country, partner_name, tadig, moc, mtc, sms_mo, sms_mt, gprs) VALUES ({string[2:]});"
        self.table_execute(insert)

    def table_make(self):
        self.table_execute(f"DROP TABLE IF EXISTS {self.name}")
        create = (f"CREATE TABLE {self.name} (`id` INT NOT NULL AUTO_INCREMENT, `area` VARCHAR(45) NULL, `country` VARCHAR(45) NULL, "
                  f"`partner_name` VARCHAR(100) NULL, `tadig` VARCHAR(10) NULL, `moc` FLOAT NULL, `mtc` INT NULL, `sms_mo` FLOAT NULL, "
                  f"`sms_mt` INT NULL, `gprs` FLOAT NULL, PRIMARY KEY (`id`));")
        self.table_execute(create)
        print("SP Table created")


class Common(table.Table):
    def table_combine(self) -> list:
        exec = (f"SELECT sparkle.country, c9.network, c9.tadig, c9.mcc, c9.mnoid, c9.profile, c9.ws_price, c9.ws_inc, c9.retail_price, c9.rp_inc, "
                f"sparkle.moc, sparkle.mtc, sparkle.sms_mo, sparkle.sms_mt, sparkle.gprs, round(((c9.ws_price/1.1)/sparkle.gprs)*100, 2) "
                f"as '(C9/Sparkle) %' FROM c9 INNER JOIN sparkle ON c9.tadig = sparkle.tadig;")
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
        combined, columns=['country', 'network', 'tadig', 'mcc', 'mnoid', 'profile', 'ws_price', 'ws_inc',
                           'retail_price', 'rp_inc', 'moc', 'mtc', 'sms_mo', 'sms_mt', 'gprs', '(C9/Sparkle)%'])
    df.to_excel(filename)


def fill_table(table, skiprows: int):
    if table.table_check():
        table.table_drop()
    table.table_make()
    xlist = read_excel(f"{table.name}.xlsx", skiprows)
    for row in xlist:
        table.table_append(row=row)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("c9", type=str, help="Файл С9")
    parser.add_argument("sparkle", type=str, help="Файл Sparkle")
    parser.add_argument("output", type=str, help="Выходной файл")
    args = parser.parse_args()

    creds = config()
    c9_table = C9(name=str(args.c9).split('.')[0], creds=creds)
    fill_table(c9_table, 3)
    c9_table.table_execute(f"UPDATE {c9_table.name} SET `4g` = NULL where `4g` = 'nan';")
    c9_table.table_execute(f"UPDATE {c9_table.name} SET `blocking` = NULL where `blocking` = 'nan';")
    sp_table = Sparkle(name=str(args.sparkle).split('.')[0], creds=creds)
    fill_table(sp_table, 1)
    cmn_table = Common(name=str(args.output), creds=creds)
    combined = cmn_table.table_combine()
    write_excel(combined, args.output)
    c9_table.table_execute(f"DROP TABLE IF EXISTS {c9_table.name}")
    sp_table.table_execute(f"DROP TABLE IF EXISTS {sp_table.name}")
    c9_table.end_table_connect()
    sp_table.end_table_connect()
