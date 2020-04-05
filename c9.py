from table import Table


class C9(Table):
    def table_append(self, row: list):
        string = ""
        for i in row:
            string = f"{string}, '{str(i).rstrip()}'"
        insert = f"INSERT into {self.name} (country, network, tadig, mcc, mnoid, profile, ws_price, ws_inc, " \
            f"retail_price, rp_inc) VALUES ({string[2:]});"
        self.table_execute(insert)

    def table_append_4g(self, row: list):
        string = ""
        for i in row:
            string = f"{string}, '{str(i).rstrip()}'"
        insert = f"INSERT into {self.name} (country, network, tadig, mcc, mnoid, profile, ws_price, ws_inc, " \
            f"retail_price, rp_inc, 4g, blocking) VALUES ({string[2:]});"
        self.table_execute(insert)

    def table_make(self):
        create = (f'CREATE TABLE {self.name} (`id` INT NOT NULL AUTO_INCREMENT, `country` VARCHAR(10) NULL, '
                  '`network` VARCHAR(120) NULL, `tadig` VARCHAR(10) NULL, `mcc` VARCHAR(15) NULL, `mnoid` INT NULL, '
                  '`profile` VARCHAR(10) NULL, `ws_price` FLOAT NULL, `ws_inc` INT NULL, `retail_price` FLOAT NULL, '
                  '`rp_inc` INT NULL, PRIMARY KEY (`id`));')
        self.table_execute(create)

    def table_make_4g(self):
        create = (f'CREATE TABLE {self.name} (`id` INT NOT NULL AUTO_INCREMENT, `country` VARCHAR(10) NULL, '
                  '`network` VARCHAR(120) NULL, `tadig` VARCHAR(10) NULL, `mcc` VARCHAR(15) NULL, `mnoid` INT NULL, '
                  '`profile` VARCHAR(10) NULL, `ws_price` FLOAT NULL, `ws_inc` INT NULL, `retail_price` FLOAT NULL, '
                  '`rp_inc` INT NULL, `4g` VARCHAR(10) NULL, `blocking` VARCHAR(50) NULL, PRIMARY KEY (`id`));')
        self.table_execute(create)
        self.table_execute(f"UPDATE {self.name} SET `4g` = NULL where `4g` = 'nan';")
        self.table_execute(f"UPDATE {self.name} SET `blocking` = NULL where `blocking` = 'nan';")
        self.table_execute(f"ALTER TABLE {self.name} ADD COLUMN `cheapest` INT NULL AFTER `blocking`;")

    def fetch_countries(self):
        self.cursor.execute(f"SELECT country from {self.name};")
        country_set = list(set(self.cursor.fetchall()))
        country_list = [x[0] for x in country_set]
        for i in country_list:
            self.table_execute(f"UPDATE {self.name} AS C INNER JOIN (SELECT country, MIN(ws_price) as MINI, "
                               f"cheapest from {self.name} where country = '{i}') AS A USING (country) "
                               f"SET C.cheapest = '1' WHERE C.ws_price = A.MINI;")
        self.cnx.commit()
