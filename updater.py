from table import Table


class Updater(Table):
    def table_append(self, row: list):
        string = ""
        for i in row:
            string = f"{string}, '{str(i).rstrip()}'"
        sid = f"'{row[3]}{row[2]}{row[4]}', {string[2:]}"
        self.table_execute(
            f"INSERT into {self.name} (id, country, network, tadig, mcc, mnoid, profile, "
            f"ws_price, ws_inc, retail_price, rp_inc, 4g, blocking) VALUES ({sid});")

    def table_make(self):
        self.table_execute(f"DROP TABLE IF EXISTS {self.name}")
        self.table_execute(
            f"CREATE TABLE {self.name} (`id` VARCHAR(20) NOT NULL, `country` VARCHAR(10) NULL, `network` VARCHAR(120) NULL, "
            f"`tadig` VARCHAR(10) NULL, `mcc` VARCHAR(15) NULL, `mnoid` INT NULL, `profile` VARCHAR(10) NULL, "
            f"`ws_price` FLOAT NULL, `ws_inc` INT NULL, `retail_price` FLOAT NULL, `rp_inc` INT NULL, `4g` VARCHAR(10) NULL, "
            f"`blocking` VARCHAR(50) NULL, PRIMARY KEY (`id`));")
        self.table_execute(f"ALTER TABLE {self.name} ADD COLUMN `cheapest` INT NULL AFTER `blocking`;")

    def fetch_countries(self, country_export):
        self.cursor.execute(f"SELECT country from {self.name};")
        country_set = list(set(self.cursor.fetchall()))
        country_list = [x[0] for x in country_set]
        if country_export:
            with open("country.txt", 'w') as file:
                for x in country_list:
                    file.write(x + '\n')
        for i in country_list:
            self.table_execute(
                f"UPDATE {self.name} AS C INNER JOIN (SELECT country, MIN(ws_price) as MINI, cheapest from {self.name} "
                f"where country = '{i}') AS A USING (country) SET C.cheapest = '1' WHERE C.ws_price = A.MINI;")
        self.cnx.commit()

    def show_difference(self, prev):
        self.table_execute(f"ALTER TABLE {self.name} ADD COLUMN `differ` INT NULL;")
        self.table_execute(
            f"UPDATE {self.name} AS C CROSS JOIN (SELECT {prev.name}.id, {prev.name}.cheapest, {self.name}.cheapest AS MARK "
            f"FROM {prev.name} CROSS JOIN {self.name} on {prev.name}.id = {self.name}.id WHERE {prev.name}.cheapest IS NULL "
            f"AND {self.name}.cheapest IS NOT NULL) AS F USING (id) SET C.differ = '1' WHERE F.MARK IS NOT NULL;")
        self.table_execute(
            f"UPDATE {self.name} AS C CROSS JOIN (SELECT {prev.name}.id, {prev.name}.cheapest as MARK, {self.name}.cheapest "
            f"FROM {prev.name} CROSS JOIN {self.name} ON {prev.name}.id = {self.name}.id WHERE {prev.name}.cheapest IS NOT NULL "
            f"AND {self.name}.cheapest IS NULL) AS F USING (id) SET C.differ = '0' WHERE F.MARK IS NOT NULL;")

    def show_new_ops(self, prev):
        self.table_execute(f"SELECT id FROM {self.name};")
        curr_set = set(self.cursor.fetchall())
        self.table_execute(f"SELECT id FROM {prev.name};")
        prev_set = set(self.cursor.fetchall())
        added_ops = curr_set - prev_set
        self.table_execute(f"ALTER TABLE {self.name} ADD COLUMN `new_ops` VARCHAR(10) NULL;")
        for i in added_ops:
            self.table_execute(f"UPDATE {self.name} SET {self.name}.new_ops = 'new' WHERE id = '{i[0]}';")
        self.table_execute(
            f"UPDATE {self.name} SET {self.name}.differ = '1' WHERE cheapest IS NOT NULL AND {self.name}.new_ops = 'new';")

    def plmn_make(self, prof):
        self.cursor.execute(f"SELECT mcc, 4g from {self.name} where cheapest is not null and profile = '{prof}';")
        g_list = self.cursor.fetchall()
        plmn_list = []
        for x in g_list:
            (plmn_list.append(f"{x[0].split('/')[0]},{x[0].split('/')[1]},3G")
             if x[1] is None else
             plmn_list.append(f"{x[0].split('/')[0]},{x[0].split('/')[1]},4G"))
        return plmn_list

    def table_combine(self, prev) -> list:
        self.table_execute(
            f"SELECT {self.name}.id, {self.name}.country, {self.name}.network, {self.name}.tadig, {self.name}.mcc, {self.name}.mnoid, "
            f"{self.name}.profile, {prev.name}.ws_price, {self.name}.ws_price, {prev.name}.retail_price, {self.name}.retail_price, "
            f"{self.name}.4g, {self.name}.blocking, {prev.name}.cheapest, {self.name}.cheapest, {self.name}.differ, {self.name}.new_ops "
            f"from {self.name} LEFT OUTER JOIN {prev.name} on {self.name}.id = {prev.name}.id;")
        return self.cursor.fetchall()
