from table import Table


class Sparkle(Table):
    def table_append(self, row: list):
        string = ""
        for i in row:
            string = f"{string}, '{str(i).rstrip()}'"
        insert = f"INSERT into {self.name} (area, country, partner_name, tadig, moc, mtc, sms_mo, sms_mt, gprs) " \
            f"VALUES ({string[2:]});"
        self.table_execute(insert)

    def table_make(self):
        create = (f'CREATE TABLE {self.name} (`id` INT NOT NULL AUTO_INCREMENT, `area` VARCHAR(10) NULL, '
                  f'`country` VARCHAR(20) NULL, `partner_name` VARCHAR(50) NULL, `tadig` VARCHAR(10) NULL, '
                  f'`moc` FLOAT NULL, `mtc` INT NULL, `sms_mo` FLOAT NULL, `sms_mt` INT NULL, `gprs` FLOAT NULL, '
                  f'PRIMARY KEY (`id`));')
        self.table_execute(create)
