import mysql.connector


class Table:

    def __init__(self, name, creds: tuple):
        self.name = name
        self.cnx, self.cursor = self.get_table_connect(creds)

    def get_connector(self) -> tuple:
        return self.cnx, self.cursor

    @staticmethod
    def get_table_connect(creds: tuple) -> tuple:
        cnx = mysql.connector.connect(
            user=creds[0], password=creds[1],
            host=creds[2], database=creds[3])
        cursor = cnx.cursor(buffered=True)
        return cnx, cursor

    def table_check(self) -> bool:
        try:
            execute = f"SELECT * FROM {self.name} LIMIT 0, 10;"
            self.table_execute(execute)
            return True
        except mysql.connector.errors.ProgrammingError:
            return False

    def table_execute(self, query: str):
        try:
            self.cursor.execute(query)
        except mysql.connector.errors.ProgrammingError:
            return False
        self.cnx.commit()

    def end_table_connect(self):
        self.cnx.commit()
        self.cursor.close()
        self.cnx.close()

    def table_read(self, field, number):
        read = f"SELECT * FROM {self.name} WHERE {field} = {number};"
        self.cursor.execute(read)
        return self.cursor.fetchone()

    def table_len(self):
        length = f"SELECT COUNT(*) FROM {self.name}"
        self.cursor.execute(length)
        return self.cursor.fetchone()

    def table_truncate(self):
        truncate = f"TRUNCATE TABLE {self.name};"
        self.table_execute(truncate)

    def table_drop(self):
        drop = f"DROP TABLE IF EXISTS {self.name};"
        self.table_execute(drop)

    def table_make(self):
        pass

    def table_append(self, row: list):
        string = ""
        for i in row:
            string = f"{string}, '{str(i).rstrip()}'"
            print(string)
        insert = f"INSERT into {self.name} VALUES ({string[2:]});"
        self.table_execute(insert)

