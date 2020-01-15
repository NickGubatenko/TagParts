import sqlite3


class DatabaseController:
    DB_PATH = 'alpha_parts.db'

    def __init__(self):
        self.connection = sqlite3.connect(self.DB_PATH)
        self.cursor = self.connection.cursor()

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.cursor.close()
        if isinstance(exc_value, Exception):
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()

    def create_parts_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS parts_table(
                            part_number TEXT PRIMARY KEY,
                            part_type TEXT,
                            part_manufacturer TEXT,
                            part_stock_quantity INT,
                            part_forbidden_date TEXT,
                            part_forbidden_reason TEXT,
                            part_usage_dec_nums TEXT
                            )""")
        self.connection.commit()
        print("parts_table was created")

    def create_tags_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS tags_table(
                            tag TEXT PRIMARY KEY
                            )""")
        self.connection.commit()
        print("tags_table was created")

    def create_partstags_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS partstags_table(
                            part_number TEXT,
                            tag TEXT,
                            PRIMARY KEY (part_number, tag),
                            FOREIGN KEY (part_number)
                                REFERENCES parts_table (part_number) 
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,
                            FOREIGN KEY (tag)
                                REFERENCES tags_table (tag) 
                                ON UPDATE CASCADE
                                ON DELETE CASCADE                                
                            )""")
        self.connection.commit()
        print("partstags_table was created")

    def add_part(self, data):
        self.cursor.execute('INSERT INTO parts_table VALUES(%s?)' % ('?, ' * (len(data) - 1)), data)
        self.connection.commit()

    def delete_table(self, table_name: str):
        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.connection.commit()
        print(f"{table_name} was deleted")

    def add_tag(self, tag):
        self.cursor.execute(f'INSERT OR IGNORE INTO tags_table VALUES("{tag}")')
        self.connection.commit()

    def del_tag(self, tag):
        self.cursor.execute(f'DELETE FROM tags_table WHERE tag="{tag}"')
        self.connection.commit()

    def bind_part_tag(self, part_number, tag):
        self.cursor.execute(f'INSERT OR IGNORE INTO partstags_table '
                            f'VALUES("{part_number}", "{tag}")')
        self.connection.commit()

    def unbind_part_tag(self, part_number, tag):
        self.cursor.execute(f'DELETE FROM partstags_table '
                            f'WHERE part_number="{part_number}" and tag="{tag}"')
        self.connection.commit()

    def search_parts(self, tags):
        tags_str = '","'.join(tags)
        query = f"""SELECT part_number FROM parts_table 
                    WHERE part_number IN 
                    (SELECT part_number FROM partstags_table
                    WHERE tag IN ("{tags_str}"))"""
        self.cursor.execute(query)
        return self.cursor.fetchall()


def create_parts_table_from_csv():
    with DatabaseController() as dbc:
        dbc.delete_table('parts_table')
        dbc.create_parts_table()
        with open('parts.csv', 'r') as file:
            for line, val in enumerate(file):
                if line > 2:
                    val = val[:-1]
                    val = val.replace("\"", "")
                    vals = val.split("!")
                    data = vals[1:]
                    dbc.add_part(data)


def create_tags_table_from_csv():
    with DatabaseController() as dbc:
        dbc.delete_table('tags_table')
        dbc.create_tags_table()
        with open('tags.csv', 'r') as file:
            for line, val in enumerate(file):
                if line > 1:
                    val = val[:-1]
                    val = val.replace("\"", "")
                    vals = val.split("!")
                    data = vals[1:][0]
                    dbc.add_tag(data)


def create_partstags_table_from_csv():
    with DatabaseController() as dbc:
        dbc.delete_table('partstags_table')
        dbc.create_partstags_table()
        with open('partstags.csv', 'r') as file:
            for line, val in enumerate(file):
                if line > 1:
                    val = val[:-1]
                    val = val.replace("\"", "")
                    vals = val.split("!")
                    part_val = vals[1:][0]
                    tag_val = vals[1:][1]
                    dbc.bind_part_tag(part_val, tag_val)


def create_db():
    with DatabaseController() as dbc:
        parts = dbc.search_parts(["USB", "Connector"])
        print(parts)

    # create_parts_table_from_csv()
    # create_tags_table_from_csv()
    # create_partstags_table_from_csv()


if __name__ == '__main__':
    create_db()
