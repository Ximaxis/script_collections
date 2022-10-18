import sqlite3
    

class BotDB:

    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()
        
    def create_table(self):
        """Первичное создание таблицы в базе если её нет"""
        self.create_table_collection()
        self.create_table_collection_history()
        self.create_table_period_count()
        self.create_table_period_count_history()

    def create_table_collection(self):
        """Таблица коллекций для алгоритма 1"""
        try:
            new_table = '''
            CREATE TABLE IF NOT EXISTS Collections (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT NOT NULL UNIQUE,
            joining_date datetime,
            count_sale INT);
            '''
            self.cursor.execute(new_table)
            self.conn.commit()

        except sqlite3.Error as error:
            print("Error while creating a sqlite table", error)

    def create_table_collection_history(self):
        """Таблица истории коллекций для алгоритма 1"""
        try:
            new_table = '''
            CREATE TABLE IF NOT EXISTS Collections_history (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT NOT NULL,
            final_date datetime,
            count_sale INT);
            '''
            self.cursor.execute(new_table)
            self.conn.commit()

        except sqlite3.Error as error:
            print("Error while creating a sqlite table", error)

    def create_table_period_count(self):
        """Таблица коллекций для алгоритма 2"""
        try:
            new_table = '''
            CREATE TABLE IF NOT EXISTS Collections_count (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT NOT NULL UNIQUE,
            checkpoint INT NOT NULL,
            joining_date datetime);
            '''
            self.cursor.execute(new_table)
            self.conn.commit()

        except sqlite3.Error as error:
            print("Error while creating a sqlite table", error)

    def create_table_period_count_history(self):
        """Таблица истории коллекций для алгоритма 2"""
        try:
            new_table = '''
            CREATE TABLE IF NOT EXISTS Collections_count_history (
            id INTEGER PRIMARY KEY,
            address TEXT NOT NULL,
            final_date datetime,
            checkpoint INT NOT NULL);
            '''
            self.cursor.execute(new_table)
            self.conn.commit()

        except sqlite3.Error as error:
            print("Error while creating a sqlite table", error)
    
    def get_all_address(self):
        """
        Получаем все уже записаные адреса из базы и возвращаем в виде словаря
        для алгоритма 1
        """
        cursor = self.conn.cursor()
        query = '''SELECT address, count_sale  FROM collections'''
        cursor.execute(query)
        raw = cursor.fetchall()
        cursor.close()
        result = {}
        if raw:
            for item in raw:
                result.update({item[0]: item[1]})
            return result
        return result

    def get_all_address_and_checkpoint(self):
        """
        Получаем все уже записаные адреса из базы и возвращаем в виде словаря
        для алгоритма 2
        """
        cursor = self.conn.cursor()
        query = '''SELECT address, checkpoint FROM collections_count'''
        cursor.execute(query)
        raw = cursor.fetchall()
        cursor.close()
        result = {}
        if raw:
            for item in raw:
                result.update({item[0]: int(item[1])})
            return result
        return result

    def get_old_address_collection_count(self, time_delta):
        """
        Получаем устаревшие адреса и возвращаем в виде словаря
        для алгоритма 2
        """
        cursor = self.conn.cursor()
        query = f'''SELECT address, checkpoint, joining_date  FROM Collections_count where joining_date < '{time_delta}' '''
        cursor.execute(query)
        raw = cursor.fetchall()
        cursor.close()
        result = {}
        if raw:
            for item in raw:
                result.update({item[0]: (int(item[1]), item[2])})
            return result
        return result

    def create_new_inset(self, rows):
        """
        Создаем новую запись для основной таблици с колекциями
        для алгоритма 1
        """
        cursor = self.conn.cursor()
        sql_query = """
        INSERT INTO Collections (name, address, joining_date, count_sale)
        VALUES (?, ?, ?, ?)"""
        cursor.executemany(sql_query, rows)
        self.conn.commit()
        cursor.close()

    def create_new_history_inset(self, rows):
        """
        Создаем новую запись для истории с колекциями 
        для алгоритма 1
        """
        cursor = self.conn.cursor()
        sql_query = """
        INSERT INTO Collections_history (name, address, final_date, count_sale)
        VALUES (?, ?, ?, ?)"""
        cursor.executemany(sql_query, rows)
        self.conn.commit()
        cursor.close()
    
    def create_new_count_inset(self, rows):
        """
        Создаем новую запись для основной таблици с чекпоинтами
        для алгоритма 2
        """
        cursor = self.conn.cursor()
        sql_query = """
        INSERT INTO Collections_count (name, address, checkpoint, joining_date)
        VALUES (?, ?, ?, ?)"""
        cursor.executemany(sql_query, rows)
        self.conn.commit()
        cursor.close()

    def create_new_count_history_inset(self, rows):
        """
        Создаем новую запись для истории с чекпоинтами
        для алгоритма 2
        """
        cursor = self.conn.cursor()
        sql_query = """
        INSERT INTO Collections_count_history (address, checkpoint, final_date)
        VALUES (?, ?, ?)"""
        cursor.executemany(sql_query, rows)
        self.conn.commit()
        cursor.close()

    def update_checkpoint_inset(self, checkpoint, address, now):
        """
        Обновляем чекпоинт и дату его наступления
        для алгоритма 2
        """
        cursor = self.conn.cursor()
        sql_query = f"""
        UPDATE Collections_count
        SET checkpoint = {checkpoint},
            joining_date = '{now}'
        WHERE address = '{address}'"""
        cursor.execute(sql_query)
        self.conn.commit()
        cursor.close()

    def delete_collections(self, address_list, time_delta):
        """
        Удаляет устаревшие или отправленые записи
        для алгоритма 1
        """
        cursor = self.conn.cursor()
        sql_query = f"""
        DELETE from Collections
        WHERE address in (?)
        OR address in (select address from Collections where joining_date < '{time_delta}')"""
        cursor.execute(sql_query, address_list if len(address_list) > 0 else ['', ])
        self.conn.commit()
        cursor.close()

    def delete_collections_count(self, time_delta):
        """
        Удаляет устаревшие записи
        для алгоритма 2
        """
        old_address = self.get_old_address_collection_count(time_delta)
        if old_address:
            raw_create = []
            for key, value in old_address.items():
                raw_create.append((key, value[0], value[1]))
            self.create_new_count_history_inset(raw_create)
            cursor = self.conn.cursor()
            address_list = [f'{i}' for i in old_address.keys()]
            sql_query = f"""
            DELETE from Collections_count
            WHERE address in (%s)
            """ % ','.join(['?'] * len(address_list))
            cursor.execute(sql_query, address_list)
            self.conn.commit()
            cursor.close()

    def close(self):
        """Закрываем соединение с БД"""
        self.conn.close()
