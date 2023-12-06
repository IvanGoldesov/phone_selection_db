import psycopg2
import pandas
import re
import os
import csv
import time
import index_interest

from psycopg2 import Error
from functools import wraps


''' 
    Translating data from .csv file. Formatting it to create table data PostgreSQL
'''
table_info = pandas.read_csv('ndtv_data_final.csv')
names_column_from_csv = table_info.head(0).columns
name_table = 'mobile_csv'
data_list_csv_name_for_SQL = []
type_column = table_info.dtypes
result_first_rows = {}
check_file_in_directory = []
symbol_case = {}

type_csv_sql = {
    'object': 'VARCHAR (100)',
    'int64': 'INT',
    'float64': 'FLOAT (2)',
}

change_numbers = {
    '1': 'one_',
    '2': 'two_',
    '3': 'three_',
    '4': 'four_',
    '5': 'five_',
    '6': 'six_',
    '7': 'seven_',
    '8': 'eight_',
    '9': 'nine_',
    '0': 'zero_',
}
change_symbol = {
    '\(':'',
    '\)':'',
    '-':'_',
    ' ':'_',
    ':':'',
    '/':'',
} 

"""
    Returns: dictionary -> name_column(str) : type (in the SQL) (str)
"""
for i,j in zip(names_column_from_csv, type_column):
    for k in change_numbers.keys():
        if i.startswith(k):
            i = re.sub(k, change_numbers[k], i)
    for k in change_symbol.keys():
        i = re.sub(k, change_symbol[k], i)
    data_list_csv_name_for_SQL.append(i)
    result_first_rows[i] = type_csv_sql[str(j)]


"""
    Returns: dict -> name_column (str) : tuple(values from csv)
"""
all_data_dict_from_csv = {}

for i,j in zip(data_list_csv_name_for_SQL, table_info):
    all_data_dict_from_csv[i] = tuple(table_info[j])


def connection(query_sql):
    """
    connection(query_sql)

    connection with database PostgreSQL 
    (Decorator)

    Args:
        connection (foo()): accept a query string function on PostgreSQL 
    """
    @wraps(query_sql)
    def wrapper(*args, **kwargs):
        try:
            connection = psycopg2.connect(user = "postgres",
                                    password = '12345678',
                                    host = '127.0.0.1',
                                    port = '5432',
                                    database = 'postgres_db',)
            connection.autocommit = True
            cursor = connection.cursor()

            responce_from_sql = query_sql(cursor, *args)
            
            return responce_from_sql 
        except (Exception, Error) as error:
            print('Error at word with PostgerSQL', error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print('Connection with PostgerSQL is closed')                
    return wrapper


@connection
def print_sql(cursor, string_query_in_sql : str) -> list:
    """
    print_sql(cursor, string_query_in_sql)

    Function for sending commands to receive data from PostreSQL
    Uses decotator connection()

    Args:
        cursor (from library psycorg2): cursor for communication between Python and PostreSQL
        string_query_in_sql (str): command string PostgreSQL

    Returns:
        list: data list for sent request
    """
    cursor.execute(string_query_in_sql)
    responce_from_sql = cursor.fetchall()
    return responce_from_sql


@connection
def create_update_table_SQL(cursor, string_query_in_sql : str) -> None:
    """
        create_update_table_SQL(cursor, string_query_in_sql

        Create or update table PostgreSQL
        Uses decotator connection()

    Args:
        cursor (from library psycorg2): cursor for communication between Python and PostreSQL
        string_query_in_sql (str): command string PostgreSQL
    """
    cursor.execute(string_query_in_sql)


@connection
def add_data_in_SQL(cursor, string_query_in_sql : str, list_data : list) -> None:
    """add_data_in_SQL(cursor, string_query_in_sql, list_data)

        Insert into PostgreSQL new data from .csv file
        Uses decotator connection()
    
    Args:
        cursor (from library psycorg2): cursor for communication between Python and PostreSQL
        string_query_in_sql (str): command string PostgreSQL
        list_data (list): data list from .csv file
    """
    cursor.executemany(string_query_in_sql, list_data)
    print(cursor.rowcount, 'Запись(и) успешно создана(ы)')


@connection
def add_column_index_interest(cursor) -> None:
    """add_column_index_interest(cursor)

        Create column "index_interest (ii)" 
        Uses decotator connection()
    
    Args:
        cursor (from library psycorg2): cursor for communication between Python and PostreSQL
    """
    cursor.execute(f"""ALTER TABLE {name_table}
                       ADD ii INT;""")
    cursor.execute(f"""UPDATE {name_table} SET ii = 0;""")


@connection
def debug_ii(cursor) ->  None:
    """debug_ii(cursor)
    
        Update index_interest (ii) by 0. DEBUG FUNCTION

    Args:
        cursor (from library psycorg2): cursor for communication between Python and PostreSQL
    """
    cursor.execute(f"""UPDATE {name_table} SET ii = 0;""")


def create_table_string(names_table : str) -> str:
    """create_table_string(names_table)

        Query string for create table to PostgreSQL with names and data types from .csv file

    Args:
        names_table (str): names_table

    Returns:
        str: query string fro create table to PostgreSQL
    """
    create_table_query = f'''CREATE TABLE {names_table} ('''
    flag = True
    counter = 0
    for i,j in result_first_rows.items():
        if counter == (len(all_data_dict_from_csv)-1):
            create_table_query += f'{i} {j}'
        elif flag:
            create_table_query += f'{i} {j} PRIMARY KEY, \n'
            flag = False
        else:
            create_table_query += f'{i} {j}, \n'
        counter+=1
    create_table_query += ');'

    return create_table_query


def record_from_insert()->list:
    """record_from_insert()

        Debug list data for tasks of the required type

    Returns:
        list: data of the required type to be transmitted to PostrgreSQL
    """
    a = []
    b = []
    length = len(all_data_dict_from_csv[data_list_csv_name_for_SQL[0]])
    for num in range(length):
        for i,j in all_data_dict_from_csv.items():
                if result_first_rows[i] == 'INT':
                    b.append(int(j[num]))
                elif result_first_rows[i] == 'VARCHAR (100)':
                    b.append(str(j[num]))
                elif result_first_rows[i] == 'FLOAT (2)':
                    b.append(float(j[num]))
        a.append(tuple(b))
        b.clear()

    return a


def insert_data(names_table : str) -> str:
    """insert_data(names_table)

        Query string for insert data into PostgreSQL using method executemany()

    Args:
        names_table (str): names_table

    Returns:
        str: query string
    """

    insert_query_str = f'''INSERT INTO {names_table} ('''
    counter = 0
    for i in all_data_dict_from_csv.keys():
        if counter==(len(all_data_dict_from_csv)-1):
            insert_query_str += f'{i}'
        else:
            insert_query_str += f'{i}, '
        counter += 1
    insert_query_str += f') VALUES ('
    
            
    counter = 0
    for i in all_data_dict_from_csv.keys():
        if counter==(len(all_data_dict_from_csv)-1):
            insert_query_str += f'%s'
        else:
            insert_query_str += f'%s, '
        counter += 1
    insert_query_str += f')'

    return insert_query_str


def track_directory(director : str) -> list:
    """track_directory(director)

        Checks tvery 5 seconds for new requests in the directory.

    Args:
        director (str): path directory requests user

    Returns:
        list: files from directory
    """
    file_list = os.listdir(director)
    time.sleep(5)
    return file_list


class Request_proccesing:
    """Request_proccesing

        Class proccesing user request (file)
    """
    def __init__(self, path_file_request : str) -> None:
        """__init__(self, path_file_request)

            Initialization class 

        Args:
            path_file_request (str): names file user request
        """
        self.path_file_request = path_file_request

    
    def get_request_from_file(self) -> list:
        """get_request_from_file()

            Get query string user from file 

        Returns:
            list: query string
        """
        f = self.path_file_request.replace('.txt','').replace('request','').strip()
        if self.path_file_request.startswith('request ') and self.path_file_request.endswith(f'{f}.txt'):
            with open(f'Request/{self.path_file_request}', 'r') as file:
                list_data = file.read().strip()
                self.list_data = list_data
                return self.list_data


    def remove_file(self) -> dict:
        """remove_file()

            Get list query string from function get_request_from_file(),
            and convert list query string for transmission to PostgreSQL.

        Returns:
            dict: data and their type
        """
        dict_values_params = {}
        self.list_data = self.list_data.split(';')
        #Divide string into a list.

        for i in range(len(self.list_data)):
           self.list_data[i] = self.list_data[i].split(':')

        #Delete spaces
        for i in range(len(self.list_data)):
            for j in range(len((self.list_data[i]))):
                self.list_data[i][j] = self.list_data[i][j].strip()

        #Check for signs more of less and get needful data type
        for k in range(len(self.list_data)):
                list_for_recording = []
                if 'None' not in self.list_data[k]:
                    if '<' in self.list_data[k][1] and '>' in self.list_data[k][1]:
                        big = self.list_data[k][1].index('>')
                        low = self.list_data[k][1].index('<')
                        if big < low:
                            symbol_case[self.list_data[k][0]] = ('>','<')
                            self.list_data[k][1] = self.list_data[k][1].replace('<','').replace('>','')
                            self.list_data[k][1] = tuple(self.list_data[k][1].split(','))
                        else:
                            symbol_case[self.list_data[k][0]] = ('<','>')
                            self.list_data[k][1] = self.list_data[k][1].replace('<','').replace('>','')
                            self.list_data[k][1] = tuple(self.list_data[k][1].split(','))
                    elif '<' in self.list_data[k][1]:
                        symbol_case[self.list_data[k][0]] = '<'
                        self.list_data[k][1] = self.list_data[k][1].replace('<','')
                    elif '>' in self.list_data[k][1]:
                        symbol_case[self.list_data[k][0]] = '>'
                        self.list_data[k][1] = self.list_data[k][1].replace('>','')

                    if result_first_rows[self.list_data[k][0]] == 'INT':
                        if type(self.list_data[k][1]) == tuple:
                            for tp in range(len(self.list_data[k][1])):
                                    list_for_recording.append(int(self.list_data[k][1][tp]))
                            dict_values_params[self.list_data[k][0]] = tuple(list_for_recording)
                            list_for_recording.clear()
                        else:
                            dict_values_params[self.list_data[k][0]] = int(self.list_data[k][1])
                        
                    elif result_first_rows[self.list_data[k][0]] == 'VARCHAR (100)':
                        dict_values_params[self.list_data[k][0]] = str(self.list_data[k][1])
                    elif result_first_rows[self.list_data[k][0]] == 'FLOAT (2)':
                        if type(self.list_data[k][1]) == tuple:
                            for tp in range(len(self.list_data[k][1])):
                                    list_for_recording.append(float(self.list_data[k][1][tp]))
                            dict_values_params[self.list_data[k][0]] = tuple(list_for_recording)
                            list_for_recording.clear()
                        else:
                            dict_values_params[self.list_data[k][0]] = float(self.list_data[k][1])
        self.dict_values_params = dict_values_params
        return self.dict_values_params
    

    def decor_string(command_sql : str):
        """decor_string(command_sql)

            DECORATOR
            Get function for a select data or update index interest (ii) from PostgreSQL.
            Inside use list from function get_request_from_file().

        Args:
            command_sql (function() : str): function string select or update request
        """
        def wrapper(self, *args, **kwargs):
            add_query = command_sql()
            length = len(self.dict_values_params)
            counter = 0
            if length > 0:
                for i,j in self.dict_values_params.items():
                    if type(j) != str:
                        symbol = symbol_case.get(i)
                        if symbol_case.get(i):
                            if len(symbol) > 1:
                                if length == 1:
                                    add_query += f'{i} {symbol_case[i][0]} {j[0]} AND {i} {symbol_case[i][1]} {j[1]}'
                                elif counter == 0:
                                    add_query += f'{i} {symbol_case[i][0]} {j[0]} AND {i} {symbol_case[i][1]} {j[1]}'
                                else:
                                    add_query += f' AND {i} {symbol_case[i][0]} {j[0]} AND {i} {symbol_case[i][1]} {j[1]}'
                            else: 
                                if length == 1:
                                    add_query += f'{i} {symbol_case[i]} {j}'
                                elif counter == 0:
                                    add_query += f'{i} {symbol_case[i]} {j}'
                                else:
                                    add_query += f' AND {i} {symbol_case[i]} {j}'
                        else:
                            if length == 1:
                                add_query += f'{i} = {j}'
                            elif counter == 0:
                                add_query += f'{i} = {j}'
                            else:
                                add_query += f' AND {i} = {j}'
                    else:
                        if length == 1:
                            add_query += f"{i} = '{j}'"
                        elif counter == 0:
                            add_query += f"{i} = '{j}'"
                        else:
                            add_query += f" AND {i} = '{j}'"
                    counter += 1
            else:
                if add_query.startwith('S'):
                    add_query = f'''SELECT * from {name_table}'''
                else:
                    add_query = f'''UPDATE {name_table} SET ii = ii+1'''
            return add_query 
        return wrapper

    @decor_string
    def add_sql() -> str:
        """add_sql()

            Select request to PostgreSQL
            Uses decotator decor_string()

        Returns:
            str: query string (start)
        """
        return f'''SELECT * FROM {name_table} WHERE '''
    
    @decor_string
    def update_sql() -> str:
        """update_sql()

            Update index interest to PostgreSQL
            Uses decotator decor_string()

        Returns:
            str: query string (start)
        """
        return f'''UPDATE {name_table} SET ii = ii+1 WHERE '''


def remove_data(list_data : list) -> list:
    """remove_data(list_data)

        Remove index interest (ii) from the received user request

    Args:
        list_data (list): data request from PostgreSQL

    Returns:
        list: new data request from PostgreSQL without ii
    """
    for i in range(len(list_data)):
        list_data[i] = list(list_data[i])
        del list_data[i][len(list_data[i])-1]
        list_data[i] = tuple(list_data[i])
    return list_data


def save_responce_in_csv(path_file_request : str, get_data : list, offer_list : list) -> None:
    """save_responce_in_csv(path_file_request, get_datat, offer_list)

        Create csv file for recording data from PoestgreSQL.
        Also output data least viewed queries.

    Args:
        path_file_request (str): name file request
        get_data (list): data user request from PoestreSQL
        offer_list (list): least requested data
    """
    f = path_file_request.replace('.txt','').replace('request','').strip()
    get_data = remove_data(get_data)
    offer_list = remove_data(offer_list)
    with open(f'Responce/responce {f}.csv', 'w', newline="") as file:
        writer = csv.writer(file)
       # result_first_rows['ii'] = 'INT'
        writer.writerow([i for i in result_first_rows.keys()])
        for i in get_data:
            writer.writerow(i)
        if offer_list:
            writer.writerow('________________________________________________________________')
            writer.writerow('SPECIAL OFFER FOR YOU!')
            writer.writerow('________________________________________________________________')
            for i in offer_list:
                writer.writerow(i)
    check_file_in_directory.append(path_file_request)
    symbol_case.clear()


if __name__ == '__main__':
    """#Create and recording data from .csv file to PostgreSQL
    c_t = create_table_string(name_table)
    create_update_table_SQL(c_t)"""

    """#Insert data into PostgreSQL from csv or by hand
    strr = insert_data(name_table)
    list_data = record_from_insert()
    b = [(1362, 'Vivo V7+', 'Vivo', 'V7', 3225, 5.99, 'Yes', 720, 1440, 8, 4000, 64.0, 16.0, 24.0, 'Android', 'Yes', 'Yes', 'Yes', 2, 'Yes', 'Yes', 10990)]
    #add_data_in_SQL(strr, list_data)
    add_data_in_SQL(strr, b)

    #Create column index interest
    add_column_index_interest()
    """
    
    name_table = 'mobile_csv'
    start_timer = time.perf_counter()
    while True:
        new_files = track_directory(os.getcwd()+'/Request/')
        if new_files != check_file_in_directory:
            for file in new_files:
                user = Request_proccesing(file)
                if file not in check_file_in_directory and user.get_request_from_file():
                    user.get_request_from_file()
                    user.remove_file()

                    #Get query string user
                    add_data_from_file = user.add_sql()
                    udpate_index_interest = user.update_sql()

                    #Senf query string from PostgreSQL
                    data_sheet = print_sql(add_data_from_file)
                    create_update_table_SQL(udpate_index_interest)

                    #List data (index interest)
                    offer_list = []
                    #Create check point for checks index interest (time > 10 seconds)
                    if index_interest.timers(start_timer):
                            start_timer = time.perf_counter()
                            procces_ii = index_interest.procces_ii()
                            offer_list = index_interest.output_low_ii(procces_ii)
                    save_responce_in_csv(file, data_sheet, offer_list)
        else:
            print('The file already exists!')



