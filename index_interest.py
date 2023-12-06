from time import perf_counter
from psycopg2 import Error
from math import ceil
import psycopg2
from main import connection
           
@connection
def take_all_ii(cursor) -> list:
    """take_all_ii(cursor)

        Get data from column index interest PostgreSQL
        Uses decotator connection()

    Args:
         cursor (from library psycorg2): cursor for communication between Python and PostreSQL

    Returns:
        list: data from column index interest
    """
    cursor.execute('SELECT ii FROM mobile_csv')
    responce_from_sql = cursor.fetchall()
    return responce_from_sql

@connection
def output_low_ii(cursor, sr_data : int) -> list:
    """output_low_ii(cursor, sr_data)

        Get data less sr_data, apply discount 10% and output to the user.
        Uses decotator connection()
        
    Args:
        cursor (from library psycorg2): cursor for communication between Python and PostreSQL
        sr_data (int): average value of the ii

    Returns:
        list: _description_
    """
    cursor.execute(f'SELECT * FROM mobile_csv WHERE ii < {sr_data}')
    responce_from_sql = cursor.fetchall()
    
    for i in range(len(responce_from_sql)):
        responce_from_sql[i] = list(responce_from_sql[i])
        responce_from_sql[i][len(responce_from_sql[i])-2] = ceil(responce_from_sql[i][len(responce_from_sql[i])-2] * 0.9)
        responce_from_sql[i] = tuple(responce_from_sql[i])

    return responce_from_sql


def procces_ii() -> int:
    """procces_ii()

        Get average value of the index interest.

    Returns:
        int: average value of the index of interest
    """
    list_data = take_all_ii()
    list_data_upgrade = []
    list(map((lambda x: list_data_upgrade.append(x[0])), list_data))
    sr_datas = ceil(sum(list_data_upgrade)/len(list_data_upgrade))
    return sr_datas
    

def timers(start : float) -> bool:
    """timers(start)

        Timer. If time's up then starts a cycle, from finds a new average number.

    Args:
        start (float): first input(start time)

    Returns:
        bool: if start > 10 seconds return True else False
    """
    if (perf_counter() - start) > 10:
            return True
    else:
         return False

