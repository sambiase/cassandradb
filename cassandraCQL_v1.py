import os
import sys

import cassandra
from cassandra.cluster import Cluster


def cassandra_connection():
    conn = Cluster(['localhost'], port=9042)
    session = conn.connect()
    return session


def list_all_keyspaces(session):
    keyspaces = session.execute('DESC KEYSPACES').all()
    os.system('clear')
    print('---| KEYSPACES |---\n')
    for rows in keyspaces:
        print(rows.keyspace_name)
    input('\n[Press any key...] ')


def list_all_tables(session):
    rows = session.execute('DESC TABLES')
    os.system('clear')
    print('---| TABLES |---\n')
    for row in rows:
        print(row.name)
    input('\n[Press any key...] ')


def select_table(session, keyspace_name, table_name):
    try:
        rows = session.execute(f"SELECT * FROM {keyspace_name}.{table_name}").all()
        os.system('clear')
        print(f'---| TABLE "{table_name.upper()}" |---\n')
        for row in rows:
            print(f'ID: {row.id}')
            print(f'Name: {row.name}')
            print(f'Age: {row.age}')
            print(f'City: {row.city}')
            print('\n')
    except cassandra.InvalidRequest as e:
        print(f'[Error] {e}')
    input('\n[Press any key...] ')


def create_keyspace(session, keyspace_name):
    try:
        session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} "
                        "WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}")
    except cassandra.AlreadyExists as e:
        print(f'[Error] {e}')
    input('\n[Press any key...] ')


def use_keyspace(session, keyspace):
    try:
        session.execute(f'USE {keyspace}')
    except cassandra.InvalidRequest as e:
        print(f'[Error] {e}')
    input('\n[Press any key...] ')


def create_table(session, table_name, keyspace_name):
    try:
        session.execute(f'CREATE TABLE {keyspace_name}.{table_name} (id int PRIMARY KEY, name varchar, age int)')
    except cassandra.InvalidRequest as e:
        print(f'[Error] {e}')
    except cassandra.AlreadyExists as e:
        print(f'[Error] {e}')
    input('\n[Press any key...] ')


if __name__ == '__main__':
    session = cassandra_connection()
    while True:
        os.system('clear')
        print('\n---| Cassandra DB |---\n')
        print('[1] - List All Keyspaces')
        print('[2] - List All Tables')
        print('[3] - Select Table')
        print('[4] - Create Keyspace')
        print('[5] - Use Keyspace')
        print('[6] - Create Table')
        print('[7] - Exit\n')
        try:
            res = int(input('Choice: '))

            if res == 1:
                list_all_keyspaces(session)
            elif res == 2:
                list_all_tables(session)
            elif res == 3:
                keyspace_name = str(input('Keyspace Name: '))
                table_name = str(input('Table Name: '))
                select_table(session, keyspace_name, table_name)
            elif res == 4:
                keyspace_name = str(input('Keyspace Name: '))
                create_keyspace(session, keyspace_name)
            elif res == 5:
                keyspace_name = str(input('Keyspace Name: '))
                use_keyspace(session, keyspace_name)
            elif res == 6:
                keyspace_name = str(input('Keyspace Name: '))
                table_name = str(input('Table Name: '))
                create_table(session, table_name, keyspace_name)
            elif res == 7:
                print('\nGoodbye :)')
                sys.exit(0)
            else:
                print('\nWrong option. Please try it again')
                input('\n[Press any key...] ')
        except ValueError as e:
            print(f'\n[ERROR] {e}')
            sys.exit(1)
