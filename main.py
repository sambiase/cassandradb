import sys

import cassandra
from cassandra.cluster import Cluster


def cassandra_connection():
    conn = Cluster(['localhost'], port=9042)
    session = conn.connect()
    return session


def list_all_keyspaces(session):
    keyspaces = session.execute('DESC KEYSPACES').all()
    for rows in keyspaces:
        print(rows.keyspace_name)


def create_keyspace(session, keyspace_name):
    try:
        session.execute(f"CREATE KEYSPACE [IF NOT EXISTS] {keyspace_name} "
                        "WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};")
    except cassandra.AlreadyExists as e:
        print(f'[Erro] {e}')

def use_keyspace(session,keyspace):
    try:
        session.execute(f'USE {keyspace}')
    except cassandra.InvalidRequest as e:
        print(f'[Erro] {e}')


def create_table(session, table_name):
    try:
        session.execute(f'CREATE TABLE {table_name} (id int PRIMARY KEY, name varchar, age int)')
    except cassandra.AlreadyExists as e:
        print(f'[Erro] {e}')


def list_all_tables(session):
    rows = session.execute('DESC TABLES')
    for row in rows:
        print(row.name)


def list_a_table(session, keyspace_name, table_name):
    try:
        session.execute(f"DESC {keyspace_name}.{table_name}")
    except cassandra.InvalidRequest as e:
        print(f'[Erro] {e}')


if __name__ == '__main__':
    session = cassandra_connection()
    while True:
        print('\n---| Cassandra DB |---\n')
        print('[1] - List All Keyspaces')
        print('[2] - List All Tables')
        print('[3] - List one Table')
        print('[4] - Create Keyspace')
        print('[5] - Use Keyspace')
        print('[4] - Create Table')
        print('[5] - Insert Data Into Table')
        print('[6] - List Table')
        print('[7] - Exit\n')
        res = int(input('Choice: '))

        if res == 1:
            list_all_keyspaces(session)
        elif res == 2:
            list_all_tables(session)
        elif res == 3:
            keyspace_name = str(input('Keyspace Name: '))
            table_name=str(input('Table Name: '))
            list_a_table(session,keyspace_name,table_name)
        elif res == 4:
            keyspace_name = str(input('Keyspace Name: '))
            create_keyspace(session,keyspace_name)
        elif res == 5:
            keyspace_name = str(input('Keyspace Name: '))
            use_keyspace(session, keyspace_name)
        elif res == 6:
            table_name = str(input('Table Name: '))
            create_table(session,table_name)
        elif res == 7:
            print('\nGoodbye :)')
            sys.exit(0)