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


def create_keyspace(session):
    session.execute("CREATE KEYSPACE [IF NOT EXISTS] football_teams "
                    "WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};")


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

if __name__ == '__main__':
    session = cassandra_connection()
    while True:
        print('\n---| Cassandra DB |---\n')
        print('[1] - List All Keyspaces')
        print('[2] - Create Keyspace')
        print('[3] - Use Keyspace')
        print('[4] - Create Table')
        print('[5] - Insert Data Into Table')
        print('[6] - List Table\n')
        res = int(input('Choice: '))

        if res == 1:
            list_all_keyspaces(session)
        elif res == 2:
            create_keyspace(session)
        elif res == 3:
            res_keyspace = str(input('Type Keyspace Name: '))
            use_keyspace(session,res_keyspace)
        elif res == 4:
            table_name = str(input('Table Name: '))
            create_table(session,table_name)