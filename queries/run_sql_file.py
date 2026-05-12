
import sys
import duckdb


def main():
    if len(sys.argv) < 2:
        print('Uso: python queries/run_sql_file.py queries/arquivo.sql')
        return

    sql_file = sys.argv[1]

    with open(sql_file, 'r', encoding='utf-8') as file:
        query = file.read()

    result = duckdb.sql(query).df()

    print(result)


if __name__ == '__main__':
    main()
