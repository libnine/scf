import json, pandas as pd, pyodbc as db, sqlalchemy as sa


if __name__ == '__main__':
    # sql server credentials
    with open('config.json', 'r') as f2:
        odbc_cfg = json.loads(f2.read())
        odbc_sql = f'DRIVER={odbc_cfg["driver"]}SERVER={odbc_cfg["server"]}DATABASE={odbc_cfg["database"]}PWD={odbc_cfg["pwd"]}'

    with open('flask.json', 'r') as f:
        sql = json.loads(f.read())
        config = f'DRIVER={sql["driver"]}SERVER={sql["server"]}DATABASE={sql["database"]}PWD={sql["pwd"]}'

    engine = sa.create_engine(f'mssql+pyodbc://{sql["user"]}:{sql["pwd"]}@{sql["server"]}:1433/{sql["database"]}?driver={sql["driver"]}')

    df = pd.read_sql_query(f'SELECT DISTINCT title FROM vintagecp', engine)
    titles = [t.replace("'", "''") for t in df['title']]
    
    con = db.connect(odbc_sql)
    cur = con.cursor()

    for n in range(len(titles)):
        df = pd.read_sql_query(f"SELECT TOP 1 player_link FROM vintagecp WHERE title = '{titles[n]}'", engine)
        links = [l for l in df['player_link']]

        for link in links:
            link = link.replace('Baseball-Card-Value-Prices.htm', '')
            link = link.replace('Basketball-Card-Value-Prices.htm', '')
            link = link.replace('Football-Card-Value-Prices.htm', '')

            set_split = [wrd for wrd in list(filter(None, link.split('/')[-1].split('-'))) if wrd in titles[n].split(' ')]
            set_name_child = ' '.join([name for name in set_split if not name.isdigit()])

            print(f'{set_name_child} : {titles[n]}')

            cur.execute("UPDATE vintagecp SET set_name_child = ? WHERE title = ?", set_name_child, titles[n])
            cur.commit()
    
    cur.close()
    con.close()
