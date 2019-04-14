import requests, json, pyodbc as db, pandas as pd


with open('config.json', 'r') as f:
    sql = json.loads(f.read())
    config = f'DRIVER={sql["driver"]}SERVER={sql["server"]}DATABASE={sql["database"]}PWD={sql["pwd"]}'


if __name__ == '__main__':
    con = db.connect(config)
    cur = con.cursor()

    ## schemas
    ## vintagecp_cards - id, player, avg_price, high_price, last_price_realized, grader, grade, year, card_number, card_id, set_link, set_name, title, date_inserted
    # cur.execute('CREATE TABLE vintagecp_cards (id int IDENTITY(1,1) PRIMARY KEY, player varchar(250), avg_price decimal(18,2), high_price decimal(18,2), last_price_realized decimal(18,2), grader varchar(100), grade varchar(25), year int, card_number varchar(100), card_id int, set_name varchar(250), date_inserted datetime default CURRENT_TIMESTAMP, CONSTRAINT unique_cards UNIQUE(player, grader, grade, year, card_number, card_id, set_name))')
    
    ## vintagecp - id, set_link (original_link), title, year, card_number, player, player_link, datetime
    # cur.execute('CREATE TABLE vintagecp (id int IDENTITY(1, 1) PRIMARY KEY, category int, sport varchar(100), set_name varchar(250), set_link varchar(500), title varchar(500), year int, card_number varchar(100), player varchar(250), player_link varchar(500), date_inserted datetime default CURRENT_TIMESTAMP)')
    
    ## add unique constraint to vintagecp to avoid dups
    # cur.execute('ALTER TABLE vintagecp ADD UNIQUE (set_name, title, year, card_number, player, player_link)')
    
    ## trunc vintagecp
    # cur.execute('TRUNCATE TABLE vintagecp')

    ## trunc vintagecp_cards
    # cur.execute('TRUNCATE TABLE vintagecp_cards')

    ## drop vintagecp
    # cur.execute('DROP TABLE vintagecp')

    # # select from tables
    # df = pd.read_sql_query('SELECT * FROM vintagecp', con)
    # df.to_csv('vintagecp.csv')

    # df = pd.read_sql_query('SELECT * FROM vintagecp_cards', con)
    # df.to_csv('vintagecp_cards.csv')

    # df = pd.read_sql_query('SELECT DISTINCT set_name FROM vintagecp ORDER BY set_name asc', con)
    # print([x for x in df['set_name']])

    # cur.commit()

    cur.close()
    del cur
    con.close()
