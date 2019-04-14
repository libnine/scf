import os, shutil, string, json, pandas as pd, pyodbc as db


if __name__ == '__main__':
    with open('config.json', 'r') as f:
        sql = json.loads(f.read())
        config = f'DRIVER={sql["driver"]}SERVER={sql["server"]}DATABASE={sql["database"]}PWD={sql["pwd"]}'

    file_name = [f for f in os.listdir() if f.endswith('.xls') or f.endswith('.xlsx')]
    
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    df = pd.read_excel(os.path.join(__location__, file_name[0]), converters={'Number': str})

    grades = []
    
    for g in df['Grade']:
        try:
            grades.append(int(g))
        
        except ValueError as _:
            try:
                grades.append(int(g.strip(string.ascii_letters)) - 2)

            except ValueError as _:
                grades.append(int(float(g[:3])) - 2)

    con = db.connect(config)
    results = []

    # get the amount of rows (range of length of any column)
    for n in range(len(grades)):
        try:
            query = f"""
            SELECT * FROM vintagecp_cards
            WHERE set_name = '{df['Set'][n]}' AND grader = '{df['Grader'][n]}'
            AND grade = '{grades[n]}' AND year = {df['Year'][n]} AND sport = '{df['Sport'][n]}'
            AND card_number = '{df['Number'][n]}'
            """
            
            rows = pd.read_sql_query(query, con)

            if rows.empty:
                for x in range(grades[n] - 1, 0, -1):
                    query = f"""
                    SELECT * FROM vintagecp_cards
                    WHERE set_name = '{df['Set'][n]}' AND grader = '{df['Grader'][n]}'
                    AND grade = '{x}' AND year = {df['Year'][n]} AND sport = '{df['Sport'][n]}'
                    AND card_number = '{df['Number'][n]}'
                    """
        
                    new_rows = pd.read_sql_query(query, con)

                    if not new_rows.empty:
                        for r in range(len(new_rows)):
                            results.append([df['Sport'][n], new_rows['grade'][r], df['Grade'][n], df['Grader'][n], new_rows['player'][r], df['Item'][n], new_rows['set_name'][r], new_rows['year'][r], new_rows['card_number'][r], new_rows['avg_price'][r], new_rows['last_price_realized'][r], new_rows['high_price'][r]])
                        break
                
                if not new_rows.empty:
                    continue
            
            if not rows.empty:
                for r in range(len(rows)):
                    results.append([df['Sport'][n], rows['grade'][r], df['Grade'][n], df['Grader'][n], rows['player'][r], df['Item'][n], rows['set_name'][r], rows['year'][r], rows['card_number'][r], rows['avg_price'][r], rows['last_price_realized'][r], rows['high_price'][r]])
                continue
            
            child_status = 0

            varchar_card_query = f"""
            SELECT * FROM vintagecp_cards
            WHERE set_name = '{df['Set'][n]}' AND grader = '{df['Grader'][n]}'
            AND grade = '{grades[n]}' AND year = {df['Year'][n]} AND sport = '{df['Sport'][n]}'
            AND card_number like '{df['Number'][n]}%' AND ISNUMERIC(card_number) <> 1
            """

            varchar_card_rows = pd.read_sql_query(varchar_card_query, con)

            if varchar_card_rows.empty:
                for i in range(grades[n] - 1, 0, -1):
                    varchar_card_query_child = f"""
                    SELECT * FROM vintagecp_cards
                    WHERE set_name = '{df['Set'][n]}' AND grader = '{df['Grader'][n]}'
                    AND grade = '{i}' AND year = {df['Year'][n]} AND sport = '{df['Sport'][n]}'
                    AND card_number like '{df['Number'][n]}%' AND ISNUMERIC(card_number) <> 1
                    """

                    varchar_card_rows_child = pd.read_sql_query(varchar_card_query_child, con)

                    if not varchar_card_rows_child.empty:
                        if len(varchar_card_rows_child) >= 1:
                            varchar_card_rows_child.sort_values('last_price_realized', ascending=True, inplace=True)
                            varchar_card_rows_child = varchar_card_rows_child.reset_index(drop=True)
                            results.append([df['Sport'][n], varchar_card_rows_child['grade'][0], df['Grade'][n], df['Grader'][n], varchar_card_rows_child['player'][0], df['Item'][n], varchar_card_rows_child['set_name'][0], varchar_card_rows_child['year'][0], varchar_card_rows_child['card_number'][0], varchar_card_rows_child['avg_price'][0], varchar_card_rows_child['last_price_realized'][0], varchar_card_rows_child['high_price'][0]])
                            child_status = 1
                            break

            # if a successful match was found in the nested for loop, exit the current loop
            if child_status == 1:
                continue

            if not varchar_card_rows.empty:
                if len(varchar_card_rows) >= 1:
                    varchar_card_rows.sort_values('last_price_realized', ascending=True, inplace=True)
                    varchar_card_rows = varchar_card_rows.reset_index(drop=True)
                    results.append([df['Sport'][n], varchar_card_rows['grade'][0], df['Grade'][n], df['Grader'][n], varchar_card_rows['player'][0], df['Item'][n], varchar_card_rows['set_name'][0], varchar_card_rows['year'][0], varchar_card_rows['card_number'][0], varchar_card_rows['avg_price'][0], varchar_card_rows['last_price_realized'][0], varchar_card_rows['high_price'][0]])
                    continue
            
            results.append([df['Sport'][n], 'n/a', df['Grade'][n], df['Grader'][n], 'n/a', df['Item'][n], df['Set'][n], df['Year'][n], df['Number'][n], 'n/a', 'n/a', 'n/a'])

        except Exception as e:
            print(f'{e}\n')
            continue

    csv_file = file_name[0].split('.')[0] + '.csv'

    with open(csv_file, 'w') as dump:
        dump.write('sport,grade_used,Grade,Grader,player,Item,Set,Year,Number,Average,Realized,high_price\n')
        for row in range(len(results)):
            str_row = [str(x) for x in results[row]]
            dump.write(f"{','.join(str_row)}\n")

    if not os.path.exists('C:/input_history/'):
        os.mkdir('C:/input_history/')
    
    shutil.move(file_name[0], os.path.join('C:/input_history/', file_name[0]))
    
    try:
        os.remove(file_name[0])

    except:
        pass
