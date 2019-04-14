import requests, json, pyodbc as db, datetime as dt, xml.etree.ElementTree as ET


def xml_req(year, set_name, grader, grade, player_name, card_number, sport_type):
    xml = '''
        <query id="1">
            <year>{year}</year>
            <set_name>{set_name}</set_name>
            <grader>{grader}</grader>
            <grade>{grade}</grade>
            <player_name>{player_name}</player_name>
            <card_number>{card_number}</card_number>
            <sport_type>{sport_type}</sport_type>
        </query>
        '''.format(year=year, set_name=set_name, grader=grader, grade=grade, player_name=player_name, card_number=card_number, sport_type=sport_type)
    
    return xml

sport = {
    1 : "baseball",
    2 : "basketball",
    3 : "football",
    4 : "hockey",
    5 : "boxing",
    6 : "non-sports"
}

if __name__ == '__main__':
    with open('config.json', 'r') as f:
        sql = json.loads(f.read())
        config = f'DRIVER={sql["driver"]}SERVER={sql["server"]}DATABASE={sql["database"]}PWD={sql["pwd"]}'

    con = db.connect(config)
    cur = con.cursor()

    cur.execute("SELECT set_name, card_number, year, id, category FROM dbo.vintagecp WHERE set_name = 'Red Heart Dog Food' AND category = 1 AND year = 1954 AND id between 158666 AND 158682")
    sel_query = cur.fetchall()

    for n in range(len(sel_query)):
        for l in range(1, 11):
            r = requests.post('http://ws.vintagecardprices.com', data={"queryXml": xml_req(set_name=sel_query[n][0], sport_type=1, player_name='', card_number=sel_query[n][1], year=sel_query[n][2], grader="psa", grade=l)}, auth=('', ''))

            response = ET.fromstring(r.text)

            try:
                # Player, avg_price, high_price, last_price_realized, grader, grade, year, card_number, card_id
                els = [el.text for el in response[4][0]]

                player_formatted = els[0].split(', ')
                player_formatted = player_formatted[1] + " " + player_formatted[0]

                cur.execute(f"""UPDATE vintagecp_cards
                SET avg_price = '{els[1]}', high_price = '{els[2]}', last_price_realized = '{els[3]}', grader = '{els[4]}', grade = '{els[5]}', date_modified = '{dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                WHERE set_name = '{sel_query[n][0]}' AND sport = '{sport[sel_query[n][4]]}' AND year = {els[6]} AND card_number = '{els[7]}' AND grader = 'psa' AND grade = '{n}' 

                IF @@ROWCOUNT = 0
                    INSERT INTO vintagecp_cards (player, avg_price, high_price, last_price_realized, grader, grade, year, card_number, card_id, set_name, vcp_id, sport, date_inserted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (player_formatted, els[1], els[2], els[3], els[4], els[5], els[6], sel_query[n][1], els[8], sel_query[n][0], sel_query[n][3], sport[sel_query[n][4]], dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                
                cur.commit()
                print(f'Success. {player_formatted} - {els[3]}')

            except IndexError as _:
                print('No results.')

            except db.IntegrityError as _:
                print('Duplicate data being skipped.')

    cur.close()
    del cur
    con.close()
