import requests, re, json, pyodbc as db, datetime as dt, xml.etree.ElementTree as ET


with open('config.json', 'r') as f:
    sql = json.loads(f.read())
    config = f'DRIVER={sql["driver"]}SERVER={sql["server"]}DATABASE={sql["database"]}PWD={sql["pwd"]}'

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

if __name__ == '__main__':
    con = db.connect(config)
    cur = con.cursor()

    with open('options.json', 'r') as o:
        o = json.loads(o.read())
        sport_type = o['category']
        set_name = o['set_name']
    
    for n in range(1, 11):
        r = requests.post('http://ws.vintagecardprices.com', data={"queryXml": xml_req(set_name=set_name, sport_type=sport_type, player_name="rose", card_number=20, year=1978, grader="psa", grade=n)}, auth=('', ''))

        if r.status_code == 404:
            print('404 error.\n')
            r.raise_for_status()
            break

        response = ET.fromstring(r.text)

        try:
        #     Player, avg_price, high_price, last_price_realized, grader, grade, year, card_number, card_id
            els = [el.text for el in response[4][0]]

            if len(els) < 5:
                print('No data for grade {}'.format(n))
                continue

            player_formatted = els[0].split(', ')
            player_formatted = player_formatted[1] + " " + player_formatted[0]
            cur.execute('INSERT INTO vintagecp_cards (player, avg_price, high_price, last_price_realized, grader, grade, year, card_number, card_id, set_name, date_inserted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (player_formatted, els[1], els[2], els[3], els[4], els[5], els[6], els[7], els[8], set_name, dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            
            cur.commit()
            print('Success {}.'.format(n))
        
        except IndexError as _:
            print('No results.')
        
        except db.IntegrityError as _:
            print('Duplicate data being skipped.')

    cur.close()
    del cur
    con.close()
