import requests, json, pyodbc as db, datetime as dt, xml.etree.ElementTree as ET


class Vintage:
    def __init__(self):
        global config

        with open('config.json', 'r') as f:
            sql = json.loads(f.read())
            config = f'DRIVER={sql["driver"]}SERVER={sql["server"]}DATABASE={sql["database"]}PWD={sql["pwd"]}'

    def xml_req(self, year, set_name, grader, grade, player_name, card_number, sport_type):
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

    def query(self, id, set_name, sport_type, player_name, card_number, year, sport):
        con = db.connect(config)
        cur = con.cursor()

        if player_name.split(' ')[-1].lower() == 'breaker':
            player_name = player_name.split(' ')[-3].lower()

        else:
            player_name = player_name.split(' ')[-1].lower()

        with open('auth.json', 'r') as auth:
            auth = json.loads(auth.read())

        for n in range(1, 11):
            r = requests.post('http://ws.vintagecardprices.com', data={"queryXml": self.xml_req(set_name=set_name, sport_type=sport_type, player_name='', card_number=card_number, year=year, grader="psa", grade=n)}, auth=(auth['user'], auth['pw']))

            if r.status_code == 404:
                print('404 error.\n')
                r.raise_for_status()
                break

            response = ET.fromstring(r.text)

            try:
                # Player, avg_price, high_price, last_price_realized, grader, grade, year, card_number, card_id
                els = [el.text for el in response[4][0]]

                player_formatted = els[0].split(', ')
                player_formatted = player_formatted[1] + " " + player_formatted[0]

                cur.execute(f"""UPDATE vintagecp_cards
                SET avg_price = '{els[1]}', high_price = '{els[2]}', last_price_realized = '{els[3]}', grader = '{els[4]}', grade = '{els[5]}', date_modified = '{dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                WHERE set_name = '{set_name}' AND sport = '{sport_type}' AND year = {els[6]} AND card_number = '{els[7]}' AND grader = 'psa' AND grade = '{n}' 

                IF @@ROWCOUNT = 0
                    INSERT INTO vintagecp_cards (player, avg_price, high_price, last_price_realized, grader, grade, year, card_number, card_id, set_name, vcp_id, sport, date_inserted) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (player_formatted, els[1], els[2], els[3], els[4], els[5], els[6], card_number, els[8], set_name, int(id), sport, dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

                cur.commit()
                # print('Success {}.'.format(n))

            except IndexError as _:
                pass

            except db.IntegrityError as _:
                pass

            except Exception as e:
                with open('flask_funcs.log', 'a') as f:
                    f.write(f'{dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n{e}\n\n')
