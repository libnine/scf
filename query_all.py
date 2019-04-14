import requests, re, json, pyodbc as db, xml.etree.ElementTree as ET


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
    with open('config.json', 'r') as f:
        sql = json.loads(f.read())
        config = f'DRIVER={sql["driver"]}SERVER={sql["server"]}DATABASE={sql["database"]}PWD={sql["pwd"]}'

    con = db.connect(config)
    cur = con.cursor()

    cur.execute('SELECT set_name, category, card_number, year FROM dbo.vintagecp')
    sel_query = cur.fetchall()

    for n in range(len(sel_query)):
        for l in range(1, 11):
            r = requests.post('http://ws.vintagecardprices.com', data={"queryXml": xml_req(set_name=sel_query[n][0].lower(), sport_type=sel_query[n][1], player_name='', card_number=sel_query[n][3], year=sel_query[n][4], grader="psa", grade=l)}, auth=('wta', 'R2rGPrHL8')).text

            response = ET.fromstring(r)
 
            try:
                # Player, avg_price, high_price, last_price_realized, grader, grade, year, card_number, card_id
                els = [el.text for el in response[4][0]]
                print(els)
            
            except IndexError as _:
                print('No results.')
                continue

    cur.close()
    del cur
    con.close()
