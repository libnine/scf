import requests, re, json, pyodbc as db, datetime as dt
from multiprocessing.dummy import Pool as ThreadPool


# sql server credentials
with open('config.json', 'r') as f:
    sql = json.loads(f.read())
    config = f'DRIVER={sql["driver"]}SERVER={sql["server"]}DATABASE={sql["database"]}PWD={sql["pwd"]}'

# settings for pulling vintagecardprices.com data
with open('options.json', 'r') as o:
    o = json.loads(o.read())
    category = o['category']
    set_name = o['set_name']

    sport = {
        1 : "baseball-card-price-guide.htm",
        2 : "basketball-card-price-guide.htm",
        3 : "football-card-price-guide.htm",
        4 : "hockey-card-price-guide.htm",
        5 : "boxing-card-price-guide.htm",
        6 : "non-sports-card-price-guide.htm"
    }

def set_scrape(link, title, year, original_link):
    # format link to php route to scrape all the players (default is 25)
    try:
        set_id = link.split('/')[4]
        link = f'http://www.vintagecardprices.com/set-profile/index.php?set_id={set_id}&limit=10000+'

        r = requests.get(link).text

        dump = re.findall('table id="setItems"(.+?)</table>', r, re.S)
        players = [p.replace('&amp; ', '') for p in re.findall('.htm">(.+?)</a></td>', dump[0], re.S)]
        players_links = ['http://www.vintagecardprices.com' + p for p in re.findall('href="(.+?)"', dump[0]) if p.endswith('.htm')]
        card_number = [p.split('-')[-5] for p in players_links]

        if len(players) == 0:
            return

        con = db.connect(config)
        cur = con.cursor()

        for cn, p, pl in zip(card_number, players, players_links):
            print(f'{cn}, {p}, {pl}')
            cur.execute('INSERT INTO vintagecp (category, sport, set_name, set_link, title, year, card_number, player, player_link, date_inserted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', 
            (category, sport[category].split('-')[0], set_name, original_link, title, year, cn, p, pl, dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            cur.commit()
        
        cur.close()
        del cur
        con.close()
    
    except db.Error as db_e:
        print(f'\n{db_e}\nPassing {link}.\n')
        return

    except IndexError as _:
        return

if __name__ == '__main__':
    try:
        url = f'http://www.vintagecardprices.com/{sport[category]}?action=search&cat={category}&set_name={set_name}'

        r = requests.get(f'http://www.vintagecardprices.com/{sport[category]}?action=search&cat={category}&set_name={set_name}').text

        dump = re.findall('<table class="listing">(.+?)</table>', r)
        titles = re.findall('title="(.+?)"', dump[0])
        links = ["http://www.vintagecardprices.com" + l for l in re.findall('href="(.+?)"', dump[0]) if l.endswith('.htm')]
        years = [y for y in re.findall('</a></td><td>(.+?)</td><td', dump[0]) if y.isdigit()]
        
        thread_list = [[links[i], titles[i], years[i], url] for i in range(len(links))]

        # multithreading; if this fails, uncomment below and use single thread
        pool = ThreadPool(3)
        p = pool.starmap(set_scrape, thread_list)
        pool.close()
        pool.join()

        ## single thread; use if multithreading does not work
        # for n in range(len(links)):
        #     set_scrape(links[n], titles[n], years[n], url)

    except db.Error as db_e:
        print(f'\nSQL error.\n{db_e}\n')

    except Exception as e:
        print(e)

    