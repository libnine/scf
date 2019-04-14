import json, requests, time, uuid, threading, pandas as pd, sqlalchemy as sa, datetime as dt
from flask import Flask, jsonify, render_template, request
from multiprocessing.dummy import Pool as ThreadPool
from flask_cors import CORS
from flask_funcs import *


# flask init
API_KEY = ''
app = Flask(__name__, static_folder='./vue/dist/static', template_folder='./vue/dist')
app.config.from_object(__name__)
CORS(app)

# load mssql config
with open('flask.json', 'r') as f:
    sql = json.loads(f.read())
    config = f'DRIVER={sql["driver"]}SERVER={sql["server"]}DATABASE={sql["database"]}PWD={sql["pwd"]}'

engine = sa.create_engine(f'mssql+pyodbc://{sql["user"]}:{sql["pwd"]}@{sql["server"]}:1433/{sql["database"]}?driver={sql["driver"]}')

# home route
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>', methods=['GET'])
def index(path):
    if app.debug:
        return requests.get('http://localhost:8080/{}'.format(path)).text

    return render_template('index.html')

@app.route('/sets', methods=['GET'])
def sets():
    df = pd.read_sql_query(f"SELECT DISTINCT set_name, year FROM vintagecp ORDER BY set_name, year asc", engine)
    return jsonify([s for s in df['set_name']], 200)

# receive post request here with vue data
@app.route('/sql', methods=['POST'])
def vue():
    category = request.get_json()['category']
    yr = request.get_json()['year']
    set_name = request.get_json()['set_name']

    sport = {
        1 : "baseball",
        2 : "basketball",
        3 : "football",
        4 : "hockey",
        5 : "boxing",
        6 : "non-sport"
    }

    try:
        if int(yr) < 1900 or int(yr) > int(dt.datetime.today().year):
            engine.execute('INSERT INTO vintagecp_executions (set_name, year, sport, status, duration, thread_id, date_run) VALUES (?, ?, ?, ?, ?, ?, ?)', (set_name, yr, sport[category], 'failed', None, str(uuid.uuid4()), dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            return jsonify('Invalid year.', 404)

    except Exception as _:
        engine.execute('INSERT INTO vintagecp_executions (set_name, year, sport, status, duration, thread_id, date_run) VALUES (?, ?, ?, ?, ?, ?, ?)', (set_name, yr, sport[category], 'failed', None, str(uuid.uuid4()), dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        return jsonify('Invalid year.', 404)

    def vue_job(category, year, set_name):
        try:
            thread_id = str(uuid.uuid4())
            now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            start = time.time()

            engine.execute('INSERT INTO vintagecp_executions (set_name, year, sport, status, duration, thread_id, date_run) VALUES (?, ?, ?, ?, ?, ?, ?)', (set_name, yr, sport[category], 'running', None, thread_id, now))

            df = pd.read_sql_query(f"SELECT * FROM vintagecp WHERE category = '{category}' AND set_name = '{set_name}' AND year = '{year}'", engine)

            # set_name, sport_type, player_name, card_number, year
            thread_list = [[df['id'][i], df['set_name'][i], category, df['player'][i], df['card_number'][i], yr, sport[category]] for i in range(len(df))]

            if len(thread_list) == 0 or df.empty:
                engine.execute('INSERT INTO vintagecp_executions (set_name, year, sport, status, duration, thread_id, date_run) VALUES (?, ?, ?, ?, ?, ?, ?)', (set_name, yr, sport[category], 'complete', ('{:.2f}'.format(time.time() - start)), thread_id, now))
                return

            f = Vintage()

            with ThreadPool(4) as pool:
                pool.starmap(f.query, thread_list)

            engine.execute('INSERT INTO vintagecp_executions (set_name, year, sport, status, duration, thread_id, date_run) VALUES (?, ?, ?, ?, ?, ?, ?)', (set_name, yr, sport[category], 'complete', ('{:.2f}'.format(time.time() - start)), thread_id, now))

        except db.OperationalError as dboe:
            with open('flask.log', 'a') as log:
                log.write(f'{now}\n{dboe}\n\n')

            time.sleep(10)

        except sa.exc.OperationalError as oe:
            with open('flask.log', 'a') as log:
                log.write(f'{now}\n{oe}\n\n')

            time.sleep(10)

        except Exception as e:
            with open('flask.log', 'a') as log:
                log.write(f'{now}\n{e}\n\n')

            engine.execute('INSERT INTO vintagecp_executions (set_name, year, sport, status, duration, thread_id, date_run) VALUES (?, ?, ?, ?, ?, ?, ?)', (set_name, yr, sport[category], 'partial completion', ('{:.2f}'.format(time.time() - start)), thread_id, now))

    t = threading.Thread(target=vue_job, args=(category, yr, set_name,))
    t.start()

    resp = jsonify('Thread started.', 200)
    resp.headers.add('Access-Control-Allow-Origin', '*')

    return resp

if __name__ == '__main__':
    app.run(host='0.0.0.0')
