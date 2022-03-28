import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from datetime import date, datetime

url = "https://teatreprincipalinca.com"

df_logs = pd.DataFrame(columns=['usuari', 'data', 'codi', 'texte', 'projecte'])

mesos = {
    'gener': "01",
    'febrer': "02",
    'març': "03",
    'abril': "04",
    'maig': "05",
    'juny': "06",
    'juliol': "07",
    'agost': "08",
    'septembre': "09",
    'octubre': "10",
    'novembre': "11",
    'desembre': "12"
}


def change_date_format(df):
    for key, value in df.iterrows():
        row = df['Data'][key]
        m = mesos[row.split()[1]]
        myDate = row.split()[0] + "-" + m + "-" + row.split()[2]
        df['Data'][key] = myDate

    df['Data'] = pd.to_datetime(df['Data'])
    return df


try:
    doc = requests.get(url, timeout=25)
    soup = BeautifulSoup(doc.text, 'lxml')
    if not soup:
        raise Exception
except:
    log = pd.Series(["Ines", datetime.now(
    ), 1, "No hi havia connexió amb la web", "Teatre Principal Inca"], df_logs.columns)
    df_logs = df_logs.append(log,  ignore_index=True)


try:
    div = soup.find(
        'div', attrs={'uk-position-relative uk-visible-toggle hp-slider hp-event-feed'})
    ul = div.find('ul')
    li = ul.find_all('li')
except:
    log = pd.Series(["Ines", datetime.now(
    ), 3, "Revisar la web. No es troben les dades", "Teatre Principal Inca"], df_logs.columns)
    df_logs = df_logs.append(log,  ignore_index=True)


llista_obres = []

try:
    for obra in li:
        btn = obra.find('a', attrs={"uk-button uk-button-default"})
        link = url+btn.get('href')
        doc = requests.get(link, timeout=25)
        soup = BeautifulSoup(doc.text, 'lxml')

        div_preu = soup.find(
            'div', attrs={'class': 'uk-width-1-2@m uk-text-right mt-25'})
        preus = div_preu.span.b.next_sibling
        preus = preus.rstrip('\€')
        preus = preus.replace(" ", "")
        if(preus != 'Gratuït'):
            preusList = preus.split(',')
        else:
            preusList = ['0']

        titol = soup.find('h1').text

        categoria = soup.find('h4').text.strip()
        categoriaList = categoria.split(' ')

        sala = soup.find('h5').text

        div_data = soup.find('div', attrs={'class': 'uk-width-expand@m'})
        data = div_data.span.b.next_sibling

        llista_obres.append([titol, categoriaList, data, sala, preusList])
except:
    log = pd.Series(["Ines", datetime.now(
    ), 3, "Revisar la web. No es troben les dades", "Teatre Principal Inca"], df_logs.columns)
    df_logs = df_logs.append(log,  ignore_index=True)


try:
    df = pd.DataFrame(llista_obres)
    df.set_axis(['Titol', 'Categoria', 'Data', 'Sala',
                'Preus'], axis=1, inplace=True)
    df = change_date_format(df)
except:
    log = pd.Series(["Ines", datetime.now(
    ), 4, "Error amb el tractat de dades. Revisar script", "Teatre Principal Inca"], df_logs.columns)
    df_logs = df_logs.append(log,  ignore_index=True)


try:
    df_preus = pd.DataFrame(df['Preus'].tolist(), index=df.index)
    df_preus = pd.concat([df, df_preus], axis=1)
    df_preus.drop(['Categoria', 'Data', 'Sala', 'Preus'], axis=1, inplace=True)
    df_preus = pd.melt(df_preus, id_vars=['Titol'], value_vars=[0, 1, 2, 3])
    df_preus.drop(['variable'], axis=1, inplace=True)
    df_preus.dropna(inplace=True)
    df_preus.reset_index(drop=True, inplace=True)
    df_preus.rename(columns={'value': 'preu'}, inplace=True)
except:
    log = pd.Series(["Ines", datetime.now(
    ), 4, "Error amb el tractat de dades. Revisar script", "Teatre Principal Inca"], df_logs.columns)
    df_logs = df_logs.append(log,  ignore_index=True)


try:
    df_categories = pd.DataFrame(df['Categoria'].tolist(), index=df.index)
    df_categories = pd.concat([df, df_categories], axis=1)
    df_categories.drop(['Categoria', 'Data', 'Sala',
                       'Preus'], axis=1, inplace=True)
    df_categories = pd.melt(df_categories, id_vars=['Titol'], value_vars=[0])
    df_categories.drop(['variable'], axis=1, inplace=True)
    df_categories.dropna(inplace=True)
    df_categories.reset_index(drop=True, inplace=True)
    df_categories.rename(columns={'value': 'categoria'}, inplace=True)
except:
    log = pd.Series(["Ines", datetime.now(
    ), 4, "Error amb el tractat de dades. Revisar script", "Teatre Principal Inca"], df_logs.columns)
    df_logs = df_logs.append(log,  ignore_index=True)


try:
    df_obres = df[['Titol', 'Data', 'Sala']]
except:
    log = pd.Series(["Ines", datetime.now(
    ), 4, "Error amb el tractat de dades. Revisar script", "Teatre Principal Inca"], df_logs.columns)
    df_logs = df_logs.append(log,  ignore_index=True)


dialect = 'mysql+pymysql://root:Bigdata2122@localhost:3306/scrapping'
sqlEngine = create_engine(dialect)


def add_db():
    query = 'SELECT COUNT(*) FROM ines_obres'
    total_obres_inici = list(sqlEngine.connect().execute(query))[0][0]

    query = str(
        f""" INSERT IGNORE INTO ines_obres VALUES {','.join([str(i) for i in list(df_obres.to_records(index=False))])}""")
    sqlEngine.connect().execute(query)

    query = str(
        f""" INSERT IGNORE INTO ines_preus VALUES {','.join([str(i) for i in list(df_preus.to_records(index=False))])}""")
    sqlEngine.connect().execute(query)

    query = str(
        f""" INSERT IGNORE INTO ines_categories VALUES {','.join([str(i) for i in list(df_categories.to_records(index=False))])}""")
    sqlEngine.connect().execute(query)

    query = 'SELECT COUNT(*) FROM ines_obres'
    total_obres_fi = list(sqlEngine.connect().execute(query))[0][0]

    if total_obres_inici == total_obres_fi:
        global df_logs
        log = pd.Series(["Ines", datetime.now(
        ), 2, "No hi havia dades noves per inserir", "Teatre Principal Inca"], df_logs.columns)
        df_logs = df_logs.append(log,  ignore_index=True)
    else:
        log = pd.Series(["Ines", datetime.now(
        ), 0, "Dades inserides sense problema", "Teatre Principal Inca"], df_logs.columns)
        df_logs = df_logs.append(log,  ignore_index=True)


try:
    add_db()
except:
    log = pd.Series(["Ines", datetime.now(
    ), 5, "Error inserint les dades. Revisar base de dades", "Teatre Principal Inca"], df_logs.columns)
    df_logs = df_logs.append(log,  ignore_index=True)


df_logs.to_sql('datalog', con=sqlEngine, if_exists='append', index=False)
