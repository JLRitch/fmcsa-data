from string import ascii_lowercase
from datetime import date
import sqlite3
import urllib3
import certifi
import lxml
import pandas as pd
from bs4 import BeautifulSoup

def htmlToDf(inSoup):
    '''
    Parameters: (BS4 Object)
    
    htmlToDf creates a cleaned dataframe from the soup object.
    '''

    colNames = ['name', 'location', 'USDOT_number', 'safety']
    dataList = []
    for row in inSoup.table.find_all('tr'):
        tdList = [td.text for td in row.find_all('td')]
        dataList.append(tdList)
    print(dataList)
    readData = pd.DataFrame(dataList, columns=colNames).dropna()
    # split the location column 'city, state' into separate columns
    readData[['city','state']] = readData['location'].str.split(',',expand=True)
    print(readData)
    return(readData)


def dfToDB(inDF, dbConn, dbCur):
    '''
    Parameters: (a dataframe, a sqlite connection, and a sqlite cursor)
    
    The function checks to see if the USDOT already exists, if not it appends it to the DB, then commits changes.
    '''

    for index, row in inDF.iterrows():
        dotToCheck = row['USDOT_number']
        dbCur.execute(f"SELECT * FROM Companies WHERE USDOT_number = {dotToCheck}")
        check = dbCur.fetchone()
        if check is None:
            dbCur.execute(f''' INSERT INTO Companies (name, city, state, USDOT_number) 
            VALUES (?,?,?,?)''', (row['name'], row['city'], row['state'], dotToCheck))
        else:
            print(f'USDOT: {dotToCheck} was found in DB')
    dbConn.commit()


def main():
    #dataList = pd.DataFrame(columns= ['name','location', 'USDOT_number'])
    conn = sqlite3.connect('fmcsa.sqlite')
    cur = conn.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS Companies(id INTEGER PRIMARY KEY autoincrement, name TEXT, city TEXT, state TEXT, USDOT_Number TEXT)')
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

    for c in ascii_lowercase:
        print('---------------------------------------------')
        print(f'Searching for Companies starting with: {c}')
        print('---------------------------------------------')
        urlString = f'https://www.fmcsa.dot.gov/safety/passenger-safety/search-results/by-company?company={c}'
        page = http.request('GET',urlString)
        txt = page.data
        soup = BeautifulSoup(txt,'lxml')
        
        # if the html data is splita across multiple pages; denoted by class 'pager-current first' existing
        if soup.find_all('li', {'class': 'pager-current first'}):
            i = 0
            while True:
                print('---------------------------------------------')
                print(f'Searching for Companies starting with: {c} page {i}')
                print('---------------------------------------------')
                urlString = f'https://www.fmcsa.dot.gov/safety/passenger-safety/search-results/by-company?page={i}&company={c}'
                page = http.request('GET',urlString)
                txt = page.data
                soup = BeautifulSoup(txt,'lxml')
                
                if soup.find_all('li', {'class': 'pager-current last'}):
                    data = htmlToDf(soup)
                    dfToDB(data, conn, cur)
                    i = 0
                    break
                
                data = htmlToDf(soup)
                dfToDB(data, conn, cur)
                i += 1

        # if not the last page
        else:
            data = htmlToDf(soup)
            dfToDB(data, conn, cur)


if __name__ == '__main__':
    main()