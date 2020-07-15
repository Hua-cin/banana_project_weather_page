import pandas as pd
import MySQLdb
import json
from datetime import date
from datetime import timedelta
import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
import urllib
import os
import re
import random
import sys
import time


# setting
db_name = "fruveg"  # define database name
table_name = "Daniel_weather"  # define weather table name
station_table_name = "Daniel_weather_station_list"  # define weather station table name

# setting abnormal station, and don't request this list
reject_station_num =['466850','C0SA60']

# line notify
token = "R8rrPYATV5xEFyXUsi1oPrO2IscWXEP56e0FubeUnX3"

# program start
start_time = datetime.datetime.now()

# headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122'
                  ' Safari/537.36'
}

#
port = 3306


def main():
    """ main function """

    # check log file exist ornot
    func_check_file("auto crawlar log")

    # write_log("test")
    msg = "01.Start auto Crewlar program."
    write_log("{}".format(msg)) # ~~~~~
    lineNotify(token, msg)

    # catch station dict
    station_dict = catch_station_list()
    write_log("{}".format("02.Load Database data (station infmoration)."))   # ~~~~~

    reject_station_list = []
    for i in  reject_station_num:   # transfer reject_station_num to reject_station_list
        reject_station_list.append(int(get_key(station_dict["Station_num"], i)[0]))

    # crawler the newest one weather information from database
    newest_date, newest_city, newest_station_num, newest_station_name = catch_newest_db_data()
    write_log("{}".format("03.Load Database data (newest weather information)."))    # ~~~~~

    # transfer the next station number, next of the newest station from database
    newest_station_key = int(get_key(station_dict["Station_num"], newest_station_num)[0])+1
    start_station_key = newest_station_key % len(station_dict["Station_num"])
    if start_station_key == 0:
        start_station_key = len(station_dict["Station_num"])

    delta_day = timedelta(days=1)  # set timedelta : 1 day

    # confirm need to add one day or not. if the day don't have full station data, the day need request again
    if start_station_key != 1:  # don't have full station data
        start_year = int(newest_date.year)
        start_month = int(newest_date.month)
        start_day = int(newest_date.day)

    else:   # have full station data
        start_year = int((newest_date + delta_day).year)
        start_month = int((newest_date + delta_day).month)
        start_day = int((newest_date + delta_day).day)

    start_point = int("{:4d}{:0>2d}".format(start_year, start_month))   # start request year-month

    # print(start_point)

    # setting stop point(next month)
    if int(datetime.datetime.now().month)+1 == 13:  # if this month = 12, next month = 12+1 = 13, need to add 1 year and change month to 1
        stop_point = int("{:4d}{:0>2d}".format(int(datetime.datetime.now().year)+1, 1))
    else:   # this month = 1~11, and next month = 2~12, no need to change year
        stop_point = int("{:4d}{:0>2d}".format(int(datetime.datetime.now().year), int(datetime.datetime.now().month)+1))

    # print(stop_point)

    request_station_num = start_station_key # request station number
    request_point = start_point # request point (year * 100 + month)
    request_day = start_day # request day



    # print(request_station_num)
    # print(request_point)
    # print(request_day)



    now_point = datetime.datetime.now().year * 100 + datetime.datetime.now().month  # now point (year *100 + month)
    # print(now_point)

    db = connect_db(db_name)

    while True: # request point is not the same with stop point (next month)

        # data is the newest, no need to update
        if datetime.date.today() == newest_date + delta_day \
                and int(get_key(station_dict["Station_num"], newest_station_num)[0]) == len(station_dict["Station_num"]):
            msg = "04.Database data is newest. No update."
            write_log("{}".format(msg))  # ~~~~~
            lineNotify(token, msg)
            break

        if check_web_update(request_point, request_day):
            msg = "05.Web has not been updated. No update."
            write_log("{}".format(msg))  # ~~~~~
            lineNotify(token, msg)
            break

        while True: # request station still need to request in this round

            if request_station_num not in reject_station_list:   # if station not in reject station list

                # request url
                url = "https://e-service.cwb.gov.tw/HistoryDataQuery/MonthDataController.do?command=viewMain&station={}&" \
                      "stname={}&datepicker={:0>4d}-{:0>2d}"\
                    .format(station_dict["Station_num"][str(request_station_num)],
                            urllib.parse.quote_plus(station_dict["Station_name"][str(request_station_num)]).replace("%", "%25"),
                            request_point//100,
                            request_point%100)
                # print(url)

                if request_point == now_point:  # if request month is this month, only need to check this month past data
                    request_table(db, request_point, request_station_num, station_dict, url,
                                  head = request_day, tail = datetime.datetime.now().day-1)

                else:   # need to request all the data for before month
                    request_table(db, request_point, request_station_num,  station_dict, url,
                                  head=request_day, tail=None)
                if request_station_num % 55 == 3:
                    time.sleep(10)

            request_station_num = request_station_num + 1   # the next station

            if request_station_num == (len(station_dict["Station_num"])+1): # if request station num out of station list, break
                break

        request_station_num = 1 # initialize to day 1 of month

        if (request_point % 100) == 12: # if requested month = 12, need to change to next year and from month = 1
            request_point = request_point + 100 - 11
        else :  # if requested month = 1~11, the next month = 2~12
            request_point = request_point + 1

        if request_point >= stop_point: # if request point (year * 100 + mont) >= stop point, break (stop)
            close_db(db)
            now = datetime.datetime.now()
            msg = "06.Database update finish."
            lineNotify(token, msg)
            write_log("{}".format(msg))  # ~~~~~
            break
        request_day = 1 # new month, and form day 1


    msg = "07.Close auto crewlar program.\n"
    write_log("{}".format(msg))  # ~~~~~
    lineNotify(token, msg)

def connect_db(db_name):
    """connect database"""

    db_conn = MySQLdb.connect(host='34.92.102.171', user='dbuser', passwd='20200428', db=db_name, port=port, charset='utf8')

    return db_conn  # return db name


def close_db(db):
    """close database"""
    db.close()


def catch_station_list():
    """ catch station information from database"""

    # db = connect_db(db_name)    # select db_name
    db_conn = MySQLdb.connect(host='34.92.102.171', user='dbuser', passwd='20200428', db=db_name, port=port, charset='utf8')
    # cursor = db.cursor()  # create cursor

    try:
        sql_str = "SELECT * FROM {0}.{1};".format(db_name, station_table_name)

        station_df = pd.read_sql(sql=sql_str, con=db_conn, index_col="num")
        new_station_df = station_df.loc[:, 'Station_num':'Data_start_date']
        new_station_df = new_station_df.drop("Address", axis=1)
        new_station_json = new_station_df.to_json()
        new_station_dict = json.loads(new_station_json)

    except Exception as err:
        msg = "08.Unable to fetch data from db. Program stop!! {}".format(err)
        write_log("{}".format(msg))  # ~~~~~
        lineNotify(token, msg)
        db_conn.close()
        sys.exit(0)

    db_conn.close()

    return new_station_dict # return station list (dict format)


def catch_newest_db_data():
    """catch the newest data from db table"""

    # db = connect_db(db_name)    # select db_name
    db_conn = MySQLdb.connect(host='34.92.102.171', user='dbuser', passwd='20200428', db=db_name, port=port, charset='utf8')
    cursor = db_conn.cursor()  # create cursor
    try:
        sql_str = "SELECT Date_, City, Station_num, Station FROM {}.{} order by Date_ desc, Station_num desc limit 1;"\
            .format(db_name, table_name)  # select the newest data

        cursor.execute(sql_str) # execute
        newest_data_tuple = cursor.fetchall()   # catch the newest data

    except Exception as err:
        msg = "09.Unable to fetch data from db. Program stop. {}\n".format(err)
        write_log("{}".format(msg))  # ~~~~~
        lineNotify(token, msg)
        db.close()
        sys.exit(0)

    db_conn.close()

    return newest_data_tuple[0] # return the newest information fron database


def  get_key(dict, value):
    """ search key from value """
    return [k for k, v in dict.items() if v == value]   # return key


def request_table(db, request_point, request_station_num, station_dict, url,  head=1, tail=None):
    """request table content"""
    t = random.randint( 1, 1)
    for x in range(t):
        # print("\rdelay {:>2d} second?".format(t-x-1), end = '')
        time.sleep(1)

    now = datetime.datetime.now()
    # print("\n turn on time -> {}      ".format(now-start_time))
    # print("request time -> {}      ".format(now))

    try:
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        df = pd.read_html(str(soup), header=1)[0]

    except Exception as err:    # if request error, sleep 600 second and try again after sleep
        msg = "10.Unable to request data. {}".format( err)
        lineNotify(token, msg)  # if request error, send Line notify
        write_log("{}".format(msg))  # ~~~~

        t = 300
        time.sleep(t)

        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        df = pd.read_html(str(soup), header=1)[0]

        msg = "11.Request data normal, continue program."
        lineNotify(token, msg)  # if request error, send Line notify
        write_log("{}".format(msg))  # ~~~~~

    except:
        msg = "12.Unable to request data again, stop program.\n"
        lineNotify(token, msg)  # if request error, send Line notify
        write_log("{}".format(msg))  # ~~~~~
        sys.exit(0)


    df2 = df.loc[head:tail] # catch request dataframe

    # insert to database
    if df2.shape[0] == 0:  # if web don't have data, this web pass
        pass
    else:
        # db = connect_db(db_name)  # connect to db

        for i in range(head,head + df2.shape[0]):   # if all don't have data, pass
            if  df2.loc[i][7]=="..." and df2.loc[i][21]=="..." and df2.loc[i][27]=="..." \
                    and df2.loc[i][1]=="..." and df2.loc[i][13]=="..." and df2.loc[i][16]=="...":
                # 7:temp, 21:precipitation, sunshine_hours, 1:pressure, 13:humidity, 16:wind_speed
                pass
            else:
                new_row_data_tupe = data_confirm(request_point, df2.loc[i], request_station_num, station_dict)
                print(new_row_data_tupe)

                insert_db_data(new_row_data_tupe, db)   # insert to db


def data_confirm(request_point, row_data, request_station_num, station_dict):
    """confirm table data format and convert """

    fun = lambda x:x if re.match(r'[\d]+',x) else -99  # lambda function, confirm number or not
    new_row_data = []   # initial row_data_tuple

    new_row_data.append("") # insert group
    date = "{:0>4}-{:0>2}-{:0>2}".format(request_point//100, request_point%100, row_data[0]) # date
    new_row_data.append(date)  # insert date
    new_row_data.append(station_dict["City"][str(request_station_num)]) # insert city
    new_row_data.append(request_station_num)   # insert S_ID
    new_row_data.append(station_dict["Station_num"][str(request_station_num)]) # insert station_num
    new_row_data.append(station_dict["Station_name"][str(request_station_num)]) # insert station name
    new_row_data.append(fun(row_data[1]))   # insert pressure
    new_row_data.append(fun(row_data[7]))   # insert temp
    new_row_data.append(fun(row_data[13]))   # insert humidity
    new_row_data.append(fun(row_data[16]))   # insert wind_speed

    if fun(row_data[21]) == "T":
        precipitation_value = 0
    elif not re.match(r'[\d]+',row_data[21]):
        precipitation_value = -99
    else :
        precipitation_value = fun(row_data[21])
    new_row_data.append(precipitation_value)  # insert precipitation
    new_row_data.append(fun(row_data[27]))   # insert sunshine_hours
    new_row_data = tuple(new_row_data) # convert to tuple

    return new_row_data   # return new tuple data


def insert_db_data( data_tuple, db ):
    """insert data"""
    cursor = db.cursor()  # create cursor

    try:
        db.autocommit(False)  # setup autocommit false

        now = datetime.datetime.now()
        sql_str = 'insert into {}(list_group, Date_, City, S_ID, Station_num, Station, Avg_Presure_hPa, Avg_Temp_C, ' \
                  'Avg_Humidity, Avg_Wind_speed, Sum_Precipitation, Sum_Sunshine_hours, log_dt) ' \
                  'values(\'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}\',\'{}\', {}, {}, {}, {}, {}, \'{}\');' \
            .format(table_name, data_tuple[0], data_tuple[1], data_tuple[2], data_tuple[3], data_tuple[4],
                    data_tuple[5], data_tuple[6], data_tuple[7], data_tuple[8], data_tuple[9], data_tuple[10], data_tuple[11], now)
        cursor.execute(sql_str)  # start insert data
        db.autocommit(True)  # setup autocommit true

        msg = "13.Insert data to db, {}, {}, {}".format(data_tuple[1], data_tuple[4], data_tuple[5])
        write_log("{}".format(msg))  # ~~~~~

    except Exception as err:
        msg = "14.Unable insert data to db, {}\n".format(err)
        write_log("{}".format(msg))  # ~~~~~
        sys.exit(0)



def check_web_update(request_point, request_day):
    """check weather update or not"""

    try:
        url = "https://e-service.cwb.gov.tw/HistoryDataQuery/MonthDataController.do?command=viewMain&station=466910&stname=%25E9%259E%258D%25E9%2583%25A8&datepicker={:0>4d}-{:0>2d}".format(
            request_point // 100, request_point % 100)
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        df = pd.read_html(str(soup), header=1, encoding = 'utf-8')[0]
        for i in range(df.shape[0]):
            if df["測站氣壓(hPa)"][i] == "...":
                first_need_update_date = i
                break
            else:
                first_need_update_date = df.shape[0]
        # print(first_need_update_date)
        # print(request_day)

        time.sleep(10)
        url = "https://e-service.cwb.gov.tw/HistoryDataQuery/MonthDataController.do?command=viewMain&station=C1Z240&stname=%25E4%25B8%25AD%25E5%25B9%25B3%25E6%259E%2597%25E9%2581%2593&datepicker={:0>4d}-{:0>2d}".format(
            request_point // 100, request_point % 100)
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        df = pd.read_html(str(soup), header=1, encoding = 'utf-8')[0]
        for j in range(df.shape[0]):
            if df["降水量(mm)"][j] == "...":
                last_need_update_date = j
                break
            else:
                last_need_update_date = df.shape[0]
        # print(last_need_update_date)

    except Exception as err:
        msg = "15.Unable request weather web, program stop and close. {}\n".format(err)
        # print(msg)
        lineNotify(token, msg)  # if request error, send Line notify
        write_log("{}".format(msg))  # ~~~~~
        sys.exit(0)

    if first_need_update_date == request_day or last_need_update_date == request_day :
        return True
    else :
        return False

def lineNotify(token, msg):
    """  """
    url = "https://notify-api.line.me/api/notify"
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {'message': msg}
    r = requests.post(url, headers=headers, params=payload)
    return r.status_code


def write_log(log):
    """ log function """
    now = datetime.datetime.now()
    today = datetime.date.today()
    print("{}, {}".format(now, log))


def func_check_file(sub_keyword):
    resource_path = r'./'+sub_keyword
    if os.path.exists(resource_path) :
        pass
    else :
        os.mkdir(resource_path)


if __name__ == "__main__":
    main()