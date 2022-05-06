"""
lib for downloading following data from RU Data API:
1. NSI: download_nsi
1.1. Info/EmitentsExt
1.2. Info/Securities
2. Ratings Dict: download_nsi
2.1. Rating/ListRatings
2.2. Rating/ListScaleValues
3. Ratings Data: download_ratings
3.1. Rating/CompanyRatingsHist
3.2. Rating/SecurityRatingsHist
4. send Zip file using Yandex: send_yandex_driver


Attributes
-- config file (see README)
    must be in the same folder as ru_data.py
-- main_folder
    folder to save data
-- MainRequestUrl
    api url
    
       
Methods
-- login
-- get_dates (Defining dates)
    - NSI (1.): 10 previous days and 90 previous days on monday 
    - Ratings Dict (2.): current data always
    - Ratings Data (3.): current and 90 previous days on monday
        P.S. Rating/CompanyRatingsHist and Rating/SecurityRatingsHist provides data 
            for previous 5 days for every input date
-- save_json
    saves request response to json file
-- download_nsi
    downloading nsi for  1. and 2.
-- download_ratings
    downloading data for 3.
-- download_ru_data_main
    Main function for downloading all data

example:
    see download_ru_data_main
    see ru_data.ipynb
"""
import time
import requests
import json
import shutil
from datetime import datetime,date,timedelta
import os
#from smtplib import SMTP_SSL
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import configparser
import pandas as pd
import markdown
from dicttoxml import dicttoxml
import glob

config = configparser.ConfigParser()
config.read("ru_data.ini");
main_folder = config["Settings"]["MainFolder"]
MainRequestUrl = config["Settings"]["MainRequestUrl"]

def login():
    LoginData={"login":config["Account"]["Login"],"password":config["Account"]["Password"]}
    LoginUrl=MainRequestUrl+"Account/Login"
    req=requests.post(LoginUrl,data=json.dumps(LoginData), headers={"Content-Type": "application/json"}).json()
    
    if "token" in req.keys():
        req["headers"] = headers = {"Content-Type": "application/json","Authorization":"Bearer "+ req["token"]}
    else:
        req = None
    
    return req

def get_dates_for_history(start_date,end_date):
    start_date = datetime.strptime(start_date,"%d.%m.%Y").date()
    end_date = datetime.strptime(end_date,"%d.%m.%Y").date()
    delta = (end_date - start_date).days

    if delta == 0:
        dates = [end_date]
    else:
        dates=[(end_date - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(0,delta,3)]
    return dates

def get_dates_from_now(delta = None):
    if delta is None:
        delta = 1
    dates=[(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(0,delta,3)]
    return dates

def get_dates(start_date = None,end_date=None):
    ct_none = [start_date,end_date].count(None)
    if  ct_none == 1:
        print("Need to define both start_date and end_date)")
        return None
    elif ct_none == 0:
        dates = get_dates_for_history(start_date,end_date)
    else:
        
        if datetime.now().weekday() == 0:
            dates = get_dates_from_now(90)
            nsi_delta = 90
        else:
            dates = get_dates_from_now()
            nsi_delta = 10
    return {"dates":dates,"nsi_delta":nsi_delta}


def nsi_data_path(method,file_ext):
    return os.path.join(main_folder,'Today',method.split("/")[-1]+'.'+file_ext)
    
def nsi_emitents_securities(method,update_date,file_ext):

    data = {"count":100000,"filter": update_date}
    if method in ["Info/EmitentsExt"]:
        data["inn_as_string"] = "TRUE"
    
    result = requests.post(
            MainRequestUrl+method,
            data=json.dumps(data),
            headers=headers)
    save_data(result,nsi_data_path(method,file_ext),method=method)
    return result
    

def download_nsi(headers,nsi_delta,file_ext):
    update_date = (datetime.now()-timedelta(days=nsi_delta)).strftime('%Y-%m-%d')
    update_date = "UPDATE_DATE > #{update_date}#".format(update_date=update_date)
    

    list_emitents=nsi_emitents_securities("Info/EmitentsExt",update_date,file_ext)
    list_securities=nsi_emitents_securities("Info/Securities",update_date,file_ext)

  

    method = "Rating/ListRatings"
    list_ratings=requests.post(
            MainRequestUrl+method,
            data=json.dumps({"count":100000}),
            headers=headers)
    save_data(list_ratings,nsi_data_path(method,file_ext),method = method)

    method = "Rating/ListScaleValues"
    list_scale_values=requests.post(
            MainRequestUrl+method,
            data=json.dumps({"count":100000}), 
            headers=headers)
    save_data(list_scale_values,nsi_data_path(method,file_ext),method = method)
    
    return {"list_emitents":pd.DataFrame(list_emitents.json()),
           "list_securities":pd.DataFrame(list_securities.json()),
           "list_ratings":pd.DataFrame(list_ratings.json()),
           "list_scale_values":pd.DataFrame(list_scale_values.json())}
    
def download_ratings(headers,date,file_ext):
    
    method = "Rating/CompanyRatingsHist"
    
    company_ratings=requests.post(
        MainRequestUrl+method,
        data=json.dumps({"count":100000,                                                                      
          "dateFrom":"1900-01-01","dateTo":date}),
        headers=headers)
    save_data(company_ratings,os.path.join(main_folder,'Today',method.split("/")[-1]+'_'+date+'.'+file_ext),method = method)
 

    method = "Rating/SecurityRatingsHist"
    sec_ratings=requests.post(
            MainRequestUrl+method,
            data=json.dumps({"count":100000,
                "dateFrom":"1900-01-01","dateTo":date}),
            headers=headers)
    save_data(company_ratings,os.path.join(main_folder,'Today',method.split("/")[-1]+'_'+date+'.'+file_ext),method = method)
    
    return {"company_ratings":pd.DataFrame(company_ratings.json()),"sec_ratings":pd.DataFrame(sec_ratings.json())}

def get_metadata_xml(method,ret_df = False):
    req = requests.get(config["Settings"]["MetadataUrl"]+method+".md")
    metadata = pd.read_html(markdown.markdown(req.text,extensions=['tables']))[-1][["Наименование","Тип"]]
    metadata.columns = ["name","type"]
    metadata["name"] = metadata["name"].str.upper()
    
    if ret_df:
        return metadata
    else:
        metadata = metadata.to_dict("records")
        metadata = "".join(["""<column name="{name}" type="{type}"/>""".format(**d) for d in metadata])
        metadata = """<?xml version="1.0" encoding="utf-8"?><tableResult><metadata /><columns>""" + metadata + "</columns>"
        return metadata

def json_to_xml(data_json):
    xml = dicttoxml(data_json,custom_root="rows", item_func=lambda x:"row",attr_type=False)
    xml = xml.decode()
    return xml

def save_data(request,path,method = None):
    file_extension = path.split(".")[-1] 
    if file_extension == "json":
        save_json(request,path)
    elif file_extension == "xml":
        save_xml(request,path,method)
            
def save_xml(request,path,method):
    xml = get_metadata_xml(method)+json_to_xml(request.json()) 
    with open(path,'w', encoding='utf-8') as f:
        f.write(xml)

        
def save_json(request,path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(request.json(), f, ensure_ascii=False, indent=4)


def send_yandex_driver(subject,filepath):

    # Compose attachment
    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(filepath,"rb").read() )
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(filepath))

    # Compose message
    msg = MIMEMultipart()
    msg['From'] = config["Yandex"]["Login"]
    msg['To'] = config["Mailing"]["To"]
    msg["Subject"]="RatingsHistory"
    msg.attach(part)

    # Send mail
    smtp = smtplib.SMTP_SSL('smtp.yandex.ru')
    smtp.connect('smtp.yandex.ru')
    smtp.login(config["Yandex"]["Login"], config["Yandex"]["Password"])
    smtp.sendmail(config["Yandex"]["Login"], config["Mailing"]["To"], msg.as_string())
    smtp.quit()

def clear_folder():
    files = glob.glob(os.path.join(config["Settings"]["MainFolder"],"Today","*"))
    for f in files:
        os.remove(f)
    
def download_ru_data_main(send = False,ret_result = False):
    """
    downloads,save and send data (if send=True)
    
    returns dict with nsi and ratings (for last used data) if ret_result=True
    
    """
    result = {}
    req_login=login()
    if req_login is None:
        print ("Login Failed")
    else:
        headers = req_login["headers"]

        #delete previous file
        try:
            os.remove(os.path.join(main_folder,"TodayRatings.zip"))
        except Exception:
            pass
        #delete previous files
        clear_folder()
        dates = get_dates(start_date=None,end_date=None)
        for date in dates["dates"]:
            data = download_ratings(headers,date,"xml")
            result.update(data)
        nsi = download_nsi(headers,dates["nsi_delta"],"xml")
        result.update(nsi)
        
        #make archive
        shutil.make_archive(os.path.join(main_folder,"TodayRatings"), 'zip', os.path.join(main_folder,"Today"))
        time.sleep(10)
        #delete downloaded files
        clear_folder()

        #send
        if send:
            if config["Mailing"]["Type"] == "Script":
                os.system(config["Mailing"]["ScriptPath"])
            elif config["Mailing"]["Type"] == "Yandex":
                send_yandex_driver('RatingsHistory',os.path.join(main_folder,"TodayRatings.zip"))

        #finish
        errors=0
        log_off = requests.post(
            MainRequestUrl+"Account/Logoff",
            headers=headers)
        
        if not ret_result:
            result = None
        return result
        
