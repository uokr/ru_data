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



-- config file (see README)
    must be in the same folder as ru_data.py
    
       
Class
--RuData
Methods
-- login
-- logoff
-- download_nsi
    downloading nsi for  1. and 2.
-- download_ratings
    downloading data for 3.
-- download_ru_data_main
    Main function for downloading all data
-- api_request
    Method for creating,sending and save request


Functions
-- get_dates (Defining dates)
    - NSI (1.): 10 previous days and 90 previous days on monday 
    - Ratings Dict (2.): current data always
    - Ratings Data (3.): current and 90 previous days on monday
        P.S. Rating/CompanyRatingsHist and Rating/SecurityRatingsHist provides data 
            for previous 5 days for every input date
-- save_json
    saves request response to json file

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

class RuData():
    
    def __init__(self,login = False,file_save_ext = "xml",download = False):
        
        self.config = configparser.ConfigParser()
        self.config.read("ru_data.ini");
        
        if file_save_ext not in ["json","xml"]:
            raise Exception("Unsupported file_save_ext. xml/json only possible now")
        self.file_save_ext = file_save_ext
        
        self.headers = None
        self.is_logged = False 
        
        if login:
            self.login()
        if download:
            self.main_download(ret_result=False,send=True)
        
    def login(self):
        LoginData = {"login":self.config["Account"]["Login"],"password":self.config["Account"]["Password"]}  
        LoginUrl = self.config["Settings"]["MainRequestUrl"]+"Account/Login"
        req=requests.post(LoginUrl,data=json.dumps(LoginData), headers={"Content-Type": "application/json"}).json()

        if "token" in req.keys():
            self.token = req["token"]
            self.headers = {"Content-Type": "application/json","Authorization":"Bearer "+ self.token}
            self.is_logged = True
    
    def logoff(self):
        requests.post(
            self.config["Settings"]["MainRequestUrl"]+"Account/Logoff",
            headers=self.headers)
    
    def nsi_data_path(self,method):
        return os.path.join(self.config["Settings"]["MainFolder"],'Today',method.split("/")[-1]+'.'+self.file_save_ext)
    def ratings_path(self,method,date):
        return os.path.join(self.config["Settings"]["MainFolder"],'Today',method.split("/")[-1]+'_'+date+'.'+self.file_save_ext)
    
    
    def save_data(self,request,method,path = None):
        if path is None:
            path = self.nsi_data_path(method)

        if self.file_save_ext == "json":
            save_json(request,path)
        elif self.file_save_ext == "xml":
            self.save_xml(request,path,method)
    
    def save_xml(self,request,path,method):
        xml = self.get_metadata_xml(method)+json_to_xml(request.json()) 
        with open(path,'w', encoding='utf-8') as f:
            f.write(xml)

    def get_metadata_xml(self,method,ret_format = "xml"):
        req = requests.get(self.config["Settings"]["MetadataUrl"]+method+".md")
        metadata = pd.read_html(markdown.markdown(req.text,extensions=['tables']))[-1][["Наименование","Тип"]]
        metadata.columns = ["name","type"]
        metadata["name"] = metadata["name"].str.upper()

        if ret_format == "df":
            return metadata
        else:
            metadata = metadata.to_dict("records")
            metadata = "".join(["""<column name="{name}" type="{type}"/>""".format(**d) for d in metadata])
            metadata = """<?xml version="1.0" encoding="utf-8"?><tableResult><metadata /><columns>""" + metadata + "</columns>"
            return metadata

     
    
    def api_request(self,method,**kwargs):

        data = {"count":100000}
        for key,value in kwargs.items():
            if key not in ["path"]:
                data[key] = value

        result = requests.post(
                self.config["Settings"]["MainRequestUrl"]+method,
                data=json.dumps(data),
                headers=self.headers)
        if "path" in kwargs.keys():
            path = kwargs["path"]
        else:
            path = None
       
        self.save_data(result,method,path=path)
        result = pd.DataFrame(result.json())
        
        return result

    def download_nsi(self,nsi_delta):
        update_date = (datetime.now()-timedelta(days=nsi_delta)).strftime('%Y-%m-%d')
        update_date = f"UPDATE_DATE > #{update_date}#"

        list_emitents = self.api_request("Info/EmitentsExt",filter=update_date)
        list_securities = self.api_request("Info/Securities",filter=update_date)

        list_ratings = self.api_request("Rating/ListRatings")
        list_scale_values = self.api_request("Rating/ListScaleValues")


        return {"list_emitents":list_emitents,
               "list_securities":list_securities,
               "list_ratings":list_ratings,
               "list_scale_values":list_scale_values}    
    
    def download_ratings(self,date):
    
        method = "Rating/CompanyRatingsHist"
        company_ratings = self.api_request(method,dateFrom="1900-01-01",dateTo=date,path=self.ratings_path(method,date))

        method = "Rating/SecurityRatingsHist"
        sec_ratings = self.api_request(method,dateFrom="1900-01-01",dateTo=date,path=self.ratings_path(method,date))


        return {"company_ratings":company_ratings,"sec_ratings":sec_ratings}


    def main_download(self,ret_result = False,send = False):
        
        result = {}
        
        if not self.is_logged:
            self.login()
            
        #delete previous files
        try:
            os.remove(os.path.join(self.config["Settings"]["MainFolder"],"TodayRatings.zip"))
        except Exception:
            pass
        clear_folder(self.config)
        
        dates = get_dates(start_date=None,end_date=None)
        for date in dates["dates"]:
            data = self.download_ratings(date)
            result.update(data)
        nsi = self.download_nsi(dates["nsi_delta"])
        result.update(nsi)
        
        #make archive
        shutil.make_archive(os.path.join(self.config["Settings"]["MainFolder"],"TodayRatings"), 'zip', os.path.join(self.config["Settings"]["MainFolder"],"Today"))
        time.sleep(10)
        #delete downloaded files
        clear_folder(self.config)

        #send
        if send:
            if self.config["Mailing"]["Type"] == "Script":
                os.system(self.config["Mailing"]["ScriptPath"])
            elif config["Mailing"]["Type"] == "Yandex":
                send_yandex_driver(self.config)

        #finish
        
        self.logoff()
        
        if not ret_result:
            result = None
        return result
            
            
def send_yandex_driver(config):

    filepath = os.path.join(config["Settings"]["MainFolder"],config["Settings"]["AttachmentFileName"])
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
    

def get_dates_period(start_date,end_date):
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
        dates = get_dates_period(start_date,end_date)
    else:
        
        if datetime.now().weekday() == 0:
            dates = get_dates_from_now(90)
            nsi_delta = 90
        else:
            dates = get_dates_from_now()
            nsi_delta = 10
    return {"dates":dates,"nsi_delta":nsi_delta}




    


def json_to_xml(data_json):
    xml = dicttoxml(data_json,custom_root="rows", item_func=lambda x:"row",attr_type=False)
    xml = xml.decode()
    return xml

            
def save_json(request,path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(request.json(), f, ensure_ascii=False, indent=4)



def clear_folder(config):
    files = glob.glob(os.path.join(config["Settings"]["MainFolder"],"Today","*"))
    for f in files:
        os.remove(f)
        
if __name__ == "__main__":
    RuData(download = True)

