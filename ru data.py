
import time
import requests
import json
import openpyxl
import shutil
from datetime import datetime,date,timedelta
#from selenium import webdriver
#from selenium.webdriver.common.keys import Keys
#from selenium.common.exceptions import NoSuchElementException
#from selenium.webdriver.support import expected_conditions as EC
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.common.by import By
#from selenium.webdriver.support.ui import Select
import os
import xlwt
import shutil
import pyautogui

def send_yandex_driver(driver_path,login,password,address,subject,files):
    driver = webdriver.Chrome(driver_path) 
    #driver.get('https://mail.yandex.ru/');
    driver.get("https://passport.yandex.ru/auth?origin=hostroot_auth_experiment&retpath=https%3A%2F%2Fmail.yandex.ru%3Fconnection_id%3Diface-1508481626719-65396382")
    driver.find_element_by_name('login').send_keys(login)
    driver.find_element_by_name('passwd').send_keys(password)
    driver.find_element_by_name('passwd').send_keys(Keys.RETURN)
    time.sleep(10)
    driver.get('https://mail.yandex6.ru/lite/compose/retpath=inbox')
    time.sleep(10)
    addr=address
    driver.find_element_by_class_name('b-form-input__input').send_keys(addr)
    time.sleep(1)
    driver.find_element_by_name('subj').send_keys(subject.decode('utf-8'))
    for file in files:
        driver.find_element_by_css_selector('[class*=\"b-compose__more-attach__link b-compose__more-attach__link-i\"]').click()
        time.sleep(5)
        print file
        driver.find_element_by_css_selector('[class*=\"b-compose__file\"]').send_keys(file.decode("cp1251"))
   
    time.sleep(5)
    driver.find_element_by_name('doit').click()
    time.sleep(5)
    driver.quit()
import pyperclip
def send_cbr_driver(driver_path,login,password,address,subject,files):
    driver = webdriver.Chrome(driver_path) 
    #driver.get('https://mail.yandex.ru/');
    driver.get("https://owa.cbr.ru")
    driver.find_element_by_xpath("//*[@id='chkBsc']").click()
    
    driver.find_element_by_name('username').send_keys(login)
    driver.find_element_by_name('password').send_keys(password)
    time.sleep(2)
    driver.find_element_by_name('password').send_keys(Keys.RETURN)
    time.sleep(10)
    #driver.find_element_by_id('_ariaID_39').click()
    
    #driver.find_element_by_xpath("//*[@id='_ariaId_39']").click()
    #driver.get('https://mail.yandex.ru/lite/compose/retpath=inbox')
    time.sleep(10)
    addr=address
    driver.find_element_by_xpath("//*[@id='lnkHdrnewmsg']").click()
    time.sleep(2)
    xpath="//*[@id='txtto']"
    driver.find_element_by_xpath(xpath).send_keys(addr)
    #driver.find_element_by_xpath(xpath).send_keys(Keys.RETURN)
    
    xpath="//*[@id='txtsbj']"
    #driver.find_element_by_name('subj').send_keys(subject.decode('utf-8'))
    time.sleep(5)
    driver.find_element_by_xpath(xpath).send_keys(subject.decode('utf-8'))

    for file in files:
        xpath="//*[@id='frm']/table/tbody/tr[2]/td[3]/table/tbody/tr[2]/td/table/tbody/tr[1]/td/table/tbody/tr[10]/td[1]/a"
        driver.find_element_by_xpath(xpath).click()
        driver.find_element_by_xpath("//*[@id='attach']").click()
        time.sleep(10)
        pyperclip.copy(file)
        #pyperclip.paste()
        pyautogui.hotkey("ctrl","v")
        time.sleep(10)
        pyautogui.press("enter")
        time.sleep(10)
        #driver.find_element_by_xpath("/html/body/div[2]/div/div[3]/div[3]/div/div[1]/div[2]/div[7]/div/div/div[2]/div[1]/span[1]/div[3]/button").click()
        print file
        #driver.find_element_by_css_selector('[class*=\"b-compose__file\"]').send_keys(file.decode("cp1251"))
        xpath="//*[@id='attachbtn']"
        driver.find_element_by_xpath(xpath).click()
        time.sleep(10)
        xpath="//*[@id='lnkHdrdone']"
        driver.find_element_by_xpath(xpath).click()
           
    time.sleep(5)
    xpath="//*[@id='lnkHdrsend']"
    driver.find_element_by_xpath(xpath).click()
    #driver.find_element_by_name('doit').click()
    time.sleep(5)
    driver.quit()    
def SaveRequestData(request,filename,date,headers,DatesType,fileextesion):
    curdir=os.path.dirname(os.path.abspath(__file__))

    fnamesuffix=("_"+date) if filename in ["CompanyRatingsHist","SecurityRatingsHist"] else ""

    fname=curdir+"\\Today\\"+filename+fnamesuffix+"."+fileextesion

    if headers["Accept"]=="application/json":
        req=request.json()

        rows=req["Rows"]

        cols=[(x.keys())[0] for x in req["Columns"]]

        if filename=="ListEmitents":
            poks=["SHORTNAME_RUS","SHORTNAME_ENG","INN","OGRN","OKPO","FININSTID","ID_EMITENT","COUNTRY_OKSM","COUNTRY_NAME_RUS","LEI_CODE"]
##            poks=cols
            #print cols
            pos=[cols.index(x) for x in poks]
            #req2=[]
            #for r in rows:
                
            #req=[[r[pos[0]],r[pos[1]],r[pos[2]],r[pos[3]],r[pos[4]],r[pos[5]]] for r in rows]
##            req=rows
            req=[[r[p] for p in pos] for r in rows]
        else:
            poks=["FINTOOLID","ISIN","REG_CODE","FININSTID"]
##            poks=cols
            pos=[cols.index(x) for x in poks]    
            req=[[r[pos[0]],r[pos[1]],r[pos[2]],r[pos[3]]] for r in rows]
##            req=rows                   
        req=[poks]+req
        if fileextesion=="xls":
            wb=xlwt.Workbook()
            sheet = wb.add_sheet("Data") 
            for i in range(len(req)):
                for j in range(len(req[i])):
                    sheet.write(i,j,req[i][j])

        else:
            wb = openpyxl.Workbook()
            ws = wb.active
            for dat in req:
                ws.append(dat)
        wb.save(fname)     
    else:
        req= request.content
  
        f=open(fname, "w")
   

        repl="rudata" +(filename if filename not in ["CompanyRatingsHist","SecurityRatingsHist"] else filename+"Last")
        f.write(req.replace("row",repl))
 
        f.close()
    
    #files.append("C:\\Users\\Al-rubayiMM\\Documents\\Python\\RU_DATA\\" + fname)
    files.append(fname)
errors=1
while errors>0:        
    try:
        print "begin"
        DatesType=["Today",""][0]
        if datetime.now().weekday()==0:
            DatesType=["Today",""][1]
        else:
            DatesType=["Today",""][0]
  
        todaydate= [datetime.today().strftime('%Y-%m-%d')]
        startdate = date(2020, 01, 20) 
        enddate = date(2020, 01, 30)
        #!!!!!!!
        enddate=datetime.today()
        startdate=enddate-timedelta(90)
        
        delta = enddate - startdate

        dates=[(enddate - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(0,delta.days + 1,3)]
        dates=todaydate if DatesType=="Today" else dates
       
        headerslogin = {"Content-Type": "application/json","Accept":"application/json"}
     
        LoginData={"login":"cbr-dofr-45","password":"Ngks72T"}
        LoginUrl="https://new-datahub.efir-net.ru/hub.axd/Account/Login"
        MainRequestUrl="https://new-datahub.efir-net.ru:443/hub.axd/"
        req=requests.post(LoginUrl,data=json.dumps(LoginData), headers=headerslogin)
        req=req.json()
        print req
        #dates=[]
        if "Token" not in req.keys():
            print "Login Failed"
            print req
            print ""
        else:
            print "Logged In"
            headers = {"Content-Type": "application/json","Accept":"application/xml"}  
            global files
            files=[]
            DataToken={"token":req["Token"]}
            Token="token="+DataToken["token"]
            
            print len(dates)
            for date in dates:
                if DatesType=="":
                    print len(dates),dates.index(date)+1,date
                #print date
                #print "CompanyRatingsHist"
                headers = {"Content-Type": "application/json","Accept":"application/xml"}  
                filterdates="dateFrom="+"1900-01-01"+"T00%3A00%3A00.0000000Z&dateTo="+date+"T00%3A00%3A00.0000000Z"
                ratingshistoryurl=MainRequestUrl+"Rating/CompanyRatingsHist?"+filterdates+"&"+Token            
                req=requests.get(ratingshistoryurl,data=json.dumps(DataToken), headers=headers)

                SaveRequestData(req,"CompanyRatingsHist",date,headers,DatesType,"xml")

                #print "SecurityRatingsHist"
                headers = {"Content-Type": "application/json","Accept":"application/xml"}  
                filterdates="dateFrom="+"1900-01-01"+"T00%3A00%3A00.0000000Z&dateTo="+date+"T00%3A00%3A00.0000000Z"
                ratingshistoryurl=MainRequestUrl+"Rating/SecurityRatingsHist?"+filterdates+"&"+Token
                #print ratingshistoryurl
                req=requests.get(ratingshistoryurl,data=json.dumps(DataToken), headers=headers)
                SaveRequestData(req,"SecurityRatingsHist",date,headers,DatesType,"xml")
                
                if dates.index(date)<len(dates)-1:
                    time.sleep(1)
            print "ListRatings"
            headers = {"Content-Type": "application/json","Accept":"application/xml"}  
            ratingshistoryurl=MainRequestUrl+"Rating/ListRatings?"+Token
            req=requests.get(ratingshistoryurl,data=json.dumps(DataToken), headers=headers)
            SaveRequestData(req,"ListRatings",date,headers,DatesType,"xml")

            print "ListEmitents" 
            headers = {"Content-Type": "application/json","Accept":"application/xml"}
            filteremitents="UPDATE_DATE%20%3E%3D%20%23"+(datetime.now()-timedelta(days=90)).strftime('%Y-%m-%d')+"T00%3A00%3A00.0000000Z%23"
            ratingshistoryurl=MainRequestUrl+"Info/EmitentsExt?filter="+filteremitents+"&"+Token
    
            req=requests.get(ratingshistoryurl,data=json.dumps(DataToken), headers=headers)
            SaveRequestData(req,"ListEmitents",date,headers,DatesType,"xml")

            print "ListSecurities"
            headers = {"Content-Type": "application/json","Accept":"application/xml"}
            filteremitents="UPDATE_DATE%20%3E%3D%20%23"+(datetime.now()-timedelta(days=90)).strftime('%Y-%m-%d')+"T00%3A00%3A00.0000000Z%23"
            ratingshistoryurl=MainRequestUrl+"Info/Securities?filter="+filteremitents+"&"+Token
            req=requests.get(ratingshistoryurl,data=json.dumps(DataToken), headers=headers)
            SaveRequestData(req,"ListSecurities",date,headers,DatesType,"xml")

                
            logoffurl="https://new-datahub.efir-net.ru/hub.axd/Account/Logoff"
            req=requests.post(logoffurl,data=json.dumps(DataToken), headers=headerslogin)
            print "Logged Out"
            shutil.make_archive("C:\Users\LatifullinPR\Documents\Python\RU_DATA\\TodayRatings", 'zip', "C:\Users\LatifullinPR\Documents\Python\RU_DATA\\Today\\")
            print "Sending"
            for f in files:
                os.remove(f)
            files=["C:\Users\LatifullinPR\Documents\Python\RU_DATA\\TodayRatings.zip"]
            mailto="al-rubayimm@cbr.ru;latufullinpr@cbr.ru"
            #if datetime.now().hour==12:
            #    mailto+="; kai6@cbr.ru"
            print errors

            try:
                os.system("C:\\Users\\LatifullinPR\\Documents\\Python\\RU_DATA\\SendMail.vbs")
                time.sleep(10)  
            except Exception as e:
                print str(e)
            errors=0
    except Exception,e:
        print "Error"
        print e
        time.sleep(60)
