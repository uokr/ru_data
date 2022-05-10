RU Data parser for official API https://docs.efir-net.ru/dh2/#/


config file ru_data.ini example

[Account] <br />
Login = *** <br />
Password = *** <br />
[Yandex] <br />
Login = user@yandex.ru <br />
Password = *** <br />
[Mailing] <br />
; Type might be Script/Yandex <br />
Type = Yandex <br />
To = user@domain.com <br />
ScriptPath = C:\\Users\\UserName\\Documents\\Python\\RU_DATA\\SendMail.vbs <br /> 
[Settings] <br />
MainFolder = /Users/UserName/Python/RU_DATA/ <br />
MainRequestUrl = https://dh2.efir-net.ru/v2/ <br />
MetadataUrl = https://docs.efir-net.ru/dh2/ <br />
AttachmentFileName = TodayRatings.zip <br />
