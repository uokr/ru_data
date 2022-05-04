Set objOutlook = CreateObject("Outlook.Application")
Set objMail = objOutlook.CreateItem(0)
'objMail.Display   'To display message
objMail.Recipients.Add ("Al-RubayiMM@cbr.ru")
'objMail.Recipients.Add ("kai6@cbr.ru")
objMail.Subject = "RatingsHistory"
'objMail.Body = "This is Email Body"
objMail.Attachments.Add("C:\Users\Al-rubayiMM\Documents\Python\RU_DATA\TodayRatings.zip")   'Make sure attachment exists at given path. Then uncomment this line.
objMail.Send   'I intentionally commented this line
'objOutlook.Quit
Set objMail = Nothing
Set objOutlook = Nothing