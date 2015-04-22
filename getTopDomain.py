#The following script is done by Bo(Jeffrey) Liu
#Please create a database instance called "indexExchange" in mysql
#Please create two tables in indexExchange, mailing and countEmail
#The following code requires to install python-mysqldb
#Date: 2015-04-07
#updated: 2015-04-08


import MySQLdb
import platform
from datetime import datetime
from datetime import timedelta

print "python version: ", platform.python_version()

#connect to database
db = MySQLdb.connect(host="localhost",user="root",passwd="rootroot",db="indexExchange")
db.autocommit(False)

#two dementional list. Each sublist consists [email_domain,count] 
#this dictionary stores the value we use to update table countEmail
dailyResult={}
#store email_domain and count in the last 30 days
thirtyDayResult={}
#store email_domain and count in total
totalResult = {}
#the growth rate we have for each domain
domainGrowth={}
#final result
highestGrowthDomain=[]

try: 

	cur=db.cursor()
	
	#cur.execute("insert into mailing value(\"testing from python\");")
	#Query the entire mailing table
	cur.execute("select * from mailing")

	#read each entry in the datatable and count the email entries based on domain. 
	#for each entry in the two dimentional array
	for row in cur.fetchall():
		temp=str(row[0]).split('@') #seperate domain address
		
		#update email_domain and count into daily result
		if temp[1] in dailyResult.keys():
			dailyResult[temp[1]]=dailyResult[temp[1]]+1
		else:
			dailyResult[temp[1]] = 1

	#update new table, countEmail
	now=datetime.now()
	date = str(now.year)+'-'+str(now.month)+'-'+str(now.day)

	for key in dailyResult.keys():
		value = dailyResult[key]
		insert='insert into countEmail value('+str(value)+',\"'+key+'\",'+'\"'+date+'\"'+');'
		cur.execute(insert)
	
	#date of the day 30 days ago
	thirtyDayAgo = datetime.now() - timedelta(days=30)
	reportDate = str(thirtyDayAgo.year)+'-'+str(thirtyDayAgo.month)+'-'+str(thirtyDayAgo.day)
	
	#query the get the email domain and count for the past 30 days period
	#value stored in dictionary thirtyDayResult
	getEmailDomain30Day='select distinct(email_domain),sum(count) from countEmail where date>=\''+reportDate+'\' group by email_domain order by sum(count) desc;'

	cur.execute(getEmailDomain30Day)

	for row in cur.fetchall():
		thirtyDayResult[row[0]]=row[1]

	#query the email domain and count against the whole record
	#value stored in dictionary totalResult
	getEmailDomainTotal = 'select distinct(email_domain),sum(count) from countEmail group by email_domain order by sum(count) desc;'
	cur.execute(getEmailDomainTotal)
	
	for row in cur.fetchall():
		print str(row[0])+'|'+str(row[1])
		totalResult[row[0]]=row[1]
	

	#compute the growth rate for each email domain collected for the past 30 days period
	#value stored in dictionary domainGrowth
	for key in thirtyDayResult:
		if key in totalResult.keys():
			domainGrowth[key]=thirtyDayResult[key]/totalResult[key]

	#sort the dictionary in desc order
	#value stored in list highestGrowthDomain
	highestGrowthDomain=sorted(domainGrowth.items(), key=lambda x:x[1], reverse=True)
	
	#Get the top fifty
	highestGrowthDomain=highestGrowthDomain[0:51]
	db.commit()

except:
	db.rollback()
finally:
	#close database
	db.close

