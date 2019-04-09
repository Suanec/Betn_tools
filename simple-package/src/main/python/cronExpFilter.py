import datetime
cronExp_lines = [i for i in st.split('\n') if i.split(' ')[4] == '*']
datetime_lines = []
for line in cronExp_lines:
    i = line.split(' ')
    minute = int(i[0]) if "*" not in i[0] else 0
    hrs = i[1] if "*" not in i[1] else "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23"
    hrss = [ int(j) for j in hrs.split(',') ]
    [datetime_lines.append(datetime.datetime(2019,01,24,hr,minute)) for hr in hrss]
datetime_lines.sort(key= lambda x : long(x.strftime("%s")))
time_windows_lines = []
for i in range(1,len(datetime_lines)):
    x = long(datetime_lines[i-1].strftime("%s"))
    y = long(datetime_lines[i].strftime("%s"))
    time_windows_lines.append(((y-x)/60, datetime_lines[i-1]))

time_windows_lines.sort(key=lambda x : x[0],reverse=True)




