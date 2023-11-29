
import jdatetime
fromDate = jdatetime.datetime.now().strftime("%Y-%m-%d").__str__() + ' 00:00 AM'
toDate = jdatetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").__str__()
print(fromDate)
print(toDate)