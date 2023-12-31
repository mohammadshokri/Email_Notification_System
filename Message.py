import jdatetime
class CreateMessage:
    @staticmethod

    def EventTemplate(totalCount, status, category, statusCode, statusName, statusDesc, resultCode, exceptionKey,
                      detail):
        message = "<html><head><style>"
        message += "body {margin: 0; display: grid; grid-template-rows: auto 1fr auto; height: 100vh;}"
        message += "div {padding: 20px;}"
        message += "#top {background-color: #808080; color: white; text-align: center;}"
        message += "#middle {background-color: white;}"
        message += "#bottom {background-color: #808080; color: white; text-align: center; padding: 10px;}"
        message += "table {width: 100%;}"  # Set the width of the table to 100%
        message += "</style></head>"
        message += "<body>"

        # Top Section
        message += "<div id='top'>"
        message += f"<strong>COUNT = {totalCount}</strong><br>"
        message += f"STATUS = {status}<br>"
        message += f"CATEGORY = {category}<br>"
        message += f"STATUSCODE = {statusCode}<br>"
        message += f"STATUS_NAME = {statusName}<br>"
        statusDesc = statusDesc.decode('utf-8')
        message += f"STATUS_DESCRIPTION = {statusDesc}<br>"
        message += f"RESULTCODE = {resultCode}<br>"
        message += f"EXCEPTIONKEY = {exceptionKey}<br>"
        message += "</div>"

        # Middle Section (including the table)
        message += "<div id='middle'>"
        if detail:
            message += "<br><br><strong>Client Failure Counts:</strong><br>"
            message += "<table border='1' style='width: 100%;'>"
            message += "<tr><th>Client Name</th><th>توضیحات</th><th>Count</th></tr>"
            for name, info in detail.items():
                message += f"<tr><td>{name}</td><td>{info.get('DESCR', '')}</td><td>{info.get('CNT', 0)}</td></tr>"
            message += "</table>"
        message += "</div>"

        # Footer Section
        message += "<div id='bottom'>"
        message += f'<p> {jdatetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").__str__()}</p>'
        message += "</div>"

        message += "</body></html>"

        return message

    @staticmethod
    def ReportTemplate(param1, param2, param3):
        message  =  (
            f"CATEGORY = {param1}<br>"
            f"DESCRIPTION = {param2}<br><br>"
            f"DETAIL = {param3}<br>"
        )
        return message

    @staticmethod
    def ReportManagementTemplate(fromDate= jdatetime.datetime.now().strftime("%Y-%m-%d").__str__()+' 00:00 AM',
                                 toDate=jdatetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").__str__(), total=None, succ=None, unSucc=None, succPerc=None, unSuccPerc=None, serviceData=None,exceptData=None,clientExceptData=None,clientExceptDataDetail=None):
        message = "<html><head><style>"
        message += "body {margin: 0; display: grid; grid-template-rows: auto 1fr auto; height: 100vh;}"
        message += "div {padding: 20px;}"
        message += "#top {background-color: #808080; color: white; text-align: center;}"
        message += "#middle {background-color: white;}"
        message += "#bottom {background-color: #808080; color: white; text-align: center; padding: 10px;}"
        message += "table {width: 100%;}"  # Set the width of the table to 100%
        message += "</style></head>"
        message += "<body>"
        # Top Section
        message += "<div id='top'>"
        message = f"<strong>Galaxy Report from {fromDate} to {toDate}</strong><br>"
        message += "</div>"

        # Body Section
        message += "<div id='middle'>"
        message += f"<table border='1'  style='width: 100%;'>"
        message += "<tr><th>Category</th><th>Number</th><th>Percent</th></tr>"
        # Add Total Events
        message += f"<tr><td>Total Events</td><td>{total}<td></td></td></tr>"
        # Add Success Events
        message += f"<tr><td>Success Events</td><td>{succ}</td><td>{succPerc}</td></td></tr>"
        # Add Unsuccess Events
        message += f"<tr><td>Unsuccess Events</td><td>{unSucc}</td><td>{unSuccPerc}</td></td></tr>"
        message += "</table>"

        if serviceData:
            message += "<br><br><strong>Top 5 Services with Most Failures:</strong><br>"
            message += "<table border='1' style='width: 100%;'>"
            message += "<tr><th>Service Name</th><th>توضیحات</th><th>Count</th></tr>"
            for service_name, service_info in serviceData.items():
                truncated_service_name = service_name[:62] + "..." + service_name[-5:] if len(service_name) > 62 else service_name
                message += f"<tr><td>{truncated_service_name}</td><td>{service_info.get('DESCR', '')}</td><td>{service_info.get('CNT', 0)}</td></tr>"
            message += "</table>"
        if exceptData:
            message += "<br><br><strong>Number of Errors Today, Categorized by Error Code:</strong><br>"
            message += "<table border='1'  style='width: 100%;'>"
            message += "<tr><th>Status Code</th><th>Description</th><th>Count</th><th>Percentage</th></tr>"
            for status_code, status_info in exceptData.items():
                message += f"<tr><td>{status_code}</td><td>{status_info.get('STATUS_DESCRIPTION', '')}</td><td>{status_info.get('CNT', 0)}</td><td>{status_info.get('PERCENTAGE', 0)}%</td></tr>"
            message += "</table>"

        if clientExceptData:
            message += "<br><br><strong>Top 5 Clients with Most Errors:</strong><br>"
            message += "<table border='1'  style='width: 100%;'>"
            message += "<tr><th>Client Name</th><th>توضیحات</th><th>Count</th></tr>"
            for client_name, client_info in clientExceptData.items():
                message += f"<tr><td>{client_name}</td><td>{client_info.get('DESCR', '')}</td><td>{client_info.get('CNT', 0)}</td></tr>"
            message += "</table>"
        message += "</div>"

        message += "<div id='bottom'>"
        message += f'<p> {jdatetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").__str__()}</p>'
        message += "</div>"

        message += "</body></html>"

        return message


    def AbnormalityTemplate(name, type, descr, lvl, subject):
        message = "<html><body style='margin: 0;'>"

        # Title Section
        message += "<div style='background-color: #333; color: white; padding: 20px; text-align: center;'>"
        message += "<h2>Incident Detection</h2>"
        message += f"<strong> {name}</strong><br>"
        message += f"<strong> {type}</strong><br>"
        message += f"<strong> {subject}</strong><br>"
        message += "</div>"

        # Body Section
        message += "<div style='background-color: #f2f2f2; padding: 20px;'>"
        message += descr.replace('\n', '<br>')
        message += "</div>"

        message += f"<div style='background-color: #333; color: white; padding: 10px; text-align: center;'>"
        message += f'<p> {lvl}</p>'
        message += f'<p> {jdatetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").__str__()}</p>'
        message += "</div>"

        message += "</body></html>"

        return message



