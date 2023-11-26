import jdatetime

class CreateMessage:
    @staticmethod
    def EventTemplate(param1, param2, param3, param4, param5, param6, param7, param8, param9):
        message  =  (
            f"COUNT = {param1}\n"
            f"STATUS = {param2}\n"
            f"CATEGORY = {param3}\n"
            f"STATUSCODE = {param4}\n"
            f"STATUS_NAME = {param5}\n"
            f"STATUS_DESCRIPTION = {param6}\n"
            f"RESULTCODE = {param7}\n"
            f"EXCEPTIONKEY = {param8}\n"
            f"DETAIL = {param9}\n"
        )
        return message

    @staticmethod
    def ReportTemplate(param1, param2, param3):
        message  =  (
            f"CATEGORY = {param1}\n"
            f"DESCRIPTION = {param2}\n\n"
            f"DETAIL = {param3}\n"
        )
        return message


# event_message  =  CreateMessage.EventTemplate(
#     "value1", "value2", "value3", "value4", "value5",
#     "value6", "value7", "value8", "{'ad': 240, 'development': 221, 'test': 53498, 'operation': 8}"
# )
# print(event_message)
# report_message  =  CreateMessage.ReportTemplate(
#     "value1", "value2", "value3")
# print(report_message)