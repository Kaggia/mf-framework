class GeneralError(Exception):
    def __init__(self, error_message, level="ERROR", status="BLOCKED"):
        self.error = error_message
        if error_message == "" or error_message == None:
            error_message = str(__class__.__name__)
        else:
            error_message = "<"+str(__class__.__name__)+">: "+error_message
        
        #push into sqs
        print(error_message)
        #print(error_message, level="ERROR", status="BLOCKED", messageType="ErrorLog")

    def __str__(self) -> str:
        return str({'error':str(self.__class__)[str(self.__class__).rfind('.')+1:str(self.__class__).rfind("'")], 'message':self.error})

#External services called
class GrowthStageFormatError(GeneralError):
    def __init__(self, error_message="", level="ERROR", status="BLOCKED"):
        super().__init__(error_message=str(self.__class__)+":"+error_message, level=level, status=status)


#Input errors
class BodyFormatError(GeneralError):
    def __init__(self, error_message="", level="ERROR", status="BLOCKED"):
        super().__init__(error_message=str(self.__class__)+":"+error_message, level=level, status=status)

class HotStartError(GeneralError):
    def __init__(self, error_message="", level="ERROR", status="BLOCKED"):
        super().__init__(error_message=str(self.__class__)+":"+error_message, level=level, status=status)

class VirtualStationDataError(GeneralError):
    def __init__(self, error_message="", level="ERROR", status="BLOCKED"):
        super().__init__(error_message=str(self.__class__)+":"+error_message, level=level, status=status)
