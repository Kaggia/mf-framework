class GeneralError(Exception):
    """
    General Error class. All custom exceptions inherit from this class.
    """
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
        
#AgroRule errors
class ColumnNameError(GeneralError):
    """
    Error class for column name errors.
    """
    
    def __init__(self, error_message="", level="ERROR", status="BLOCKED"):
        super().__init__(error_message=str(self.__class__)+":"+error_message, level=level, status=status)

#If check on agro format rule fails it raises
class AgroRuleFormatError(GeneralError):
    """
    Error class for agro rule format errors.
    """
    def __init__(self, error_message="", level="ERROR", status="BLOCKED"):
        super().__init__(error_message=str(self.__class__)+":"+error_message, level=level, status=status)

#If a complex number, resulting from a equation or equation on condition,
#is detected then raise an error
class ComplexValueError(GeneralError):
    """
    Error class for complex value errors.
    """
    def __init__(self, error_message="", level="ERROR", status="BLOCKED"):
        super().__init__(error_message=str(self.__class__)+":"+error_message, level=level, status=status)

#If a nested rule is detected then raise an error
class NestedRuleError(GeneralError):
    """
    Error class for nested rule errors.
    """
    def __init__(self, error_message="", level="ERROR", status="BLOCKED"):
        super().__init__(error_message=str(self.__class__)+":"+error_message, level=level, status=status)


#OutputModel errors
class InvalidOutputAgroRule(GeneralError):
    """
    Error class for invalid output agro rule errors.
    """
    def __init__(self, error_message="", level="ERROR", status="BLOCKED"):
        super().__init__(error_message=str(self.__class__)+":"+error_message, level=level, status=status)

#OutputModel errors
class DataFormatException(GeneralError):
    """
    Error class for data format exception.
    """
    def __init__(self, error_message="", level="ERROR", status="BLOCKED"):
        super().__init__(error_message=str(self.__class__)+":"+error_message, level=level, status=status)
