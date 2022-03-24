import pytz
from datetime import datetime


def replaceDateTimeWithTZ(newDict, k, v):
        tz = pytz.timezone("Asia/Kolkata")          
        if k == 'created_at':    
            formatIST = '%d/%m/%Y %I:%M:%S %p'
            
        elif k in ['identification_data_created', 
                   'identification_data_modified', 
                   'claims_documentation_data_created', 
                   'claims_documentation_data_modified', 
                   'registration_documentation_data_created', 
                   'registration_documentation_data_modified',
                   'registration_application_data_created', 
                   'registration_application_data_modified']:
            formatIST = '%m/%d/%Y, %I:%M:%S %p'
            
        else:
            return newDict
        
        #newDict.pop(k, None)
        formatUTC = '%d/%m/%Y %H:%M:%S'
        formattedDt = datetime.strptime(v, formatIST)
        formattedDtString = datetime.strftime(tz.normalize(tz.localize(formattedDt)).astimezone(pytz.utc), formatUTC)
        newDict[k] = formattedDtString            
        return newDict

def getFormattedData(newDict, k, v):
      
      import string
      #Remove blank spaces in fields
      newKey = k.replace(' ', '_')
      #Remove dollars in fields
      newKey = newKey.replace('$', '_')
      #Remove square brackets in fields
      table = str.maketrans('[]', '__')
      newKeyFinal = newKey.translate(table) 
      newDict[newKeyFinal] = v
  
      newDict = replaceDateTimeWithTZ(newDict, newKeyFinal, v)
      return newDict

def parseJsonFile(jsonDict):
        new = {}                                          
        for k, v in jsonDict.items():                            
            k = k.encode('raw-unicode-escape').split(b'\\u')[0].decode()
            if isinstance(v, dict):
                v = parseJsonFile(v)
            new = getFormattedData(new, k, v)    
            #new = replaceDollarsInKeys(new, k, v)
            #new = replaceSquareBracketsInFields(new, k, v)
            #new = replaceDateTimeWithTZ(new, k, v)
        return new
        