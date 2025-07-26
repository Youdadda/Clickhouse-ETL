from config import pipeline_logger
import re
from datetime import datetime
import json

winlog_schema = r"winlog*"
event_schema = r"event*"
host_schema = r"host*"
user_schema=r"user*"
### process the data

def extract_columns(data:dict):
    """
    Extracts and organizes fields from a log record dictionary into logical groups.

    Groups fields matching 'winlog*', 'host*', and 'user*' into separate dictionaries.
    Extracts fields matching 'event*' and adds them directly to the result dictionary.
    The '@timestamp' field is extracted as 'timestamp'.
    All remaining fields are grouped under the 'data' key.

    Args:
        data (dict): The input log record with various fields.

    Returns:
        dict: A dictionary with the following keys:
            - 'timestamp': The value of '@timestamp' from the input.
            - 'winlog': Dictionary of fields matching 'winlog*'.
            - 'host': Dictionary of fields matching 'host*'.
            - 'user': Dictionary of fields matching 'user*'.
            - event fields: Each 'event*' field as a separate key.
            - 'data': Dictionary of all remaining fields.
    """
    winlog_col = {}
    host_col = {}
    user_col = {}
    event_keys = []
    remains = data.copy()
    return_dict = {}
    return_dict['timestamp'] = datetime.fromisoformat(data["@timestamp"].replace("Z", "+00:00"))
    remains.pop("@timestamp")
    for field in data.keys():
        if re.match(winlog_schema,field ):
            winlog_col[field] = data[field]
            remains.pop(field)
        elif re.match(host_schema,field):
            host_col[field] = data[field]
            remains.pop(field)
        elif re.match(user_schema,field):
            user_col[field] = data[field]
            remains.pop(field)
        elif re.match(event_schema, field):
            
            return_dict[field.replace(".","_")] = json.dumps(data[field])
            event_keys.append(field)
            remains.pop(field)
    return_dict["winlog"] = json.dumps(winlog_col)
    return_dict["user"] = json.dumps(user_col)
    return_dict["host"] = json.dumps(host_col)
    return_dict['data'] = json.dumps(remains)
    
    return return_dict 