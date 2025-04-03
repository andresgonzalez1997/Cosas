import json

from sharepoint_interface.sharepoint import SharePointFunctions

def get_sharepoint_interface(sharepoint_name):
    credentials = None
    
    if str(sharepoint_name).lower() == "retailpricing":
        with open("sharepoint_interface/credentials/retailpricing_sharepoint.json") as f:
            credentials = json.load(f)
    
    if not credentials: return False
    sp = SharePointFunctions(credentials)
    return sp