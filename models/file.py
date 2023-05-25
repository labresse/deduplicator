from typing import List
from models.extenthash import extenthashes

class file:
    hashes: extenthashes
    path: str

    def __init__(self, obj):
        self.locationobjectid = obj[0]
        self.modifiedtime = obj[1]
        self.ishashed = obj[2]
        
        
        
        
        
    
    
    
    