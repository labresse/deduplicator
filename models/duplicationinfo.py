from typing import List
from models.extenthash import extenthash
 
class duplicationinfo:
    source: extenthash
    destinations: List[extenthash]
    
    def __init__(self,src, dest):
        self.source = src
        self.destinations = dest
