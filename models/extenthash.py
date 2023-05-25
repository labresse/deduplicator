from btrfs.ctree import ULL
from typing import List

class extenthash:
    filePath: str
    
    def __init__(self,locationobjectid, offset, numbytes, hash):
        self.locationobjectid = locationobjectid
        self.offset = offset
        self.numbytes = numbytes
        self.hash = hash
    
    @classmethod
    def fromDbObj(cls, obj):
        return cls(obj[0], obj[1], obj[2], obj[3])
    
class extenthashes:
    extenthashes1: List[extenthash]
    extenthashes2: List[extenthash]
    extenthashes4: List[extenthash]
    extenthashes8: List[extenthash]
    extenthashes16: List[extenthash]
    extenthashes32: List[extenthash]
    extenthashes4096: List[extenthash]
    def __init__(self) -> None:
        self.extenthashes1 = [] 
        self.extenthashes2 = [] 
        self.extenthashes4 = [] 
        self.extenthashes8 = [] 
        self.extenthashes16 = [] 
        self.extenthashes32 = []
        self.extenthashes4096 = []
        self.hashdata1 = []
        self.hashdata2 = []
        self.hashdata4 = []
        self.hashdata8 = []
        self.hashdata16 = []
        self.hashdata32 = []
        self.hashdata4096 = []
    
HASH_SIZES = [
    1,
    2,
    4,
    8,
    16,
    32,
    4096
]