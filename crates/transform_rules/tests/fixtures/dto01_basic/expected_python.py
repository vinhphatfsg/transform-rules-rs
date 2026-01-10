from dataclasses import dataclass, field
from typing import Optional, Any

@dataclass
class RecordUser:
    age: int
    name: Optional[Any] = None

@dataclass
class Record:
    id: str
    user: RecordUser
    active: bool
    status: str
    source: str
    price: Optional[float] = None
    meta: Optional[Any] = None
    # json: "user-name"
    user_name: Optional[Any] = field(default=None, metadata={"json_key": "user-name"})
    # json: "class"
    class_: Optional[Any] = field(default=None, metadata={"json_key": "class"})
