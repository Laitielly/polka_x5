from pydantic import BaseModel

class Command(BaseModel):
    action: str
    sku: str
    qty: int

class ItemOut(BaseModel):
    sku: str
    qty: int