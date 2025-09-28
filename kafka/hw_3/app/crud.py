from .db import SessionLocal
from .models import Item
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.exc import StaleDataError

# sync helpers intended to run via asyncio.to_thread

def list_items_sync():
    with SessionLocal() as s:
        return [{"sku": r.sku, "qty": r.qty} for r in s.query(Item).all()]


def change_qty_sync(action: str, sku: str, qty: int, max_retries: int = 5):
    if qty < 0:
        return {"success": False, "reason": "negative_qty"}
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            with SessionLocal() as s:
                item = s.query(Item).filter_by(sku=sku).with_for_update(read=False).one_or_none()
                if item is None:
                    if action == 'decrease':
                        return {"success": False, "reason": "not_found"}
                    item = Item(sku=sku, qty=0)
                    s.add(item)
                    s.flush()

                if action == 'increase':
                    item.qty = item.qty + qty
                elif action == 'decrease':
                    if item.qty < qty:
                        return {"success": False, "reason": "insufficient", "item": {"sku": item.sku, "qty": item.qty}}
                    item.qty = item.qty - qty
                else:
                    return {"success": False, "reason": "bad_action"}

                s.commit()
                return {"success": True, "item": {"sku": item.sku, "qty": item.qty}}
        except StaleDataError:
            # optimistic lock conflict — retry
            continue
        except OperationalError:
            return {"success": False, "reason": "db_error"}
    return {"success": False, "reason": "conflict_retry_failed"}
