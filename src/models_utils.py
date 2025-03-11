
from fastapi.responses import HTMLResponse
from typing import List
from pydantic import BaseModel

# Permite insertar 1-1000 registros en una sola petición
def validate_batch_size(items: list, min_size: int = 1, max_size: int = 1000):
    if not (min_size < len(items) <= max_size):
        return HTMLResponse(
            content=f"<h1>Error:</h1><pre>Batch size must be between {min_size} and {max_size}. Received: {len(items)}</pre>",
            status_code=400
        )
    return None 

# Elimina duplicados en los items que ingresan 
def remove_duplicates_items(items: List[BaseModel]) -> List[BaseModel]:
    seen = set()
    unique_items = []    
    for item in items:
        item_tuple = tuple(sorted(item.model_dump().items()))
        if item_tuple not in seen:
            seen.add(item_tuple)
            unique_items.append(item)
    return unique_items