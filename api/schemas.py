from typing import Optional, Literal

from pydantic import BaseModel 

ActionType = Literal["click", "view", "scroll", "purchase"]

class EventIn(BaseModel):
    """API'nin POST /events body'sinde kabul ettiği şema."""
    user_id: int
    url: str
    action: ActionType           
    session_id: str
    timestamp: int         # epoch seconds
    amount: Optional[float] = None  


class WeightsOut(BaseModel):
    """GET /metrics/weights'in response'u."""
    exists: bool
    update_count: int = 0
    last_accuracy: Optional[float] = None
    last_batch_size: Optional[int] = None
    weights: Optional[dict[str, float]] = None    # {"clicks_in_session": 0.12, ...}
    intercept: Optional[float] = None


class PredictionIn(BaseModel):
    """GET /predictions query params."""
    clicks_in_session: int
    time_on_page: int      # saniye


class PredictionOut(BaseModel):
    model_ready: bool
    purchase_probability: float