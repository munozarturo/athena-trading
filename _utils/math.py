from typing import Any
import numpy as np
import math
from _utils.typing import ListLike
from _utils.val import val_instance

def perc(perc: float) -> float:
    return perc / 100

def bps(bps: int) -> float:
    return bps / 10_000

def isnan(a: Any) -> bool:
    return math.isnan(a)

def round(a: int | float, decimal_points: int) -> int | float:
    return np.round(a, decimal_points)

def average(a: Any) -> Any:
    """
    Returns the average of the elements in a.

    Args:
        a (Any): Any array-like object.

    Returns:
        Any: The average.
    """
    
    return round(sum(a) / len(a), 6)

def lin_reg_slope(x: ListLike, y: ListLike) -> float:
    """
    Returns the linear regression of the slope of x against y.

    Using the following equation:

    Where Y = bX + a,
    and n = sample size.

    a = ((Σy)(Σx**2) - (Σx)(Σxy)) / (n(Σx**2) - (Σx)**2)
    b = (n(Σxy) - (Σx)(Σy)) / (n(Σx**2) - (Σx)**2)

    Args:
        x (ListLike): Data x.
        y (ListLike): Data y.

    Returns:
        float: Slope of x against y (b).
    """

    if len(x) != len(y):
        raise ValueError(
            f"sample size of 'x' and sample size of 'y' must be the same.")

    n = len(x)

    sigma_x: float = sum(x)
    sigma_y: float = sum(y)
    sigma_xy: float = sum([x[i] * y[i] for i in range(len(x))])
    sigma_x2: float = sum([v**2 for v in x])
    
    if n * sigma_x2 - (sigma_x ** 2) == 0:
        return 0

    return round((n * sigma_xy - (sigma_x * sigma_y)) / (n * sigma_x2 - (sigma_x ** 2)), 6)