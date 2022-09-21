from types import UnionType
from typing import Type, Union, Any, get_origin, get_args
import pandas as pd
from datetime import tzinfo

from varname import argname

_ListLike = Union[list, tuple, dict, pd.DataFrame]
_PathLike = Union[str, bytes, int]

def val_instance(__o: Any, _type: Type | type | UnionType | tuple) -> None:
    """
    Validates instance.

    Args:
        __o (Any): __oect to be validated.
        _type (Type | type | UnionType | tuple): Valid types.

    Raises:
        TypeError: If the __oect <__o> is not an instance of <_type>.
    """

    _exp = None

    if get_origin(_type) == Union:
        _type = get_args(_type)
        _exp = ', '.join([v.__name__ for v in _type])
    elif isinstance(_type, tuple):
        # If it's a tuple, break every union type down into inidividual types.
        _types = []
        
        for _t in _type:
            if get_origin(_t) == Union:
                _types += list(get_args(_t))
            else:
                _types.append(_t)
        
        _exp = ', '.join([v.__name__ for v in _types])
    elif not isinstance(_type, (Type, type, UnionType)):
        raise TypeError(
            f"Expected {(Type, type, UnionType, tuple)} for '_type', got {type(_type)}.")

    if not _exp:
        _expected = _type.__name__
    else:
        _expected = _exp

    if not isinstance(__o, _type):
        raise TypeError(
            f"Expected {_expected} for '{argname('__o')}', got {type(__o).__name__}.")


def val_subclass(__o: Any, _type: Type | type | UnionType | tuple) -> None:
    """
    Validates subclass.

    Args:
        __o (Any): __oect to be validated.
        _type (Type | type | UnionType | tuple): Valid types.

    Raises:
        TypeError: If the __oect <__o> is not a subclass of <_type>.
    """

    _exp = None

    if get_origin(_type) == Union:
        _type = get_args(_type)
        _exp = ', '.join([v.__name__ for v in _type])
    elif isinstance(_type, tuple):
        # If it's a tuple, break every union type down into inidividual types.
        _types = []
        
        for _t in _type:
            if get_origin(_t) == Union:
                _types += list(get_args(_t))
            else:
                _types.append(_t)
        
        _exp = ', '.join([v.__name__ for v in _types])
    elif not isinstance(_type, (Type, type, UnionType)):
        raise TypeError(
            f"Expected {(Type, type, UnionType, tuple)} for '_type', got {type(_type)}.")

    if not _exp:
        _expected = _type.__name__
    else:
        _expected = _exp

    if not issubclass(__o.__class__, _type):
        raise TypeError(
            f"Expected {_expected} for '{argname('__o')}', got {__o.__class__}.")


def defval_instance(__o: Any | None, _type: Type | type | UnionType | tuple, defval: Any | None) -> Any:
    """
    Validates an instance of an __o against _type, if __o is None then the defval is set.

    Args:
        __o (Any | None): __oect to be validated.
        _type (Type | type | UnionType | tuple): Valid types.
        defval (Any | None): Value to be set.

    Returns:
        Any: Validated __oect, with default value set if necessary.
    """
        
    if __o is None:
        return defval
    else:
        val_instance(__o, _type)
    
    return __o


def defval_subclass(__o: Any | None, _type: Type | type | UnionType | tuple, defval: Any | None) -> Any:
    """
    Validates that __o is a subclass of _type, if __o is None then the defval is set.

    Args:
        __o (Any | None): __oect to be validated.
        _type (Type | type | UnionType | tuple): Valid subclasses.
        defval (Any | None): Value to be set.

    Returns:
        Any: Validated __oect, with default value set if necessary.
    """
    
    if __o is None:
        return defval
    else:
        val_subclass(__o, _type)
        
    return __o