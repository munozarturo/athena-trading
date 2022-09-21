from collections import defaultdict
from types import NoneType
from typing import Any, Iterable
from typing_extensions import Self
from _utils.val import val_instance


# todo: Make objects that can inherit from Dispatch and still work.


class Error(Exception):
    pass


class DispatchError(Error):
    pass


class BrokerError(Error):
    pass


class DataStreamError(Error):
    pass



def uids_range(_start: int, _end: int, _step: int = 1, _exclude: Any = []) -> set:
    """
    Will return a set of intgers unique identification numbers (UIDs) in a range from `_start` to
    `_end` (inclusive).

    Args:
        _start (int): start of uid range.
        _end (int): end of uid range.
        _step (int, optional): step for range. Dafaults to 1.
        _exclude (Subscriptable, optional): if defined any generated uids found in `_exclude` will be removed from returned set. Defaults to None.

    Returns:
        set: uids
    """
    
    val_instance(_start, int)
    val_instance(_end, int)
    val_instance(_step, int)
    
    try:
        Any in _exclude
    except TypeError:
        raise TypeError(f"`_exclude` must be subscriptable.")
    
    _uids = set()
    
    _range = list(range(_start, _end + 1, _step))
    
    if _exclude:
        for _uid in _range:
            if _uid in _exclude:
                _uids.add(_uid)
    else:
        _uids = set(_range)
        
    return _uids


class Dispatch(object):
    __instances = defaultdict(None)
    
    @property
    def dispatchname(self) -> str:
        return self.__dispatchname

    @dispatchname.setter
    def dispatchname(self, _dispatchname: str) -> None:
        val_instance(_dispatchname, str)

        self.__dispatchname = _dispatchname

    @dispatchname.deleter
    def dispatchname(self) -> None:
        raise DispatchError(f"can't delete `dispatchname`")
    
    def __new__(cls: type[Self], *args, **kwargs) -> Self:
        _dispatchname = args[0]
        
        if not _dispatchname in cls.__instances:
            cls.__instances[_dispatchname] = (super(Dispatch, cls).__new__(cls), args, kwargs)
            
        _cls, _args, _kwargs = cls.__instances[_dispatchname]
        
        if not args == _args:
            raise DispatchError(f"singleton dispatch object '{_dispatchname}' re-initialized with different *args.")
            
        if not kwargs == _kwargs:
            raise DispatchError(f"singleton dispatch object '{_dispatchname}' re-initialized with different **kwargs.")
            
        return _cls
    
    @property
    def uids(self) -> set:
        return self.__uids
    
    @uids.setter
    def uids(self, _uids: Iterable) -> None:
        if self.__dispatched:
            raise DispatchError(f"can't change `uids` after dispatch.")
        
        self.__uids = set(_uids)
        self.__available = set(_uids)
    
    @uids.deleter
    def uids(self) -> None:
        self.__uids = set()
    
    @property
    def active_limit(self) -> int | NoneType:
        return self._active_limit
    
    @active_limit.setter
    def active_limit(self, _active_limit: int | NoneType) -> int:
        val_instance(_active_limit, (int, NoneType))
        
        if not _active_limit is None:
            if _active_limit == -1:
                self._active_limit = None
            elif _active_limit >= 0:
                self._active_limit = _active_limit
            else:
                raise ValueError(f"`active_limit` must be 'None', '-1' or a positive integer value.")
            
        self._active_limit = None

    def __init__(self, dispatchname: str, uids: set, active_limit: int | NoneType = None) -> None:
        """
        Creates a Singleton dispatch object which will generate unique ids to be used as unique identifiers.

        If more than one Dispatch object with the same `dispatchname`s are instanciated every
        instanciation after the first one will return the first instanciation.
        
        Initializing two or more Dispatch objects with the same `dispatchname` will raise an error.

        Args:
            dispatchname (str): dispatch object name to be shared as the singleton identifier
            uids (set): set of uids to be assigned
            active_limit (int | NoneType, optional): active limit of instanciations. Defaults to no limit (-1 is also interpreted as no limit).
        """
        
        self.__dispatchname: str = None  
        self.__uids: set = set()
        self.__occupied: set = set()
        self.__available: set = set()
        self.__dispatched: bool = False
        
        self._active_limit = None
        
        self.dispatchname = dispatchname
        self.uids = uids
        self.active_limit = active_limit
        
    def allocate_uid(self) -> int:
        if not self.active_limit is None:
            if len(self.__occupied) > self.active_limit:
                raise DispatchError(f"`active_limit` reached.")
        
        if not self.has_free():
            raise DispatchError(f"exhausted all available uids.")
        
        if not self.__dispatched:
            self.__dispatched = True
            
        uid = self.__available.pop()
        self.__occupied.add(uid)
        
        return uid
    
    def has_free(self) -> bool:
        return len(self.__available) > 0

    def free_uid(self, uid: int) -> int:
        val_instance(uid, int)
        
        self.__occupied.remove(uid)
        self.__available.add(uid)
        
        return uid


class DataStream:
    pass


class Broker:
    pass
