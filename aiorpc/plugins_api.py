import inspect
from dataclasses import dataclass
from typing import Callable, Any, Optional, TypeVar, Dict, Type, Generic, Coroutine, Tuple


T = TypeVar('T')
ALL_CONFIG_VARS: Dict[str, Tuple[Type[T], 'ConfigVar']] = {}


class ConfigVar(Generic[T]):
    def __init__(self, name: str, tp: Type[T]) -> None:
        ALL_CONFIG_VARS[name] = (tp, self)
        self.val: Optional[T] = None

    def set(self, v: T) -> Optional[T]:
        old_v = self.val
        self.val = v
        return old_v

    def __call__(self) -> T:
        assert self.val
        return self.val


def validate_name(name: str):
    assert not name.startswith("_")
    assert name != 'streamed'


exposed = {}
exposed_async = {}


def expose_func(module: str, func: Callable):
    if inspect.iscoroutinefunction(func):
        validate_name(module)
        validate_name(func.__name__)
        exposed_async[module + "::" + func.__name__] = func
    else:
        validate_name(module)
        validate_name(func.__name__)
        exposed[module + "::" + func.__name__] = func
    return func


on_server_startup = []


def register_startup(func: Callable[[], Coroutine[Any, Any, None]]) -> Callable[[], Coroutine[Any, Any, None]]:
    on_server_startup.append(func)
    return func


on_server_shutdown = []


def register_shutdown(func: Callable[[], Coroutine[Any, Any, None]]) -> Callable[[], Coroutine[Any, Any, None]]:
    on_server_startup.append(func)
    return func


@dataclass
class RPCClass(Generic[T]):
    pack: Callable[[T], Dict[str, Any]]
    unpack: Callable[[Dict[str, Any]], T]


def default_pack(val: Any) -> Dict[str, Any]:
    return val.__dict__


def default_unpack(tp: T) -> Callable[[Dict[str, Any]], T]:
    def unpack_closure(attrs: Dict[str, Any]) -> T:
        obj = tp.__new__(tp)
        obj.__dict__.update(attrs)
        return obj
    return unpack_closure


exposed_types: Dict[str, RPCClass] = {}


def expose_type(tp: Type[T],
                pack: Callable[[T], Dict[str, Any]] = None,
                unpack: Callable[[Dict[str, Any]], T] = None) -> Type[T]:
    if pack is None:
        if hasattr(tp, "__to_json__"):
            pack = tp.__json_reduce__
        else:
            pack = default_pack
    if unpack is None:
        if hasattr(tp, "__from_json__"):
            unpack = tp.__from_json__
        else:
            unpack = default_unpack(tp)
    exposed_types[f"{tp.__module__}::{tp.__name__}"] = RPCClass(pack, unpack)
    return tp
