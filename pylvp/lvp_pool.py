from types import MappingProxyType
from serial.tools.list_ports import comports
from threading import Thread
from typing import List

from .lvp import LVP


class LVPPool:
    """
    Communicates with more than one arduino simultaneously.
    """

    @classmethod
    def all_devices(cls, exclude=(), kind=LVP, merge_log=True, **kwargs):
        """
        Create an LVP pool using all connections
        """
        kwargs.setdefault('log_id', merge_log)
        kwargs.setdefault('log_path_with_id', not merge_log)
        devices = [p.device for p in comports() if p.device not in exclude]
        return cls([kind(device, **kwargs) for device in devices])

    def __init__(self, devices):
        self.devices = MappingProxyType({dev.id: dev for dev in devices})

    def __iter__(self):
        return iter(self.devices.values())

    def __len__(self):
        return len(self.devices)

    def __repr__(self):
        return f"<LVPPool with {[d.id for d in self]}>"

    def _parallel_map(
        self, func, devices=None, args=(), kwargs=MappingProxyType({}), timeout=None
    ):
        devices = self if devices is None else list(devices)

        results = [None] * len(devices)

        def target(i, dev):
            results[i] = func(dev, *args, **kwargs)

        threads = [Thread(target=target, args=item) for item in enumerate(devices)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join(timeout=timeout)

        return {dev.id: res for dev, res in zip(devices, results)}

    def query(self, query) -> List[LVP]:
        """
        Filter devices using query.

        Return a list of all selected devices.
        """
        if query == ...:
            return list(self.devices.values())
        elif isinstance(query, LVP):
            return self.query(query.id)
        elif isinstance(query, (list, tuple)):
            result = set()
            for q in query:
                result.update(self.query(q))
            return list(result)
        elif query in self.devices:
            return [self.devices[query]]
        raise ValueError("invalid query")

    def get(self, ref, *args, timeout=None):
        """
        Get set of values from all selected devices.
        """

        devs = self.query(ref)
        return self._parallel_map(lambda d: d.get(*args), devs)

    def set(self, ref, *args, timeout=None, **kwargs):
        """
        Set of values on all selected all devices.
        """
        devs = self.query(ref)
        self._parallel_map(lambda d: d.set(*args, **kwargs), devs)

    def declare(self, spec, bind=True):
        """
        Declare function.
        """
        funcs = {dev.id: dev.declare(spec, bind=False) for dev in self}
        name = spec.split("(")[0]

        def func(query, *args, **kwargs):
            def target(dev):
                return funcs[dev.id](*args, **kwargs)

            return self._parallel_map(target, self.query(query))

        func.__name__ = name
        func.__doc__ = f"Calls the {spec} lvp function"
        if bind:
            setattr(self, name, func)
        return func

    def exec(self, query, cmd):
        self._parallel_map(lambda d: d.exec(cmd), self.query(query))

    def background(self, query, cmd):
        self._parallel_map(lambda d: d.exec(cmd), self.query(query))


p = LVPPool.all_devices()
print(list(p))