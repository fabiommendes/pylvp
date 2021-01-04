import time
from collections import deque
import re
from contextlib import contextmanager
from datetime import datetime
import serial
from serial.tools.list_ports import comports
from typing import Union, Tuple, Callable
from threading import Thread, Lock

FUNC_SPEC_RE = re.compile(r"(?P<name>[\w_]+)\((?P<args>[^()]*)\)")
GET_RESP_RE = re.compile(r"Parameter\s+'(?P<name>[\w_]+)':\s+(?P<value>[^\n\r]+)")
SET_RESP_RE = re.compile(
    r"Parameter\s+'(?P<name>[\w_]+)'\s+set to\s+'(?P<value>[^\n\r']+)'"
)
ACK_MSG = b"Manual connection established.\n\r\n"
Value = Union[str, float, int]


class LVP:
    """
    Represents a connection with an arduino running the LVP communication
    protocol.
    """

    @property
    def id(self):
        if self._id is None:
            return self.device.split("/")[-1]
        return self._id

    def __init__(
        self,
        device=None,
        baud=9600,
        log=None,
        echo=False,
        functions=(),
        cooldown=2.0,
        id=None,
        log_path=None,
        log_id=False,
        log_path_with_id=False,
    ):
        self._id = id
        self._quiet = False
        self._init = False
        self._lock = Lock()

        # Logger is a function that receives messages and do something with
        # them
        self._messages = deque([], 64)
        self._log = log or default_logger(
            self, path=log_path, prepend_id=log_id, path_with_id=log_path_with_id
        )
        if echo:
            log = self.log
            self._log = lambda msg: (print(msg), log(msg))

        # Create serial connection, but do not initialize arduino. We must
        # wait the cooldown period before interacting with it to let it
        # boot properly and respond to the "manual_connect" command.
        self.device = device or default_device()
        self._serial = serial.Serial(self.device, baudrate=baud)
        self._connect_deadline = time.time() + cooldown

        # Functions can be created during intialization
        for spec in functions:
            self.declare(spec)

    def __getattr__(self, attr) -> Callable:
        raise AttributeError(attr)

    def __repr__(self):
        idmsg = f"({self._id})" if self._id else ""
        return f"<LVP instance at {self.device!r}{idmsg}>"

    def log(self, msg):
        self._messages.append(msg)
        self._log(msg)

    def init(self, force=False):
        if force or not self._init:
            time.sleep(max(0, self._connect_deadline - time.time()))
            with self._lock:
                self._serial.timeout = 1 / 16

                msg = "..."
                while msg:
                    msg = self._serial.readline()
                    self.log(msg)

                self._serial.timeout = None
                self._send(b"manual_connect\n")
                self.log(self._serial.read_until(ACK_MSG))
            self._init = True

    def send(self, msg: Union[str, bytes], cycles=None, timeout=None) -> str:
        """
        Send message to arduino and return the response.
        """
        if isinstance(msg, str):
            msg = msg.encode("ascii")
        if not msg.endswith(b"\n"):
            msg += b"\n"

        self.init()
        with self._lock:
            self._serial.timeout = timeout
            self._send(msg)
            out = self._recv(b"\r\n", flush=True)
        self.log(out)
        return out.decode("ascii").replace("\r\n", "\n")

    def _send(self, msg: bytes) -> None:
        self._serial.write(msg)
        self.log(b">>> " + msg)

    def _recv(self, until=None, flush=False, timeout=1 / 64) -> bytes:
        if self._quiet:
            return b""

        if until:
            out = self._serial.read_until(until)
        elif isinstance(until, int):
            out = self._serial.read(until)
        else:
            out = self._serial.readline()

        if flush:
            msg = b"..."
            self._serial.timeout = timeout
            while msg:
                msg = self._serial.readline()
                out += msg
        return out

    def get(self, *args) -> Union[Value, Tuple[Value, ...]]:
        """
        Get value assigned to variable.
        """
        if len(args) == 1:
            if isinstance(args[0], str):
                return self._get(args[0])
            else:
                args = args[0]
        return tuple(self._get(name) for name in args)

    def _get(self, name):
        out = self.send(f"get({name})")
        m = GET_RESP_RE.search(out)

        data = m and m.groupdict()
        if not m or data["name"] != name:
            raise RuntimeError(f"bad response: {out!r}")

        return normalize_response(data["value"])

    def set(self, *args, **kwargs) -> None:
        """
        Set value assigned to variable.
        """
        if len(args) == 1:
            kwargs = {**args[0], **kwargs}
        elif len(args) == 2:
            key, value = args
            kwargs[key] = value
        elif len(args) > 2:
            raise TypeError("function accepts at most 2 positional arguments")

        for k, v in kwargs.items():
            self._set(k, v)

    def _set(self, name: str, value: Value):
        out = self.send(f"set({name},{value})")
        m = SET_RESP_RE.search(out)

        data = m and m.groupdict()
        if not m or data["name"] != name:
            raise RuntimeError(f"bad response: {out!r}")

    def exec(self, cmd) -> str:
        """
        Executes command and returns the resulting messages
        """
        return self.send(cmd)

    def background(self, command: str, period: int = 5 * 60, echo=False):
        """
        Execute command in the background every 'period' seconds.

        It returns a cancellation function that can stop the background task
        when executed.
        """

        self.init()
        keep = True

        def task():
            while keep:
                if echo:
                    print(f"[bg] executing {command}")
                self.exec(command)
                time.sleep(period)

        def stop():
            nonlocal keep
            keep = False

        thread = Thread(target=task)
        thread.start()
        return stop

    def declare(self, spec, bind=True):
        """
        Declare a LVP function from specification.
        """
        m = FUNC_SPEC_RE.fullmatch(spec)
        if not m:
            raise ValueError(f"invalid specification: {spec!r}")
        data = m.groupdict()
        name = data["name"]
        argnames = [arg.strip() for arg in data["args"].split(",")]

        def func(*args, quiet=False):
            with self._maybe_quiet(quiet):
                self.set(dict(zip(argnames, args)))
                return self.exec(name)

        func.__name__ = name
        func.__doc__ = f"Calls the {spec} lvp function"
        if bind:
            setattr(self, name, func)
        return func

    def interact(self):
        """
        Start a communication loop with arduino.
        """
        while True:
            msg = input(">>> ")
            if msg == "quit":
                break
            print(self.send(msg))

    @contextmanager
    def quiet(self):
        """
        A context manager that execute commands in quiet mode.
        """
        is_quiet = self._quiet
        if not is_quiet:
            self._send(b"quiet_connect\n")
        yield self
        self._quiet = is_quiet
        if not is_quiet:
            self.send("manual_connect")

    @contextmanager
    def _force_manual(self):
        """
        A context manager that execute commands in manual_connect mode.
        """
        is_quiet = self._quiet
        self._quiet = False
        if is_quiet:
            self.send("manual_connect")
        yield self
        self._quiet = is_quiet
        if not is_quiet:
            self._send(b"quiet_connect\n")

    @contextmanager
    def _maybe_quiet(self, flag):
        if flag:
            with self.quiet() as conn:
                yield conn
        else:
            yield self


def default_device():
    """
    Returns the single serial device available or raises a ValueError.
    """
    lst = comports()
    if len(lst) == 1:
        return lst[0].device
    elif len(lst) == 0:
        raise ValueError("no serial device found!")
    else:
        port_list = "\n".join(f"  * {p.device}" for p in lst)
        msg = (
            "multiple serial devices found!\n"
            "Specify the device parameter with one of the following ports:\n"
            f"{port_list}"
        )
        raise ValueError(msg)


def default_logger(device, path=None, prepend_id=False, path_with_id=False):
    """
    Log is a function that receives a binary messasge and take some action.

    The default logger is print.
    """

    def log(msg):
        if path is None:
            now = datetime.now()
            ext = f"-{device.id}.log" if path_with_id else ".log"
            path_ = now.strftime("%Y%m%d%H%M") + ext
        else:
            path_ = path

        if isinstance(msg, str):
            msg = msg.encode("utf8")
        with open(path_, "ba") as fd:
            if prepend_id:
                timestamp = datetime.now().isoformat().rpartition('.')[0]
                prefix = '[%s] [%s]  ' % (device.id, timestamp)
                prefix = prefix.encode('utf8')
                msg = prefix_lines(prefix, msg)
            fd.write(msg)

    return log


def normalize_response(value: str) -> Value:
    """
    Try to coerce string value to integer or float.

    Return string if it does not succeed.
    """
    value = value.strip()
    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        pass

    return value


def prefix_lines(prefix: bytes, msg: bytes) -> bytes:
    lines = [prefix + ln for ln in msg.splitlines(True)]
    return b''.join(lines)

# lvp = LVP(functions=['print()', 'blink(n)'])
