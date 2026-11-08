
from __future__ import annotations

import socket
import threading
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple

import protocolo as p

Address = Tuple[str, int]


class TcpChannel:
    """Envolve um socket TCP."""

    RECV_SIZE = 65536

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._buffer = p.LineBuffer()
        self._pending: Deque[bytes] = deque()
        self._send_lock = threading.Lock()
        self._closed = False
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:  # p.ex. socketpair() em testes
            pass

    @classmethod
    def connect(cls, host: str, port: int, timeout: Optional[float] = 5.0) -> "TcpChannel":
        return cls(socket.create_connection((host, port), timeout=timeout))

    @property
    def peername(self) -> Optional[Address]:
        try:
            return self._sock.getpeername()[:2]
        except OSError:
            return None

    def settimeout(self, timeout: Optional[float]) -> None:
        self._sock.settimeout(timeout)

    def send(self, msg: Dict[str, Any]) -> None:
        self.send_bytes(p.encode(msg))

    def send_bytes(self, data: bytes) -> None:
        """Envia bytes já codificados; garante que mensagens não se misturem."""
        with self._send_lock:
            self._sock.sendall(data)

    def recv(self) -> Optional[Dict[str, Any]]:
       
        while not self._pending:
            data = self._sock.recv(self.RECV_SIZE)
            if not data:
                return None
            self._pending.extend(self._buffer.feed(data))
        return p.decode(self._pending.popleft())

    def close(self) -> None:
        """Fecha o canal; também desbloqueia um recv."""
        if self._closed:
            return
        self._closed = True
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass


class UdpEndpoint:
    """Socket UDP que envia e recebe uma mensagem por datagrama."""

    MAX_DATAGRAM = 65535

    def __init__(self, bind_addr: Optional[Address] = None) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if bind_addr is not None:
            try:
                self._sock.bind(bind_addr)
            except OSError:
                self._sock.close()
                raise

    @property
    def sockname(self) -> Address:
        return self._sock.getsockname()[:2]

    def settimeout(self, timeout: Optional[float]) -> None:
        self._sock.settimeout(timeout)

    def send_to(self, msg: Dict[str, Any], addr: Address) -> None:
        self._sock.sendto(p.encode(msg), addr)

    def recv(self) -> Tuple[Dict[str, Any], Address]:
       
        data, addr = self._sock.recvfrom(self.MAX_DATAGRAM)
        return p.decode(data), addr[:2]

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass
