
from __future__ import annotations

import logging
import socket
import threading
from typing import Callable, Optional

from . import protocolo as p
from .sala import Room, Session
from .transporte import UdpEndpoint

log = logging.getLogger("netcanvas.heartbeat")


class UdpPingResponder(threading.Thread):
    def __init__(self, endpoint: UdpEndpoint, on_ping: Callable[[Optional[int]], None]) -> None:
        super().__init__(name="udp-ping", daemon=True)
        self._endpoint = endpoint
        self._on_ping = on_ping
        self._stop_event = threading.Event()
        endpoint.settimeout(0.5)  # acorda periodicamente para checar o pedido de parada

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                msg, addr = self._endpoint.recv()
            except socket.timeout:
                continue
            except p.ProtocolError as exc:
                log.debug("datagrama UDP ignorado: %s", exc)
                continue
            except OSError:
                break
            if msg["type"] != p.PING:
                continue
            try:
                self._on_ping(msg.get("client_id"))
                self._endpoint.send_to(p.make_pong(msg["timestamp"]), addr)
            except OSError as exc:
                log.debug("falha ao responder PING UDP: %s", exc)
            except Exception:  # callback
                log.exception("erro no callback de PING")

    def stop(self) -> None:
        self._stop_event.set()


class Reaper(threading.Thread):
    def __init__(
        self,
        room: Room,
        timeout: float,
        on_expired: Callable[[Session], None],
        interval: Optional[float] = None,
    ) -> None:
        super().__init__(name="reaper", daemon=True)
        self._room = room
        self._timeout = timeout
        self._on_expired = on_expired
        self.interval = interval if interval is not None else max(0.1, timeout / 3)
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.wait(self.interval):
            for session in self._room.expired(self._timeout):
                try:
                    self._on_expired(session)
                except Exception: 
                    log.exception("erro ao remover sessão inativa")

    def stop(self) -> None:
        self._stop_event.set()
