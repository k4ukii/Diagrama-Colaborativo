

from __future__ import annotations

import logging
import queue
import socket
import threading
import time
from typing import Any, Dict, Iterable, List, Optional

from . import protocolo as p
from .transporte import Address, TcpChannel, UdpEndpoint

log = logging.getLogger("netcanvas.client")

IO_TIMEOUT = 5.0  


class JoinError(Exception):
    """Não foi possível conectar ou o servidor recusou o JOIN."""


class NetClient:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        ping_interval: float = 3.0,
        use_udp: bool = True,
        connect_timeout: float = 5.0,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.ping_interval = ping_interval
        self.connect_timeout = connect_timeout
        self.events: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.client_id: Optional[int] = None
        self.rtt_ms: Optional[float] = None

        self._use_udp = use_udp
        self._channel: Optional[TcpChannel] = None
        self._udp: Optional[UdpEndpoint] = None
        self._udp_addr: Optional[Address] = None
        self._threads: List[threading.Thread] = []
        self._stop = threading.Event()
        self._fail_lock = threading.Lock()
        self._failed = False
        self._last_pong = time.monotonic()

    #  conexão 
    def connect(self) -> List[Dict[str, Any]]:
        """Conecta, entra na sala e devolve o histórico de formas já desenhadas."""
        try:
            channel = TcpChannel.connect(self.host, self.port, timeout=self.connect_timeout)
        except OSError as exc:
            raise JoinError(f"não foi possível conectar a {self.host}:{self.port}: {exc}") from exc

        try:
            channel.send(p.make_join(self.username))
            channel.settimeout(self.connect_timeout)
            reply = channel.recv()
        except (OSError, p.ProtocolError) as exc:
            channel.close()
            raise JoinError(f"falha no handshake: {exc}") from exc
        if reply is None:
            channel.close()
            raise JoinError("o servidor encerrou a conexão durante o JOIN")
        if reply["type"] == p.ERROR:
            channel.close()
            raise JoinError(reply["reason"])
        if reply["type"] != p.WELCOME:
            channel.close()
            raise JoinError(f"resposta inesperada ao JOIN: {reply['type']}")

        channel.settimeout(IO_TIMEOUT)
        self._channel = channel
        self.client_id = reply["client_id"]
        self._last_pong = time.monotonic()

        if self._use_udp:
            peer = channel.peername
            self._udp = UdpEndpoint()
            self._udp.settimeout(0.5)
            self._udp_addr = (peer[0] if peer else self.host, reply["udp_port"])

        self._start_thread(self._listen_tcp, "listen-tcp")
        if self._udp is not None:
            self._start_thread(self._listen_udp, "listen-udp")
        self._start_thread(self._heartbeat, "heartbeat")
        return reply["history"]

    def _start_thread(self, target: Any, name: str) -> None:
        thread = threading.Thread(target=target, name=f"client-{name}", daemon=True)
        self._threads.append(thread)
        thread.start()

    # envio 
    def send_draw(self, shape: str, coords: Iterable[int], color: str) -> bool:
        return self._send(p.make_draw(shape, coords, color))

    def send_clear(self) -> bool:
        return self._send(p.make_clear())

    def _send(self, msg: Dict[str, Any]) -> bool:
        if self._channel is None or self._failed or self._stop.is_set():
            return False
        try:
            self._channel.send(msg)
            return True
        except OSError as exc:
            self._fail(f"falha ao enviar: {exc}")
            return False

    #  escuta 
    def _listen_tcp(self) -> None:
        channel = self._channel
        assert channel is not None
        while not self._stop.is_set():
            try:
                msg = channel.recv()
            except socket.timeout:
                continue
            except p.FrameTooLarge:
                self._fail("o servidor enviou uma mensagem grande demais")
                return
            except p.ProtocolError as exc:
                log.warning("mensagem inválida do servidor ignorada: %s", exc)
                continue
            except OSError as exc:
                self._fail(f"conexão perdida: {exc}")
                return
            if msg is None:
                self._fail("o servidor encerrou a conexão")
                return
            if msg["type"] == p.PONG:
                self._on_pong(msg)
            else:
                self.events.put(msg)

    def _listen_udp(self) -> None:
        udp = self._udp
        assert udp is not None
        while not self._stop.is_set():
            try:
                msg, _ = udp.recv()
            except socket.timeout:
                continue
            except p.ProtocolError:
                continue
            except OSError:
                return
            if msg["type"] == p.PONG:
                self._on_pong(msg)

    def _on_pong(self, msg: Dict[str, Any]) -> None:
        self._last_pong = time.monotonic()
        rtt = max(0.0, (time.time() - msg["timestamp"]) * 1000.0)
        self.rtt_ms = rtt
        self.events.put({"type": "_RTT", "rtt_ms": rtt})

    # heartbeat 
    def _heartbeat(self) -> None:
        """PING periódico (UDP por padrão) e detecção de servidor mudo.

        Sem PONG por 3 intervalos, troca para TCP (UDP pode estar bloqueado por
        firewall/NAT); sem PONG por 5 intervalos, considera o servidor morto.
        """
        dead_after = 5 * self.ping_interval
        while not self._stop.wait(self.ping_interval):
            silence = time.monotonic() - self._last_pong
            if self._udp is not None and silence > 3 * self.ping_interval:
                self._switch_to_tcp()
            elif silence > dead_after:
                self._fail(f"o servidor não responde há {silence:.0f}s")
                return
            ping = p.make_ping(time.time(), self.client_id)
            try:
                if self._udp is not None and self._udp_addr is not None:
                    self._udp.send_to(ping, self._udp_addr)
                elif self._channel is not None:
                    self._channel.send(ping)
            except OSError as exc:
                if self._udp is not None:
                    log.debug("falha ao enviar PING UDP: %s", exc)
                else:
                    self._fail(f"falha ao enviar PING: {exc}")
                    return

    def _switch_to_tcp(self) -> None:
        udp, self._udp = self._udp, None
        if udp is not None:
            udp.close()
        self.events.put({"type": "_NOTICE", "text": "UDP indisponível: PING/PONG passou a usar TCP"})

    
    # encerra
    def _fail(self, reason: str) -> None:
        """Encerra por falha (uma única vez) e avisa a interface."""
        if self._stop.is_set():
            return  # encerramento solicitado pelo usuário
        with self._fail_lock:
            if self._failed:
                return
            self._failed = True
        self.events.put({"type": "_DISCONNECTED", "reason": reason})
        self._shutdown_sockets()

    def _shutdown_sockets(self) -> None:
        if self._channel is not None:
            self._channel.close()
        if self._udp is not None:
            self._udp.close()

    def close(self) -> None:
        """Encerra o cliente; seguro chamar mais de uma vez."""
        self._stop.set()
        self._shutdown_sockets()
        current = threading.current_thread()
        for thread in self._threads:
            if thread is not current:
                thread.join(1.0)
