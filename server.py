"""
Threads em execução:

aceitação  - ``accept()`` em laço, uma conexão TCP por cliente;
atendimento - uma thread dedicada por cliente (``recv`` bloqueante isolado);
UDP        - responde PING por datagrama (``UdpPingResponder``);
reaper     - remove sessões silenciosas (``Reaper``).

"""

from __future__ import annotations

import argparse
import logging
import socket
import threading
import time
from typing import Any, Dict, List, Optional

from . import protocolo as p
from .heartbeat import Reaper, UdpPingResponder
from .sala import AlreadyJoined, Room, Session, UsernameTaken
from .transporte import TcpChannel, UdpEndpoint

log = logging.getLogger("netcanvas.server")

IO_TIMEOUT = 5.0     # limite de uma operação de envio; também acorda o recv periodicamente
ACCEPT_POLL = 0.5    # intervalo para o laço de accept checar o pedido de parada


class NetCanvasServer:
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 5000,
        udp_port: Optional[int] = None,
        timeout: float = 15.0,
        max_history: int = 5000,
    ) -> None:
        """``port=0`` escolhe uma porta livre. ``udp_port=None`` usa o mesmo número do TCP.

        ``timeout`` é o tempo máximo sem nenhum sinal de vida (mensagem TCP ou PING UDP)
        antes de o servidor considerar o cliente morto.
        """
        self.host = host
        self.port = port
        self.udp_port = udp_port
        self.timeout = timeout
        self._room = Room(max_history=max_history)
        self._stopping = threading.Event()
        self._tcp: Optional[socket.socket] = None
        self._udp: Optional[UdpEndpoint] = None
        self._responder: Optional[UdpPingResponder] = None
        self._reaper: Optional[Reaper] = None
        self._accept_thread: Optional[threading.Thread] = None

    # ciclo 
    def start(self) -> None:
        """Abre os sockets e inicia as threads; retorna imediatamente."""
        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            tcp.bind((self.host, self.port))
            tcp.listen(128)
        except OSError:
            tcp.close()
            raise
        tcp.settimeout(ACCEPT_POLL)
        self.port = tcp.getsockname()[1]
        try:
            udp = UdpEndpoint(bind_addr=(self.host, self.port if self.udp_port is None else self.udp_port))
        except OSError:
            tcp.close()
            raise
        self.udp_port = udp.sockname[1]
        self._tcp, self._udp = tcp, udp

        self._responder = UdpPingResponder(udp, self._on_udp_ping)
        self._reaper = Reaper(self._room, self.timeout, self._on_expired)
        self._accept_thread = threading.Thread(target=self._accept_loop, name="accept", daemon=True)
        for thread in (self._responder, self._reaper, self._accept_thread):
            thread.start()
        log.info("servidor ouvindo em %s: TCP %d, UDP %d", self.host, self.port, self.udp_port)

    def serve_forever(self) -> None:
        self.start()
        try:
            while not self._stopping.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            log.info("encerrando por Ctrl+C")
        finally:
            self.stop()

    def stop(self) -> None:
        if self._stopping.is_set():
            return
        self._stopping.set()
        for worker in (self._responder, self._reaper):
            if worker is not None:
                worker.stop()
        if self._tcp is not None:
            self._tcp.close()
        for session in self._room.all_sessions():
            self._drop(session, "servidor encerrando")
        for thread in (self._accept_thread, self._responder, self._reaper):
            if thread is not None:
                thread.join(2.0)
        if self._udp is not None:
            self._udp.close()
        log.info("servidor encerrado")

    # ------------------------------------------------------------ consultas ----
    def users(self) -> List[str]:
        return self._room.users()

    def history_size(self) -> int:
        return self._room.history_size()

    # aceitação
    def _accept_loop(self) -> None:
        assert self._tcp is not None
        while not self._stopping.is_set():
            try:
                conn, addr = self._tcp.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.settimeout(IO_TIMEOUT)
            session = self._room.add_session(TcpChannel(conn), addr)
            log.info("conexão %d aceita de %s:%d", session.id, addr[0], addr[1])
            threading.Thread(
                target=self._serve_client, args=(session,), name=f"client-{session.id}", daemon=True
            ).start()

    # atendimento
    def _serve_client(self, session: Session) -> None:
        channel = session.channel
        reason = "conexão encerrada pelo cliente"
        try:
            while not self._stopping.is_set():
                try:
                    msg = channel.recv()
                except socket.timeout:
                    continue
                except p.FrameTooLarge:
                    reason = "mensagem grande demais"
                    break
                except p.ProtocolError as exc:
                    self._send(session, p.make_error(str(exc)))
                    continue
                if msg is None:
                    break
                session.touch()
                self._dispatch(session, msg)
        except OSError as exc:
            reason = f"erro de socket: {exc}"
        finally:
            self._drop(session, reason)

    def _dispatch(self, session: Session, msg: Dict[str, Any]) -> None:
        msg_type = msg["type"]
        if msg_type == p.PING:
            self._send(session, p.make_pong(msg["timestamp"]))
        elif msg_type == p.JOIN:
            self._handle_join(session, msg)
        elif not session.joined:
            self._send(session, p.make_error("envie JOIN antes de qualquer outra mensagem"))
        elif msg_type == p.DRAW:
            self._broadcast(msg, self._room.draw(session, msg))
        elif msg_type == p.CLEAR:
            self._broadcast(msg, self._room.clear(session))
        else:
            self._send(session, p.make_error(f"mensagem {msg_type} não é aceita do cliente"))

    def _handle_join(self, session: Session, msg: Dict[str, Any]) -> None:
        username = msg["username"].strip()

        def send_welcome(history: List[Dict[str, Any]]) -> None:
            session.channel.send(p.make_welcome(session.id, self.udp_port or 0, history))

        try:
            self._room.join(session, username, send_welcome)
        except UsernameTaken:
            self._send(session, p.make_error(f"o nome '{username}' já está em uso"))
            return
        except AlreadyJoined:
            self._send(session, p.make_error("você já entrou na sala"))
            return
        except OSError:
            self._drop(session, "falha ao enviar WELCOME")
            return
        log.info("%s entrou (cliente %d)", username, session.id)
        self._broadcast_status()

    # envio
    def _send(self, session: Session, msg: Dict[str, Any]) -> bool:
        try:
            session.channel.send(msg)
            return True
        except OSError:
            self._drop(session, "falha de envio")
            return False

    def _broadcast(self, msg: Dict[str, Any], targets: List[Session]) -> None:
        if not targets:
            return
        data = p.encode(msg) 
        for target in targets:
            try:
                target.channel.send_bytes(data)
            except OSError:
                self._drop(target, "falha de envio")

    def _broadcast_status(self) -> None:
        msg, targets = self._room.status()
        self._broadcast(msg, targets)

    # remoção
    def _drop(self, session: Session, reason: str) -> None:
        """Remove a sessão, fecha o socket e atualiza o STATUS (idempotente)."""
        was_joined = self._room.remove_session(session)
        if was_joined is None:
            return
        session.channel.close()
        log.info("cliente %d (%s) removido: %s", session.id, session.username or session.addr, reason)
        if was_joined and not self._stopping.is_set():
            self._broadcast_status()

    def _on_udp_ping(self, client_id: Optional[int]) -> None:
        self._room.touch(client_id)

    def _on_expired(self, session: Session) -> None:
        self._drop(session, f"sem sinal de vida há mais de {self.timeout:g}s")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Servidor do NetCanvas")
    parser.add_argument("--host", default="0.0.0.0", help="endereço de escuta (padrão: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5000, help="porta TCP (padrão: 5000)")
    parser.add_argument("--udp-port", type=int, default=None, help="porta UDP (padrão: igual à TCP)")
    parser.add_argument("--timeout", type=float, default=15.0, help="segundos sem sinal de vida até remover o cliente")
    parser.add_argument("--max-history", type=int, default=5000, help="formas guardadas para novos clientes")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s %(threadName)s: %(message)s",
    )
    NetCanvasServer(args.host, args.port, args.udp_port, args.timeout, args.max_history).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
