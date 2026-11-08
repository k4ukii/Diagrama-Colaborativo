

from __future__ import annotations

import itertools
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

from . import protocolo as p


class UsernameTaken(Exception):
    """Já existe um usuário com esse nome na sala."""


class AlreadyJoined(Exception):
    """A sessão já enviou um JOIN aceito."""


@dataclass(eq=False)
class Session:
    id: int
    channel: Any  # precisa de send/send_bytes/close (TcpChannel)
    addr: Any
    username: Optional[str] = None
    last_seen: float = field(default_factory=time.monotonic)

    @property
    def joined(self) -> bool:
        return self.username is not None

    def touch(self) -> None:
        self.last_seen = time.monotonic()


class Room:
    def __init__(self, max_history: int = 5000) -> None:
        self._lock = threading.Lock()
        self._sessions: Dict[int, Session] = {}
        self._history: Deque[Dict[str, Any]] = deque(maxlen=max_history)
        self._ids = itertools.count(1)

    #ciclo de vida das sessões
    def add_session(self, channel: Any, addr: Any) -> Session:
        with self._lock:
            session = Session(id=next(self._ids), channel=channel, addr=addr)
            self._sessions[session.id] = session
            return session

    def remove_session(self, session: Session) -> Optional[bool]:
        """Remove a sessão"""
        with self._lock:
            if self._sessions.pop(session.id, None) is None:
                return None
            return session.joined

    def join(
        self,
        session: Session,
        username: str,
        on_registered: Callable[[List[Dict[str, Any]]], None],
    ) -> None:
        
        key = username.casefold()
        with self._lock:
            if session.joined:
                raise AlreadyJoined()
            if any(s.joined and s.username.casefold() == key for s in self._sessions.values()):
                raise UsernameTaken(username)
            session.username = username
            on_registered(list(self._history))

    # desenho 
    def draw(self, sender: Session, msg: Dict[str, Any]) -> List[Session]:
        
        with self._lock:
            self._history.append(msg)
            return self._others(sender)

    def clear(self, sender: Session) -> List[Session]:
    
        with self._lock:
            self._history.clear()
            return self._others(sender)

    # consultas 
    def status(self) -> Tuple[Dict[str, Any], List[Session]]:
        with self._lock:
            joined = self._joined()
            return p.make_status([s.username for s in joined]), joined

    def users(self) -> List[str]:
        with self._lock:
            return [s.username for s in self._joined()]

    def all_sessions(self) -> List[Session]:
        with self._lock:
            return list(self._sessions.values())

    def history_size(self) -> int:
        with self._lock:
            return len(self._history)

    # vivacidade 
    def touch(self, client_id: Optional[int]) -> bool:
        with self._lock:
            session = self._sessions.get(client_id)
            if session is None:
                return False
            session.touch()
            return True

    def expired(self, timeout: float, now: Optional[float] = None) -> List[Session]:
        now = time.monotonic() if now is None else now
        with self._lock:
            return [s for s in self._sessions.values() if now - s.last_seen > timeout]

    # internos  #
    def _joined(self) -> List[Session]:
        return sorted((s for s in self._sessions.values() if s.joined), key=lambda s: s.id)

    def _others(self, sender: Session) -> List[Session]:
        return [s for s in self._joined() if s is not sender]
