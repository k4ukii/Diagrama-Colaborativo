"""
Tipos de mensagem (8):

    JOIN     C -> S   entrar na sala com um username
    WELCOME  S -> C   confirma o JOIN: client_id, porta UDP e histórico de desenho
    ERROR    S -> C   recusa/erro de protocolo, com o motivo em texto
    DRAW     C <-> S  nova forma (rect | oval | line) com coordenadas e cor
    CLEAR    C <-> S  limpa o canvas de todos
    STATUS   S -> C   quantidade e lista de usuários ativos
    PING     C -> S   sonda de vivacidade com carimbo temporal (TCP ou UDP)
    PONG     S -> C   resposta ao PING, devolvendo o mesmo carimbo
"""

from __future__ import annotations

import json
import math
import re
from typing import Any, Dict, Iterable, List

JOIN = "JOIN"
WELCOME = "WELCOME"
ERROR = "ERROR"
DRAW = "DRAW"
CLEAR = "CLEAR"
STATUS = "STATUS"
PING = "PING"
PONG = "PONG"

SHAPES = ("rect", "oval", "line")
USERNAME_MAX = 20
COORD_LIMIT = 100_000
MAX_FRAME_BYTES = 4 * 1024 * 1024  # tamanho máximo de uma linha (WELCOME com histórico cabe)

_COLOR_RE = re.compile(r"#[0-9a-fA-F]{6}")


class ProtocolError(ValueError):
    """Mensagem malformada ou semanticamente inválida."""


class FrameTooLarge(ProtocolError):
    """Uma linha ultrapassou MAX_FRAME_BYTES sem terminador; o fluxo é irrecuperável."""



# Validação

def _require(condition: bool, text: str) -> None:
    if not condition:
        raise ProtocolError(text)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _v_join(msg: Dict[str, Any]) -> None:
    name = msg.get("username")
    _require(isinstance(name, str), "JOIN: 'username' deve ser texto")
    stripped = name.strip()
    _require(
        0 < len(stripped) <= USERNAME_MAX and stripped.isprintable(),
        f"JOIN: 'username' deve ter de 1 a {USERNAME_MAX} caracteres imprimíveis",
    )


def _v_draw(msg: Dict[str, Any]) -> None:
    _require(msg.get("shape") in SHAPES, f"DRAW: 'shape' deve ser um de {SHAPES}")
    coords = msg.get("coords")
    _require(
        isinstance(coords, list)
        and len(coords) == 4
        and all(_is_int(c) and abs(c) <= COORD_LIMIT for c in coords),
        "DRAW: 'coords' deve ser uma lista de 4 inteiros [x0, y0, x1, y1]",
    )
    color = msg.get("color")
    _require(
        isinstance(color, str) and _COLOR_RE.fullmatch(color) is not None,
        "DRAW: 'color' deve estar no formato #RRGGBB",
    )


def _v_welcome(msg: Dict[str, Any]) -> None:
    _require(_is_int(msg.get("client_id")), "WELCOME: 'client_id' deve ser inteiro")
    port = msg.get("udp_port")
    _require(_is_int(port) and 0 <= port <= 65535, "WELCOME: 'udp_port' inválida")
    history = msg.get("history")
    _require(isinstance(history, list), "WELCOME: 'history' deve ser uma lista")
    for item in history:
        _require(
            isinstance(item, dict) and item.get("type") == DRAW,
            "WELCOME: 'history' deve conter apenas mensagens DRAW",
        )
        _v_draw(item)


def _v_status(msg: Dict[str, Any]) -> None:
    count = msg.get("active_users")
    _require(_is_int(count) and count >= 0, "STATUS: 'active_users' inválido")
    users = msg.get("users_list")
    _require(
        isinstance(users, list) and all(isinstance(u, str) for u in users),
        "STATUS: 'users_list' deve ser uma lista de textos",
    )


def _v_ping(msg: Dict[str, Any]) -> None:
    _require(_is_number(msg.get("timestamp")), "PING: 'timestamp' deve ser numérico")
    if "client_id" in msg:
        _require(_is_int(msg["client_id"]), "PING: 'client_id' deve ser inteiro")


def _v_pong(msg: Dict[str, Any]) -> None:
    _require(_is_number(msg.get("timestamp")), "PONG: 'timestamp' deve ser numérico")


def _v_error(msg: Dict[str, Any]) -> None:
    _require(isinstance(msg.get("reason"), str), "ERROR: 'reason' deve ser texto")


def _v_clear(msg: Dict[str, Any]) -> None:
    return None


_VALIDATORS = {
    JOIN: _v_join,
    WELCOME: _v_welcome,
    ERROR: _v_error,
    DRAW: _v_draw,
    CLEAR: _v_clear,
    STATUS: _v_status,
    PING: _v_ping,
    PONG: _v_pong,
}

MESSAGE_TYPES = tuple(_VALIDATORS)


def validate(msg: Any) -> Dict[str, Any]:
    """Levanta ProtocolError se ``msg`` não for uma mensagem válida; senão a devolve."""
    _require(isinstance(msg, dict), "a mensagem deve ser um objeto JSON")
    msg_type = msg.get("type")
    _require(
        isinstance(msg_type, str) and msg_type in _VALIDATORS,
        f"tipo de mensagem desconhecido: {msg_type!r}",
    )
    _VALIDATORS[msg_type](msg)
    return msg



# Serialização

def encode(msg: Dict[str, Any]) -> bytes:
    """Valida e serializa a mensagem em uma linha JSON terminada por '\\n'."""
    validate(msg)
    return (json.dumps(msg, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def decode(line: "bytes | str") -> Dict[str, Any]:
    """Converte uma linha (sem ou com '\\n') em mensagem validada."""
    try:
        text = line.decode("utf-8") if isinstance(line, (bytes, bytearray)) else line
        obj = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"JSON inválido: {exc}") from exc
    return validate(obj)


class LineBuffer:
    """Acumula bytes de um fluxo TCP e devolve as linhas completas (framing).

    Tolera fragmentação (uma mensagem em vários ``recv``) e coalescência (várias
    mensagens em um único ``recv``). Linhas vazias são ignoradas.
    """

    def __init__(self, max_bytes: int = MAX_FRAME_BYTES) -> None:
        self._buf = b""
        self._max = max_bytes

    def feed(self, data: bytes) -> List[bytes]:
        *lines, rest = (self._buf + data).split(b"\n")
        self._buf = rest
        if len(rest) > self._max or any(len(line) > self._max for line in lines):
            self._buf = b""
            raise FrameTooLarge(f"linha maior que {self._max} bytes")
        return [line for line in lines if line.strip()]


# --------------------------------------------------------------------------- #
# Construtores de mensagens
# --------------------------------------------------------------------------- #
def make_join(username: str) -> Dict[str, Any]:
    return {"type": JOIN, "username": username}


def make_welcome(client_id: int, udp_port: int, history: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    return {"type": WELCOME, "client_id": client_id, "udp_port": udp_port, "history": list(history)}


def make_error(reason: str) -> Dict[str, Any]:
    return {"type": ERROR, "reason": reason}


def make_draw(shape: str, coords: Iterable[int], color: str) -> Dict[str, Any]:
    return {"type": DRAW, "shape": shape, "coords": list(coords), "color": color}


def make_clear() -> Dict[str, Any]:
    return {"type": CLEAR}


def make_status(users: Iterable[str]) -> Dict[str, Any]:
    users = list(users)
    return {"type": STATUS, "active_users": len(users), "users_list": users}


def make_ping(timestamp: float, client_id: "int | None" = None) -> Dict[str, Any]:
    msg: Dict[str, Any] = {"type": PING, "timestamp": timestamp}
    if client_id is not None:
        msg["client_id"] = client_id
    return msg


def make_pong(timestamp: float) -> Dict[str, Any]:
    return {"type": PONG, "timestamp": timestamp}
