

from __future__ import annotations

import argparse
import queue
import tkinter as tk
from tkinter import colorchooser, messagebox, simpledialog
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import protocolo as p
from .cliente import JoinError, NetClient

POLL_MS = 25
MAX_EVENTS_PER_TICK = 200
CANVAS_W, CANVAS_H = 900, 560
DEFAULT_COLOR = "#1f77b4"
SHAPE_LABELS = (("Retângulo", "rect"), ("Oval", "oval"), ("Linha", "line"))


class App:
    def __init__(self, root: tk.Tk, client: NetClient, history: Sequence[Dict[str, Any]]) -> None:
        self.root = root
        self.client = client
        self.running = True
        self.connected = True
        self.color = DEFAULT_COLOR
        self._start: Optional[Tuple[int, int]] = None
        self._preview: Optional[int] = None

        self._build_ui()
        for item in history:  # formas desenhadas antes de eu entrar
            self.draw_shape(item["shape"], item["coords"], item["color"])

        root.protocol("WM_DELETE_WINDOW", self.on_close)
        root.after(POLL_MS, self._poll)

    # ------------------------------------------------------------------ UI ----
    def _build_ui(self) -> None:
        self.shape = tk.StringVar(value="rect")
        self.users_var = tk.StringVar(value="Usuários: -")
        self.rtt_var = tk.StringVar(value="RTT: -")
        self.notice_var = tk.StringVar(value="")

        toolbar = tk.Frame(self.root)
        toolbar.pack(side=tk.TOP, fill=tk.X, padx=6, pady=4)
        for label, value in SHAPE_LABELS:
            tk.Radiobutton(
                toolbar, text=label, value=value, variable=self.shape, indicatoron=False, padx=10, pady=2
            ).pack(side=tk.LEFT, padx=2)
        tk.Button(toolbar, text="Cor...", command=self.pick_color).pack(side=tk.LEFT, padx=(12, 2))
        self.swatch = tk.Label(toolbar, width=3, bg=self.color, relief=tk.SUNKEN)
        self.swatch.pack(side=tk.LEFT)
        tk.Button(toolbar, text="Limpar tela", command=self.clear_all).pack(side=tk.RIGHT, padx=2)

        self.canvas = tk.Canvas(
            self.root, width=CANVAS_W, height=CANVAS_H, bg="white", highlightthickness=1, cursor="crosshair"
        )
        self.canvas.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=6)
        self.canvas.bind("<ButtonPress-1>", self._on_press)
        self.canvas.bind("<B1-Motion>", self._on_drag)
        self.canvas.bind("<ButtonRelease-1>", self._on_release)

        status = tk.Frame(self.root)
        status.pack(side=tk.BOTTOM, fill=tk.X, padx=6, pady=4)
        tk.Label(status, textvariable=self.users_var, anchor="w").pack(side=tk.LEFT)
        tk.Label(status, textvariable=self.notice_var, fg="#b45309").pack(side=tk.LEFT, padx=12)
        tk.Label(status, textvariable=self.rtt_var, anchor="e").pack(side=tk.RIGHT)

    def pick_color(self) -> None:
        _, hex_color = colorchooser.askcolor(color=self.color, parent=self.root)
        if hex_color:
            self.color = hex_color.lower()
            self.swatch.configure(bg=self.color)

    # ------------------------------------------------------------- desenho ----
    def _create_item(self, shape: str, coords: Sequence[int], color: str, dash: Optional[Tuple[int, int]] = None) -> int:
        options: Dict[str, Any] = {"width": 2}
        options["fill" if shape == "line" else "outline"] = color
        if dash:
            options["dash"] = dash
        create = {
            "rect": self.canvas.create_rectangle,
            "oval": self.canvas.create_oval,
            "line": self.canvas.create_line,
        }[shape]
        return create(*coords, **options)

    def draw_shape(self, shape: str, coords: Sequence[int], color: str) -> None:
        self._create_item(shape, coords, color)

    def _on_press(self, event: Any) -> None:
        if not self.connected:
            return
        self._start = (event.x, event.y)
        self._preview = self._create_item(self.shape.get(), (event.x, event.y, event.x, event.y), self.color, dash=(4, 2))

    def _on_drag(self, event: Any) -> None:
        if self._start is not None and self._preview is not None:
            self.canvas.coords(self._preview, self._start[0], self._start[1], event.x, event.y)

    def _on_release(self, event: Any) -> None:
        if self._start is None:
            return
        x0, y0 = self._start
        if self._preview is not None:
            self.canvas.delete(self._preview)
        self._start = self._preview = None
        if (x0, y0) == (event.x, event.y):
            return  # clique sem arrastar não cria forma
        shape, coords = self.shape.get(), [x0, y0, event.x, event.y]
        self.draw_shape(shape, coords, self.color)  # desenho local imediato
        self.client.send_draw(shape, coords, self.color)

    def clear_all(self) -> None:
        if not self.connected:
            return
        self.canvas.delete("all")
        self.client.send_clear()

    # ------------------------------------------------------------- eventos ----
    def _poll(self) -> None:
        try:
            for _ in range(MAX_EVENTS_PER_TICK):
                self._handle(self.client.events.get_nowait())
        except queue.Empty:
            pass
        if self.running:
            self.root.after(POLL_MS, self._poll)

    def _handle(self, event: Dict[str, Any]) -> None:
        kind = event["type"]
        if kind == p.DRAW:
            self.draw_shape(event["shape"], event["coords"], event["color"])
        elif kind == p.CLEAR:
            self.canvas.delete("all")
        elif kind == p.STATUS:
            self.users_var.set(f"Usuários ({event['active_users']}): " + ", ".join(event["users_list"]))
        elif kind == p.ERROR:
            self.notice_var.set(event["reason"])
        elif kind == "_RTT":
            self.rtt_var.set(f"RTT: {event['rtt_ms']:.1f} ms")
        elif kind == "_NOTICE":
            self.notice_var.set(event["text"])
        elif kind == "_DISCONNECTED":
            self.connected = False
            self.notice_var.set("Desconectado")
            self.rtt_var.set("RTT: -")
            self.canvas.configure(cursor="X_cursor")
            messagebox.showwarning("NetCanvas", f"Conexão encerrada: {event['reason']}", parent=self.root)

    def on_close(self) -> None:
        self.running = False
        self.client.close()
        self.root.destroy()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Cliente gráfico do NetCanvas")
    parser.add_argument("--host", default="127.0.0.1", help="endereço do servidor")
    parser.add_argument("--port", type=int, default=5000, help="porta TCP do servidor")
    parser.add_argument("--user", default=None, help="nome de usuário (se omitido, será perguntado)")
    parser.add_argument("--no-udp", action="store_true", help="faz PING/PONG por TCP em vez de UDP")
    parser.add_argument("--ping-interval", type=float, default=3.0, help="segundos entre PINGs")
    args = parser.parse_args(argv)

    root = tk.Tk()
    root.withdraw()  # esconde a janela até a conexão dar certo
    username = args.user or simpledialog.askstring("NetCanvas", "Seu nome de usuário:", parent=root)
    if not username:
        return 1

    client = NetClient(args.host, args.port, username.strip(), ping_interval=args.ping_interval, use_udp=not args.no_udp)
    try:
        history = client.connect()
    except JoinError as exc:
        messagebox.showerror("NetCanvas", str(exc), parent=root)
        return 1

    root.title(f"NetCanvas - {username.strip()} @ {args.host}:{args.port}")
    root.deiconify()
    App(root, client, history)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
