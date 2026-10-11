"""Inicia o BTC Signal Watch (btc_signal_watch_pc.py) dentro do APK.

O arquivo do app NÃO é alterado: aqui só ajustamos o que muda no Android
(onde gravar o banco, notificações nativas e fuso horário).
"""
import os
import shutil
import sys
import threading
from os.path import dirname, join, exists

_started = False
_lock = threading.Lock()


def _copy_icons(home):
    src = join(dirname(__file__), "icon-192.png")
    if not exists(src):
        return
    for name in ("icon-192.png", "icon-512.png"):
        dst = join(home, name)
        if not exists(dst):
            shutil.copyfile(src, dst)


def _patch_notifications(m):
    """O app chama o comando 'termux-notification'. Aqui ele vira
    uma notificação nativa do Android."""
    try:
        from java import jclass
        notifier = jclass("com.btcsignal.watch.Notifier")
    except Exception:
        return

    class _Subprocess:
        DEVNULL = -3

        @staticmethod
        def run(cmd, **kwargs):
            try:
                title = cmd[cmd.index("--title") + 1]
                content = cmd[cmd.index("--content") + 1]
                notifier.show(title, content)
            except Exception:
                pass

    class _Shutil:
        @staticmethod
        def which(name):
            return "termux-notification" if name == "termux-notification" else None

    m.subprocess = _Subprocess
    m.shutil = _Shutil


def start(port=8080):
    global _started
    with _lock:
        if _started:
            return "ja_iniciado"

        home = os.environ["HOME"]
        os.makedirs(home, exist_ok=True)
        os.chdir(home)
        _copy_icons(home)

        # Fuso de Brasília: se o Android não tiver o banco de fusos, usa UTC-3 fixo
        try:
            from zoneinfo import ZoneInfo
            ZoneInfo("America/Sao_Paulo")
        except Exception:
            sys.modules["zoneinfo"] = None

        import btc_signal_watch_pc as m

        m.DB_PATH = join(home, "signals_pc_v1_8_manual.db")
        m.PORT = int(port)
        if not hasattr(m, "Path"):  # o app usa Path() sem importar; evita erro nas rotas de ícone
            import pathlib
            m.Path = pathlib.Path

        # Abre a porta primeiro: se já houver outro servidor (ex.: Termux), não duplica a análise
        try:
            server = m.ThreadingHTTPServer(("127.0.0.1", m.PORT), m.Handler)
        except OSError:
            return "porta_ocupada"

        _patch_notifications(m)
        m.init_db()
        m.init_test_db()
        if m.websocket is not None:
            threading.Thread(target=m.websocket_candle_loop, daemon=True, name="coinbase-candles-ws").start()
        threading.Thread(target=m.market_data_refresh_loop, daemon=True, name="coinbase-rest-refresh").start()
        threading.Thread(target=m.monitor_loop, daemon=True, name="signal-monitor").start()
        threading.Thread(target=server.serve_forever, daemon=True, name="http-server").start()
        _started = True
        return "ok"
