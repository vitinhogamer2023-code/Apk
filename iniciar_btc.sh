#!/data/data/com.termux/files/usr/bin/bash

cd "$HOME/btc-signal"

# Mantém o Android/Termux acordado enquanto o app estiver rodando
termux-wake-lock 2>/dev/null || true

echo "======================================"
echo " BTC SIGNAL WATCH — MODO 24 HORAS"
echo "======================================"
echo "O app será reiniciado automaticamente se cair."
echo ""

trap 'echo ""; echo "Programa encerrado."; exit 0' INT TERM
while true
do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Iniciando BTC Signal Watch..."
    python btc_signal_watch_pc.py
    STATUS=$?
    [ "$STATUS" -eq 130 ] && exit 0
    echo "App encerrado. Reiniciando em 5 segundos..."
    sleep 5
done
