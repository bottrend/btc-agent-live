# BTC Agent Live

Event-driven BTC scoring service. Data is recalculated only when a source emits new data.

## Scoring
Every atomic feature has raw value + score 0–100. Features roll up into 10 groups and a final 0–100 score. Missing data is not silently scored as neutral.

## Current collector
OKX public WebSocket: BTC-USDT ticker, books5, trades, BTC-USDT-SWAP funding and open interest. Architecture is intended to add independent free/public sources incrementally.

## Railway
Runs as a worker with `python main.py`. Secrets are never committed. Optional private API/Telegram credentials belong in Railway environment variables.
