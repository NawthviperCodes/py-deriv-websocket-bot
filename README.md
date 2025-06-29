# py-deriv-websocket-bot


# py-deriv-websocket-bot

An advanced multi-timeframe volatility trading bot built in Python using asynchronous programming, pandas dataframes, and the Deriv WebSocket API. It performs breakout and confluence analysis on synthetic indices like VIX75 (R_75) in real time.

## 🚀 Features

- Multi-timeframe confluence strategy
- Breakout detection on VIX75 / synthetic indices
- Real-time data stream via Deriv WebSocket API
- Asynchronous concurrency with `asyncio`
- Telegram signal alerts
- Technical indicators with `ta-lib` and `ta` packages
- Modular and extensible architecture

## ⚙️ Tech Stack

- Python 3.11+
- pandas
- ta-lib / `ta`
- asyncio
- websockets
- Deriv WebSocket API
- Telegram Bot API

## 📈 How it works

- Collects tick data from Deriv’s WebSocket
- Aggregates data into multiple timeframes
- Applies technical indicators (EMA, RSI, MACD, ATR, etc.)
- Identifies breakouts and sweeps
- Sends real-time trade signals to Telegram

## 🔒 Disclaimer

This project is for **educational and research** purposes only. Trading synthetic indices involves substantial risk, and you are solely responsible for your trades.

## 🚀 Getting Started

1. Clone the repo:
   ```bash
   git clone https://github.com/YOUR_USERNAME/py-deriv-websocket-bot.git
   cd py-deriv-websocket-bot
