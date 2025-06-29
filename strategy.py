import pandas as pd
import numpy as np
import logging
import ta
from datetime import time
from typing import Dict

logger = logging.getLogger(__name__)

class MultiTimeframeStrategy:
    def __init__(self, risk_percent=0.01):
        self.risk_percent = risk_percent
        self.timeframes = {
            'H1': 3600,
            'M15': 900,
            'M5': 300,
            'M1': 60
        }

    def resample_data(self, tick_data: pd.DataFrame, timeframe_seconds: int) -> pd.DataFrame:
        if tick_data.empty:
            return pd.DataFrame()
        df = tick_data.copy()
        df['time_bin'] = pd.to_datetime(df.index).floor(f'{timeframe_seconds}s')
        ohlc_data = df.groupby('time_bin').agg({
            'price': ['first', 'max', 'min', 'last'],
            'volume': 'sum'
        }).round(4)
        ohlc_data.columns = ['open', 'high', 'low', 'close', 'volume']
        return ohlc_data

    def calculate_trend_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.copy()
        data['ema_20'] = ta.trend.ema_indicator(data['close'], window=20)
        data['ema_50'] = ta.trend.ema_indicator(data['close'], window=50)
        data['ema_200'] = ta.trend.ema_indicator(data['close'], window=200)
        data['trend_up'] = (data['ema_20'] > data['ema_50']) & (data['ema_50'] > data['ema_200'])
        data['trend_down'] = (data['ema_20'] < data['ema_50']) & (data['ema_50'] < data['ema_200'])
        data['trend_neutral'] = ~(data['trend_up'] | data['trend_down'])
        data['adx'] = ta.trend.adx(data['high'], data['low'], data['close'], window=14)
        data['strong_trend'] = data['adx'] > 25
        data['pivot_high'] = data['high'].rolling(window=10, center=True).max() == data['high']
        data['pivot_low'] = data['low'].rolling(window=10, center=True).min() == data['low']
        return data

    def calculate_momentum_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.copy()
        data['rsi'] = ta.momentum.rsi(data['close'], window=14)
        data['rsi_oversold'] = data['rsi'] < 30
        data['rsi_overbought'] = data['rsi'] > 70
        data['rsi_bullish'] = (data['rsi'] > 50) & (data['rsi'].shift(1) <= 50)
        data['rsi_bearish'] = (data['rsi'] < 50) & (data['rsi'].shift(1) >= 50)
        macd = ta.trend.MACD(data['close'])
        data['macd'] = macd.macd()
        data['macd_signal'] = macd.macd_signal()
        data['macd_histogram'] = macd.macd_diff()
        data['macd_bullish'] = (data['macd'] > data['macd_signal']) & (data['macd'].shift(1) <= data['macd_signal'].shift(1))
        data['macd_bearish'] = (data['macd'] < data['macd_signal']) & (data['macd'].shift(1) >= data['macd_signal'].shift(1))
        data['stoch_k'], data['stoch_d'] = ta.momentum.stoch(data['high'], data['low'], data['close'])
        data['stoch_bullish'] = (data['stoch_k'] > data['stoch_d']) & (data['stoch_k'].shift(1) <= data['stoch_d'].shift(1))
        data['stoch_bearish'] = (data['stoch_k'] < data['stoch_d']) & (data['stoch_k'].shift(1) >= data['stoch_d'].shift(1))
        return data

    def calculate_entry_indicators(self, data: pd.DataFrame, lookback=15) -> pd.DataFrame:
        data = data.copy()

        # Defensive: check for ATR window length
        if len(data) >= 14:
            data['atr'] = ta.volatility.average_true_range(data['high'], data['low'], data['close'], window=14)
        else:
            logger.warning(f"Not enough candles ({len(data)}) for ATR(window=14). Filling ATR with fallback.")
            data['atr'] = np.nan
            data['atr'] = data['high'] - data['low']  # rough range as fallback

        data['support'] = data['low'].rolling(window=lookback).min()
        data['resistance'] = data['high'].rolling(window=lookback).max()
        data['vol_ma'] = data['volume'].rolling(window=20).mean()
        data['volume_spike'] = data['volume'] > data['vol_ma'] * 1.5
        data['upper_wick'] = data['high'] - data[['open', 'close']].max(axis=1)
        data['lower_wick'] = data[['open', 'close']].min(axis=1) - data['low']
        data['body_size'] = abs(data['close'] - data['open'])

        data['sweep_up'] = (
            (data['upper_wick'] > 0.6 * data['atr']) &
            (data['high'] > data['resistance'].shift(1)) &
            (data['body_size'] > 0.3 * data['atr'])
        )

        data['sweep_down'] = (
            (data['lower_wick'] > 0.6 * data['atr']) &
            (data['low'] < data['support'].shift(1)) &
            (data['body_size'] > 0.3 * data['atr'])
        )

        data['range_size'] = data['resistance'] - data['support']
        data['in_range'] = (data['close'] >= data['support'] * 0.999) & (data['close'] <= data['resistance'] * 1.001)
        data['breakout_strength_up'] = (data['close'] - data['resistance'].shift(1)) / data['atr']
        data['breakout_strength_down'] = (data['support'].shift(1) - data['close']) / data['atr']

        return data

    def get_higher_tf_bias(self, h1_data: pd.DataFrame, current_time) -> Dict:
        if h1_data.empty or len(h1_data) < 50:
            return {'bias': 'neutral', 'strength': 0, 'confluence': False}
        latest = h1_data.iloc[-1]
        if latest['trend_up'] and latest['strong_trend']:
            bias = 'bullish'
            strength = min(latest['adx'] / 50, 1.0)
        elif latest['trend_down'] and latest['strong_trend']:
            bias = 'bearish'
            strength = min(latest['adx'] / 50, 1.0)
        else:
            bias = 'neutral'
            strength = 0.3
        confluence = False
        if bias == 'bullish':
            confluence = latest['rsi'] > 45 and latest['macd'] > latest['macd_signal']
        elif bias == 'bearish':
            confluence = latest['rsi'] < 55 and latest['macd'] < latest['macd_signal']
        return {'bias': bias, 'strength': strength, 'confluence': confluence, 'adx': latest['adx'], 'rsi': latest['rsi']}

    def get_medium_tf_confluence(self, m15_data: pd.DataFrame) -> Dict:
        if m15_data.empty or len(m15_data) < 20:
            return {'bullish_signals': 0, 'bearish_signals': 0, 'momentum': 'neutral'}
        latest = m15_data.iloc[-1]
        recent = m15_data.iloc[-3:]
        bullish_signals = bearish_signals = 0
        if latest['rsi_bullish'] or (latest['rsi'] > 50 and recent['rsi'].mean() > latest['rsi'] - 5):
            bullish_signals += 1
        if latest['rsi_bearish'] or (latest['rsi'] < 50 and recent['rsi'].mean() < latest['rsi'] + 5):
            bearish_signals += 1
        if latest['macd_bullish']:
            bullish_signals += 1
        if latest['macd_bearish']:
            bearish_signals += 1
        if latest['stoch_bullish']:
            bullish_signals += 1
        if latest['stoch_bearish']:
            bearish_signals += 1
        if bullish_signals >= 2:
            momentum = 'bullish'
        elif bearish_signals >= 2:
            momentum = 'bearish'
        else:
            momentum = 'neutral'
        return {'bullish_signals': bullish_signals, 'bearish_signals': bearish_signals, 'momentum': momentum, 'rsi': latest['rsi'], 'macd_above_signal': latest['macd'] > latest['macd_signal']}

    def is_active_session(self, dt_index) -> bool:
        hour = dt_index.hour if hasattr(dt_index, 'hour') else dt_index.time().hour
        london = 8 <= hour <= 17
        ny = 13 <= hour <= 22
        overlap = 13 <= hour <= 17
        return london or ny, overlap

    def execute_strategy(self, tick_data: pd.DataFrame) -> pd.DataFrame:
        h1 = self.resample_data(tick_data, self.timeframes['H1'])
        m15 = self.resample_data(tick_data, self.timeframes['M15'])
        m5 = self.resample_data(tick_data, self.timeframes['M5'])
        if any(df.empty for df in [h1, m15, m5]):
            logger.warning("Insufficient data for multi-timeframe strategy")
            return pd.DataFrame()
        h1 = self.calculate_trend_indicators(h1)
        m15 = self.calculate_momentum_indicators(m15)
        m5 = self.calculate_entry_indicators(m5)
        m5['signal'] = 0
        m5['sl'] = np.nan
        m5['tp'] = np.nan
        m5['position_size'] = 1.0
        m5['confluence_score'] = 0
        m5['htf_bias'] = 'neutral'
        for i in range(50, len(m5)):
            current_time = m5.index[i]
            current = m5.iloc[i]
            prev = m5.iloc[i - 1]
            in_session, high_liquidity = self.is_active_session(current_time)
            if not in_session:
                continue
            htf_bias = self.get_higher_tf_bias(h1, current_time)
            mtf_confluence = self.get_medium_tf_confluence(m15)
            score = 0
            if htf_bias['confluence']:
                score += 2
            if htf_bias['strength'] > 0.6:
                score += 2
            if mtf_confluence['momentum'] != 'neutral':
                score += min(max(mtf_confluence['bullish_signals'], mtf_confluence['bearish_signals']), 3)
            if high_liquidity:
                score += 1
            if current['volume_spike']:
                score += 2
            m5.loc[m5.index[i], 'confluence_score'] = score
            m5.loc[m5.index[i], 'htf_bias'] = htf_bias['bias']
            if score < 5:
                continue
            recent_sweeps = m5['sweep_up'].iloc[i - 5:i].sum() + m5['sweep_down'].iloc[i - 5:i].sum()
            valid_buy = (
                htf_bias['bias'] in ['bullish', 'neutral'] and
                mtf_confluence['momentum'] in ['bullish', 'neutral'] and
                recent_sweeps >= 1 and
                current['close'] > prev['resistance'] * 1.0015 and
                current['breakout_strength_up'] > 0.5 and
                current['volume_spike'] and
                m5['in_range'].iloc[i - 5:i].sum() >= 2
            )
            valid_sell = (
                htf_bias['bias'] in ['bearish', 'neutral'] and
                mtf_confluence['momentum'] in ['bearish', 'neutral'] and
                recent_sweeps >= 1 and
                current['close'] < prev['support'] * 0.9985 and
                current['breakout_strength_down'] > 0.5 and
                current['volume_spike'] and
                m5['in_range'].iloc[i - 5:i].sum() >= 2
            )
            base_size = self.risk_percent * 10000 / (current['atr'] * 2)
            size = min(base_size * min(score / 5, 2), 2.0)
            if valid_buy:
                m5.loc[m5.index[i], 'signal'] = 1
                m5.loc[m5.index[i], 'sl'] = current['close'] - current['atr'] * 1.8
                m5.loc[m5.index[i], 'tp'] = current['close'] + current['atr'] * 3.0
                m5.loc[m5.index[i], 'position_size'] = size
                logger.info(f"BUY Signal at {current_time} price={current['close']:.2f}")
            elif valid_sell:
                m5.loc[m5.index[i], 'signal'] = -1
                m5.loc[m5.index[i], 'sl'] = current['close'] + current['atr'] * 1.8
                m5.loc[m5.index[i], 'tp'] = current['close'] - current['atr'] * 3.0
                m5.loc[m5.index[i], 'position_size'] = size
                logger.info(f"SELL Signal at {current_time} price={current['close']:.2f}")
        return m5

def breakout_strategy(data, lookback=15, risk_percent=0.01):
    strategy = MultiTimeframeStrategy(risk_percent)
    if 'open' in data.columns and 'high' in data.columns:
        result = strategy.calculate_entry_indicators(data, lookback)
        result['signal'] = 0
        result['sl'] = np.nan
        result['tp'] = np.nan
        result['position_size'] = 1.0
        for i in range(lookback + 20, len(result)):
            current = result.iloc[i]
            prev = result.iloc[i - 1]
            in_session, _ = strategy.is_active_session(result.index[i])
            if not in_session:
                continue
            recent_sweeps = result['sweep_up'].iloc[i - 5:i].sum() + result['sweep_down'].iloc[i - 5:i].sum()
            valid_buy = (
                recent_sweeps >= 1 and
                current['close'] > prev['resistance'] * 1.002 and
                current['volume_spike'] and
                result['in_range'].iloc[i - 5:i].sum() >= 2
            )
            valid_sell = (
                recent_sweeps >= 1 and
                current['close'] < prev['support'] * 0.998 and
                current['volume_spike'] and
                result['in_range'].iloc[i - 5:i].sum() >= 2
            )
            size = min(1.0, (risk_percent * 10000) / (current['atr'] * 2))
            if valid_buy:
                result.loc[result.index[i], 'signal'] = 1
                result.loc[result.index[i], 'sl'] = current['close'] - current['atr'] * 1.5
                result.loc[result.index[i], 'tp'] = current['close'] + current['atr'] * 2.5
                result.loc[result.index[i], 'position_size'] = size
            elif valid_sell:
                result.loc[result.index[i], 'signal'] = -1
                result.loc[result.index[i], 'sl'] = current['close'] + current['atr'] * 1.5
                result.loc[result.index[i], 'tp'] = current['close'] - current['atr'] * 2.5
                result.loc[result.index[i], 'position_size'] = size
        return result
    else:
        return strategy.execute_strategy(data)
