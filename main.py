import asyncio
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
import websockets
import websockets.exceptions
from telegram_bot import send_telegram_message
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DERIV_API_TOKEN, DERIV_APP_ID
from strategy import MultiTimeframeStrategy, breakout_strategy

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DerivWSClient:
    def __init__(self, app_id, api_token):
        self.app_id = app_id
        self.api_token = api_token
        self.websocket = None
        self.uri = "wss://ws.binaryws.com/websockets/v3?app_id=" + str(app_id)
        self.data_buffer = []
        self.connected = False
        self.heartbeat_task = None
        self.last_received = None
        self.subscription_active = False
        self.processed_signals = set()  # Track processed signals to avoid duplicates

    async def connect(self):
        """Connect to Deriv WebSocket API"""
        if self.websocket:
            try:
                await self.websocket.close()
            except:
                pass
            
        self.websocket = await websockets.connect(self.uri)
        self.connected = True
        self.last_received = datetime.now()
        self.subscription_active = False
        logger.info("Connected to Deriv WebSocket")

    async def monitor_connection(self):
        """Monitor connection health"""
        while self.connected:
            try:
                await asyncio.sleep(30)
                # If we're subscribed but not getting data, assume connection issue
                if self.subscription_active and (datetime.now() - self.last_received).total_seconds() > 60:
                    logger.warning("No data received for 60 seconds, connection may be stale")
                    self.connected = False
                    break
            except Exception as e:
                logger.error(f"Connection monitoring failed: {e}")
                self.connected = False
                break

    async def authorize(self):
        """Authorize the connection"""
        auth_msg = {
            "authorize": self.api_token
        }
        await self.send(auth_msg)
        response = await self.receive()
        if response.get("error"):
            raise ConnectionError(f"Authorization failed: {response['error']['message']}")
        logger.info("Successfully authorized with Deriv API")

    async def subscribe_ticks(self, symbol="R_75"):
        """Subscribe to tick data with proper error handling"""
        sub_msg = {
            "ticks": symbol,
            "subscribe": 1
        }
        await self.send(sub_msg)
        logger.info(f"Sent subscription request for {symbol}")
        
        # Wait for subscription response
        max_attempts = 5
        attempts = 0
        
        while attempts < max_attempts:
            try:
                response = await asyncio.wait_for(self.receive(), timeout=5)
                logger.info(f"Subscription response: {response}")
                
                if response.get("error"):
                    error_msg = response['error']['message']
                    logger.error(f"Subscription error: {error_msg}")
                    # Try alternative symbol names
                    if symbol == "R_75":
                        logger.info("Trying alternative symbol: volatility_75")
                        return await self.subscribe_ticks("volatility_75")
                    elif symbol == "volatility_75":
                        logger.info("Trying alternative symbol: RDBULL")
                        return await self.subscribe_ticks("RDBULL")
                    else:
                        raise ConnectionError(f"Subscription failed: {error_msg}")
                
                if response.get("msg_type") == "tick":
                    self.subscription_active = True
                    logger.info(f"Successfully subscribed to {symbol} - received first tick")
                    return response
                elif "subscription" in response:
                    logger.info(f"Subscription confirmed for {symbol}")
                    self.subscription_active = True
                    return response
                    
                attempts += 1
                
            except asyncio.TimeoutError:
                attempts += 1
                logger.warning(f"Subscription attempt {attempts}/{max_attempts} timed out")
                
        raise TimeoutError(f"Failed to confirm subscription after {max_attempts} attempts")

    async def send(self, message):
        """Send a message to the WebSocket"""
        if not self.connected:
            raise ConnectionError("WebSocket is not connected")
            
        if isinstance(message, dict):
            message = json.dumps(message)
        await self.websocket.send(message)
        logger.debug(f"Sent: {message}")

    async def receive(self):
        """Receive a message from the WebSocket"""
        if not self.connected:
            raise ConnectionError("WebSocket is not connected")
            
        try:
            response = await self.websocket.recv()
            self.last_received = datetime.now()
            parsed_response = json.loads(response)
            logger.debug(f"Received: {parsed_response}")
            return parsed_response
        except websockets.exceptions.ConnectionClosed as e:
            logger.error(f"Connection closed: {e}")
            self.connected = False
            self.subscription_active = False
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode message: {response}")
            raise

    async def get_available_symbols(self):
        """Get list of available symbols for trading"""
        try:
            symbols_msg = {
                "active_symbols": "brief",
                "product_type": "basic"
            }
            await self.send(symbols_msg)
            response = await self.receive()
            
            if response.get("active_symbols"):
                symbols = response["active_symbols"]
                volatility_symbols = [s for s in symbols if "volatility" in s.get("symbol", "").lower() or "R_" in s.get("symbol", "")]
                logger.info(f"Available volatility symbols: {[s['symbol'] for s in volatility_symbols[:10]]}")
                return [s['symbol'] for s in volatility_symbols]
            return []
        except Exception as e:
            logger.error(f"Failed to get symbols: {e}")
            return []

    async def collect_extended_data(self, limit=500, max_retries=3, timeout=600):
        """Collect extended market data for multi-timeframe analysis"""
        retries = 0
        self.data_buffer = []
        
        while retries < max_retries:
            try:
                await self.connect()
                await self.authorize()
                
                # Get available symbols first
                symbols = await self.get_available_symbols()
                if symbols:
                    logger.info(f"Found {len(symbols)} available symbols")
                
                # Try to subscribe to ticks
                await self.subscribe_ticks()
                
                # Start connection monitoring
                self.heartbeat_task = asyncio.create_task(self.monitor_connection())
                
                # Collect data with progress updates
                start_time = datetime.now()
                consecutive_timeouts = 0
                max_consecutive_timeouts = 5
                last_progress_report = 0
                
                while len(self.data_buffer) < limit:
                    try:
                        # Receive data with timeout
                        response = await asyncio.wait_for(self.receive(), timeout=15)
                        
                        if response.get('tick'):
                            tick = response['tick']
                            self.data_buffer.append({
                                'time': datetime.fromtimestamp(tick['epoch']),
                                'price': tick['quote'],
                                'volume': 1  # Deriv doesn't provide volume, use 1 as placeholder
                            })
                            
                            # Progress reporting
                            if len(self.data_buffer) - last_progress_report >= 50:
                                logger.info(f"Collected {len(self.data_buffer)}/{limit} ticks - Latest: {tick['quote']:.4f}")
                                last_progress_report = len(self.data_buffer)
                            
                            consecutive_timeouts = 0  # Reset timeout counter
                            
                        elif response.get('error'):
                            logger.error(f"Received error: {response['error']}")
                            break
                        else:
                            logger.debug(f"Received non-tick message: {response.get('msg_type', 'unknown')}")
                            
                        # Check overall timeout
                        if (datetime.now() - start_time).total_seconds() > timeout:
                            logger.warning(f"Overall timeout of {timeout} seconds reached")
                            break
                            
                    except asyncio.TimeoutError:
                        consecutive_timeouts += 1
                        logger.warning(f"Timeout waiting for data ({consecutive_timeouts}/{max_consecutive_timeouts})")
                        
                        if consecutive_timeouts >= max_consecutive_timeouts:
                            logger.error("Too many consecutive timeouts, giving up")
                            break
                            
                        # Try to re-subscribe
                        try:
                            await self.subscribe_ticks()
                        except Exception as e:
                            logger.error(f"Failed to re-subscribe: {e}")
                            break
                
                if len(self.data_buffer) > 0:
                    logger.info(f"Successfully collected {len(self.data_buffer)} data points")
                    df = pd.DataFrame(self.data_buffer)
                    df.set_index('time', inplace=True)
                    df.sort_index(inplace=True)
                    return df
                else:
                    raise ValueError("No data collected")
                
            except Exception as e:
                retries += 1
                logger.warning(f"Attempt {retries}/{max_retries} failed: {str(e)}")
                if retries >= max_retries:
                    logger.error("Max retries reached, giving up")
                    raise
                await asyncio.sleep(5)
            finally:
                self.connected = False
                self.subscription_active = False
                if self.heartbeat_task:
                    self.heartbeat_task.cancel()
                if self.websocket:
                    try:
                        await self.websocket.close()
                    except:
                        pass

def analyze_market_data(tick_data, use_multi_timeframe=True):
    """Analyze market data using the enhanced strategy"""
    try:
        logger.info(f"Analyzing {len(tick_data)} ticks of market data")
        logger.info(f"Price range: {tick_data['price'].min():.4f} - {tick_data['price'].max():.4f}")
        logger.info(f"Time range: {tick_data.index[0]} to {tick_data.index[-1]}")
        
        if use_multi_timeframe and len(tick_data) >= 300:
            # Use full multi-timeframe strategy for sufficient data
            logger.info("Using multi-timeframe strategy")
            strategy = MultiTimeframeStrategy(risk_percent=0.02)
            result = strategy.execute_strategy(tick_data)
        else:
            # Use simplified strategy for limited data
            logger.info("Using simplified breakout strategy (insufficient data for multi-timeframe)")
            # Convert tick data to basic OHLC first
            tick_data_copy = tick_data.copy()
            tick_data_copy['time_bin'] = pd.to_datetime(tick_data_copy.index).floor('60s')  # 1-minute candles
            
            ohlc_data = tick_data_copy.groupby('time_bin').agg({
                'price': ['first', 'max', 'min', 'last'],
                'volume': 'sum'
            }).round(4)
            
            ohlc_data.columns = ['open', 'high', 'low', 'close', 'volume']
            result = breakout_strategy(ohlc_data, lookback=10, risk_percent=0.02)
        
        return result
        
    except Exception as e:
        logger.error(f"Error in market analysis: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return pd.DataFrame()

def format_signal_message(signal_row, signal_time, signal_type):
    """Format signal message for Telegram"""
    price = signal_row.get('close', signal_row.get('price', 0))
    sl = signal_row.get('sl', 0)
    tp = signal_row.get('tp', 0)
    position_size = signal_row.get('position_size', 1.0)
    confluence_score = signal_row.get('confluence_score', 0)
    htf_bias = signal_row.get('htf_bias', 'neutral')
    
    # Calculate risk-reward ratio
    if signal_type == 'BUY' and sl > 0 and tp > 0:
        risk = abs(price - sl)
        reward = abs(tp - price)
        rr_ratio = reward / risk if risk > 0 else 0
    elif signal_type == 'SELL' and sl > 0 and tp > 0:
        risk = abs(sl - price)
        reward = abs(price - tp)
        rr_ratio = reward / risk if risk > 0 else 0
    else:
        rr_ratio = 0
    
    # Determine signal strength based on confluence
    if confluence_score >= 8:
        strength = "🔥 STRONG"
    elif confluence_score >= 6:
        strength = "⚡ GOOD"
    elif confluence_score >= 4:
        strength = "⚠️ MODERATE"
    else:
        strength = "❓ WEAK"
    
    message = (
        f"🎯 R_75 SIGNAL ALERT 🎯\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Signal: {signal_type} {strength}\n"
        f"⏰ Time: {signal_time.strftime('%H:%M:%S')}\n"
        f"💰 Price: {price:.4f}\n"
        f"🛑 Stop Loss: {sl:.4f}\n"
        f"🎯 Take Profit: {tp:.4f}\n"
        f"📈 R:R Ratio: {rr_ratio:.1f}:1\n"
        f"📊 Position Size: {position_size:.2f}\n"
        f"🔍 Confluence: {confluence_score}/10\n"
        f"📊 HTF Bias: {htf_bias.upper()}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    
    return message

async def continuous_monitoring(ws_client, check_interval=300):
    """Continuously monitor for new signals"""
    logger.info("Starting continuous monitoring mode")
    last_signal_time = datetime.now()
    
    while True:
        try:
            logger.info("Collecting fresh market data for analysis...")
            
            # Collect fresh data
            tick_data = await ws_client.collect_extended_data(
                limit=400,  # Enough for multi-timeframe analysis
                max_retries=2,
                timeout=300
            )
            
            if tick_data is not None and not tick_data.empty:
                # Analyze for signals
                result = analyze_market_data(tick_data, use_multi_timeframe=True)
                
                if result is not None and not result.empty:
                    # Find new signals since last check
                    new_signals = result[
                        (result['signal'] != 0) & 
                        (result.index > last_signal_time)
                    ]
                    
                    if not new_signals.empty:
                        logger.info(f"Found {len(new_signals)} new signals")
                        
                        for signal_time, signal_row in new_signals.iterrows():
                            signal_type = "BUY" if signal_row['signal'] == 1 else "SELL"
                            
                            # Create unique signal identifier
                            signal_id = f"{signal_time}_{signal_type}_{signal_row.get('close', 0):.4f}"
                            
                            if signal_id not in ws_client.processed_signals:
                                message = format_signal_message(signal_row, signal_time, signal_type)
                                
                                try:
                                    send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, message)
                                    logger.info(f"Sent {signal_type} signal for {signal_time}")
                                    ws_client.processed_signals.add(signal_id)
                                    last_signal_time = max(last_signal_time, signal_time)
                                except Exception as e:
                                    logger.error(f"Failed to send Telegram message: {e}")
                    else:
                        logger.info("No new signals found")
                else:
                    logger.warning("No analysis result obtained")
            else:
                logger.error("Failed to collect market data")
            
            # Clean up old processed signals (keep last 100)
            if len(ws_client.processed_signals) > 100:
                # Convert to list, sort, and keep last 50
                signals_list = list(ws_client.processed_signals)
                ws_client.processed_signals = set(signals_list[-50:])
            
            logger.info(f"Waiting {check_interval} seconds before next check...")
            await asyncio.sleep(check_interval)
            
        except Exception as e:
            logger.error(f"Error in continuous monitoring: {e}")
            logger.info("Retrying in 60 seconds...")
            await asyncio.sleep(60)

async def main():
    try:
        logger.info("Starting enhanced VIX75 multi-timeframe analysis bot")
        logger.info("This bot uses advanced multi-timeframe confluence analysis")
        
        ws_client = DerivWSClient(DERIV_APP_ID, DERIV_API_TOKEN)  
        
        try:
            # Choose operation mode
            run_continuous = True  # Set to False for single analysis
            
            if run_continuous:
                logger.info("Running in continuous monitoring mode")
                await continuous_monitoring(ws_client, check_interval=300)  # Check every 5 minutes
            else:
                logger.info("Running single analysis")
                
                # Single analysis run
                tick_data = await ws_client.collect_extended_data(
                    limit=400,
                    max_retries=3,
                    timeout=400
                )
                
                if tick_data is not None and not tick_data.empty:
                    result = analyze_market_data(tick_data, use_multi_timeframe=True)
                    
                    if result is not None and not result.empty:
                        signals = result[result['signal'] != 0]
                        
                        if not signals.empty:
                            logger.info(f"Generated {len(signals)} signals")
                            
                            for signal_time, signal_row in signals.iterrows():
                                signal_type = "BUY" if signal_row['signal'] == 1 else "SELL"
                                message = format_signal_message(signal_row, signal_time, signal_type)
                                
                                try:
                                    send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, message)
                                    logger.info(f"Sent {signal_type} signal")
                                except Exception as e:
                                    logger.error(f"Failed to send Telegram message: {e}")
                        else:
                            logger.info("No trading signals generated in this analysis")
                            # Send status update
                            status_msg = (
                                f"📊 R_75 Analysis Complete\n"
                                f"⏰ Time: {datetime.now().strftime('%H:%M:%S')}\n"
                                f"📈 Data Points: {len(tick_data)}\n"
                                f"🔍 Signals Found: None\n"
                                f"💡 Market may be ranging or lacking confluence"
                            )
                            send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, status_msg)
                    else:
                        logger.warning("Strategy analysis returned no results")
                else:
                    logger.error("No market data collected")

        except Exception as e:
            logger.error(f"Failed to collect or process data: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            # Send error notification
            error_msg = (
                f"⚠️ R_75 Bot Error\n"
                f"Time: {datetime.now().strftime('%H:%M:%S')}\n"
                f"Error: {str(e)[:100]}...\n"
                f"Bot will attempt to restart..."
            )
            try:
                send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, error_msg)
            except:
                pass

    except Exception as e:
        logger.error(f"Critical error in main: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")