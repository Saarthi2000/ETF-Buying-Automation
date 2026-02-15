"""
Dhan ETF Investment Strategy - FORWARD TESTING / PAPER TRADING VERSION
=======================================================================

This script simulates the strategy in real-time WITHOUT placing actual orders.
Perfect for testing and validating your strategy before going live.

Features:
- Tracks all buy signals in real-time
- Records hypothetical trades to CSV
- Calculates paper trading P&L
- Shows what orders WOULD have been placed
- No real money at risk!

Requirements:
pip install dhanhq pandas numpy schedule
"""

import pandas as pd
import numpy as np
from datetime import datetime, time
from dateutil import parser
import schedule
import time as time_module
import json
import os
from dhanhq import dhanhq
from dotenv import find_dotenv
from dotenv import load_dotenv
from pprint import pprint

dotenv_file = find_dotenv()
load_dotenv(dotenv_file)

# ================================
# CONFIGURATION - UPDATE THESE
# ================================

# Dhan API Credentials (Only needed for fetching price data - NOT for placing orders)
CLIENT_ID = "YOUR_CLIENT_ID"
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"
API_KEY = "YOUR_API_KEY"

# Strategy Parameters
SMA_PERIOD = 11
INITIAL_CAPITAL = 100000  # Starting virtual capital (₹1,00,000)
BUY_QUANTITY = 10
BUY_LEVEL_1 = 1.5
BUY_LEVEL_2 = 3.0
MAX_BUYS_PER_DAY = 2

# Trading Hours
TRADING_START = time(9, 15)
TRADING_END = time(15, 30)

# File Paths for Logs
TRADES_LOG_FILE = "forward_test_trades.csv"
DAILY_SUMMARY_FILE = "forward_test_daily_summary.csv"
PORTFOLIO_FILE = "forward_test_portfolio.csv"

# ETF Watchlist with Security IDs
WATCHLIST = [
    {"security_id": "10822", "name": "Motilal Oswal BSE Quality ETF"},
    {"security_id": "14233", "name": "HDFC Nifty Small Cap 250 ETF"},
    {"security_id": "17152", "name": "ICICI Pru Nifty Midcap 150 ETF"},
    {"security_id": "17272", "name": "SBI Gold ETF"},
    {"security_id": "20257", "name": "Edelweiss Silver ETF"},
    {"security_id": "21478", "name": "UTI Nifty Next 50 ETF"},
    {"security_id": "22440", "name": "UTI Nifty Bank ETF"},
    {"security_id": "22739", "name": "Motilal Oswal Nasdaq 100 ETF"},
    {"security_id": "30109", "name": "Mirae Asset BSE Select IPO ETF"},
    {"security_id": "3507", "name": "Mirae Asset NYSE FANG+ ETF"},
    {"security_id": "522", "name": "ICICI Pru Bharat 22 ETF"},
    {"security_id": "5782", "name": "Mirae Asset S&P 500 Top 50 ETF"},
    {"security_id": "7412", "name": "Kotak Nifty Alpha 50 ETF"},
    {"security_id": "755881", "name": "ICICI Pru Nifty EV & New Age Auto ETF"},
    {"security_id": "7838", "name": "Tata Nifty 50 ETF"},
    {"security_id": "7979", "name": "Mirae Asset Nifty India Mfg. ETF"},
    {"security_id": "7074", "name": "Mirae Asset Hang Seng Tech ETF"}
]

# ================================
# GLOBAL VARIABLES
# ================================

dhan = None
daily_buys = {}
previous_close_prices = {}
virtual_portfolio = {}  # {security_id: {"quantity": 0, "avg_price": 0, "invested": 0}}
virtual_cash = INITIAL_CAPITAL
trade_counter = 0
daily_trades = []

# ================================
# FILE INITIALIZATION
# ================================

def initialize_log_files():
    """Create CSV files with headers if they don't exist"""
    
    # Trades log
    if not os.path.exists(TRADES_LOG_FILE):
        trades_df = pd.DataFrame(columns=[
            'Trade_ID', 'Date', 'Time', 'ETF_Name', 'Security_ID', 
            'Signal_Type', 'Buy_Price', 'Quantity', 'Trade_Value',
            'Prev_Close', 'Drop_Percent', 'SMA_Status', 'Cash_After'
        ])
        trades_df.to_csv(TRADES_LOG_FILE, index=False)
        print(f"✓ Created {TRADES_LOG_FILE}")
    
    # Daily summary
    if not os.path.exists(DAILY_SUMMARY_FILE):
        daily_df = pd.DataFrame(columns=[
            'Date', 'Total_Trades', 'Total_Invested', 'Cash_Remaining',
            'Portfolio_Value', 'Unrealized_PnL', 'Total_Capital'
        ])
        daily_df.to_csv(DAILY_SUMMARY_FILE, index=False)
        print(f" Created {DAILY_SUMMARY_FILE}")
    
    # Portfolio holdings
    if not os.path.exists(PORTFOLIO_FILE):
        portfolio_df = pd.DataFrame(columns=[
            'Date', 'Security_ID', 'ETF_Name', 'Quantity', 
            'Avg_Buy_Price', 'Current_Price', 'Invested_Amount',
            'Current_Value', 'Unrealized_PnL', 'PnL_Percent'
        ])
        portfolio_df.to_csv(PORTFOLIO_FILE, index=False)
        print(f"Created {PORTFOLIO_FILE}")

# ================================
# UTILITY FUNCTIONS
# ================================

def initialize_dhan_client():
    """Initialize Dhan API client for data fetching only"""
    global dhan
    try:
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        print("Dhan API client initialized (READ-ONLY mode for testing)")
        return True
    except Exception as e:
        print(f"Error initializing Dhan client: {e}")
        return False

def is_trading_time():
    """Check if current time is within trading hours"""
    now = datetime.now().time()
    current_day = datetime.now().weekday()
    
    if current_day >= 5:  # Weekend
        return False
    
    return TRADING_START <= now <= TRADING_END

def get_historical_data(security_id, days=30):
    """Fetch historical daily data for calculating SMA"""
    try:
        from_date = (datetime.now() - pd.Timedelta(days=days)).strftime('%Y-%m-%d')
        to_date = datetime.now().strftime('%Y-%m-%d')
        
        data = dhan.historical_daily_data(
            security_id=security_id,
            exchange_segment=dhan.NSE,
            instrument_type=dhan.ETF,
            from_date=from_date,
            to_date=to_date
        )
        
        if data['status'] == 'success':
            df = pd.DataFrame(data['data'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            return df
        return None
    except Exception as e:
        print(f"Error fetching historical data for {security_id}: {e}")
        return None

def calculate_sma(df, period=11):
    """Calculate Simple Moving Average"""
    if df is None or len(df) < period:
        return None
    df['SMA'] = df['close'].rolling(window=period).mean()
    return df

def is_sma_falling(df, lookback=3):
    """Check if SMA is in a falling trend"""
    if df is None or 'SMA' not in df.columns:
        return False
    
    sma_values = df['SMA'].tail(lookback).values
    
    if len(sma_values) >= lookback:
        is_falling = all(sma_values[i] > sma_values[i+1] for i in range(len(sma_values)-1))
        return is_falling
    return False

def get_current_price(security_id):
    """Get current live price (LTP)"""
    try:
        quote = dhan.get_quote(
            security_id=security_id,
            exchange_segment=dhan.NSE,
            instrument_type=dhan.ETF
        )
        
        if quote['status'] == 'success':
            ltp = quote['data']['LTP']
            return ltp
        return None
    except Exception as e:
        print(f"Error fetching current price for {security_id}: {e}")
        return None

def simulate_buy_order(security_id, quantity, price, etf_name, signal_type, prev_close, drop_pct):
    """
    Simulate a buy order WITHOUT actually placing it
    Record the trade and update virtual portfolio
    """
    global virtual_cash, trade_counter, daily_trades, virtual_portfolio
    
    trade_value = quantity * price
    
    # Check if we have enough virtual cash
    if virtual_cash < trade_value:
        print(f" INSUFFICIENT VIRTUAL CASH: Need ₹{trade_value:.2f}, Have ₹{virtual_cash:.2f}")
        return False
    
    # Deduct from virtual cash
    virtual_cash -= trade_value
    trade_counter += 1
    
    # Update virtual portfolio
    if security_id not in virtual_portfolio:
        virtual_portfolio[security_id] = {
            "name": etf_name,
            "quantity": 0,
            "total_invested": 0,
            "avg_price": 0
        }
    
    portfolio = virtual_portfolio[security_id]
    portfolio["total_invested"] += trade_value
    portfolio["quantity"] += quantity
    portfolio["avg_price"] = portfolio["total_invested"] / portfolio["quantity"]
    
    # Log the trade
    trade_record = {
        'Trade_ID': f'T{trade_counter:04d}',
        'Date': datetime.now().strftime('%Y-%m-%d'),
        'Time': datetime.now().strftime('%H:%M:%S'),
        'ETF_Name': etf_name,
        'Security_ID': security_id,
        'Signal_Type': signal_type,
        'Buy_Price': round(price, 2),
        'Quantity': quantity,
        'Trade_Value': round(trade_value, 2),
        'Prev_Close': round(prev_close, 2),
        'Drop_Percent': round(drop_pct, 2),
        'SMA_Status': 'Falling',
        'Cash_After': round(virtual_cash, 2)
    }
    
    # Append to CSV
    pd.DataFrame([trade_record]).to_csv(TRADES_LOG_FILE, mode='a', header=False, index=False)
    daily_trades.append(trade_record)
    
    # Print simulated order
    print(f"\n{'='*70}")
    print(f" PAPER TRADE #{trade_counter} - {signal_type}")
    print(f"{'='*70}")
    print(f"ETF: {etf_name}")
    print(f"Buy Price: ₹{price:.2f} | Quantity: {quantity}")
    print(f"Trade Value: ₹{trade_value:.2f}")
    print(f"Previous Close: ₹{prev_close:.2f} | Drop: {drop_pct:.2f}%")
    print(f"Virtual Cash Remaining: ₹{virtual_cash:.2f}")
    print(f"{'='*70}\n")
    
    return True

def calculate_portfolio_value():
    """Calculate current portfolio value based on live prices"""
    total_value = 0
    total_invested = 0
    
    for security_id, holding in virtual_portfolio.items():
        if holding["quantity"] > 0:
            current_price = get_current_price(security_id)
            if current_price:
                current_value = holding["quantity"] * current_price
                total_value += current_value
                total_invested += holding["total_invested"]
    
    return total_value, total_invested

def reset_daily_counters():
    """Reset daily buy counters and save daily summary"""
    global daily_buys, daily_trades
    
    # Save yesterday's summary if there were trades
    if daily_trades:
        save_daily_summary()
    
    daily_buys = {}
    daily_trades = []
    
    print("\n" + "="*70)
    print(f"NEW TRADING DAY: {datetime.now().strftime('%Y-%m-%d')}")
    print("Daily counters reset")
    print("="*70 + "\n")

def update_previous_close_prices():
    """Update previous day's closing prices"""
    global previous_close_prices
    print("\nUpdating previous day closing prices...")
    
    for etf in WATCHLIST:
        security_id = etf['security_id']
        name = etf['name']
        
        df = get_historical_data(security_id, days=10)
        if df is not None and len(df) >= 2:
            prev_close = df.iloc[-2]['close']
            previous_close_prices[security_id] = prev_close
            print(f"  {name}: ₹{prev_close:.2f}")
    
    print("✓ Previous close prices updated\n")

def save_daily_summary():
    """Save end-of-day summary"""
    portfolio_value, total_invested = calculate_portfolio_value()
    unrealized_pnl = portfolio_value - total_invested
    total_capital = virtual_cash + portfolio_value
    
    summary = {
        'Date': datetime.now().strftime('%Y-%m-%d'),
        'Total_Trades': len(daily_trades),
        'Total_Invested': round(total_invested, 2),
        'Cash_Remaining': round(virtual_cash, 2),
        'Portfolio_Value': round(portfolio_value, 2),
        'Unrealized_PnL': round(unrealized_pnl, 2),
        'Total_Capital': round(total_capital, 2)
    }
    
    pd.DataFrame([summary]).to_csv(DAILY_SUMMARY_FILE, mode='a', header=False, index=False)
    
    print("\n" + "="*70)
    print("📊 END OF DAY SUMMARY")
    print("="*70)
    print(f"Trades Today: {len(daily_trades)}")
    print(f"Total Invested Today: ₹{total_invested:.2f}")
    print(f"Virtual Cash: ₹{virtual_cash:.2f}")
    print(f"Portfolio Value: ₹{portfolio_value:.2f}")
    print(f"Unrealized P&L: ₹{unrealized_pnl:.2f} ({(unrealized_pnl/total_invested*100) if total_invested > 0 else 0:.2f}%)")
    print(f"Total Capital: ₹{total_capital:.2f}")
    print("="*70 + "\n")

def save_portfolio_snapshot():
    """Save current portfolio holdings snapshot"""
    snapshot_date = datetime.now().strftime('%Y-%m-%d')
    portfolio_records = []
    
    for security_id, holding in virtual_portfolio.items():
        if holding["quantity"] > 0:
            current_price = get_current_price(security_id)
            if current_price:
                invested = holding["total_invested"]
                current_value = holding["quantity"] * current_price
                pnl = current_value - invested
                pnl_pct = (pnl / invested * 100) if invested > 0 else 0
                
                record = {
                    'Date': snapshot_date,
                    'Security_ID': security_id,
                    'ETF_Name': holding["name"],
                    'Quantity': holding["quantity"],
                    'Avg_Buy_Price': round(holding["avg_price"], 2),
                    'Current_Price': round(current_price, 2),
                    'Invested_Amount': round(invested, 2),
                    'Current_Value': round(current_value, 2),
                    'Unrealized_PnL': round(pnl, 2),
                    'PnL_Percent': round(pnl_pct, 2)
                }
                portfolio_records.append(record)
    
    if portfolio_records:
        pd.DataFrame(portfolio_records).to_csv(PORTFOLIO_FILE, mode='a', header=False, index=False)

# ================================
# MAIN STRATEGY LOGIC
# ================================

def check_and_simulate_strategy():
    """
    Main strategy execution - SIMULATION MODE
    """
    if not is_trading_time():
        return
    
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔍 Scanning ETFs (Paper Trading Mode)...")
    print(f"Virtual Cash Available: ₹{virtual_cash:.2f}")
    
    signals_found = 0
    
    # Scan each ETF
    for etf in WATCHLIST:
        security_id = etf['security_id']
        name = etf['name']
        
        # Check daily limit
        if daily_buys.get(security_id, 0) >= MAX_BUYS_PER_DAY:
            continue
        
        try:
            # Get data and check conditions
            df = get_historical_data(security_id, days=30)
            if df is None:
                continue
            
            df = calculate_sma(df, SMA_PERIOD)
            
            if not is_sma_falling(df):
                continue
            
            if security_id not in previous_close_prices:
                continue
            prev_close = previous_close_prices[security_id]
            
            current_price = get_current_price(security_id)
            if current_price is None:
                continue
            
            price_drop_pct = ((prev_close - current_price) / prev_close) * 100
            current_buys = daily_buys.get(security_id, 0)
            
            # Check buy signals
            if current_buys == 0 and price_drop_pct >= BUY_LEVEL_1:
                signals_found += 1
                if simulate_buy_order(security_id, BUY_QUANTITY, current_price, 
                                     name, "BUY #1 (1.5% Drop)", prev_close, price_drop_pct):
                    daily_buys[security_id] = 1
            
            elif current_buys == 1 and price_drop_pct >= BUY_LEVEL_2:
                signals_found += 1
                if simulate_buy_order(security_id, BUY_QUANTITY, current_price, 
                                     name, "BUY #2 (3.0% Drop)", prev_close, price_drop_pct):
                    daily_buys[security_id] = 2
        
        except Exception as e:
            print(f"Error processing {name}: {e}")
            continue
    
    if signals_found == 0:
        print("No buy signals detected in this scan.")

# ================================
# REPORTING FUNCTIONS
# ================================

def show_portfolio_summary():
    """Display current portfolio holdings"""
    print("\n" + "="*70)
    print(" CURRENT PORTFOLIO HOLDINGS")
    print("="*70)
    
    if not virtual_portfolio or all(h["quantity"] == 0 for h in virtual_portfolio.values()):
        print("Portfolio is empty - No positions yet")
        print("="*70 + "\n")
        return
    
    total_invested = 0
    total_current = 0
    
    for security_id, holding in virtual_portfolio.items():
        if holding["quantity"] > 0:
            current_price = get_current_price(security_id)
            if current_price:
                invested = holding["total_invested"]
                current_value = holding["quantity"] * current_price
                pnl = current_value - invested
                pnl_pct = (pnl / invested * 100) if invested > 0 else 0
                
                total_invested += invested
                total_current += current_value
                
                print(f"\n{holding['name']}")
                print(f"  Quantity: {holding['quantity']} | Avg Price: ₹{holding['avg_price']:.2f}")
                print(f"  Invested: ₹{invested:.2f} | Current: ₹{current_value:.2f}")
                print(f"  P&L: ₹{pnl:.2f} ({pnl_pct:+.2f}%)")
    
    total_pnl = total_current - total_invested
    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0
    
    print(f"\n{'='*70}")
    print(f"TOTAL INVESTED: ₹{total_invested:.2f}")
    print(f"CURRENT VALUE: ₹{total_current:.2f}")
    print(f"UNREALIZED P&L: ₹{total_pnl:.2f} ({total_pnl_pct:+.2f}%)")
    print(f"VIRTUAL CASH: ₹{virtual_cash:.2f}")
    print(f"TOTAL CAPITAL: ₹{virtual_cash + total_current:.2f}")
    print("="*70 + "\n")

def generate_performance_report():
    """Generate comprehensive performance report"""
    try:
        trades_df = pd.read_csv(TRADES_LOG_FILE)
        
        if len(trades_df) == 0:
            print("No trades executed yet!")
            return
        
        print("\n" + "="*70)
        print(" FORWARD TESTING PERFORMANCE REPORT")
        print("="*70)
        
        print(f"\nTotal Trades: {len(trades_df)}")
        print(f"Total Invested: ₹{trades_df['Trade_Value'].sum():.2f}")
        print(f"Average Trade Size: ₹{trades_df['Trade_Value'].mean():.2f}")
        
        print(f"\nETFs Traded:")
        etf_counts = trades_df['ETF_Name'].value_counts()
        for etf, count in etf_counts.items():
            print(f"  {etf}: {count} trades")
        
        print(f"\nSignal Distribution:")
        signal_counts = trades_df['Signal_Type'].value_counts()
        for signal, count in signal_counts.items():
            print(f"  {signal}: {count}")
        
        portfolio_value, total_invested = calculate_portfolio_value()
        unrealized_pnl = portfolio_value - total_invested
        total_capital = virtual_cash + portfolio_value
        
        print(f"\nCurrent Status:")
        print(f"  Portfolio Value: ₹{portfolio_value:.2f}")
        print(f"  Unrealized P&L: ₹{unrealized_pnl:.2f} ({(unrealized_pnl/total_invested*100) if total_invested > 0 else 0:.2f}%)")
        print(f"  Virtual Cash: ₹{virtual_cash:.2f}")
        print(f"  Total Capital: ₹{total_capital:.2f}")
        print(f"  Return on Capital: {((total_capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100):.2f}%")
        
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"Error generating report: {e}")

# ================================
# SCHEDULING
# ================================

def schedule_tasks():
    """Schedule all strategy tasks"""
    
    # Morning tasks at 9:00 AM
    schedule.every().monday.at("09:00").do(reset_daily_counters)
    schedule.every().tuesday.at("09:00").do(reset_daily_counters)
    schedule.every().wednesday.at("09:00").do(reset_daily_counters)
    schedule.every().thursday.at("09:00").do(reset_daily_counters)
    schedule.every().friday.at("09:00").do(reset_daily_counters)
    
    schedule.every().monday.at("09:00").do(update_previous_close_prices)
    schedule.every().tuesday.at("09:00").do(update_previous_close_prices)
    schedule.every().wednesday.at("09:00").do(update_previous_close_prices)
    schedule.every().thursday.at("09:00").do(update_previous_close_prices)
    schedule.every().friday.at("09:00").do(update_previous_close_prices)
    
    # Run strategy check every 1 minute
    schedule.every(1).minutes.do(check_and_simulate_strategy)
    
    # Portfolio snapshot at 3:25 PM (before market close)
    schedule.every().monday.at("15:25").do(save_portfolio_snapshot)
    schedule.every().tuesday.at("15:25").do(save_portfolio_snapshot)
    schedule.every().wednesday.at("15:25").do(save_portfolio_snapshot)
    schedule.every().thursday.at("15:25").do(save_portfolio_snapshot)
    schedule.every().friday.at("15:25").do(save_portfolio_snapshot)
    
    # Daily summary at 3:30 PM
    schedule.every().monday.at("15:30").do(show_portfolio_summary)
    schedule.every().tuesday.at("15:30").do(show_portfolio_summary)
    schedule.every().wednesday.at("15:30").do(show_portfolio_summary)
    schedule.every().thursday.at("15:30").do(show_portfolio_summary)
    schedule.every().friday.at("15:30").do(show_portfolio_summary)

# ================================
# MAIN EXECUTION
# ================================

def main():
    """Main function"""
    print("="*70)
    print("FORWARD TESTING / PAPER TRADING MODE")
    print("Dhan ETF Investment Strategy Simulator")
    print("="*70)
    print("\n  NO REAL ORDERS WILL BE PLACED")
    print("✓ All trades are simulated")
    print("✓ Results saved to CSV files for analysis\n")
    print(f"Initial Virtual Capital: ₹{INITIAL_CAPITAL:,.2f}")
    print(f"Strategy: Buy on dips when SMA({SMA_PERIOD}) is falling")
    print(f"Buy Levels: {BUY_LEVEL_1}% and {BUY_LEVEL_2}% drops")
    print(f"Quantity per buy: {BUY_QUANTITY}")
    print(f"ETFs in Watchlist: {len(WATCHLIST)}")
    print("="*70 + "\n")
    
    # Initialize
    initialize_log_files()
    
    if not initialize_dhan_client():
        print("Failed to initialize Dhan client. Check credentials.")
        return
    
    reset_daily_counters()
    update_previous_close_prices()
    schedule_tasks()
    
    print("\n Forward testing is now running...")
    print("Press Ctrl+C to stop and see final report\n")
    
    # Main loop
    try:
        while True:
            schedule.run_pending()
            time_module.sleep(30)
    except KeyboardInterrupt:
        print("\n\n  Forward testing stopped by user\n")
        show_portfolio_summary()
        generate_performance_report()
        print(f"\n All results saved to:")
        print(f"  - {TRADES_LOG_FILE}")
        print(f"  - {DAILY_SUMMARY_FILE}")
        print(f"  - {PORTFOLIO_FILE}")
    except Exception as e:
        print(f"\n\n Error: {e}")

if __name__ == "__main__":
    main()