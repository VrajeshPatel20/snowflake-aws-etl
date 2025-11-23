"""
GenAI-powered Market Trend Analyzer and Investment Recommendation System.
"""
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


class MarketTrendAnalyzer:
    """
    GenAI-powered analyzer for stock market trends and investment recommendations.
    """
    
    def __init__(self, llm_provider: str = "openai", api_key: Optional[str] = None):
        """
        Initialize the Market Trend Analyzer.
        
        Args:
            llm_provider: LLM provider ("openai", "anthropic", "local")
            api_key: API key for the LLM provider
        """
        self.llm_provider = llm_provider
        self.api_key = api_key or os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY") or os.getenv("GEMINI_API_KEY")
        self.llm_client = self._initialize_llm()
    
    def _initialize_llm(self):
        """Initialize the LLM client based on provider."""
        if self.llm_provider == "openai":
            try:
                import openai
                if self.api_key:
                    openai.api_key = self.api_key
                return openai
            except ImportError:
                print("Warning: openai package not installed. Install with: pip install openai")
                return None
        elif self.llm_provider == "anthropic":
            try:
                import anthropic
                return anthropic.Anthropic(api_key=self.api_key) if self.api_key else None
            except ImportError:
                print("Warning: anthropic package not installed. Install with: pip install anthropic")
                return None
        elif self.llm_provider == "gemini":
            try:
                if self.api_key:
                    os.environ["GEMINI_API_KEY"] = self.api_key
                    from google import genai
                    client = genai.Client()
                return client
            except ImportError:
                print("Warning: google-gemini package not installed. Install with: pip install google-gemini")
                return None
        else:
            return None
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate technical indicators from stock data.
        
        Args:
            df: DataFrame with columns: timestamp, open, high, low, close, volume
            
        Returns:
            DataFrame with additional technical indicator columns
        """
        if df.empty:
            return df
        
        # Ensure proper data types
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        df['high'] = pd.to_numeric(df['high'], errors='coerce')
        df['low'] = pd.to_numeric(df['low'], errors='coerce')
        
        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        # Moving Averages
        df['sma_20'] = df['close'].rolling(window=20, min_periods=1).mean()
        df['sma_50'] = df['close'].rolling(window=50, min_periods=1).mean()
        df['ema_12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema_26'] = df['close'].ewm(span=26, adjust=False).mean()
        
        # MACD
        df['macd'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        
        # RSI (Relative Strength Index)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # Bollinger Bands
        df['bb_middle'] = df['close'].rolling(window=20, min_periods=1).mean()
        bb_std = df['close'].rolling(window=20, min_periods=1).std()
        df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
        df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
        
        # Volume indicators
        df['volume_sma'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma']
        
        # Price change indicators
        df['price_change'] = df['close'].pct_change()
        df['volatility'] = df['price_change'].rolling(window=20, min_periods=1).std()
        
        # Trend indicators
        df['trend'] = 'neutral'
        df.loc[df['close'] > df['sma_20'], 'trend'] = 'bullish'
        df.loc[df['close'] < df['sma_20'], 'trend'] = 'bearish'
        
        return df
    
    def generate_market_insights(self, ticker: str, stock_data: List[Dict[str, Any]], 
                                technical_indicators: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate market insights using GenAI.
        
        Args:
            ticker: Stock ticker symbol
            stock_data: Raw stock data
            technical_indicators: DataFrame with technical indicators
            
        Returns:
            Dictionary with market insights and recommendations
        """
        # Prepare data summary for LLM
        if technical_indicators.empty:
            return {
                "error": "Insufficient data for analysis",
                "recommendation": "WAIT",
                "confidence": 0.0
            }
        
        latest = technical_indicators.iloc[-1]
        recent_data = technical_indicators.tail(30)
        
        # Calculate key metrics
        current_price = float(latest['close'])
        price_change_30d = ((current_price - recent_data.iloc[0]['close']) / recent_data.iloc[0]['close']) * 100
        volatility = float(latest['volatility']) * 100 if 'volatility' in latest else 0
        rsi = float(latest['rsi']) if 'rsi' in latest else 50
        trend = latest.get('trend', 'neutral')
        
        # Prepare prompt for LLM
        prompt = self._create_analysis_prompt(
            ticker=ticker,
            current_price=current_price,
            price_change_30d=price_change_30d,
            volatility=volatility,
            rsi=rsi,
            trend=trend,
            technical_data=latest.to_dict()
        )
        
        # Get LLM analysis
        llm_analysis = self._get_llm_analysis(prompt)
        
        # Generate recommendation
        recommendation = self._generate_recommendation(
            rsi=rsi,
            trend=trend,
            price_change=price_change_30d,
            volatility=volatility,
            llm_insights=llm_analysis
        )
        
        return {
            "ticker": ticker,
            "current_price": current_price,
            "price_change_30d": price_change_30d,
            "volatility": volatility,
            "rsi": rsi,
            "trend": trend,
            "technical_summary": {
                "sma_20": float(latest.get('sma_20', 0)),
                "sma_50": float(latest.get('sma_50', 0)),
                "macd": float(latest.get('macd', 0)),
                "rsi": rsi,
                "bb_position": self._get_bb_position(latest)
            },
            "llm_insights": llm_analysis,
            "recommendation": recommendation["action"],
            "confidence": recommendation["confidence"],
            "holding_period": recommendation["holding_period"],
            "alternative_strategies": recommendation["alternatives"],
            "analysis_date": datetime.now().isoformat()
        }
    
    def _create_analysis_prompt(self, ticker: str, current_price: float, 
                               price_change_30d: float, volatility: float,
                               rsi: float, trend: str, technical_data: Dict) -> str:
        """Create prompt for LLM analysis."""
        return f"""Analyze the following stock market data for {ticker} and provide investment insights:

Current Price: ${current_price:.2f}
30-Day Price Change: {price_change_30d:.2f}%
Volatility: {volatility:.2f}%
RSI (Relative Strength Index): {rsi:.2f}
Trend: {trend}

Technical Indicators:
- SMA 20: ${technical_data.get('sma_20', 0):.2f}
- SMA 50: ${technical_data.get('sma_50', 0):.2f}
- MACD: {technical_data.get('macd', 0):.4f}
- Bollinger Bands Position: {self._get_bb_position(technical_data)}

Please provide:
1. Market trend analysis (bullish, bearish, or neutral)
2. Entry recommendation (BUY, SELL, or WAIT)
3. Suggested holding period (short-term: 1-7 days, medium-term: 1-4 weeks, long-term: 1+ months)
4. Risk assessment
5. Alternative investment strategies for safer gains (bonds, ETFs, index funds, etc.)

Format your response as a structured analysis with clear recommendations."""
    
    def _get_llm_analysis(self, prompt: str) -> str:
        """Get analysis from LLM."""
        if not self.llm_client:
            return "LLM not configured. Using rule-based analysis only."
        
        try:
            if self.llm_provider == "openai":
                from openai import OpenAI
                client = OpenAI(api_key=self.api_key) if self.api_key else OpenAI()
                response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": "You are a financial market analyst with expertise in technical analysis and investment strategies."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3,
                    max_tokens=1000
                )
                return response.choices[0].message.content
            elif self.llm_provider == "anthropic":
                message = self.llm_client.messages.create(
                    model="claude-3-sonnet-20240229",
                    max_tokens=1000,
                    temperature=0.3,
                    system="You are a financial market analyst with expertise in technical analysis and investment strategies.",
                    messages=[{"role": "user", "content": prompt}]
                )
                return message.content[0].text
            elif self.llm_provider == "gemini":
                from google.genai import types

                response = self.llm_client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents="How does AI work?",
                    config=types.GenerateContentConfig(
                        thinking_config=types.ThinkingConfig(thinking_budget=0) # Disables thinking
                    ),
                )
                print(response.text)
        except Exception as e:
            print(f"Error getting LLM analysis: {e}")
            return f"LLM analysis unavailable: {str(e)}"
        
        return "LLM analysis unavailable"
    
    def _get_bb_position(self, data: Dict) -> str:
        """Get Bollinger Bands position."""
        if 'close' not in data or 'bb_upper' not in data or 'bb_lower' not in data:
            return "unknown"
        close = data['close']
        upper = data['bb_upper']
        lower = data['bb_lower']
        if close > upper:
            return "above_upper"
        elif close < lower:
            return "below_lower"
        else:
            return "within_bands"
    
    def _generate_recommendation(self, rsi: float, trend: str, 
                                price_change: float, volatility: float,
                                llm_insights: str) -> Dict[str, Any]:
        """Generate investment recommendation based on technical analysis and LLM insights."""
        
        # Rule-based recommendation logic
        recommendation_score = 0
        confidence = 0.5
        
        # RSI analysis
        if rsi < 30:
            recommendation_score += 2  # Oversold, potential buy
        elif rsi > 70:
            recommendation_score -= 2  # Overbought, potential sell
        elif 30 <= rsi <= 50:
            recommendation_score += 1  # Neutral to slightly bullish
        
        # Trend analysis
        if trend == "bullish":
            recommendation_score += 1
        elif trend == "bearish":
            recommendation_score -= 1
        
        # Price change analysis
        if price_change > 5:
            recommendation_score -= 1  # May be overvalued
        elif price_change < -5:
            recommendation_score += 1  # May be undervalued
        
        # Volatility consideration
        if volatility > 3:
            confidence -= 0.2  # High volatility reduces confidence
        
        # Determine action
        if recommendation_score >= 2:
            action = "BUY"
            confidence = min(0.9, confidence + 0.2)
            holding_period = "medium-term (1-4 weeks)" if volatility < 2 else "short-term (1-7 days)"
        elif recommendation_score <= -2:
            action = "SELL"
            confidence = min(0.9, confidence + 0.2)
            holding_period = "immediate"
        else:
            action = "WAIT"
            confidence = 0.6
            holding_period = "monitor for 1-2 weeks"
        
        # Alternative strategies
        alternatives = [
            {
                "strategy": "Index Funds (S&P 500, NASDAQ)",
                "risk": "Low-Medium",
                "expected_return": "7-10% annually",
                "rationale": "Diversified exposure with lower volatility"
            },
            {
                "strategy": "Bond ETFs",
                "risk": "Low",
                "expected_return": "3-5% annually",
                "rationale": "Stable income with capital preservation"
            },
            {
                "strategy": "Dividend Stocks",
                "risk": "Medium",
                "expected_return": "4-7% annually",
                "rationale": "Regular income with potential capital appreciation"
            }
        ]
        
        return {
            "action": action,
            "confidence": round(confidence, 2),
            "holding_period": holding_period,
            "alternatives": alternatives,
            "reasoning": f"RSI: {rsi:.1f}, Trend: {trend}, Price Change: {price_change:.2f}%"
        }
    
    def analyze_multiple_stocks(self, stock_data_dict: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """
        Analyze multiple stocks and provide portfolio-level insights.
        
        Args:
            stock_data_dict: Dictionary mapping ticker to stock data list
            
        Returns:
            Dictionary with portfolio analysis and recommendations
        """
        analyses = {}
        
        for ticker, data in stock_data_dict.items():
            if not data:
                continue
            
            df = pd.DataFrame(data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = self.calculate_technical_indicators(df)
            analysis = self.generate_market_insights(ticker, data, df)
            analyses[ticker] = analysis
        
        # Portfolio-level recommendations
        buy_count = sum(1 for a in analyses.values() if a.get('recommendation') == 'BUY')
        sell_count = sum(1 for a in analyses.values() if a.get('recommendation') == 'SELL')
        wait_count = sum(1 for a in analyses.values() if a.get('recommendation') == 'WAIT')
        
        portfolio_sentiment = "bullish" if buy_count > sell_count else "bearish" if sell_count > buy_count else "neutral"
        
        return {
            "individual_analyses": analyses,
            "portfolio_summary": {
                "total_stocks": len(analyses),
                "buy_recommendations": buy_count,
                "sell_recommendations": sell_count,
                "wait_recommendations": wait_count,
                "overall_sentiment": portfolio_sentiment
            },
            "portfolio_recommendation": self._get_portfolio_recommendation(analyses),
            "analysis_date": datetime.now().isoformat()
        }
    
    def _get_portfolio_recommendation(self, analyses: Dict[str, Dict]) -> Dict[str, Any]:
        """Get portfolio-level recommendation."""
        avg_confidence = np.mean([a.get('confidence', 0.5) for a in analyses.values()])
        buy_ratio = sum(1 for a in analyses.values() if a.get('recommendation') == 'BUY') / len(analyses) if analyses else 0
        
        if buy_ratio > 0.6 and avg_confidence > 0.7:
            return {
                "action": "AGGRESSIVE_BUY",
                "rationale": "Strong buy signals across portfolio with high confidence",
                "suggested_allocation": "70% stocks, 30% bonds"
            }
        elif buy_ratio > 0.4:
            return {
                "action": "MODERATE_BUY",
                "rationale": "Moderate buy signals, consider gradual entry",
                "suggested_allocation": "50% stocks, 50% bonds"
            }
        elif buy_ratio < 0.3:
            return {
                "action": "DEFENSIVE",
                "rationale": "Weak signals, consider defensive positioning",
                "suggested_allocation": "30% stocks, 70% bonds/cash"
            }
        else:
            return {
                "action": "WAIT",
                "rationale": "Mixed signals, wait for clearer direction",
                "suggested_allocation": "40% stocks, 60% bonds/cash"
            }

