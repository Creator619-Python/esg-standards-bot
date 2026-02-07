# ESG Standards Intelligence Bot 🌍

A production-ready Telegram bot for searching ESG standards across multiple frameworks with enterprise-grade analytics.

## Features
- 🌍 50+ languages with auto-translation
- 📊 Cross-framework search (ESRS, GRI, SASB, ISO, BRSR)
- 🎯 Confidence scoring (0-100%)
- 📈 Google Sheets analytics + GA4 telemetry
- ⚡ Real-time performance monitoring
- 🚀 24/7 deployment on Render (free tier)

## Live Bot
Add [@YourBotName](https://t.me/YourBotName) on Telegram

## Quick Deployment

### 1. Fork this Repository
Click "Fork" button on GitHub

### 2. Deploy to Render (Free)
[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy)

Or manually:
1. Go to [Render Dashboard](https://dashboard.render.com)
2. Click "New +" → "Web Service"
3. Connect your GitHub repository
4. Configure:
   - **Name**: `esg-bot`
   - **Environment**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python esg_bot.py`
   - **Plan**: Free

### 3. Set Environment Variables in Render
