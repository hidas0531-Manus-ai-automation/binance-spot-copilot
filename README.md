# Binance Spot Trading Co-pilot

Railway deployment configuration for the Python bot.

## Important: source file status

The GitHub repository currently contains `requirements.txt` but does **not** contain `bot.py`. Railway cannot start the application until the bot source file is added at the repository root. The deployment command is configured as `python bot.py run` and will fail with a missing-file error until then.

## Deploy on Railway (after `bot.py` is added)

1. In Railway, create a project and choose **Deploy from GitHub repo**; select `hidas0531-Manus-ai-automation/binance-spot-copilot`.
2. Railway detects the Python project. The configured build command installs `requirements.txt`; the start command runs `python bot.py run`.
3. Keep the bot in paper mode initially. Do not set `LIVE_TRADING=true` while testing.
4. Add a Railway Volume mounted at `/app/data` and set `DATA_DIR=/app/data`. This keeps state, logs, trade history, and review files across redeployments.
5. Set any required environment variables in Railway's **Variables** page. Never commit API keys or secrets. For live trading, use a dedicated Binance API key with withdrawals disabled, and only enable live trading after careful paper testing.
6. Check Railway deployment logs and health/status before relying on the service.

## Configuration

The bot's defaults are paper-only (`LIVE_TRADING=false`). The `railway.json` start command runs the long-lived worker process. Railway will restart it on failure. Persistent storage requires a mounted volume; setting `DATA_DIR` alone does not provide persistence.

## Connect Grok or another assistant

This repository is public, so share this repository URL with the assistant: https://github.com/hidas0531-Manus-ai-automation/binance-spot-copilot

For current source context, point it to the repository or the exact file URL. The assistant does not get Railway account access just from a GitHub URL; connect the repository to Railway in Railway's GitHub deployment flow, and configure variables/volume in Railway itself.
