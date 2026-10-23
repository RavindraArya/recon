@echo off
REM Starts the Recon web app. Open http://<this-server-name>:8080 in a browser.
REM Logins come from UI_USERS / UI_VIEWERS in .env (next to recon_config.yaml).
REM For use on this PC only, change --host 0.0.0.0 to --host 127.0.0.1
cd /d "%~dp0"
python app.py -c recon_config.yaml --host 0.0.0.0 --port 8080
