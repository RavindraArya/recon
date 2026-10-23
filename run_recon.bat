@echo off
REM Daily reconciliation run. Schedule this in Windows Task Scheduler.
REM Set passwords as environment variables (or in the scheduled task), not in this file.
cd /d "%~dp0"
python recon.py -c recon_config.yaml
if %ERRORLEVEL%==1 echo Mismatches found - see recon_output folder
if %ERRORLEVEL%==2 echo One or more tables failed - see log above
