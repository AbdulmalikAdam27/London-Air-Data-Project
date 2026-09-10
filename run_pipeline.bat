@echo off
cd /d "%~dp0"
".venv\Scripts\python.exe" run.py >> logs\pipeline.log 2>&1
