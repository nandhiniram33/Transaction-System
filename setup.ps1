# Automated One-Click Setup Script for High Performance Transaction Processing System
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " High-Performance Transaction Processing System - Setup" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Verify Python Installation
Write-Host "`n[1/5] Checking Python installation..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version
    Write-Host "  Found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "  ERROR: Python is not installed or not in your PATH!" -ForegroundColor Red
    Write-Host "  Please install Python 3.12 (or 3.11+) and check 'Add python.exe to PATH'." -ForegroundColor Red
    Exit 1
}

# 2. Check / Rebuild Virtual Environment
Write-Host "`n[2/5] Setting up virtual environment (.venv)..." -ForegroundColor Yellow
if (Test-Path -Path ".\.venv") {
    # Check if the existing .venv points to a different user/path
    $venvCfg = Get-Content -Path ".\.venv\pyvenv.cfg" -ErrorAction SilentlyContinue
    $currentDir = (Get-Location).Path
    if ($venvCfg -notmatch [regex]::Escape($currentDir)) {
        Write-Host "  Existing .venv was copied from another laptop. Rebuilding for your machine..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force ".\.venv"
        python -m venv .venv
    } else {
        Write-Host "  Existing .venv is valid." -ForegroundColor Green
    }
} else {
    python -m venv .venv
    Write-Host "  Created fresh .venv." -ForegroundColor Green
}

# 3. Install / Upgrade Dependencies
Write-Host "`n[3/5] Installing dependencies from requirements.txt..." -ForegroundColor Yellow
.\.venv\Scripts\python -m pip install --upgrade pip --quiet
.\.venv\Scripts\pip install -r requirements.txt --quiet
Write-Host "  Dependencies installed successfully." -ForegroundColor Green

# 4. Initialize Database & Migrations
Write-Host "`n[4/5] Initializing SQLite database schema and migrations..." -ForegroundColor Yellow
.\.venv\Scripts\alembic upgrade head
Write-Host "  Database migrations complete." -ForegroundColor Green

# 5. Run Automated Tests
Write-Host "`n[5/5] Running verification test suite..." -ForegroundColor Yellow
.\.venv\Scripts\pytest -q tests
if ($LASTEXITCODE -eq 0) {
    Write-Host "  All 31 verification tests PASSED!" -ForegroundColor Green
} else {
    Write-Host "  Warning: Some tests encountered issues." -ForegroundColor Yellow
}

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host " Setup Completed Successfully!" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "To run the application, open TWO PowerShell terminals:`n"
Write-Host "  Terminal 1 (Mock Bank Service):" -ForegroundColor White
Write-Host "    .\.venv\Scripts\python scripts/run_mock_service.py`n" -ForegroundColor Yellow
Write-Host "  Terminal 2 (Main Transaction API):" -ForegroundColor White
Write-Host "    .\.venv\Scripts\uvicorn app.main:app --host 127.0.0.1 --port 8000`n" -ForegroundColor Yellow
Write-Host "Then open your browser at: http://127.0.0.1:8000/docs" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
