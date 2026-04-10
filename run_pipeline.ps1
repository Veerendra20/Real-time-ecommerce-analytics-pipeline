# --- Master Startup Script for Real-Time Analytics Pipeline ---

# 1. Environment Configuration
$RootDir = Get-Location
$env:JAVA_HOME = "$RootDir\j"
$env:PATH = "$env:JAVA_HOME\bin;" + $env:PATH

Write-Host "--- Initializing Real-Time Analytics Pipeline ---" -ForegroundColor Cyan

# 2. Start Zookeeper
Write-Host "Step 1/5: Launching Zookeeper..." -ForegroundColor Gray
$ZK_Command = "cd '$RootDir\k'; .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $ZK_Command

# Wait for Zookeeper
Write-Host "Waiting 15s for Zookeeper synchronization..."
Start-Sleep -Seconds 15

# 3. Start Kafka
Write-Host "Step 2/5: Launching Kafka Broker..." -ForegroundColor Gray
$K_Command = "cd '$RootDir\k'; .\bin\windows\kafka-server-start.bat .\config\server.properties"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $K_Command

# Wait for Kafka
Write-Host "Waiting 30s for Kafka broker to stabilize..."
Start-Sleep -Seconds 30

# 4. Start Transaction Simulator
Write-Host "Step 3/5: Starting Transaction Simulator (Python 3.12)..." -ForegroundColor Gray
$Sim_Command = "cd '$RootDir'; py -3.12 transaction_simulator.py"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $Sim_Command

# 5. Start Analytics Consumer
Write-Host "Step 4/5: Starting Analytics Consumer (Python 3.12)..." -ForegroundColor Gray
$Cons_Command = "cd '$RootDir'; py -3.12 analytics_consumer.py"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $Cons_Command

# 6. Start Dashboard
Write-Host "Step 5/5: Launching Interactive Dashboard..." -ForegroundColor Green
py -3.12 -m streamlit run dashboard.py
