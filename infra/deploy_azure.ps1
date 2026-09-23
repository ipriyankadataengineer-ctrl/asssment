# ==============================================================================
# SKYPOINTS AZURE DEPLOYMENT SCRIPT
# Provisions ADLS Gen2, builds Medallion containers, and syncs pipeline
# ==============================================================================

param(
    [string]$ResourceGroupName = "rg-skypoints-loyalty",
    [string]$Location = "centralindia"
)

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "Deploying SkyPoints Azure Infrastructure..." -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

# 1. Ensure Resource Group Exists
Write-Host "[1/4] Ensuring Resource Group '$ResourceGroupName' in '$Location'..."
az group create --name $ResourceGroupName --location $Location --output none
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to create resource group."
    exit 1
}

# 2. Deploy Bicep Template (ADLS Gen2 + Medallion Containers)
Write-Host "[2/4] Deploying Bicep Template (ADLS Gen2 Storage + Containers)..."
$deployment = az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file (Join-Path $PSScriptRoot "main.bicep") `
    --output json | ConvertFrom-Json

if ($LASTEXITCODE -ne 0) {
    Write-Error "Bicep deployment failed."
    exit 1
}

$storageAccountName = $deployment.properties.outputs.storageAccountName.value
Write-Host "Storage Account Deployed: $storageAccountName" -ForegroundColor Green

# 3. Retrieve Storage Connection String
Write-Host "[3/4] Retrieving Connection String..."
$connString = az storage account show-connection-string `
    --name $storageAccountName `
    --resource-group $ResourceGroupName `
    --query connectionString `
    --output tsv

$env:AZURE_STORAGE_CONNECTION_STRING = $connString
$env:AZURE_STORAGE_ACCOUNT = $storageAccountName

# 4. Trigger Live Pipeline Upload
Write-Host "[4/4] Executing Live ETL Pipeline with Azure Sync..." -ForegroundColor Yellow
python -m src.pipeline

Write-Host "=================================================================" -ForegroundColor Green
Write-Host "AZURE LIVE DEPLOYMENT COMPLETE!" -ForegroundColor Green
Write-Host "Storage Account: $storageAccountName" -ForegroundColor Green
Write-Host "Containers: landing, bronze, silver, gold" -ForegroundColor Green
Write-Host "Check in Azure Portal: portal.azure.com" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Green
