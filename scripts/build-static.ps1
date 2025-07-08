# PowerShell script to build static files directory
# This script copies web assets from various sources into a single static directory

$ErrorActionPreference = "Stop"

Write-Host "Building static files directory..." -ForegroundColor Green

# Create static directory if it doesn't exist
if (!(Test-Path "static")) {
    New-Item -ItemType Directory -Path "static" -Force | Out-Null
}

# Function to copy files incrementally
function Copy-Incremental {
    param(
        [string]$Source,
        [string]$Destination,
        [string]$Description
    )
    
    if (!(Test-Path $Source)) {
        Write-Host "Source $Source does not exist, skipping..." -ForegroundColor Yellow
        return
    }

    Write-Host "Copying files from $Description..." -ForegroundColor Blue
    
    $sourceItems = Get-ChildItem -Path $Source -Recurse
    $copiedCount = 0
    $skippedCount = 0
    
    foreach ($item in $sourceItems) {
        $relativePath = $item.FullName.Substring($Source.Length).TrimStart('\')
        $destPath = Join-Path $Destination $relativePath
        
        if ($item.PSIsContainer) {
            # Create directory if it doesn't exist
            if (!(Test-Path $destPath)) {
                New-Item -ItemType Directory -Path $destPath -Force | Out-Null
            }
        } else {
            # Check if file needs to be copied
            $shouldCopy = $false
            
            if (!(Test-Path $destPath)) {
                $shouldCopy = $true
            } else {
                $sourceFile = Get-Item $item.FullName
                $destFile = Get-Item $destPath
                
                # Compare file size first (faster)
                if ($sourceFile.Length -ne $destFile.Length) {
                    $shouldCopy = $true
                } elseif ($sourceFile.LastWriteTime -gt $destFile.LastWriteTime) {
                    $shouldCopy = $true
                }
            }
            
            if ($shouldCopy) {
                # Ensure destination directory exists
                $destDir = Split-Path $destPath -Parent
                if (!(Test-Path $destDir)) {
                    New-Item -ItemType Directory -Path $destDir -Force | Out-Null
                }
                
                Copy-Item -Path $item.FullName -Destination $destPath -Force
                $copiedCount++
            } else {
                $skippedCount++
            }
        }
    }
    
    Write-Host "  Copied: $copiedCount files, Skipped: $skippedCount files" -ForegroundColor Gray
}

# Copy from web/public (our custom files)
Copy-Incremental -Source "crates\web\public" -Destination "static" -Description "crates/web/public"

# Copy from node-red editor (third-party files)
Copy-Incremental -Source "3rd-party\node-red\packages\node_modules\@node-red\editor-client\public" -Destination "static" -Description "node-red editor"

Write-Host "Static files build complete!" -ForegroundColor Green
Write-Host "Static directory contents:" -ForegroundColor Cyan
Get-ChildItem "static" | ForEach-Object { Write-Host "  $($_.Name)" }
