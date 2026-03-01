<#
.SYNOPSIS
    init_skills.ps1 - One-time init: downloads Databricks skills AND sets up the MCP server.

.DESCRIPTION
    Run this script once on a new machine after cloning the repo.
    It handles two separate things:

    1. SKILLS (SKILL.md prompt files for Claude):
       Downloaded from two GitHub sources and copied from the custom_skills\ folder.
       Placed in <workspace-root>\.claude\skills\.
       Skills tell Claude HOW to work with Databricks but do not execute any code.

       Sources:
         a. https://github.com/databricks-solutions/databricks-exec-code-mcp  (5 skills, renamed)
         b. https://github.com/databricks-solutions/ai-dev-kit                 (16 skills)
         c. custom_skills\ folder in this project                              (copied as-is)

    2. MCP SERVER (databricks-exec-code-mcp):
       A local Python service that provides the actual Databricks MCP tools
       (list_clusters, databricks_command, list_catalogs, etc.).
       Without this running, Claude can only read skill docs - it cannot execute
       code on Databricks.
       Cloned and installed at <workspace-root>\databricks-exec-code-mcp\.
       ~/.claude/mcp.json is created/updated automatically with the correct paths.

    NOTE: After running this script, fill in DATABRICKS_HOST and DATABRICKS_TOKEN
    in ~/.claude/mcp.json if it was newly created, then restart Claude Code.

.PARAMETER Force
    Re-download and overwrite all skills, even if they already exist.

.PARAMETER SkipMcp
    Skip MCP server setup (only download/update skills).

.EXAMPLE
    # Normal init (skip existing skills, set up MCP server if missing)
    .\download_skills\init_skills.ps1

    # Force re-download everything
    .\download_skills\init_skills.ps1 -Force

    # Skills only, skip MCP server
    .\download_skills\init_skills.ps1 -SkipMcp
#>

param(
    [switch]$Force,
    [switch]$SkipMcp
)

# -- Paths ---------------------------------------------------------------------
$scriptDir      = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot    = Split-Path -Parent $scriptDir         # AI_Coding_Agent_for_Databricks
$workspaceRoot  = Split-Path -Parent $projectRoot       # VS_Studio (parent of the project)
$skillsTarget   = Join-Path $workspaceRoot ".claude\skills"
$customSrc      = Join-Path $scriptDir "custom_skills"

# -- Header --------------------------------------------------------------------
Write-Host ""
Write-Host "Databricks Skills Init" -ForegroundColor Cyan
Write-Host ("=" * 52) -ForegroundColor Cyan
Write-Host "Workspace root : $workspaceRoot"
Write-Host "Skills target  : $skillsTarget"
Write-Host "Force          : $Force"
Write-Host ""

if (-not (Test-Path $skillsTarget)) {
    New-Item -ItemType Directory -Path $skillsTarget -Force | Out-Null
    Write-Host "Created skills folder: $skillsTarget" -ForegroundColor Green
}

$downloaded = 0
$skipped    = 0
$failed     = 0

# -- Helper: download one skill ------------------------------------------------
function Install-Skill {
    param(
        [string]$TargetName,   # destination folder name  e.g. databricks-general-skill-testing
        [string]$Url           # raw GitHub URL to SKILL.md
    )

    $dest = Join-Path $skillsTarget $TargetName

    if ((Test-Path (Join-Path $dest "SKILL.md")) -and -not $Force) {
        Write-Host "  SKIP   $TargetName" -ForegroundColor DarkGray
        $script:skipped++
        return
    }

    try {
        New-Item -ItemType Directory -Path $dest -Force | Out-Null
        Invoke-WebRequest -Uri $Url -OutFile (Join-Path $dest "SKILL.md") -UseBasicParsing -ErrorAction Stop
        Write-Host "  OK     $TargetName" -ForegroundColor Green
        $script:downloaded++
    }
    catch {
        Write-Host "  FAIL   $TargetName  ($_)" -ForegroundColor Red
        Remove-Item -Path $dest -Recurse -Force -ErrorAction SilentlyContinue
        $script:failed++
    }
}

# -- Source 1: databricks-exec-code-mcp (5 skills, renamed to general-skill) --
# Downloaded first - ai-dev-kit will overwrite on overlap when -Force is used,
# ensuring the more complete ai-dev-kit version always wins.
$mcpBase   = "https://raw.githubusercontent.com/databricks-solutions/databricks-exec-code-mcp/main/skills"
$mcpSkills = [ordered]@{
    "databricks-testing"          = "databricks-general-skill-testing"
    "databricks-data-engineering" = "databricks-general-skill-data-engineering"
    "databricks-ml-pipeline"      = "databricks-general-skill-ml-pipeline"
    "databricks-bundle-deploy"    = "databricks-general-skill-bundle-deploy"
    "databricks-unity-catalog"    = "databricks-general-skill-unity-catalog"
}

Write-Host "Source 1: databricks-exec-code-mcp  ($($mcpSkills.Count) skills, renamed)" -ForegroundColor Yellow
foreach ($src in $mcpSkills.Keys) {
    Install-Skill -TargetName $mcpSkills[$src] -Url "$mcpBase/$src/SKILL.md"
}

# -- Source 2: ai-dev-kit (16 skills, already named correctly) -----------------
# Downloaded second - overwrites Source 1 where they overlap (ai-dev-kit wins).
$aiBase    = "https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/databricks-skills"
$aiSkills  = @(
    "databricks-general-skill-aibi-dashboards"
    "databricks-general-skill-bundle-deploy"
    "databricks-general-skill-data-engineering"
    "databricks-general-skill-dbsql"
    "databricks-general-skill-genie"
    "databricks-general-skill-jobs"
    "databricks-general-skill-ml-pipeline"
    "databricks-general-skill-mlflow-evaluation"
    "databricks-general-skill-model-serving"
    "databricks-general-skill-python-sdk"
    "databricks-general-skill-spark-declarative-pipelines"
    "databricks-general-skill-spark-structured-streaming"
    "databricks-general-skill-synthetic-data-generation"
    "databricks-general-skill-testing"
    "databricks-general-skill-unity-catalog"
    "databricks-general-skill-vector-search"
)

Write-Host ""
Write-Host "Source 2: ai-dev-kit  ($($aiSkills.Count) skills)" -ForegroundColor Yellow
foreach ($skill in $aiSkills) {
    Install-Skill -TargetName $skill -Url "$aiBase/$skill/SKILL.md"
}

# -- Source 3: Custom skills from project (copy, never overwrite by default) ---
Write-Host ""
Write-Host "Source 3: custom_skills\  (project-local)" -ForegroundColor Yellow

if (Test-Path $customSrc) {
    foreach ($dir in Get-ChildItem -Path $customSrc -Directory) {
        $dest = Join-Path $skillsTarget $dir.Name
        if ((Test-Path (Join-Path $dest "SKILL.md")) -and -not $Force) {
            Write-Host "  SKIP   $($dir.Name)" -ForegroundColor DarkGray
            $skipped++
        }
        else {
            Copy-Item -Path $dir.FullName -Destination $dest -Recurse -Force
            Write-Host "  OK     $($dir.Name)" -ForegroundColor Green
            $downloaded++
        }
    }
}
else {
    Write-Host "  (custom_skills\ folder not found - skipping)" -ForegroundColor DarkGray
}

# -- Summary: skills -----------------------------------------------------------
Write-Host ""
Write-Host ("=" * 52) -ForegroundColor Cyan
Write-Host "Skills  -  Installed : $downloaded   Skipped : $skipped   Failed : $failed"
if ($failed -gt 0) {
    Write-Host "Check your internet connection and retry failed skills." -ForegroundColor Red
}

# -- MCP Server Setup ----------------------------------------------------------
if (-not $SkipMcp) {
    Write-Host ""
    Write-Host "MCP Server Setup" -ForegroundColor Cyan
    Write-Host ("=" * 52) -ForegroundColor Cyan

    $mcpDir    = Join-Path $workspaceRoot "databricks-exec-code-mcp"
    $mcpVenv   = Join-Path $mcpDir ".venv"
    $mcpPython = Join-Path $mcpVenv "Scripts\python.exe"
    $mcpScript = Join-Path $mcpDir "mcp_tools\tools.py"
    $mcpRepo   = "https://github.com/databricks-solutions/databricks-exec-code-mcp.git"
    $mcpJson   = Join-Path $env:USERPROFILE ".claude\mcp.json"
    $mcpOk     = $true

    Write-Host "MCP server target : $mcpDir"

    # Step 1: Clone if missing
    if (-not (Test-Path $mcpDir)) {
        Write-Host "  Cloning MCP server repo..." -ForegroundColor Yellow
        git clone $mcpRepo $mcpDir 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  FAIL: git clone failed. Ensure git is installed and you have internet access." -ForegroundColor Red
            $mcpOk = $false
        } else {
            Write-Host "  OK  : repo cloned to $mcpDir" -ForegroundColor Green
        }
    } else {
        Write-Host "  OK  : repo already exists - skipping clone" -ForegroundColor DarkGray
    }

    # Step 2: Create venv + install deps if venv missing
    if ($mcpOk -and -not (Test-Path $mcpPython)) {
        Write-Host "  Creating Python venv..." -ForegroundColor Yellow
        & python -m venv $mcpVenv
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  FAIL: python -m venv failed. Ensure Python 3.11+ is on PATH." -ForegroundColor Red
            $mcpOk = $false
        } else {
            Write-Host "  Installing dependencies..." -ForegroundColor Yellow
            & $mcpPython -m pip install -r (Join-Path $mcpDir "requirements.txt") -q
            if ($LASTEXITCODE -ne 0) {
                Write-Host "  FAIL: pip install failed." -ForegroundColor Red
                $mcpOk = $false
            } else {
                Write-Host "  OK  : venv created and dependencies installed" -ForegroundColor Green
            }
        }
    } elseif (Test-Path $mcpPython) {
        Write-Host "  OK  : venv already exists - skipping install" -ForegroundColor DarkGray
    }

    # Step 3: Create or update ~/.claude/mcp.json
    if ($mcpOk) {
        $claudeDir = Join-Path $env:USERPROFILE ".claude"
        if (-not (Test-Path $claudeDir)) {
            New-Item -ItemType Directory -Path $claudeDir -Force | Out-Null
        }

        if (Test-Path $mcpJson) {
            # Update existing mcp.json: fix command + args paths, keep credentials
            $cfg = Get-Content $mcpJson -Raw | ConvertFrom-Json
            $currentCmd = $cfg.mcpServers.databricks.command
            if ($currentCmd -ne $mcpPython) {
                $cfg.mcpServers.databricks.command = $mcpPython
                $cfg.mcpServers.databricks.args    = @($mcpScript)
                $cfg | ConvertTo-Json -Depth 10 | Set-Content $mcpJson -Encoding UTF8
                Write-Host "  OK  : mcp.json updated with new MCP server path" -ForegroundColor Green
            } else {
                Write-Host "  OK  : mcp.json already has correct path - no changes" -ForegroundColor DarkGray
            }
        } else {
            # Create new mcp.json with placeholder credentials
            $newCfg = [ordered]@{
                mcpServers = [ordered]@{
                    databricks = [ordered]@{
                        command = $mcpPython
                        args    = @($mcpScript)
                        env     = [ordered]@{
                            DATABRICKS_HOST  = "https://<your-workspace>.azuredatabricks.net"
                            DATABRICKS_TOKEN = "dapi<your-personal-access-token>"
                        }
                    }
                }
            }
            $newCfg | ConvertTo-Json -Depth 10 | Set-Content $mcpJson -Encoding UTF8
            Write-Host "  OK  : mcp.json created at $mcpJson" -ForegroundColor Green
            Write-Host ""
            Write-Host "  ACTION REQUIRED: Edit $mcpJson" -ForegroundColor Yellow
            Write-Host "    Set DATABRICKS_HOST  to your workspace URL" -ForegroundColor Yellow
            Write-Host "    Set DATABRICKS_TOKEN to your PAT (dapi...)" -ForegroundColor Yellow
        }
    }
}

# -- Final message -------------------------------------------------------------
Write-Host ""
Write-Host ("=" * 52) -ForegroundColor Cyan
Write-Host "Restart Claude Code to pick up new skills and MCP server." -ForegroundColor Yellow
Write-Host ""
