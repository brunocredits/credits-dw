@echo off
setlocal EnableDelayedExpansion

echo ============================================================
echo  Credits DW - Reset de Ambiente (Windows)
echo ============================================================
echo.

if not exist .env (
    echo [ERRO] Arquivo .env nao encontrado!
    pause
    exit /b 1
)

echo ATENCAO: Isso vai APAGAR todos os dados das tabelas Bronze 
echo e limpar os arquivos processados.
set /p confirm="Tem certeza? (s/N): "

if /i not "!confirm!"=="s" (
    echo Cancelado.
    exit /b 0
)

echo.
echo 1. Limpar Arquivos Processados e Logs...
rem Usando powershell para limpar pastas de forma recursiva sem erro de permissao se possivel
powershell -Command "Remove-Item -Path 'docker\data\processed\*' -Recurse -Force -ErrorAction SilentlyContinue"
powershell -Command "Remove-Item -Path 'logs\*' -Recurse -Force -ErrorAction SilentlyContinue"
echo    [OK] Arquivos limpos.

echo.
echo 2. Truncar Tabelas do Banco (via Docker)...
cd docker
docker compose build --quiet
docker compose run --rm etl-processor python python/scripts/truncate_tables.py
cd ..

if %ERRORLEVEL% EQU 0 (
    echo.
    echo [SUCESSO] Ambiente resetado! Pronto para nova carga.
) else (
    echo.
    echo [ERRO] Falha ao resetar banco de dados.
)

pause
