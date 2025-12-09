@echo off
setlocal

echo ============================================================
echo  Credits DW - Pipeline de Ingestao Bronze (Windows)
echo ============================================================
echo.

if not exist .env (
    echo [ERRO] Arquivo .env nao encontrado!
    echo Copie o .env.example e renomeie para .env
    pause
    exit /b 1
)

cd docker

echo [INFO] Construindo imagem Docker...
docker compose build --quiet

echo.
echo [INFO] Executando pipeline...
docker compose run --rm etl-processor python python/scripts/run_pipeline.py

echo.
if %ERRORLEVEL% EQU 0 (
    echo [SUCESSO] Pipeline concluido!
    echo Verifique os logs na pasta logs/
) else (
    echo [ERRO] Ocorreu um erro na execucao.
)

cd ..
pause
