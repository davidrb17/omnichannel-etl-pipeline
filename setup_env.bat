@echo off


echo ======================================
echo SETUP ENTORNO ETL TARJETAS DE CREDITO
echo ======================================

REM 1. Crear entorno virtual si no existe
IF NOT EXIST venv (
    echo Creando entorno virtual...
    python -m venv venv
) ELSE (
    echo El entorno virtual ya existe.
)

REM 2. Activar el entorno virtual
echo Activando entorno virtual...
call "%cd%\venv\Scripts\activate.bat"

REM 4. Instalar dependencias
echo Instalando dependencias...
pip install pandas numpy sqlalchemy python-dotenv matplotlib fpdf pyodbc mysql-connector-python psycopg2-binary chardet

REM 5. Generar requeriments.txt
echo Generando requirements.txt
pip freeze > requirements.txt

echo =================================
echo SETUP COMPLETADO CORRECTAMENTE
echo =================================
pause
