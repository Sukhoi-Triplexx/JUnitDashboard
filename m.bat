@echo off
echo Building VUnit...
pyinstaller --onefile --add-data "templates;templates" --add-data "static;static" --add-data "flask.ico;." --hidden-import psycopg2 --hidden-import psycopg2.extras --hidden-import psycopg2.pool --hidden-import reportlab --icon flask.ico --name VUnit main.py
echo Done! Binary: dist\VUnit.exe

:: Копируем в сетевую папку
copy /Y dist\VUnit.exe X:\7.ОИТ\Чуев\builds
echo Copied to network share!

pause