@echo off

SET RUN_DOTPY=python.exe C://Users//anupam.soni//PycharmProjects//EIA//main.py
SET PYTHON_PATH=C:\Python36-32
color 
set SCRIPT= main.py


REM Hunt around for python
IF EXIST "python.exe" (
  SET SCRIPT=%RUN_DOTPY%
) ELSE (
  IF EXIST "%PYTHON_PATH%" (
    SET SCRIPT=%PYTHON_PATH%\%RUN_DOTPY%
  ) ELSE (
    IF EXIST %PYTHON% SET SCRIPT=%PYTHON%\%RUN_DOTPY%
  )
)

IF NOT "" == "%SCRIPT%" (
  %SCRIPT%
  pause
) ELSE (
  echo.
  echo Python.exe is not in the path!
  pause
)
:end


C:\Users\anupam.soni\PycharmProjects\EIA