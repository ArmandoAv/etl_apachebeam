:::::::::::::::::::::::::::::::::::::::::::::::::::::::
::                                                   ::
:: Process: Extract - ETL Apache Beam Project        ::
:: Created by: Armando Avila                         ::
:: Purpose: Executable file of the Extract process   ::
:: Comment: Runs Extract.py file without parameters, ::
::          gets date and time for the log file name ::
::          in the format YYYYMMDDHHMISS             ::
:: Created: 2024-03                                  ::
:: Modified by:                                      ::
:: Modified date:                                    ::
:: Modifications:                                    ::
::                                                   ::
:::::::::::::::::::::::::::::::::::::::::::::::::::::::

:: Start the process
echo Start the Extract process

:: Gets date and time
set dt=%date:~-4%%date:~3,2%%date:~0,2%%time:~0,2%%time:~3,2%%time:~6,2%

:: Executes Extract.py file
python ../src/Extract.py | tee ../logs/Extract_Process_%dt%.out

:: Finish the process
echo Finish the Extract process
