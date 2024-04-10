:::::::::::::::::::::::::::::::::::::::::::::::::::::::
::                                                   ::
:: Process: Load - ETL Apache Beam Project           ::
:: Created by: Armando Avila                         ::
:: Purpose: Executable file of the Load process      ::
:: Comment: Runs Load.py file without parameters,    ::
::          gets date and time for the log file name ::
::          in the format YYYYMMDDHHMISS             ::
:: Created: 2024-03                                  ::
:: Modified by:                                      ::
:: Modified date:                                    ::
:: Modifications:                                    ::
::                                                   ::
:::::::::::::::::::::::::::::::::::::::::::::::::::::::

:: Start the process
echo Start the Load process

:: Gets date and time
set dt=%date:~-4%%date:~3,2%%date:~0,2%%time:~0,2%%time:~3,2%%time:~6,2%

:: Executes Load.py file
python ../src/Load.py | tee ../logs/Load_Process_%dt%.out

:: Finish the process
echo Finish the Load process
