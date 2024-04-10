:::::::::::::::::::::::::::::::::::::::::::::::::::::::
::                                                   ::
:: Process: Create - ETL Apache Beam Project         ::
:: Created by: Armando Avila                         ::
:: Purpose: Executable file of the Create process    ::
:: Comment: Runs Create.py file without parameters,  ::
::          gets date and time for the log file name ::
::          in the format YYYYMMDDHHMISS             ::
:: Created: 2024-03                                  ::
:: Modified by:                                      ::
:: Modified date:                                    ::
:: Modifications:                                    ::
::                                                   ::
:::::::::::::::::::::::::::::::::::::::::::::::::::::::

:: Start the process
echo Start the Create process

:: Gets date and time
set dt=%date:~-4%%date:~3,2%%date:~0,2%%time:~0,2%%time:~3,2%%time:~6,2%

:: Executes Create.py file
python ../src/Create.py | tee ../logs/Create_Process_%dt%.out

:: Finish the process
echo Finish the Create process
