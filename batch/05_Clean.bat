:::::::::::::::::::::::::::::::::::::::::::::::::::::::
::                                                   ::
:: Process: Clean - ETL Apache Beam Project          ::
:: Cleand by: Armando Avila                          ::
:: Purpose: Executable file of the Clean process     ::
::          this process must be run after the ETL   ::
::          ETL                                      ::
:: Comment: Runs Clean.py file without parameters,   ::
::          gets date and time for the log file name ::
::          in the format YYYYMMDDHHMISS             ::
:: Created: 2024-03                                  ::
:: Modified by:                                      ::
:: Modified date:                                    ::
:: Modifications:                                    ::
::                                                   ::
:::::::::::::::::::::::::::::::::::::::::::::::::::::::

:: Start the process
echo Start the Clean process

:: Gets date and time
set dt=%date:~-4%%date:~3,2%%date:~0,2%%time:~0,2%%time:~3,2%%time:~6,2%

:: Executes Clean.py file
python ../src/Clean.py | tee ../logs/Clean_Process_%dt%.out

:: Finish the process
echo Finish the Clean process
