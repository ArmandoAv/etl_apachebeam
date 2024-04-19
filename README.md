# ETL Apache Beam

This project contains the entire track of an ETL process on Windows, with Apache Beam and Python

## About The Project

The following software was used to carry out this project:

- SQL Server
- MongoDB
- Python

### Prerequisites

The following list is what is needed to use the software.

- Python 3.8.10
- SQLEXPRADV_x64_ENU.exe
- mongodb-win32-x86_64-2012plus-4.2.25-signed.msi

In the following link is the version of Python 3.8.10

https://www.python.org/downloads/release/python-3810/

In the following link there is the express version of SQL Server

https://www.microsoft.com/es-mx/sql-server/sql-server-downloads

In the following link there is the MongoDB Community Server version

https://www.mongodb.com/try/download/community

### Installation

To install the software mentioned above, the following steps must be performed. The following steps are to install the required software in a windows environment.

#### Python

To install Python, run the file python-3.8.10-amd64.exe which was downloaded from the link. When executing the file, in the same process it indicates that if you want to save the Python variable in the environment variables, so it is no longer necessary to add them when finishing the installation.

A cmd terminal is opened and validated, execute the next command

```
python --version
```

Python has a library with which you can install plugins on the command line called pip. Copy the script from the following link into a notepad

https://bootstrap.pypa.io/get-pip.py

Save the copied script as get-pip.py and open a cmd terminal in the path where you saved the file and executed the following command

```
python get-pip.py
```

The following environment variable must be created

```
PIP_HOME = C:\Users\.....\AppData\Local\Programs\Python\Python38
```

The %PIP_HOME%\Scripts variable is added to the Path variable.

#### SQL Server

To install SQL Server, run the file SQLEXPRADV_x64_ENU.exe which was downloaded from the link, creating the 'sa' user that is the administrator of the server and which has all the permissions of the database.

Once the SQL Server is installed, you must open the SQL Server Management Studio and copy the contents of the Source_Database_Script.sql file located in the database directory.

#### MongoDB

Open a cmd terminal with admin privileges, and run the next commands:

```
mkdir C:\data\db\
mkdir C:\data\log\

C:\download_msi_path_file\mongodb-win32-x86_64-2012plus-4.2.25-signed.msi
```
When the installer asks for the data and log paths, you must enter the path of the created directories.

Change in the C:\Program Files\MongoDB\Server\4.2\bin\mongod.cfg file the next records

```
storage:

  dbPath: c:\data\db

  journal:

    enabled: true

systemLog:

  destination: file

  logAppend: true

  path:  c:\data\log\mongod.log
```

Run the next command in a cmd terminal with admin privileges

```
cd C:\Program Files\MongoDB\Server\4.2\bin
mongod.exe
```

If the installation is succesful, at the end of executing previous command it should give a result similar to the following

```
[initandlisten] **          server with --bind_ip 127.0.0.1 to disable this warning.
[initandlisten] waiting for connections on port 27017
```

#### env file

It is necessary to create an environment file called .env in the path where the project will be. The file must contain the following

```
# Database source parameters
CONN_STR_SOURCE = 'DRIVER={SQL Server};SERVER=localhost;DATABASE=COVID;UID=sa;PWD=<your_password>;'
DB_SOURCE_NAME = COVID
DB_SOURCE_SCHEMA = dbo

# Database target parameters
CONN_STR_TARGET = 'mongodb://localhost:27017'
DB_TARGET_NAME = covid
COLLECT_TARGET_COVID_NAME = covid_analysis
COLLECT_TARGET_DATE_NAME = covid_date_analysis
COLLECT_TARGET_COUNTRY_NAME = covid_country
```

## Usage

To run this project, you first need to make a copy of the files and directories, this can be done with the following command in a cmd terminal

```
git clone https://github.com/ArmandoAv/etl_apachebeam.git
```

To obtain the COVID-19 Activity.csv file, you must download it from the following link: 

https://data.world/covid-19-data-resource-hub/covid-19-case-counts

That's because the file is larger than GitHub allows. This file must be downloaded to the input path.

Once you have all the files locally, you can run the process.
The process is divided in parts

1. Create
1. Extraction
1. Transformation
1. Load
1. Clean

Since it is an ETL process, it must be executed in order. A Python virtual environment is needed, the following command must be executed from a cmd terminal in the path where the project was copied.

```
python -m venv venv
```

Once the virtual environment is created, it is activated in the same terminal with the following command

```
venv\Scripts\activate
```

To deactivate the virtual environment in the same path you have to execute the following command

```
deactivate
```

The following python libraries should be installed with the following commands

```
pip install apache-beam[gcp] httplib2 protobuf
pip install pyodbc
pip install python-dotenv
pip install python-decouple
pip install pymongo
```

The files that are executed are those in the src path as well as the files that are in the functions path.

The files in the src path contain the structure, variables, schemas, and SQL scripts of the ETL process. The files in the functions path contain the functions that help in the execution of the files.

> [!NOTE]
> The execution of the processes must be carried out on the same day, since the files that are generated in their names have the current date, so if they are executed on a different day, the **transform** and **load** processes will fail because they cannot find the files.
>
> Furthermore, each process depends on the previous one, so it must always have been executed in order.

### Create

To execute the create process, you only have to execute the Create.bat file with the following command in the batch directory

```
Create.bat
```

This process creates a log file in the logs path automatically and an out type file with the information that the console leaves.

This process loads the COVID-19-Countries.csv file into the COVID_19_COUNTRIES table in the SQL Server. This process simulates the previous processes that are executed to create the information in the source systems, to later carry out an ETL process.

### Extract

To execute the extract process, you only have to execute the Extract.bat file with the following command in the batch directory

```
Extract.bat
```

This process creates a log file in the logs path automatically and an out type file with the information that the console leaves.

This process extracts the information loaded from the COVID_19_COUNTRIES table in SQL Server and creates a csv file.

### Transform

To execute the extract process, you only have to execute the Extract.bat file with the following command in the batch directory

```
Transform.bat
```

This process creates a log file in the logs path automatically and an out type file with the information that the console leaves.

This process creates three json files that will be loaded into three collections in the MongoDB database.

### Load

To execute the extract process, you only have to execute the Extract.bat file with the following command in the batch directory

```
Load.bat
```

This process creates a log file in the logs path automatically and an out type file with the information that the console leaves.

This process uploads the json files created in the Transform Process into collections into the MongoDB database.

### Clean

To execute the extract process, you only have to execute the Extract.bat file with the following command in the batch directory

```
Clean.bat
```

This process creates an out type file with the information that the console leaves.

This process cleans the paths and deletes some log files.

### Considerations

As seen so far, the process has the ability to clean up multiple directories upon completion of the upload process. Therefore, it is necessary to copy the Notes.txt file found in the path where the project will be to the tmp and output paths when you want to upload any modification to the GitHub repository. If you clean the log path, you will also need to copy the Notes.txt file to that path.

The use of directories, files, and processes is described in more detail in the Process_execution.docx file, located in the docs directory.

## Contact

You can contact me in my LinkedIn profile

Armando Avila - [@Armando Avila](https://www.linkedin.com/in/armando-avila-419a8623/)

Project Link: [https://github.com/ArmandoAv/etl_apachebeam](https://github.com/ArmandoAv/etl_apachebeam)
