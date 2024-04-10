-------------------------------------------------------------
------------------- CREATE SOURCE DATABASE ------------------
-------------------------------------------------------------
-------------------------------------------------------------

--- CREATE DATABASE
CREATE DATABASE COVID
GO

--- USE DATABASE
USE COVID
GO

-------------------------------------------------------------
-------------------------------------------------------------
----------------- CREATE TABLE SOURCE SISTEM ----------------
-------------------------------------------------------------
-------------------------------------------------------------

--- TABLE COVID-19-COUNTRIES
CREATE TABLE COVID_19_COUNTRIES (
  COUNTRY_ALPHA_3_CODE VARCHAR(5),
  COUNTRY_SHORT_NAME VARCHAR(200),
  COUNTRY_ALPHA_2_CODE VARCHAR(5)
)
GO

CREATE NONCLUSTERED INDEX IDX_COUNTRIES
ON COVID_19_COUNTRIES (COUNTRY_ALPHA_3_CODE)
GO
