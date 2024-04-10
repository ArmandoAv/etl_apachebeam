"""
    File with ETL schemas
"""

# The libraries are imported
#import json


# Activity json file schema
activity_json_schema = {
    "fields": [
        {"name": "PEOPLE_POSITIVE_CASES_COUNT", "type": "INTEGER"},
        {"name": "COUNTY_NAME", "type": "STRING"},
        {"name": "PROVINCE_STATE_NAME", "type": "STRING"},
        {"name": "REPORT_DATE", "type": "DATE"},
        {"name": "CONTINENT_NAME", "type": "STRING"},
        {"name": "DATA_SOURCE_NAME", "type": "STRING"},
        {"name": "PEOPLE_DEATH_NEW_COUNT", "type":"INTEGER"},
        {"name": "COUNTY_FIPS_NUMBER", "type": "STRING"},
        {"name": "COUNTRY_ALPHA_3_CODE", "type": "STRING"},
        {"name": "COUNTRY_NAME", "type": "STRING"},
        {"name": "COUNTRY_ALPHA_2_CODE", "type": "STRING"},
        {"name": "PEOPLE_POSITIVE_NEW_CASES_COUNT", "type": "INTEGER"},
        {"name": "PEOPLE_DEATH_COUNT", "type": "INTEGER"}
    ]
}

# Country json file schema
country_json_schema = {
    "fields": [
        {"name": "COUNTRY_ALPHA_3_CODE", "type": "STRING"},
        {"name": "COUNTRY_NAME", "type": "STRING"},
        {"name": "COUNTRY_ALPHA_2_CODE", "type": "STRING"}
    ]
}
