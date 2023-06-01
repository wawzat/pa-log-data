# pa_log_data.py
 
## Python program to periodically log PurpleAir sensor data to Google Sheets from the PurpleAir API for sensors within a defined rectangular area.

### Notes:
 
- Rename config_template.py to config.py.
- Edit config.py with your API keys, document names, file paths and preferences.
- Edit config.py BBOX_DICT with your region abbreviations and coordinates. 
- bbox finder is handy for getting rectangular coordinates: http://bboxfinder.com/#0.000000,0.000000,0.000000,0.000000 
- Name the Google Sheets worksheet document with the name you provided for DOCUMENT_NAME in config.py 
- Manually create empty Google Sheets sheets with names corresponding to regions.
  - For example, for the BBOX_DICT below:
    - BBOX_DICT = {
        'DT': (('-118.5298', '35.7180', '-118.4166', '35.8188'), 'DT', 'Delta Transit'),  
        'QC': (('-118.455864', '35.855306', '-118.185326', '35.018518'), 'Qual College'),  
        'YB': (('-118.877808', '35.650065', '-118.628899', '35.776579'), 'YB', 'Yucatan Borrego'),  
        'CP': (('-118.807770', '35.901766', '-118.550964', '35.079394'), 'CP', 'Cirrus Park')  
        }
    - You would manually create Google sheets with the following names:
        - DT
        - QC
        - YB
        - CP
        - DT Proc
        - QC Proc
        - YB Proc
        - CP Proc
        - Health
        - Regional
 
    - The two character sheets will contain raw logged data and the sheets with a "Proc" suffix will contain processed data for each corresponding region.

- Setup an application and API keys for Google Sheets. https://developers.google.com/workspace/guides/create-credentials 
- Read the gspread documentation for authentication instructions. https://docs.gspread.org/en/v5.7.2/oauth2.html 
- PurpleAir API keys may be obtained from PurpleAir. https://community.purpleair.com/t/creating-api-keys/3951 
- Sensor data are gathered for a "local" region and other regions as defined in config.py.
- Sensor Confidence is calculated as the percentage of data discarded after cleaning with EPA criteria and may not match displayed PurpleAir Confidence percentages.
- The WRITE_CSV parameter only captures the latest local sensor data to a local CSV file. Used for testing purposes.
- requirements.txt is included for installing the required non-standard Python libraries (i.e., pip: -r requirements.txt)
- To prevent the Google Sheets document from becoming too large you should periodically archive data to another worksheet and delete data from the master document.
- You can use the Google Sheets worksheet as source data for a Google Looker Studio dashboard. https://lookerstudio.google.com/ 
  
  
 
# sheet_size_reduce.py  

## Python program to trim the earliest rows from a specified Google Sheets sheet.  

### Notes:  

- Edit the code to change defaults in the argparse section.
- For performance reasons I typically only keep three to four weeks of data in my raw logged data sheets.
- This program automates deleting old data.
- I manually copy sheets to an archive folder each month.  
  
  
# pa_get_history.py
  
## Python program to get PurpleAir historical sensor data and save to Google Sheets.
  
### Notes:
  
- Default Sensor names are stored in the sensors_current dictionary in cofig.py