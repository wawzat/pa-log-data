# pa-log_data
 
## Perodically logs PurpleAir sensor data to Google Sheets from the PurpleAir API for sensors within a defeined rectangular area.

### Notes:
 
- Rename config_template.py to config.py.
- Edit config.py with your API keys and preferences.
- Edit config.py BBOX_DICT with your region abbreviations and coordinates. 
- Create empty Google Sheets sheets with names corresponding to regions.
  - For example: if your BBOX_DICT is per below:
    - BBOX_DICT = {
        'DT': (('-118.5298', '35.7180', '-118.4166', '35.8188'), 'DT, 'Delta Transit') ,
        'QC': (('-118.455864', '35.855306', '-118.185326', '35.018518'), 'Qual College'
        'YB': (('-118.877808', '35.650065', '-118.628899', '35.776579'), 'YB', 'Yucatan Borrego'),
        'CP': (('-118.807770', '35.901766', '-118.550964', '35.079394'), 'CP', 'Cirrus Park')
        }
    - You would create Google sheets with the following names:
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
 
- Search Google for istructions for how to setup an application and API keys for Google Sheets. https://developers.google.com/workspace/guides/create-credentials 
- PurpleAir API keys may be obtained from PurpleAir. https://community.purpleair.com/t/creating-api-keys/3951 
- Sensor data are gathered for a "local" region and other regions as defined in config.py.
- Sensor Confidence is calculated as the percentage of data discarded after cleaning using EPA criteria and may not match displayed PurpleAir Confidence percentages.
- The WRITE_CSV parameter only captures the latest local sensor data. Used for testing purposes.
