#PurpleAir Keys
PURPLEAIR_READ_KEY = ''
PURPLEAIR_WRITE_KEY = ''

GSPREAD_SERVICE_ACCOUNT_JSON_PATH = r'C:\Users\username\AppData\Roaming\gspread\service_account.json'
SERVICE_ACCOUNT_USER_EMAIL = 'username@project_name.iam.gserviceaccount.com'


#user_directory = r' '
MATRIX5 = r'c:\Users\username\OneDrive\Documents\House\PurpleAir'

local_region = 'DT'
#           SW lon / lat            NE lon / lat
BBOX_DICT = { 'DT': (('-118.5298', '35.7180', '-118.4166', '35.8188'), 'DT', 'Delta Transit'),
'QC': (('-118.455864', '35.855306', '-118.185326', '35.018518'), 'Qual College'),
'YB': (('-118.877808', '35.650065', '-118.628899', '35.776579'), 'YB', 'Yucatan Borrego'),
'CP': (('-118.807770', '35.901766', '-118.550964', '35.079394'), 'CP', 'Cirrus Park')
}

# Durations in seconds
STATUS_INTERVAL_DURATION: int = 1
LOCAL_INTERVAL_DURATION: int = 1800
REGIONAL_INTERVAL_DURATION: int = 3900
PROCESS_INTERVAL_DURATION: int = 10000

PROCESS_RESAMPLE_RULE: str = '2H'
WRITE_CSV: bool = False

# set the name of the Google Sheets document
DOCUMENT_NAME: str = 'pa_data'
HISTORICAL_DOCUMENT_NAME: str = 'pa_history'
# set the names of the worksheets in the Google Sheets document
CURRENT_WORKSHEET_NAME: str = 'Current'
LOCAL_WORKSHEET_NAME: str = 'DT'
REGIONAL_KEYS = ('QC', 'YB', 'CP')
OUT_WORKSHEET_HEALTH_NAME: str = 'Health'

google_account = 'username@gmail.com'

cols_1 = ['time_stamp', 'time_stamp_pacific']
cols_2 = ['sensor_index', 'name', 'latitude', 'longitude']
cols_3 = ['altitude']
cols_4 = ['rssi']
cols_5 = ['uptime']
cols_6 = ['humidity', 'temperature', 'pressure', 'voc']
cols_7 = ['pm1.0_atm_a', 'pm1.0_atm_b', 'pm2.5_atm_a', 'pm2.5_atm_b', 'pm10.0_atm_a', 'pm10.0_atm_b',
        'pm1.0_cf_1_a', 'pm1.0_cf_1_b', 'pm2.5_cf_1_a',  'pm2.5_cf_1_b', 'pm10.0_cf_1_a', 'pm10.0_cf_1_b',
        '0.3_um_count', '0.5_um_count', '1.0_um_count', '2.5_um_count', '5.0_um_count', '10.0_um_count']
cols_8 = ['pm25_epa', 'Ipm25']
cols = cols_1 + cols_2 + cols_3 + cols_4 + cols_5 + cols_6 + cols_7 + cols_8

tv_sensors_all = {
    'SQTQ_RTTP_2533': {'ID': 12561, 'Neighborhood': 'Delta Bastion'},
    'EE Park': {'ID': 72725, 'Neighborhood': 'Goldilocks'},
    'Rec Area': {'ID': 1243, 'Neighborhood': 'Rec Zone'},
    'Tawankef': {'ID': 1154, 'Neighborhood': 'TCF'},
}

tv_sensors = {
    'SQTQ_RTTP_2533': {'ID': 12561, 'Neighborhood': 'Delta Bastion'},
    'Tawankef': {'ID': 1154, 'Neighborhood': 'TCF'},
}
