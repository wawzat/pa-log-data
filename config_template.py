#PurpleAir Keys
PURPLEAIR_READ_KEY = ''
PURPLEAIR_WRITE_KEY = ''

GSPREAD_SERVICE_ACCOUNT_JSON_PATH = r'C:\Users\username\AppData\Roaming\gspread\service_account.json'
SERVICE_ACCOUNT_USER_EMAIL = 'username@project_name.iam.gserviceaccount.com'


#user_directory = r' '
MATRIX5 = r'c:\Users\username\OneDrive\Documents\House\PurpleAir'

# First dictionary entry should be for the "local" region"
#           SW lon / lat            NE lon / lat
BBOX_DICT = { 'DT': (('-118.5298', '35.7180', '-118.4166', '35.8188'), 'DT', 'Delta Transit'),
'QC': (('-118.455864', '35.855306', '-118.185326', '35.018518'), 'Qual College'),
'YB': (('-118.877808', '35.650065', '-118.628899', '35.776579'), 'YB', 'Yucatan Borrego'),
'CP': (('-118.807770', '35.901766', '-118.550964', '35.079394'), 'CP', 'Cirrus Park')
}

# Durations in seconds
STATUS_INTERVAL_DURATION: int = 5
LOCAL_INTERVAL_DURATION: int = 1200
REGIONAL_INTERVAL_DURATION: int = 3800
PROCESS_INTERVAL_DURATION: int = 5000

PROCESS_RESAMPLE_RULE: str = '2H'
WRITE_CSV: bool = False

# set the name of the Google Sheets document
DOCUMENT_NAME: str = 'pa_data'
HISTORICAL_DOCUMENT_NAME: str = 'pa_history'
# set the names of the worksheets in the Google Sheets document
LOCAL_WORKSHEET_NAME: str = 'DT'
REGIONAL_KEYS = ('QC', 'YB', 'CP')
OUT_WORKSHEET_HEALTH_NAME: str = 'Health'

google_account = 'username@gmail.com'

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
