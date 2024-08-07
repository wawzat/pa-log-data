# user directory = ' '
STORAGE_ROOT_PATH = 'd:/Users/wawzat/OneDrive/Documents/House/PurpleAir'

# download directory
DOWNLOAD_DIRECTORY = 'd:/Users/wawzat/Downloads'

# Edge webdriver location
EDGE_WEBDRIVER_PATH = 'd:/Users/wawzat/OneDrive/Documents/1_Programming/edge_webdriver/msedgedriver.exe'

SCAQMD_SITE = 'https://xappprod.aqmd.gov/aqdetail/AirQuality/HistoricalData'
SCAQMD_STATION = 'Lake Elsinore Area'

LOCAL_REGION = 'TV'
#           SW lon / lat            NE lon / lat
#           nwlng, selat, selng, nwlat
BBOX_DICT = {
    'TV': (('-117.5298', '33.7180', '-117.4166', '33.8188'), 'TV', 'Temescal Valley') ,
    'RS': (('-117.455864', '33.855306', '-117.185326', '34.018518'), 'RS', 'Riverside'),
    'OC': (('-117.877808', '33.650065', '-117.628899', '33.776579'), 'OC', 'Orange County'),
    'CEP': (('-117.807770', '33.901766', '-117.550964', '34.079394'), 'CEP', 'Chino, Eastvale, Pomona')
    }

# Durations in seconds
STATUS_INTERVAL_DURATION: int = 1
LOCAL_INTERVAL_DURATION: int = 21600
REGIONAL_INTERVAL_DURATION: int = 1296000
PROCESS_INTERVAL_DURATION: int = 22000

PROCESS_RESAMPLE_RULE: str = '2H'

# set the name of the Google Sheets document
DOCUMENT_NAME: str = 'pa_data'
HISTORICAL_DOCUMENT_NAME: str = 'pa_history'
# set the names of the worksheets in the Google Sheets document
CURRENT_WORKSHEET_NAME: str = 'Current'
LOCAL_WORKSHEET_NAME: str = 'TV'
REGIONAL_KEYS = ('OC', 'RS', 'CEP')
OUT_WORKSHEET_HEALTH_NAME: str = 'Health'


cols_1 = ['time_stamp', 'time_stamp_pacific']
cols_2 = ['sensor_index']
cols_3 = ['name']
cols_4 = ['rssi']
cols_5 = ['uptime']
cols_6 = ['pm2.5_atm_a', 'pm2.5_atm_b']
cols_7 = ['Ipm25']

local_cols = cols_1 + cols_2 + cols_3 + cols_4 + cols_5 + cols_6 + cols_7
regional_cols = cols_1 + cols_2 + cols_3 + cols_6 + cols_7

tv_sensors_all = {
    'AQMD_NASA_249': {'ID': 28551, 'Neighborhood': 'Lemon Grove'},
    'GE Office': {'ID': 82755, 'Neighborhood': 'Glen Eden'},
    'Glen Eden Rec Area': {'ID': 9182, 'Neighborhood': 'Glen Eden'},
    'Horsethief - Temescal Valley': {'ID': 10204, 'Neighborhood': 'HCR'},
    'Oz Terramor': {'ID': 9356, 'Neighborhood': 'Terramor'},
    'PA-II': {'ID': 9262, 'Neighborhood': 'Creekside'},
    'Retreat 7th Fairway (what was)': {'ID': 9404, 'Neighborhood': 'The Retreat'},
    'SCTV_03': {'ID': None, 'Neighborhood': 'Sycamore Creek'},
    'SCTV_04 (Painted Hills)': {'ID': 9422, 'Neighborhood': 'Painted Hills'},
    'SCTV_05': {'ID': 9394, 'Neighborhood': 'Montecito Ranch'},
    'SCTV_08': {'ID': 9410, 'Neighborhood': 'Trilogy'},
    'SCTV_09': {'ID': 9390, 'Neighborhood': 'HCR'},
    'SCTV_11': {'ID': 9194, 'Neighborhood': 'Trilogy'},
    'SCTV_12': {'ID': 9196, 'Neighborhood': 'The Retreat'},
    'SCTV_14': {'ID': 9186, 'Neighborhood': 'Sycamore Creek'},
    'SCTV_15 (Dawson Canyon)': {'ID': 9192, 'Neighborhood': 'Dawson Canyon'},
    'SCTV_16': {'ID': 9382, 'Neighborhood': 'Trilogy'},
    'SCTV_18': {'ID': 9396, 'Neighborhood': 'California Meadows'},
    'SCTV_19': {'ID': 9178, 'Neighborhood': 'Glen Eden'},
    'SCTV_22': {'ID': 9386, 'Neighborhood': 'The Retreat'},
    'SCTV_26': {'ID': 9208, 'Neighborhood': 'Sycamore Creek'},
    'SCTV_27': {'ID': 9206, 'Neighborhood': 'Sycamore Creek'},
    'SCTV_28': {'ID': 9184, 'Neighborhood': 'Trilogy'},
    'SCTV_29': {'ID': 9176, 'Neighborhood': 'Near HCR'},
    'SCTV_30': {'ID': 9180, 'Neighborhood': 'Trilogy'},
    'SCTV_31': {'ID': 9314, 'Neighborhood': 'Sycamore Creek'},
    'SCTV_32': {'ID': 9306, 'Neighborhood': 'HCR'},
    'SCTV_33': {'ID': 9362, 'Neighborhood': 'HCR'},
    'SCTV_34': {'ID': None, 'Neighborhood': 'Trilogy'},
    'SCTV_35': {'ID': 9340, 'Neighborhood': 'Lemon Grove'},
    'SCTV_36 (Sky View)': {'ID': 9408, 'Neighborhood': 'Glen Eden'},
    'SCTV_39': {'ID': 9364, 'Neighborhood': 'Montecito Ranch'},
    'SCTV_40': {'ID': 9352, 'Neighborhood': 'Glen Eden'},
    'SCTV_41 (Deerweed)': {'ID': 9402, 'Neighborhood': 'Wildrose Ranch'},
    'SCTV_42': {'ID': 9336, 'Neighborhood': 'Trilogy'},
    'SCTV_43': {'ID': 9376, 'Neighborhood': 'Montecito Ranch'},
    'SCTV_45': {'ID': 9392, 'Neighborhood': 'Butterfield Estates'},
    'SCTV_48': {'ID': 9344, 'Neighborhood': 'Wildrose Ranch'},
    'SCTV_50': {'ID': 9350, 'Neighborhood': 'Dos Lagos'},
    'SCTV_54': {'ID': 9452, 'Neighborhood': 'Butterfield Estates'},
    'SVCT_21': {'ID': 9198, 'Neighborhood': 'The Retreat'},
    'Temescal Valley': {'ID': 9338, 'Neighborhood': 'Trilogy'},
    'Temescal Valley 2': {'ID': 26127, 'Neighborhood': 'HCR'},
    'Temescal': {'ID': 9172, 'Neighborhood': 'Terramor'},
}

sensors_current = {
    'AQMD_NASA_249': {'ID': 28551, 'Neighborhood': 'Lemon Grove'},
    'GE Office': {'ID': 82755, 'Neighborhood': 'Glen Eden'},
    'Glen Eden Rec Area': {'ID': 9182, 'Neighborhood': 'Glen Eden'},
    'Horsethief - Temescal Valley': {'ID': 10204, 'Neighborhood': 'HCR'},
    'Oz Terramor': {'ID': 9356, 'Neighborhood': 'Terramor'},
    'Retreat 7th Fairway (what was)': {'ID': 9404, 'Neighborhood': 'The Retreat'},
    'SCTV_03': {'ID': 9398, 'Neighborhood': 'Sycamore Creek'},
    'SCTV_04 (Painted Hills)': {'ID': 9422, 'Neighborhood': 'Painted Hills'},
    'SCTV_05': {'ID': 9394, 'Neighborhood': 'Montecito Ranch'},
    'SCTV_09': {'ID': 9390, 'Neighborhood': 'HCR'},
    'SCTV_12': {'ID': 9196, 'Neighborhood': 'The Retreat'},
    'SCTV_15 (Dawson Canyon)': {'ID': 9192, 'Neighborhood': 'Dawson Canyon'},
    'SCTV_16': {'ID': 9382, 'Neighborhood': 'Trilogy'},
    'SCTV_22': {'ID': 9386, 'Neighborhood': 'The Retreat'},
    'SCTV_26': {'ID': 9208, 'Neighborhood': 'Sycamore Creek'},
    'SCTV_27': {'ID': 9206, 'Neighborhood': 'Sycamore Creek'},
    'SCTV_30': {'ID': 9180, 'Neighborhood': 'Trilogy'},
    'SCTV_31': {'ID': 9314, 'Neighborhood': 'Sycamore Creek'},
    'SCTV_35': {'ID': 9340, 'Neighborhood': 'Lemon Grove'},
    'SCTV_36 (Sky View)': {'ID': 9408, 'Neighborhood': 'Glen Eden'},
    'SCTV_39': {'ID': 9364, 'Neighborhood': 'Montecito Ranch'},
    'SCTV_40': {'ID': 9352, 'Neighborhood': 'Glen Eden'},
    'SCTV_42': {'ID': 9336, 'Neighborhood': 'Trilogy'},
    'SCTV_43': {'ID': 9376, 'Neighborhood': 'Montecito Ranch'},
    'SCTV_45': {'ID': 9392, 'Neighborhood': 'Butterfield Estates'},
    'Temescal Valley': {'ID': 9338, 'Neighborhood': 'Trilogy'},
    'Temescal Valley 2': {'ID': 26127, 'Neighborhood': 'HCR'},
    'Temescal': {'ID': 9172, 'Neighborhood': 'Terramor'},
}

XL_EXCLUDE_LIST = ('combined_summarized_xl.xlsx', 'combined_sheets_xl.xlsx')
CSV_EXCLUDE_LIST = ('LE_REF_CO.csv', 'LE_REF_NO2.csv', 'LE_REF_O3.csv', 'LE_REF_PM2.5.csv', 'LE_REF_T.csv', 'LE_REF_WD.csv', 'LE_REF_WS.csv')

#Used for pa_get_history
ALL_FIELD_LIST = "rssi,uptime,humidity_a,temperature_a,pressure_a,voc_a,pm1.0_atm_a,pm1.0_atm_b,pm2.5_atm_a,pm2.5_atm_b,pm10.0_atm_a,pm10.0_atm_b,pm1.0_cf_1_a,pm1.0_cf_1_b,pm2.5_cf_1_a,pm2.5_cf_1_b,pm10.0_cf_1_a,pm10.0_cf_1_b,0.3_um_count,0.5_um_count,1.0_um_count,2.5_um_count,5.0_um_count,10.0_um_count"
CUSTOM_FIELD_LIST = "rssi,uptime,humidity_a,temperature_a,pressure_a,voc_a,pm1.0_atm_a,pm1.0_atm_b,pm2.5_atm_a,pm2.5_atm_b,pm10.0_atm_a,pm10.0_atm_b,pm1.0_cf_1_a,pm1.0_cf_1_b,pm2.5_cf_1_a,pm2.5_cf_1_b,pm10.0_cf_1_a,pm10.0_cf_1_b,0.3_um_count,0.5_um_count,1.0_um_count,2.5_um_count,5.0_um_count,10.0_um_count"
MINIMAL_FIELD_LIST = "rssi,uptime,humidity_a,pm2.5_atm_a,pm2.5_atm_b,pm2.5_cf_1_a,pm2.5_cf_1_b"