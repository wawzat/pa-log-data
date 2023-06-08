
"""
This module contains classes for converting air quality data from one unit to another. 

Classes:
- AQI: Converts PM2.5 concentration to Air Quality Index (AQI) value.
- EPA: Converts PM2.5 concentration and relative humidity to EPA concentration value.

Functions:
- None

Exceptions:
- None

Usage:
- Import the module and use the AQI and EPA classes to convert air quality data.

Dependencies:
- logging module
"""
import logging

class AQI:
    @staticmethod
    def calculate(PM, *args):
        # Calculate average of the arguments
        total = PM
        count = 1
        for arg in args:
            total += arg
            count += 1
        PM2_5 = total / count
        PM2_5 = int(PM2_5 * 10) / 10.0
        if PM2_5 < 0:
            PM2_5 = 0
        #AQI breakpoints (0,    1,     2,    3    )
        #                (Ilow, Ihigh, Clow, Chigh)
        pm25_aqi = {
            'good': (0, 50, 0, 12),
            'moderate': (51, 100, 12.1, 35.4),
            'sensitive': (101, 150, 35.5, 55.4),
            'unhealthy': (151, 200, 55.5, 150.4),
            'very': (201, 300, 150.5, 250.4),
            'hazardous': (301, 500, 250.5, 500.4),
            'beyond_aqi': (301, 500, 250.5, 500.4)
            }
        try:
            if (0.0 <= PM2_5 <= 12.0):
                aqi_cat = 'good'
            elif (12.1 <= PM2_5 <= 35.4):
                aqi_cat = 'moderate'
            elif (35.5 <= PM2_5 <= 55.4):
                aqi_cat = 'sensitive'
            elif (55.5 <= PM2_5 <= 150.4):
                aqi_cat = 'unhealthy'
            elif (150.5 <= PM2_5 <= 250.4):
                aqi_cat = 'very'
            elif (250.5 <= PM2_5 <= 500.4):
                aqi_cat = 'hazardous'
            elif (PM2_5 >= 500.5):
                aqi_cat = 'beyond_aqi'
            Ihigh = pm25_aqi.get(aqi_cat)[1]
            Ilow = pm25_aqi.get(aqi_cat)[0]
            Chigh = pm25_aqi.get(aqi_cat)[3]
            Clow = pm25_aqi.get(aqi_cat)[2]
            Ipm25 = int(round(
                ((Ihigh - Ilow) / (Chigh - Clow) * (PM2_5 - Clow) + Ilow)
                ))
            return Ipm25
        except Exception as e:
            logging.exception('calc_aqi() error')

class EPA:
    @staticmethod
    def calculate(RH, PM, *args):
        # Calculate average of the arguments
        total = PM
        count = 1
        for arg in args:
            total += arg
            count += 1
        PM2_5 = total / count
        try: 
            # If either PM2_5 or RH is a string, the EPA conversion value will be set to 0.
            if any(isinstance(x, str) for x in (PM2_5, RH)):
                PM2_5_epa = 0
            elif PM2_5 <= 343:
                PM2_5_epa = round((0.52 * PM2_5 - 0.086 * RH + 5.75), 3)
            elif PM2_5 > 343:
                PM2_5_epa = round((0.46 * PM2_5 + 3.93 * 10 ** -4 * PM2_5 ** 2 + 2.97), 3)
            else:
                PM2_5_epa = 0
            return PM2_5_epa
        except Exception as e:
            logging.exception('calc_epa() error')

