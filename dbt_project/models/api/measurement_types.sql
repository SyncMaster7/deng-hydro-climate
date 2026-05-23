select *
from (values
    ('wl_avg',  'Water level — average',            'cm',      'hydro'),
    ('wl_min',  'Water level — minimum',            'cm',      'hydro'),
    ('wl_max',  'Water level — maximum',            'cm',      'hydro'),
    ('wt_avg',  'Water temperature — average',      '°C',      'hydro'),
    ('wt_min',  'Water temperature — minimum',      '°C',      'hydro'),
    ('wt_max',  'Water temperature — maximum',      '°C',      'hydro'),
    ('q_avg',   'Discharge — average',              'm³/s',    'hydro'),
    ('q_min',   'Discharge — minimum',              'm³/s',    'hydro'),
    ('q_max',   'Discharge — maximum',              'm³/s',    'hydro'),
    ('pr1h',    'Precipitation — hourly total',     'mm',      'meteo'),
    ('ta',      'Air temperature',                  '°C',      'meteo'),
    ('tan1h',   'Air temperature — minimum',        '°C',      'meteo'),
    ('tax1h',   'Air temperature — maximum',        '°C',      'meteo'),
    ('rh',      'Relative humidity',                '%',       'meteo'),
    ('pa0',     'Atmospheric pressure',             'hPa',     'meteo'),
    ('ws10m',   'Wind speed at 10 m',               'm/s',     'meteo'),
    ('wsx1h',   'Wind gust — maximum hourly',       'm/s',     'meteo'),
    ('wd10m',   'Wind direction at 10 m',           'degrees', 'meteo'),
    ('sdur1h',  'Sunshine duration — hourly',       'min',     'meteo')
) as t(element_code, description, unit, source)
