select *
from (values
    ('wl_avg',  'Veetase — avg',            'cm',      'hydro'),
    ('wl_min',  'Veetase — min',            'cm',      'hydro'),
    ('wl_max',  'Veetase — max',            'cm',      'hydro'),
    ('wt_avg',  'Veetemperatuur — avg',     '°C',      'hydro'),
    ('wt_min',  'Veetemperatuur — min',     '°C',      'hydro'),
    ('wt_max',  'Veetemperatuur — max',     '°C',      'hydro'),
    ('q_avg',   'Äravool — avg',            'm³/s',    'hydro'),
    ('q_min',   'Äravool — min',            'm³/s',    'hydro'),
    ('q_max',   'Äravool — max',            'm³/s',    'hydro'),
    ('pr1h',    'Sademed — tund sum',       'mm',      'meteo'),
    ('ta',      'Õhutemperatuur',               '°C',      'meteo'),
    ('tan1h',   'Õhutemperatuur — min',     '°C',      'meteo'),
    ('tax1h',   'Õhutemperatuur — max',     '°C',      'meteo'),
    ('rh',      'Suhteline õhuniiskus',         '%',       'meteo'),
    ('pa0',     'Õhurõhk',                      'hPa',     'meteo'),
    ('ws10m',   'Tuule kiirus — 10 min avg',         'm/s',     'meteo'),
    ('wsx1h',   'Tuule kiirus — tund max',   'm/s',     'meteo'),
    ('wd10m',   'Tuule suund — 10 min valdav',       'degrees', 'meteo'),
    ('sdur1h',  'Päikesepaiste kestus — tund sum',   'min',     'meteo')
) as t(element_code, description, unit, source)
