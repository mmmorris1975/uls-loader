import io, csv
from zipfile import ZipFile

class uls_dialect(csv.Dialect):
    delimiter = '|'
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\r\n'
    quoting = csv.QUOTE_NONE # needed to deal with some screwed up quoting in FCC source data
csv.register_dialect("uls", uls_dialect)

# We only need to define the data fields from the beginning up to the last field we're interested in,
# the reset will be put as an array under a key called 'None'
COMMON_FIELDS = ('rec_type', 'sys_id', 'file_no', 'ebf_num', 'call_sign')

def normalize_float(str_val):
  # Normalize the string value in the data file to a float with a default of 0.0 if the field exists but is blank
  float_val = 0.0

  if str_val and len(str_val.strip()) > 0:
    float_val = float(str_val)

  return float_val

def dms_to_dec(deg, min, sec, dir):
  fract_min = normalize_float(sec) / 60.0
  fract_deg = (normalize_float(min) + fract_min) / 60.0
  dms = abs(float(deg)) + fract_deg

  if dir.casefold().startswith(('s', 'w')):
    dms *= -1

  return dms

def get_rows(zip_file, file_name, fields=None, delim='|', encoding='latin_1'):
  with ZipFile(zip_file) as zf:
    with zf.open(file_name) as datfile:
      p = csv.DictReader(io.TextIOWrapper(datfile, encoding), COMMON_FIELDS + fields, dialect = 'uls')

      for row in p:
        yield row
