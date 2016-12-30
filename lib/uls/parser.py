import logging
from .common import get_rows, dms_to_dec

logger = logging.getLogger('uls').getChild(__name__)

def parse_en(zip_file):
  file_name = 'EN.dat'

  # Seems to be about 90% 'L' (licensee) types and 10% 'O' (owner) types, call signs may overlap for type
  fields = ('entity_type', 'licensee_id', 'entity_name')

  logger.info("Parsing %s for file %s" % (file_name, zip_file))
  for row in get_rows(zip_file, file_name, fields):
    try:
      # entity_name field may be empty, but only seen with 'contact' entity types (entity_type code starts with 'C')
      # so they can probably be safely skipped during processing, entity_type and entity_name are what we need
      type = row.get('entity_type', 'unk')
      sys_id = row.get('sys_id', False)

      if sys_id and type and not type.casefold().startswith('c'):
        data = { 'call_sign': row['call_sign'], 'sys_id': row['sys_id'], 'entity_type': row['entity_type'], 'entity_name': row['entity_name'] }
        yield(data)
      else:
        next
    except:
      logger.error("Error parsing this data: " + str(row) + "\n")
      raise
  logger.info("Finished parsing %s for file %s" % (file_name, zip_file))

def parse_fr(zip_file):
  file_name = 'FR.dat'
  fields = ('freq_action', 'loc_num', 'ant_num', 'class_stn_code', 'op_alt_code', 'freq_assigned',
    'fr_up_band', 'fr_carrier', 'op_begin_time', 'op_end_time', 'power_output', 'power_erp', 'tolerance', 'fr_ind',
    'status', 'eirp', 'xmitter_make', 'xmitter_model', 'xmitter_auto_power', 'num_units', 'num_pagers', 'freq_num')

  logger.info("Parsing %s for file %s" % (file_name, zip_file))
  for row in get_rows(zip_file, file_name, fields):
    try:
      fr = row.get('freq_assigned') 
      sys_id = row.get('sys_id', False)

      if sys_id and fr and len(fr.strip()) > 0:
        data = { 'call_sign': row['call_sign'], 'sys_id': row['sys_id'], 'loc_num': row['loc_num'], 'ant_num': row['ant_num'],
          'class_stn_code': row['class_stn_code'], 'freq_assigned': fr, 'freq_num': row['freq_num'] }
        yield(data)
      else:
        next
    except:
      logger.error("Error parsing this data: " + str(row) + "\n")
      raise
  logger.info("Finished parsing %s for file %s" % (file_name, zip_file))

def parse_hd(zip_file):
  file_name = 'HD.dat'
  fields = ('license_status', 'radio_svc_code')

  logger.info("Parsing %s for file %s" % (file_name, zip_file))
  for row in get_rows(zip_file, file_name, fields):
    try:
      # Really only interested in license_status 'A' (active) records
      status = row.get('license_status', 'unk')
      sys_id = row.get('sys_id', False)

      if sys_id and status and status.casefold() == 'a':
        data = { 'call_sign': row['call_sign'], 'sys_id': row['sys_id'], 'radio_svc_code': row['radio_svc_code'] }
        yield(data)
      else:
        next
    except:
      logger.error("Error parsing this data: " + str(row) + "\n")
      raise
  logger.info("Finished parsing %s for file %s" % (file_name, zip_file))

def parse_lo(zip_file):
  file_name = 'LO.dat'
  fields = ('loc_action', 'loc_type_code', 'loc_class_code', 'loc_num', 'site_status',
    'fixed_loc', 'loc_addr', 'loc_city', 'loc_co', 'loc_st', 'op_radius', 'area_op_code', 'clearance_ind',
    'grnd_elev', 'lat_deg', 'lat_min', 'lat_sec', 'lat_dir', 'lon_deg', 'lon_min', 'lon_sec', 'lon_dir',
    'max_lat_deg', 'max_lat_min', 'max_lat_sec', 'max_lat_dir', 'max_lon_deg', 'max_lon_min', 'max_lon_sec',
    'max_lon_dir', 'nepa', 'quiet_zone_notify_date', 'tower_reg_num', 'height_of_support', 'height_of_structure',
    'structure_type', 'airport_id', 'loc_name')

  logger.info("Parsing %s for file %s" % (file_name, zip_file))
  for row in get_rows(zip_file, file_name, fields):
    try:
      # assume that if lat/lon degree fields are empty then there's nothing of value in the record to process
      lat_deg = row.get('lat_deg')
      lon_deg = row.get('lon_deg')
      sys_id  = row.get('sys_id', False)
        
      if sys_id and lat_deg and lon_deg and len(lat_deg.strip()) > 0 and len(lon_deg.strip()) > 0:
        # Assume N, W for lat, lon if value is empty
        lat_dir = row.get('lat_dir')
        lon_dir = row.get('lon_dir')

        if not lat_dir or len(lat_dir.strip()) == 0:
          lat_dir = 'N'

        if not lon_dir or len(lon_dir.strip()) == 0:
          lon_dir = 'W'

        data = { 'call_sign': row['call_sign'], 'sys_id': row['sys_id'], 'loc_num': row['loc_num'],
          'lat_dec': dms_to_dec(row['lat_deg'], row['lat_min'], row['lat_sec'], lat_dir),
          'lon_dec': dms_to_dec(row['lon_deg'], row['lon_min'], row['lon_sec'], lon_dir),
          'loc_name': row['loc_name'], 'loc_city': row['loc_city'], 'loc_co': row['loc_co'], 'loc_st': row['loc_st'] }
        yield(data)
      else:
        next
    except:
      logger.error("Error parsing this data: " + str(row) + "\n")
      raise
  logger.info("Finished parsing %s for file %s" % (file_name, zip_file))
