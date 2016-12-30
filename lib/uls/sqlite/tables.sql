CREATE TABLE 'en' (
  'call_sign' TEXT,
  'sys_id' TEXT,
  'entity_type' TEXT,
  'entity_name' TEXT,
  PRIMARY KEY (call_sign, sys_id, entity_type)
);

-- Since we'd need to use all fields in the table to form a PK,
-- it's a bit pointless. Once we design queries we'll add appropiate
-- indexes (call_sign and loc_num are join fields, sys_id?)
CREATE TABLE 'fr' (
  'call_sign' TEXT,
  'sys_id' TEXT,
  'loc_num' INTEGER,
  'ant_num' INTEGER,
  'class_stn_code' TEXT,
  'freq_assigned' NUMERIC,
  'freq_num' INTEGER
);
CREATE INDEX i_fr_call_sign on fr(call_sign);

CREATE TABLE 'hd' (
  'call_sign' TEXT,
  'sys_id' TEXT,
  'radio_svc_code' TEXT,
  PRIMARY KEY (call_sign, sys_id)
);

CREATE TABLE 'lo' (
  'call_sign' TEXT,
  'sys_id' TEXT,
  'loc_num' INTEGER,
  'lat_dec' NUMERIC,
  'lon_dec' NUMERIC,
  'loc_name' TEXT,
  'loc_city' TEXT,
  'loc_co' TEXT,
  'loc_st' TEXT,
  PRIMARY KEY (call_sign, sys_id, loc_num)
);
