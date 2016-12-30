CREATE TABLE en (
  call_sign varchar(20),
  sys_id numeric(9,0) not null,
  entity_type varchar(5),
  entity_name varchar(255),
  PRIMARY KEY (call_sign, sys_id, entity_type)
);

-- Since we'd need to use all fields in the table to form a PK having one is a bit pointless.
-- However, having an index on call_sign will be needed for the extract queries
CREATE TABLE fr (
  call_sign varchar(20),
  sys_id numeric(9,0) not null,
  loc_num INTEGER DEFAULT 1,
  ant_num INTEGER DEFAULT 1,
  class_stn_code varchar(10),
  freq_assigned numeric(16,8),
  freq_num INTEGER DEFAULT 1
);

CREATE TABLE hd (
  call_sign varchar(20),
  sys_id numeric(9,0) not null,
  radio_svc_code varchar(5),
  PRIMARY KEY (call_sign, sys_id)
);

CREATE TABLE lo (
  call_sign varchar(20),
  sys_id numeric(9,0) not null,
  loc_num INTEGER DEFAULT 1,
  coord POINT,
  loc_name varchar(255),
  loc_city varchar(255),
  loc_co varchar(255),
  loc_st varchar(10),
  PRIMARY KEY (call_sign, sys_id, loc_num)
);

CREATE VIEW active_en AS
  select en.call_sign,en.sys_id,en.entity_type,en.entity_name,hd.radio_svc_code from hd join en using(call_sign);

CREATE VIEW active_lo AS
  select lo.call_sign,lo.sys_id,lo.loc_num,lo.coord,lo.loc_name,lo.loc_city,lo.loc_co,lo.loc_st from hd join lo using(call_sign);

CREATE VIEW active_fr AS
  select fr.call_sign,fr.sys_id,fr.loc_num,fr.ant_num,fr.class_stn_code,fr.freq_assigned,fr.freq_num from hd join fr using(call_sign);
