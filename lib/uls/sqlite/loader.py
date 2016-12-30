import os, sqlite3

class Loader():

  def __init__(self, db):
    self.db = db

    try:
      with open(os.path.dirname(__file__) + os.sep + 'tables.sql', 'r') as fd:
        sql = fd.read()

      with self._connect() as c:
        c.executescript(sql)
    except:
      print("ERROR!")
      raise
    finally:
      if c:
        c.close()

  def _connect(self):
    return sqlite3.connect(self.db)

  def _insert(self, sql, iter):
    try:
      with self._connect() as c:
        c.executemany(sql, iter)
    except:
      print("SQLITE ERROR!")
      raise
    finally:
      if c:
        c.close()

  def insert_en_data(self, value_iter):
    sql = "insert into en (upper(call_sign), sys_id, upper(entity_type), entity_name) values (:call_sign, :sys_id, :entity_type, :entity_name)"
    self._insert(sql, value_iter)

  def insert_fr_data(self, value_iter):
    sql = ("insert into fr (upper(call_sign), sys_id, loc_num, ant_num, upper(class_stn_code), freq_assigned, freq_num) "
      "values (:call_sign, :sys_id, :loc_num, :ant_num, :class_stn_code, :freq_assigned, :freq_num)")
    self._insert(sql, value_iter)

  def insert_hd_data(self, value_iter):
    sql = "insert into hd (upper(call_sign), sys_id, upper(radio_svc_code)) values (:call_sign, :sys_id, :radio_svc_code)"
    self._insert(sql, value_iter)

  def insert_lo_data(self, value_iter):
    # Use 'insert or replace' to get around some legitimate duplicate data in the source files. It's not a perfect solution
    # since it may cause some inconsistency if the PK fields are the same, but have different location data, and we cause
    # the row to be overwritten. However, that is stemming from an issue in the source data which we can't control
    sql = ("insert or replace into lo (upper(call_sign), sys_id, loc_num, lat_dec, lon_dec, loc_name, loc_city, loc_co, upper(loc_st)) "
      "values (:call_sign, :sys_id, :loc_num, :lat_dec, :lon_dec, :loc_name, :loc_city, :loc_co, :loc_st)")
    self._insert(sql, value_iter)
