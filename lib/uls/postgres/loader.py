import psycopg2
import logging

class Loader():

  logger = None

  def __init__(self, db, create=True):
    self.db = 'uls_template'
    self.logger = logging.getLogger('uls').getChild('postgres').getChild(__name__)

    sql = 'create database %s with template %s' % (db, self.db)
    if create:
      self.executecmd(sql)

    self.db = db

  def _connect(self):
    return psycopg2.connect(database=self.db)

  def executecmd(self, sql):
    c = None
    try:
      with self._connect() as c:
        c.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        with c.cursor() as cur:
          self.logger.info("Executing DB command '" + sql + "'")
          cur.execute(sql)
    finally:
      if c != None:
        c.close()

  def _insert(self, sql, iter):
    c = None
    try:
      with self._connect() as c:
        c.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

        with c.cursor() as cur:
          cur.executemany(sql, iter)
    finally:
      if c != None:
        c.close()

  def insert_en_data(self, value_iter):
    sql = ("insert into en (call_sign, sys_id, entity_type, entity_name) "
      "values (upper(%(call_sign)s), %(sys_id)s, upper(%(entity_type)s), %(entity_name)s)")
    self._insert(sql, value_iter)

  def insert_fr_data(self, value_iter):
    sql = ("insert into fr (call_sign, sys_id, loc_num, ant_num, class_stn_code, freq_assigned, freq_num) "
      "values (upper(%(call_sign)s), %(sys_id)s, COALESCE(NULLIF(%(loc_num)s, '')::int, 1), COALESCE(NULLIF(%(ant_num)s, '')::int, 1), "
      "upper(%(class_stn_code)s), %(freq_assigned)s, COALESCE(NULLIF(%(freq_num)s, '')::int, 1))")
    self._insert(sql, value_iter)

  def insert_hd_data(self, value_iter):
    sql = "insert into hd (call_sign, sys_id, radio_svc_code) values (upper(%(call_sign)s), %(sys_id)s, upper(%(radio_svc_code)s))"
    self._insert(sql, value_iter)

  def insert_lo_data(self, value_iter):
    # Postgres < 9.5 doesn't have the conflict resolution clauses like sqlite does (thankfully we grabbed 9.6 from testing)
    sql = ("insert into lo (call_sign, sys_id, loc_num, coord, loc_name, loc_city, loc_co, loc_st) "
      "values (upper(%(call_sign)s), %(sys_id)s, COALESCE(NULLIF(%(loc_num)s, '')::int, 1), point(%(lat_dec)s, %(lon_dec)s), "
      "%(loc_name)s, %(loc_city)s, %(loc_co)s, upper(%(loc_st)s)) "
      "ON CONFLICT ON CONSTRAINT lo_pkey DO UPDATE set coord = EXCLUDED.coord, loc_name = EXCLUDED.loc_name, "
      "loc_co = EXCLUDED.loc_co, loc_st = EXCLUDED.loc_st")

    self._insert(sql, value_iter)

  def add_indexes(self):
    idx_cmds = (
      "CREATE INDEX IF NOT EXISTS i_en_call_sign on en (call_sign)",
      "CREATE INDEX IF NOT EXISTS i_fr_call_sign on fr (call_sign)",
      "CREATE INDEX IF NOT EXISTS i_hd_call_sign on hd (call_sign)",
      "CREATE INDEX IF NOT EXISTS i_lo_call_sign on lo (call_sign)",
      "CREATE INDEX IF NOT EXISTS i_lo_coord_gist on lo using gist (coord)"
    )

    for c in idx_cmds:
      self.executecmd(c)

  def refresh_materialized_views(self):
    cmds = (
      "REFRESH MATERIALIZED VIEW active_rsc",
      "REFRESH MATERIALIZED VIEW state_counties"
    )

    for c in cmds:
      self.executecmd(c)

  def vacuum(self):
    self.executecmd('VACUUM ANALYZE')
