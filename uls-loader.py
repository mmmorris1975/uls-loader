#!/usr/bin/env python3

import time
import logging
import argparse
import multiprocessing as mp

import uls.parser as parser
import uls.transfer as ftp

from uls.postgres import Loader

def process_file(f, p):
  try:
    p['db_method'](p['parser'](f))
  except:
    logging.exception("Error processing data from file " + f)
    raise

def parse_cmdline():
  default_files = ('l_coast.zip', 'l_LMbcast.zip', 'l_LMcomm.zip', 'l_LMpriv.zip', 'l_mdsitfs.zip', 'l_micro.zip')
  default_db_name = 'uls_' + time.strftime('%Y%m%d')

  parser = argparse.ArgumentParser("Download FCC ULS license data and load to local database")
  parser.add_argument('-l', '--loglevel', choices = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default = 'INFO', help = 'log level')
  parser.add_argument('--no-create-db', default = False, action = 'store_true', help = 'do not create database, assume it already exists')
  parser.add_argument('--no-download', default = False, action = 'store_true', help = 'do not download files, use existing files')
  parser.add_argument('-f', '--files', action = 'append', help = 'specific zip file(s) to process')
  parser.add_argument('-d', '--db', default = default_db_name, help = 'database name')

  args = parser.parse_args()

  if not args.files or len(args.files) < 1:
    args.files = default_files

  return args

def configure_logging(log_level):
  log_format  = '%(asctime)s %(levelname)s %(message)s'
  date_format = '%Y-%m-%d %H:%M:%S %Z'
  logging.basicConfig(level = log_level, format = log_format, datefmt = date_format)

def fill_q(files, q):
  for f in files:
    q.put(f)

if __name__ == '__main__':
  # About a 6 hour run-time start to finish. The 45 minutes to download be what it be,
  # since we're constrained by our internet pipe. However, it takes about an hour to process
  # the l_micro data, and 3 hours to process the l_LMpriv data. I wonder if we switch from
  # inserts to 'COPY FROM STDIN' if we'll see some speed ups?
  #
  # It takes about 20 minutes to create our indexes, and another 5-10 to do the vacuum, neither
  # are avoidable, and we're constrained by the resources of the system we run on.
  #
  # After further review, the 'COPY FROM' idea won't work, since we're letting the hash data
  # determine our insert fields, and also doing some processing of the data in the INSERT
  # statements (upper-casing and coalescing values). Seeing as how we'd have to re-order the
  # data, and do some of the pre-processing ourselves, we probably chew up a fair bit of any
  # speed gains we'd get from 'COPY FROM'

  opts = parse_cmdline()
  configure_logging(opts.loglevel)

  q  = mp.Queue()
  db = Loader(opts.db, not opts.no_create_db)

  parsers = [
    { 'parser': parser.parse_en, 'db_method': db.insert_en_data },
    { 'parser': parser.parse_fr, 'db_method': db.insert_fr_data },
    { 'parser': parser.parse_hd, 'db_method': db.insert_hd_data },
    { 'parser': parser.parse_lo, 'db_method': db.insert_lo_data },
  ]

  if opts.no_download:
    p = mp.Process(target=fill_q, args=(opts.files, q))
  else:
    p = mp.Process(target=ftp.transfer_all, args=(opts.files, q))

  p.start()

  finished = 0
  while finished < len(opts.files):
    jobs = []
    file = q.get()
    logging.debug("Received message from queue: " + file)
    finished += 1

    for r in parsers:
      proc = mp.Process(target=process_file, args=(file, r))
      proc.start()
      jobs.append(proc)

    while len(jobs) > 0:
      for j in jobs:
        if j.is_alive():
          j.join(10)
        else:
          jobs.remove(j)

  p.join()

  db.add_indexes()
  db.refresh_materialized_views()
  db.vacuum()
  logging.info("Finished loading all ULS data")

# These are legit dupe records - should be able to handle this, but will need to re-examine use of executemany() for
# inserts since it puts us in an all or nothing situation...either all rows get committed, or they all get rolled back
# LMpriv LO sys_id=1941967 - Some business communication provider in FL (450-466MHz)
# micro  LO sys_id=1955221 - State of WA DNR (6GHz)
#
# Updated insert_lo_data function to use 'insert or replace' to work around this 
# ^^^ Not available in Postgres until version 9.5 (good thing we grabbed 9.6 from testing repo)
