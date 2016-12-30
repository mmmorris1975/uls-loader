import os, time, logging
import ftplib, socket
import multiprocessing

uls_host  = 'wireless.fcc.gov'
base_path = '/pub/uls/complete'

logger = logging.getLogger('uls').getChild(__name__)

def transfer_file(file_name, retries = 5, sleep_t = 2):
  with open(file_name, 'ab') as f:
    size = os.stat(f.fileno()).st_size

    with ftplib.FTP(host=uls_host, timeout=10) as ftp:
      ftp.login()
      ftp.cwd(base_path)

      try:
        ftp.retrbinary('RETR ' + file_name, f.write, rest = size)
      except (ftplib.error_temp, socket.timeout) as e:
        # Sometimes near the end of the transfer things will block waiting for the last
        # bits of data, setting the timeout in the ftp context helps, and this will do
        # a few retries in order to get those last bits of data, and retry on temporary
        # errors during the transfer
        logger.warn("FTP ERROR for file " + file_name + ", retrying")
        if retries > 0:
          f.flush()
          time.sleep(sleep_t)
          transfer_file(file_name, retries - 1, sleep_t * 2)
        else:
          logger.error("FTP ERROR retrieving file " + file_name + ", no more retries remaining")
          raise

  return file_name

def transfer_all(file_list, q):
  procs = []
  with multiprocessing.Pool() as p:
    for f in file_list:
      logger.info("Starting transfer for " + f)
      r = p.apply_async(transfer_file, (f,))
      procs.append(r)

    while len(procs) > 0:
      for p in procs:
        try:
          res = p.get(2)
          procs.remove(p)
          q.put(res)
          logger.info("Transfer of " + res + " complete")
        except multiprocessing.context.TimeoutError:
          logger.debug("Timeout waiting for proc")

  logger.debug("Exiting transfer_all")
  q.close()
