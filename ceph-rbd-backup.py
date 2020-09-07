#!/usr/bin/env python

import json
import subprocess
import datetime
import logging
import sys
import os
import ConfigParser
import requests



## Ceph credentials
ceph_conf_prod = '/etc/ceph/ceph.conf'
ceph_conf_backup = '/etc/ceph-backup/ceph.conf'
ceph_keyring_prod = '/etc/ceph/ceph.client.admin.keyring'
ceph_keyring_backup = '/etc/ceph-backup/ceph.client.admin.keyring'
ceph_username_prod = 'admin'
ceph_username_backup = 'admin'
ceph_snapshots_mount_path = '/mnt/ceph-snapshots'

rbd_backup_conf = '/etc/ceph/ceph-rbd-backup.conf'
ceph_prod_pool = 'SSD'
ceph_backup_pool = 'backup'

replication_lockfile_pattern = '/tmp/ceph-rbd-back-%s.lock'

def notify(msg):
    url = 'https://api.telegram.org/bot1109329413:AAFIvUc8kCoT4da1ATnE7ycQ-TZv2BQQCpQ/sendMessage'
    body = {'chat_id':'-343794401', 'text': msg}
    requests.post(url, body)

class Rbd:
  def __init__(self, config, keyring, username, pool, noop=False):
    self.config = config
    self.keyring = keyring
    self.username = username
    self.noop = noop
    self.pool = pool

  def _rbd_base_cmd(self, json=True):
    return ['rbd', '-c', self.config, '--keyring', self.keyring, '--id', self.username, '-p', self.pool] + (json and ['--format', 'json'] or [])


  def _rbd_exec_plain(self, *args):
    rbd_cmd = self._rbd_base_cmd(json=False) + list(args)
    logging.debug("_rbd_exec_plain cmd: " + repr(rbd_cmd))
    rbd_sp = subprocess.Popen(rbd_cmd, stdout=subprocess.PIPE)
    out, err = rbd_sp.communicate()
    logging.debug("_rbd_exec_plain stdout: " + out)
    return out.strip()

  def _rbd_exec_simple(self, *args):
    rbd_cmd = self._rbd_base_cmd() + list(args)
    logging.debug("_rbd_exec_simple cmd: " + repr(rbd_cmd))
    rbd_sp = subprocess.Popen(rbd_cmd, stdout=subprocess.PIPE)
    out, err = rbd_sp.communicate()
    logging.debug("_rbd_exec_simple stdout: " + out)
    result = json.loads(out)
    return result

  def _rbd_exec_noout(self, *args):
    rbd_cmd = self._rbd_base_cmd(json=False) + list(args)
    logging.debug("_rbd_exec_noout cmd: " + repr(rbd_cmd))
    if self.noop:
      logging.info("_rbd_exec_noout noop! (%s)" %(' '.join(rbd_cmd)))
      return None
    rbd_sp = subprocess.Popen(rbd_cmd)
    out, err = rbd_sp.communicate()

  def _rbd_exec_pipe_source(self, *args):
    rbd_cmd = self._rbd_base_cmd(json=False) + list(args)
    logging.debug("_rbd_exec_pipe_source cmd: " + repr(rbd_cmd))
    if self.noop:
      logging.info("_rbd_exec_pipe_source noop! (%s)" %(' '.join(rbd_cmd)))
      return None
    rbd_sp = subprocess.Popen(rbd_cmd, stdout=subprocess.PIPE)
    return rbd_sp.stdout

  def _rbd_exec_pipe_dest(self, source_pipe, *args):
    rbd_cmd = self._rbd_base_cmd(json=False) + list(args)
    logging.debug("_rbd_exec_pipe_dest cmd: " + repr(rbd_cmd))
    if self.noop:
      logging.info("_rbd_exec_pipe_dest noop! (%s)" %(' '.join(rbd_cmd)))
      return None
    rbd_sp = subprocess.Popen(rbd_cmd, stdin=source_pipe, stdout=subprocess.PIPE)
    source_pipe.close() # ?
    out, err = rbd_sp.communicate()
    logging.debug(out)

  def list(self):
    return self._rbd_exec_simple('list')

  def create(self, image_name, size):
    self._rbd_exec_noout('create', image_name, '--size', str(size))

  def snap_list(self, image_name):
    return self._rbd_exec_simple('snap', 'list', image_name)

  def snap_purge(self, image_name):
    return self._rbd_exec_noout('snap', 'purge', image_name)

  def info(self, image_name):
    return self._rbd_exec_simple('info', image_name)

  def remove(self, image_name):
    return self._rbd_exec_noout('rm', image_name)

  def snap_list_names(self, image_name):
    return [snap['name'] for snap in self.snap_list(image_name)]

  def snap_create(self, image_name, snap_name):
    self._rbd_exec_noout('snap', 'create', image_name, '--snap', snap_name)

  def snap_rm(self, image_name, snap_name):
    self._rbd_exec_noout('snap', 'rm', image_name, '--snap', snap_name)

  def export_diff(self, image_name, snap_name, from_snap_name=None):
    from_snap_args = from_snap_name and ['--from-snap', from_snap_name] or []
    return self._rbd_exec_pipe_source('export-diff', image_name, '-', '--snap', snap_name, *from_snap_args)

  def import_diff(self, image_name, source_pipe):
    return self._rbd_exec_pipe_dest(source_pipe, 'import-diff', '-', image_name)

  def showmapped(self):
    return self._rbd_exec_simple('showmapped').values()
      
  def map(self, image_name, snap_name):
    return self._rbd_exec_plain('map', image_name, '--snap', snap_name, '-o', 'ro')

  def unmap(self, device):
    self._rbd_exec_noout('unmap', device)


class Volume:
  def __init__(self, image, device):
    self.image = image
    self.device = device
    self.mountpoint = self._get_mountpoint()
    self.frozen = False

  def __del__(self):
    # ensure we don't leave frozen filesystems behind
    if self.frozen:
      sp_unfreeze = subprocess.Popen(['fsfreeze', '--unfreeze', self.mountpoint])
      sp_unfreeze.communicate()

  def _vol_exec_raw(self, *args):
    logging.debug("_vol_exec_raw cmd: " + repr(args))
    sp_vol = subprocess.Popen(args, stdout=subprocess.PIPE)
    out, err = sp_vol.communicate()
    logging.debug("_vol_exec_raw out: " + out)
    return out

  def _get_mountpoint(self, first_only=True):
    ret = self._vol_exec_raw('findmnt', '-o', 'TARGET', '-n', self.device)
    mpts = ret.strip().split("\n")
    if first_only:
      return mpts and mpts[0] or None
    else:
      return mpts

  def mounted(self):
    return self.mountpoint is not None

  def freeze(self):
    if not self.mounted():
      raise VolumeException("Cannot freeze not mounted volume '%s'" %(self.device))
    self.frozen = True
    self._vol_exec_raw('fsfreeze', '--freeze', self.mountpoint)

  def unfreeze(self):
    if not self.mounted():
      raise VolumeException("Cannot unfreeze not mounted volume '%s'" %(self.device))
    self._vol_exec_raw('fsfreeze', '--unfreeze', self.mountpoint)
    self.frozen = False


class Mount:
  def __init__(self, path, device=None, options=[]):
    self.path = path
    self.device = device
    self.options = options

  def _mount_exec_noout(self, *args):
    logging.debug("_mount_exec_raw cmd: " + repr(args))
    sp_mount = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = sp_mount.communicate()
    logging.debug("_mount_exec_raw out: " + out)
    logging.debug("_mount_exec_raw err: " + err)

  def mkdirs(self):
    if not os.path.exists(self.path):
      os.makedirs(self.path)
    return self

  def rmdir(self):
    if os.path.isdir(self.path):
      os.rmdir(self.path)
    return self

  def mount(self):
    if self.device is None:
      raise MountException('Mount.mount() requires a device')
    self.mkdirs()
    self._mount_exec_noout('mount', self.device, self.path)
    return self

  def umount(self):
    self._mount_exec_noout('umount', self.path)
    return self


if __name__=="__main__":

  import argparse
  parser = argparse.ArgumentParser(description='Ceph backup / replication tool')
  parser.add_argument('action', help='action to perform', choices=['replicate', 'snapshot', 'expire', 'mount', 'check', 'clean'])
  parser.add_argument('--image', help='single image to process instead of all')
  parser.add_argument('--debug', help='enable debug logging', action='store_true')
  parser.add_argument('--noop', help='don\'t do any action, only log', action='store_true')
  args = parser.parse_args()

  level = args.debug and logging.DEBUG or logging.INFO
  logging.basicConfig(format='%(levelname)s %(message)s', level=level)

  config = ConfigParser.RawConfigParser()
  if os.path.exists(rbd_backup_conf):
    logging.debug("Reading configuration file")
    config.read(rbd_backup_conf)
  else:
    logging.debug("No configuration found")

  ceph_prod = Rbd(ceph_conf_prod, ceph_keyring_prod, ceph_username_prod, ceph_prod_pool, args.noop)
  ceph_backup = Rbd(ceph_conf_backup, ceph_keyring_backup, ceph_username_backup, ceph_backup_pool, args.noop)

  if args.action == "snapshot":
    volumes = ceph_prod.list()
    logging.info("Starting snapshot of all mounted volumes")
    errors = False

    notify("Starting snapshot all volumes in the CEPH Cluster. Number of volumes: {}".format(len(volumes)))
    for volume in volumes:
      if args.image and volume != args.image: continue
      logging.info("Creating snapshot for volume '%s'" %(volume))
      today = datetime.date.today().strftime("%Y-%m-%d")
      if today in ceph_prod.snap_list_names(volume):
        logging.error("Image '%s' already has snapshot '%s' - continuing with next image" %(volume, today))
        continue
      try:
        #if volume.mounted():
        #  volume.freeze()
        ceph_prod.snap_create(volume, today)
        #if volume.mounted():
        #  volume.unfreeze()
      except VolumeException, e:
        logging.error(e.message + " - continuing with next volume")
        continue
    logging.info("Finished snapshot of all mounted volumes" + (errors and " (with errors)" or ""))
    notify("Finished snapshot of all volumes today")

  elif args.action == "replicate":
    logging.info("Starting replication of images to destination")
    notify("Starting replicate all volumes to CEPH Backup Cluster")
    for image in ceph_prod.list():
      if args.image and image != args.image: continue
      logging.info("Replicating image '%s'" %(image))
      if os.path.exists(replication_lockfile_pattern %(image)):
        logging.info("Lock file found, skipping replication")
        continue
      open(replication_lockfile_pattern %(image),'w')
      if image not in ceph_backup.list():
        logging.info("Creating new image '%s' on destination" %(image))
        ceph_backup.create(image, 1)
      try:
        latest_bk_snap = ceph_backup.snap_list_names(image)[-1]
      except IndexError:
        latest_bk_snap = None
      try:
        latest_prd_snap = ceph_prod.snap_list_names(image)[-1]
      except IndexError:
        logging.error("No snapshots on prod for image '%s' - continuing with next image" %(image))
        os.unlink(replication_lockfile_pattern %(image))
        continue
      if latest_bk_snap == latest_prd_snap:
        logging.error("Latest snapshot '%s' for image '%s' already present on backup - skipping" %(latest_prd_snap, image))
        os.unlink(replication_lockfile_pattern %(image))
        continue
      elif latest_bk_snap not in ceph_prod.snap_list_names(image):
        logging.info("Latest backup snapshot '%s' for image '%s' missing on prod, doing full replication" %(latest_bk_snap, image))
        ceph_backup.import_diff(image, ceph_prod.export_diff(image, latest_prd_snap) )
      else:
        logging.info("Doing diff replication for image '%s'" %(image))
        ceph_backup.import_diff(image, ceph_prod.export_diff(image, latest_prd_snap, latest_bk_snap) )
      os.unlink(replication_lockfile_pattern %(image))
    notify("Finish replicate all volumes to CEPH Backup Cluster") 

  elif args.action == "check":
    errors = []
    for image in ceph_prod.list():
      if args.image and image != args.image: continue
      if image not in ceph_backup.list():
        errors.append("%s: missing image on backup cluster" %(image))
        continue
      try:
        latest_bk_snap = ceph_backup.snap_list_names(image)[-1]
      except IndexError:
        latest_bk_snap = None
      latest_prd_snaps = ceph_prod.snap_list_names(image)[-2:]
      if latest_bk_snap not in latest_prd_snaps:
        errors.append("%s: latest backup snapshot %s not up-to-date with production %s" %(image, latest_bk_snap, latest_prd_snaps[-1]))
    if not errors:
      descr = args.image and "Backup for image %s"%(args.image) or "All backups"
      print "BACKUP OK - %s OK" %(descr)
      sys.exit(0)
    elif len(errors) == 1:
      print "BACKUP ERROR - %s" %(errors[0])
      sys.exit(2)
    else:
      print "BACKUP ERROR - %d errors\n%s" %(len(errors), "\n".join(errors))
      sys.exit(2)

  elif args.action == "expire":
    notify("Starting remove old snapshot in SSD Cluster")
    SSD_volume_list = ceph_prod.list()
    backup_volume_list = ceph_backup.list()
    for image in SSD_volume_list:
      if args.image and image != args.image: continue
      try:
        prod_oldest_to_keep = (datetime.date.today() - datetime.timedelta(days=config.getint('backup', 'prod_retention'))).strftime("%Y-%m-%d")
        prod_to_delete = [snap for snap in ceph_prod.snap_list_names(image) if snap < prod_oldest_to_keep]
      except ConfigParser.NoSectionError, ConfigParser.NoOptionError:
        logging.warn("No retention configured for image '%s' on prod - not removing snapshots" %(image))
        prod_to_delete = []
      for snap in prod_to_delete:
        logging.info("Deleting expired snapshot '%s' of image '%s' on prod" %(snap, image))
        ceph_prod.snap_rm(image, snap)
      if image not in backup_volume_list:
        logging.warn("Missing image '%s' on backup cluster" %(image))
        continue
      try:
        backup_oldest_to_keep = (datetime.date.today() - datetime.timedelta(days=config.getint('backup', 'backup_retention'))).strftime("%Y-%m-%d")
        backup_to_delete = [snap for snap in ceph_backup.snap_list_names(image) if snap < backup_oldest_to_keep]
      except ConfigParser.NoSectionError, ConfigParser.NoOptionError:
        logging.info("No retention configured for image '%s' on backup - not removing snapshots" %(image))
        backup_to_delete = []
      for snap in backup_to_delete:
        logging.info("Deleting expired snapshot '%s' of image '%s' on backup" %(snap, image))
        ceph_backup.snap_rm(image, snap)
   
   
    notify("Finished remove old snapshot in SSD Cluster")
## Do delete image in backup cluster if that image is not existed in SSD cluster for 7 days
  # get list deleted image
  # Get the latest snapshot
  # if the lastest snapshot = now - 7 days
  elif args.action == "clean":
    notify("Starting clean deleted volumes (in 7days) in Backup Cluster")
    SSD_volume_list = ceph_prod.list()
    backup_volume_list = ceph_backup.list()
    deleted_volumes = []
    for backup_vol in backup_volume_list:
        if backup_vol not in SSD_volume_list and 'backup' not in backup_vol:
            deleted_volumes.append(backup_vol)
    notify("Found {} deleted volumes".format(len(deleted_volumes)))
    need_clean_volumes = []
    for vol in deleted_volumes:
        print(vol)
        snaps = ceph_backup.snap_list_names(vol)
        print(snaps)
	if snaps:
            try:
	        delta = datetime.datetime.now() - datetime.datetime.strptime(snaps[-1], '%Y-%m-%d')
	        if delta.days > 7:
                    need_clean_volumes.append(vol)
            except:
                # Truong hop backup manual
                # bo qua truong hop nay
                continue
        else:
            created_date = ceph_backup.info(vol)['create_timestamp']
            delta = datetime.datetime.now() - datetime.datetime.strptime(created_date, '%a %b %d %H:%M:%S %Y')
            if delta.days > 7:
                need_clean_volumes.append(vol)
    notify("Clean volume: {} in backup cluster".format(need_clean_volumes))
    for vol in need_clean_volumes:
        ceph_backup.snap_purge(vol)
        ceph_backup.remove(vol)
    notify("Finished clean volume in backup cluster")


  elif args.action == "mount":
    for image in ceph_prod.list():
      if args.image and image != args.image: continue
      if not config.has_option('image', 'mount_snapshots'): continue
      logging.info("Checking snapshots mounts for image '%s'" %(image))
      oldest_to_mount = (datetime.date.today() - datetime.timedelta(days=config.getint('image', 'mount_snapshots'))).strftime("%Y-%m-%d")
      # cleanup old mounted + dir
      snaps_dirs = os.listdir(os.path.join(ceph_snapshots_mount_path, image))
      print snaps_dirs
      for snap in snaps_dirs:
        if snap < oldest_to_mount:
          logging.info("Unmounting + rmdir snapshot '%s' of image '%s'" %(snap, image))
          Mount(os.path.join(ceph_snapshots_mount_path, image, snap)).umount().rmdir()
      # cleanup old mapped
      mapped = dict([(m['snap'], m['device']) for m in ceph_prod.showmapped() if m['name'] == image and m['snap'] != '-'])
      for snap in mapped:
        if snap < oldest_to_mount:
          logging.info("Unmapping snapshot '%s' of image '%s'" %(snap, image))
          ceph_prod.unmap(mapped[snap])
      # mount new snapshots
      for i in range(config.getint(image, 'mount_snapshots'), 0, -1):
        snap = (datetime.date.today() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        snap_dir = os.path.join(ceph_snapshots_mount_path, image, snap)
        if snap not in mapped:
          logging.info("Mounting snapshot '%s' of image '%s'" %(snap, image))
          device = ceph_prod.map(image, snap)
          Mount(snap_dir, device).mount()
