import datetime
import math
import logging
import os
import stat
import pandas as pd
import paramiko
import sqlite3
import yaml
from itertools import chain
from multiprocessing import Process, Queue
from paramiko.client import SSHClient
from tqdm import tqdm


def list_sftp_epns(host, user, key_path, data_path):
    with SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, username=user, key_filename=key_path)

        with ssh.open_sftp() as sftp:
            sftp.chdir(data_path)
            return sftp.listdir()


def is_dir(sftp_attr):
    return stat.S_ISDIR(sftp_attr.st_mode)


def list_all_files(sftp_client, path):
    sftp_client.chdir(path)
    names = sftp_client.listdir()
    attrs = sftp_client.listdir_attr()

    for name, attr in zip(names, attrs):
        if is_dir(attr):
            for x in list_all_files(sftp_client, os.path.join(path, name)):
                yield x
        else:
            yield os.path.join(path, name)


def create_file(sftp, epn, path):
    try:
        stats = sftp.lstat(path)
        size = stats.st_size
        mtime = datetime.datetime.fromtimestamp(stats.st_mtime)
    except FileNotFoundError as fno:
        logging.error("Could not calculate stats on: {}\n{}".format(path, fno))
        size = 0
        mtime = datetime.datetime.now()
    return epn, path, size, mtime


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


class Worker(object):
    def __init__(self, name, queue, process, db):
        self.name = name
        self.queue = queue
        self.process = process
        self.db = db


def epn_worker(i, queue, db_path, epns, hostname, user, key_path):
    logging.basicConfig(filename="worker{}_errors.log".format(1), level=logging.ERROR)

    with SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=hostname, username=user, key_filename=key_path)

        with ssh.open_sftp() as sftp:
            conn = sqlite3.connect(db_path)
            conn.execute('''create table files
                            (epn text,
                             path text unique,
                             size bigint,
                             modified datetime)''')
            for i, epn in enumerate(epns):
                files = (create_file(sftp, epn, pth) for pth in
                         list_all_files(sftp, os.path.join('/data', epn)))
                with conn:
                    conn.executemany("insert into files(epn, path, size, modified) values (?, ?, ?, ?)", files)

                queue.put(i + 1)

            conn.close()


def process_epns(epns, hostname, user, key_path, processes=4):
    count = 0

    workers = []
    with tqdm(total=len(epns)) as pbar:
        for i, eps in enumerate(chunks(epns, int(math.ceil(len(epns) / processes)))):
            q = Queue()
            db_path = "files{}.db".format(i)

            worker = Process(target=epn_worker, args=(i, q, db_path, eps, hostname, user, key_path))
            worker.daemon = True
            worker.start()

            workers.append(Worker("Worker {}".format(i + 1), q, worker, db_path))

        while count < len(epns):
            count = 0
            for worker in workers:
                count += worker.queue.get()

            pbar.update(count)

        for worker in workers:
            worker.process.join()


if __name__ == '__main__':
    try:
        config_file = open("config.yml", "r")
    except IOError:
        print('Config file not found')
        exit(1)
    else:
        try:
            cfg = yaml.load(config_file, Loader=yaml.FullLoader)
            epns_path = cfg["epns_path"]
            hostname = cfg["hostname"]
            user = cfg["user"]
            key_path = cfg["key_path"]
        except yaml.YAMLError as e:
            print(e)
            exit(1)

    print("Config loaded: ", cfg)
    epns_names = os.listdir(epns_path)
    epns = {name: pd.read_excel(os.path.join(epns_path, name)) for name in epns_names}
    epns = pd.concat(epns).reset_index()
    epns.columns = ["File name", "File index", "EPN", "Start", "End", "Title", "Firt name", "Last name", "Email"]

    sftp_epn_list = list_sftp_epns(hostname, user, key_path, 'data')
    print("sftp epn list length %s", len(sftp_epn_list))
    matched_epns = set(epns['EPN'].tolist()).intersection(set(sftp_epn_list))
    print("matched epn list length %s", len(matched_epns))
    process_epns(list(matched_epns), hostname, user, key_path)
