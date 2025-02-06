import argparse
import configparser
import glob
import os
import subprocess
from multiprocessing import Pool

import tqdm
from tqdm import tqdm

parser = argparse.ArgumentParser(description='lidar parser refarence system')
parser.add_argument('--src', metavar='DIR', help='path to data directory.')
parser.add_argument('--dst', metavar='DIR', help='path to output directory.')
parser.add_argument(
    '--config', default='./config/camera_parser.ini', help='path to config file.')

args = parser.parse_args()
config_path = args.config
dst_dir = args.dst
config_ini = configparser.ConfigParser()
config_ini.read(config_path)
camera_parser = config_ini['EXE']['camera_parser']


def make_dir(dir):
    """make directory
    Args:
        dir(str): directory path
    """
    if not os.path.isdir(dir):
        os.umask(0)
        os.makedirs(dir, mode=0o777)


def decode(input_file):
    subprocess.call([camera_parser, input_file, dst_dir])


def wrap_decode(num):
    return decode(*num)


def main():
    args = parser.parse_args()
    src_dir = args.src

    make_dir(dst_dir)
    input_files = glob.glob(os.path.join(src_dir, "*.bin"))

    with Pool(processes=16) as pool:
        imap = pool.imap_unordered(decode, input_files)
        result = list(tqdm(imap))


if __name__ == "__main__":
    main()
