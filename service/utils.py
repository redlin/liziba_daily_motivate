import os

def create_dir_if_not_exists(path):
  if not os.path.isdir(path):
    # print('create folder: {}'.format(path))
    os.mkdir(path)

def current_dir():
  return os.getcwd()