import os.path
import numpy as np
import pickle
import shutil
import uuid


backup_dir = './backup'
output_dir = './output'


def fs_init():
    if os.path.exists(backup_dir):
        shutil.rmtree(backup_dir)
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    os.makedirs(backup_dir)
    os.makedirs(output_dir)


def fs_close():
    if os.path.exists(backup_dir):
        shutil.rmtree(backup_dir)


def fs_recover_info():
    pass


def load_block(i_th_pattern, col_ind):
    filename = f"{backup_dir}/col_{i_th_pattern}_{col_ind}.pkl"
    if os.path.exists(filename) and os.path.getsize(filename) != 0:
        with open(filename, 'rb') as f:
            data = pickle.load(f)
            return data
    else:
        return None


def save_block(subvector, i_th_pattern, i, j):
    col_ind = j
    filename = f"{backup_dir}/col_{i_th_pattern}_{col_ind}.pkl"
    with open(filename, 'wb') as f:
        pickle.dump(subvector, f)


def save_topK(topKs, i_th_pattern):
    filename = f"{backup_dir}/topk_{i_th_pattern}.pkl"
    with open(filename, 'wb') as f:
        pickle.dump(topKs, f)


def load_topK(i_th_pattern):
    filename = f"{backup_dir}/topk_{i_th_pattern}.pkl"
    if os.path.exists(filename) and os.path.getsize(filename) != 0:
        with open(filename, 'rb') as f:
            data = pickle.load(f)
            return data
    else:
        return None


def save_output(i_th_pattern, align, topK_val, topK_id=None):
    if topK_id is None:
        topK_id = uuid.uuid4()
    with open(f"{output_dir}/{i_th_pattern}_{topK_id}.txt", 'a') as file:
        file.writelines([topK_val, *align])
