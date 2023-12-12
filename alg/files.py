import os.path
import numpy as np
import pickle
import shutil
import uuid
import math


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


def fs_recover_info(config):
    print(config)
    backup_folder = config['backup_folder']
    db_size = os.path.getsize(config['database'])
    block_size = int(math.sqrt(db_size))
    print(block_size)
    pattern_len = len(config['patterns'])

    file_names = os.listdir(backup_folder)
    for f in file_names:
        fpath = os.path.join(backup_folder, f)
        if os.path.getsize(fpath) == 0:
            os.remove(fpath)
    file_names = os.listdir(backup_folder)

    state = [{
        'topk': None, 'latest_col': []
    } for _ in range(pattern_len)]

    for file_name in file_names:
        file_name = file_name.split('.')[0]
        file_arr = file_name.split('_')
        pattern = int(file_arr[1])
        if file_arr[0] == 'col':
            state[pattern]['latest_col'].append(int(file_arr[2]))
        if file_arr[0] == 'topk':
            state[pattern]['topk'] = load_topK(pattern)

    for pattern in range(pattern_len):
        blocks = state[pattern]['latest_col']
        blocks.sort()
        if len(blocks) == 0 or (len(blocks) == 1 and blocks[0] != min(block_size, db_size)):
            state[pattern]['latest_col'] = None
        elif len(blocks) == 1:
            state[pattern]['latest_col'] = blocks[0]
        else:
            latest_col = blocks[0]
            for i in range(1, len(blocks)):
                if blocks[i] - blocks[i-1] <= block_size:
                    latest_col = blocks[i]
                else:
                    break
            state[pattern]['latest_col'] = latest_col
    print(state)
    return state


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
        file.write(str(topK_val) + '\n')
        file.write(align[0])
        file.write('\n')
        file.write(align[1])
