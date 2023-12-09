import os.path
import numpy as np
import pickle
#设置存储路径
dir_block = "./"
dir_topK = "./"

def load_block(i_th_pattern, i, j):
    filename = f"block_{i_th_pattern}.pkl"  
    filepath = os.path.join(dir_block, filename)
    try:
        with open(filename, 'rb') as file:
            while 1:
                try:
                    data = pickle.load(file)
                    if data['i'] == i and data['j'] == j:
                        return data['subvector']
                except EOFError:
                    break
    except IOError as e:
        print("文件读取错误")
        return None
    print("没有找到匹配的数据块")
    return None

def save_block(subvector, i_th_pattern, i, j):
    if not os.path.exists(dir_block):
        os.makedirs(dir_block)
    filename = f"block_{i_th_pattern}.pkl"
    data = {
        'subvector': subvector,
        'i': i,
        'j': j
    }
    filepath = os.path.join(dir_block,filename)
    if os.path.exists(filepath):
        mode = 'ab'
    else:
        mode = 'wb'
    with open(filepath, mode) as file:
        pickle.dump(data, file)
        print("!")
    return None

"""
topKs: 列表，其中每个元素是一个字典?
"""
def save_topK(topKs, i_th_pattern):
    if not os.path.exists(dir_topK):
        os.makedirs(dir_topK)
    x,y = topKs[0]["xy"] 
    filename = f"topks_{x}_{y}.npy" #暂时用列表第一个元素的坐标作为标识
    filepath = os.path.join(dir_topK, filename)
    np.save(filepath, topKs)
    return None

def save_output(i_th_pattern, align, topK_val):
    #TODO
    with open(f"data/output/{i_th_pattern}", 'a') as file:
        # 追加内容到文件
        file.write(f"topK_val: {topK_val} \n")
        file.write(f"alignment: {align} \n")
        file.write("\n")
