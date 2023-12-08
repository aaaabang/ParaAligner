from seq import read_fna
import numpy as np

#设置空位罚分和置换矩阵
gap_penalty = -2
substi = "ACGT"
substi_matrix = [[1,-1,-1,-1],
                 [-1,1,-1,-1],
                 [-1,-1,1,-1],
                 [-1,-1,-1,1]]
substi_matrix = [[3 * substi_matrix[i][j] for j in range(len(substi_matrix[i]))] for i in range(len(substi_matrix))]


#设置pattern和sequence的路径
path_p = "./"
path_s = "./"


"""
参数：
    left_vec - 左边界的得分向量，一维数组，如没有则全置为0。注：左边界需要多给一个值 
    up_vec - 上边界的得分向量，一维数组，如没有则全置为0
    i_vec - 子块的索引，用于从文件系统中读取pattern的位置
    start_s - sequence的起始索引
    end_s - sequence的结束索引
    K - topK值的个数
返回值：
    right_vec: 得分矩阵的最右列向量
    bottom_vec: 得分矩阵的最下方行向量
    topK_dict: 前topK个最大值及其坐标(value,(x,y))的有序列表（降序），如果第K个值有重复，列表长度可能大于K


"""
def fill_matrix(left_vec, up_vec, i_vec, start_s, end_s, K):
    # 从文件系统读对应的pattern
    # 每个subvec一定是等长的吗？这里是按等长写的，不行的话纵坐标也传个start_ind和end_ind
    len_p = len(left_vec) - 1 
    start_p = i_vec * len_p
    end_p = start_p + len_p - 1
    pattern_vec = read_fna(path_p, start_p, end_p)
    # pattern_vec = "GGTTGACTA" #测试用
    # pattern_vec = "GACT"
    # pattern_vec = "G"

    # 从文件系统读对应的sequence
    seq_vec = read_fna(path_s, start_s, end_s)
    # seq_vec = "TGTTACGG" #测试用
    # seq_vec = "TTA"

    # 初始化得分矩阵
    len_r = len_p
    len_c = len(seq_vec)
    score_matrix = np.zeros((len_r + 1, len_c + 1), dtype = int)
    score_matrix[: , 0] = left_vec #第一列设为传来的左边界值
    score_matrix[0, 1:] = up_vec #第一行设为传来的上边界值

    # 计算得分矩阵
    for i in range(1,len_r+1):
        for j in range(1,len_c+1):
            a = substi.index(seq_vec[j-1])
            b = substi.index(pattern_vec[i-1])
            similarity = substi_matrix[a][b]
            score_matrix[i][j] = max([0,
                                      score_matrix[i-1][j-1] + similarity, 
                                      score_matrix[i-1][j] + gap_penalty,         
                                      score_matrix[i][j-1] + gap_penalty
            ])
    right_vec = score_matrix[:,len_c]
    bottom_vec = score_matrix[len_r-1, 1:]

    # 返回所有大于等于第topk个值的[值,坐标](可能多于k个)
    topK_dict = []
    flat = score_matrix[1:,1:].flatten()
    topK_indices_flat = np.argsort(flat)[::-1][:K]
    topK_values = flat[topK_indices_flat]
    topK_xy = np.unravel_index(topK_indices_flat, (len_r, len_c))
    for value,xy in zip(topK_values, zip(*topK_xy)):
        topK_dict.append((value, xy))
    Kth_value = topK_values[-1]
    Kvalue_indices_flat = np.argwhere(flat == Kth_value).flatten()
    if len(Kvalue_indices_flat) > 1:
        for index in Kvalue_indices_flat:
            if index not in topK_indices_flat:
                xy = np.unravel_index(index, (len_r, len_c))
                topK_dict.append((Kth_value, xy))

    # print(score_matrix) #测试用
    # print(right_vec)
    # print(bottom_vec)
    # print(topK_dict)

    return right_vec, bottom_vec, topK_dict


def trace_back():
    # TODO
    pass


#测试用
# left_vector = [1,6,4,2,1]
# up_vector = [4,9,2]
# left_vector = np.zeros((10,),dtype = int)
# up_vector = np.zeros((8,),dtype = int)
# fill_matrix(left_vector, up_vector, 0,0,0,9)