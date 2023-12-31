import os.path
from .seq import read_str, get_str_length
from .files import load_block
from network.constant.params import SUBVEC_SIZE
from network.constant.params import SUBVEC_SIZE
import numpy as np
import math

#设置空位罚分和置换矩阵
substi = "ACGTN"
gap_penalty = -2
match = 3
mismatch = -3
substi_matrix = np.zeros((5,5),dtype = int)
for i in range(5):
    for j in range(5):
        if i == j:
            substi_matrix[i][j] = match
        else:
            substi_matrix[i][j] = mismatch


# substi_matrix = [[1,-1,-1,-1,-1],
#                  [-1,1,-1,-1,-1],
#                  [-1,-1,1,-1,-1],
#                  [-1,-1,-1,1,-1],
#                  [-1,-1,-1,-1,1]]
# substi_matrix = [[3 * substi_matrix[i][j] for j in range(len(substi_matrix[i]))] for i in range(len(substi_matrix))]

#block读取的文件路径
dir_block = "./"


"""
参数：
    left_vec - 左边界的得分向量，一维数组，如没有则全置为0。注：左边界需要多给一个值 
    up_vec - 上边界的得分向量，一维数组，如没有则全置为0
    i_vec - 子块的索引
    seq_vec - 相应的sequence向量
    pattern_vec - 相应的pattern向量
    K - topK值的个数
返回值：
    right_vec: 得分矩阵的最右列向量
    bottom_vec: 得分矩阵的最下方行向量
    topK_list: 前topK个最大值及其坐标(value,(x,y))的有序列表（降序），如果第K个值有重复，列表长度可能大于K
"""
def fill_matrix(left_vec, up_vec, i_vec, seq_vec, pattern_vec, K, start_ind):
    left_vec = np.array(left_vec)
    len_p = len(pattern_vec)
    len_s = len(seq_vec)
    # i_seq = int(start_ind/len_s)
    # print(f"len_p:{len_p}")
    # print(f"len(left_vec):{len(left_vec)}")
    # print(f"len_s:{len_s}")
    # print(f"i_vec:{i_vec}")

    # 初始化得分矩阵
    len_r = len_p
    len_c = len_s
    score_matrix = np.zeros((len_r + 1, len_c + 1), dtype = int)
    if len(left_vec)>len_p+1: #p只有一块，且小于SUBVEC_SIZE-1的情况:取subvec的前len_p个值
        left_vec = left_vec[:len_p+1]
    score_matrix[: , 0] = left_vec #第一列设为传来的左边界值
    score_matrix[0, 1:] = up_vec #第一行设为传来的上边界值

    # 计算得分矩阵
    for i in range(1,len_r+1):
        for j in range(1,len_c+1):
            # print(f"len_p:{len_p}")
            # print(f"(i-1,j-1):{(i-1,j-1)}")
            # print(f"pattern_vec[i-1]:{pattern_vec[i-1]}")
            a = substi.index(seq_vec[j-1])
            b = substi.index(pattern_vec[i-1])          
            similarity = substi_matrix[a][b]
            score_matrix[i][j] = max([0,
                                      score_matrix[i-1][j-1] + similarity, 
                                      score_matrix[i-1][j] + gap_penalty,         
                                      score_matrix[i][j-1] + gap_penalty
            ])
    right_vec = score_matrix[:,len_c]
    bottom_vec = score_matrix[len_r, 1:]

    # 返回所有大于等于第topk个值的[值,坐标](可能多于k个)
    topK_list = []
    flat = score_matrix[1:,1:].flatten()
    topK_indices_flat = np.argsort(flat)[::-1][:K]
    topK_values = flat[topK_indices_flat]
    topK_xy = np.unravel_index(topK_indices_flat, (len_r, len_c))
    for value,(x,y)in zip(topK_values, zip(*topK_xy)):
        # 计算绝对坐标
        x_abs = i_vec * (SUBVEC_SIZE - 1) + x
        # y_abs = i_seq * len_s + y
        y_abs = start_ind+y
        topK_list.append((value, (x_abs,y_abs)))
        # print(f"(x,y):{(x,y)}")
        # print(f"(x_abs,y_abs):{(x_abs,y_abs)}")

    Kth_value = topK_values[-1]
    Kvalue_indices_flat = np.argwhere(flat == Kth_value).flatten()
    if len(Kvalue_indices_flat) > 1:
        for index in Kvalue_indices_flat:
            if index not in topK_indices_flat:
                x,y = np.unravel_index(index, (len_r, len_c))
                x_abs = i_vec * (SUBVEC_SIZE - 1) + x
                # y_abs = i_seq * len_s + y
                y_abs = start_ind+y
                topK_list.append((Kth_value, (x_abs,y_abs)))
    
    # for i in range(score_matrix.shape[0]):
    #         for j in range(score_matrix.shape[1]):
    #             if score_matrix[i][j] == 68:
    #                 print("yes")
    #                 print((i,j))
    #                 print(start_ind)
    #                 print(topK_list)
    #                 exit()
    #             else:
    #                 print("no")

    # print(score_matrix) #测试用
    # print(f"left_vec:{left_vec}")
    # print(f"right_vec:{right_vec}")
    # print(f"shape left_vec:{left_vec.shape}")
    # print(f"shape right_vec:{right_vec.shape}")

    # print(right_vec)
    # print(bottom_vec)
    print(topK_list)
    # print(topK_list)

    return right_vec, bottom_vec, topK_list

"""
参数：
    topK - 一个字典 {“value":value,"i_subvec":i_subvec,"xy":(x,y)} #键值待统一       注：y需要从子块坐标变成整块的坐标
    start_s - K值所在seq子块的起始索引
    end_s - K值所在seq子块的结束索引
    path_s - sequence读取路径
    path_p - pattern读取路径
    i_th_pattern 


返回：
    aligned_p_all - pattern的alignment结果
    aligned_s_all - seq的alignment结果

"""
def trace_back(topK, start_s, end_s, path_s, path_p, i_th_pattern):
    # n=1 #测试用
    # block_size = 4
    database_size = get_str_length(path_s)
    block_size = int(math.sqrt(database_size))
    i_subseq = int(start_s/block_size)
    len_p = 0 #后面赋值
    continued = 1

    # print("slave_start_s:",start_s)
    # print("end_s:",end_s)
    # print("topk_xy:",topK["xy"])
    # print("block_size:",block_size)

    aligned_p_s = []
    aligned_s_s = []

    x,y = topK["xy"]
    # 转换为相对坐标
    y -= start_s
    

    while continued:

        #读取相应子sequece、左边界值、pattern
        # print("start_s:",start_s)
        # print("end_s:",end_s)
        seq_vec = read_str(path_s, start_s, end_s)

        if start_s == 0: #如果是sequence最左边一块，left_vec全置0
            if len_p == 0: #如果topK坐标在最左边一块，读取第二块的subvec长度
                len_p = len(load_block(i_th_pattern,block_size-1))
            left_vec = np.zeros((len_p+1,),dtype = int)
        else:
            left_vec = load_block(i_th_pattern, start_s - 1)
        # print("block_size:",block_size)
        # print("left_vec:",left_vec)
        # print("len_p:",len_p)

        pattern_vec = read_str(path_p, 0, len(left_vec) - 1)
        len_p = len(pattern_vec)
        
        #初始化得分矩阵和回溯矩阵
        len_r = len_p
        len_c = len(seq_vec)
        score_matrix = np.zeros((len_r + 1, len_c + 1), dtype=int)
        score_matrix[:, 0] = left_vec  # 第一列设为读取的左边界值
        trace_matrix = np.zeros((len_r , len_c ), dtype=int)

        #重新计算得分矩阵,并记录回溯矩阵
        for i in range(1,len_r+1):
            for j in range(1,len_c+1):
                a = substi.index(seq_vec[j-1])
                b = substi.index(pattern_vec[i-1])
                similarity = substi_matrix[a][b]
                diag = score_matrix[i-1][j-1] + similarity
                up = score_matrix[i-1][j] + gap_penalty
                left = score_matrix[i][j-1] + gap_penalty
                score = max(0, diag, up, left)
                score_matrix[i][j] = score
                # print("score:",score)
                # print("diag:",diag)
                # print("up:",up)
                # print("left:",left)
                if score == 0:
                    trace_matrix[i-1][j-1] = 0
                elif score == diag:
                    trace_matrix[i-1][j-1] = 1 
                elif score == left:
                    trace_matrix[i-1][j-1] = 3
                elif score == up:
                    trace_matrix[i-1][j-1] = 2            
                # print("seq_vec[j-1]:",seq_vec[j-1])
                # print("pattern_vec[i-1]:",pattern_vec[i-1])
                # print("trace_matrix[i-1][j-1]:",trace_matrix[i-1][j-1])
        
        # print(score_matrix[:12,:])
        # print(trace_matrix[:11,:])
        
        # print(seq_vec)

        # 回溯
        aligned_p = ""
        aligned_s = ""
        #将seq坐标转换为trace
        i , j = x,y
        # print("(x,y)",(x,y))
        # print("trace.shape",trace_matrix.shape)
        x_current = x
        y_current = y
        while trace_matrix[i][j] != 0 and i >= 0 and j >= 0:
            # print("i_subseq",i_subseq)
            # print("(i,j):",(i,j))
            # print("trace_matrix[i][j]:", trace_matrix[i][j])
            # print("seq_vec[j]",seq_vec[j])
            # print("pattern_vec[i]",pattern_vec[i])
            # print("seq_vec",seq_vec)
            # print("pattern_vec",pattern_vec)
            x_current = i
            y_current = j
    
            if j==0 and i_subseq==0: #如果是最左边一块且j=0
                print("end bc j reach boundary")
                continued = 0
            if i==0 and trace_matrix[i][j] == 2: #如果是最左边一块且j=0
                print("end bc i reach boundary")
                continued = 0
            if trace_matrix[i][j] == 1:
                aligned_p = pattern_vec[i] + aligned_p
                aligned_s = seq_vec[j] + aligned_s
                i -= 1
                j -= 1
            elif trace_matrix[i][j] == 2:
                aligned_p = pattern_vec[i] + aligned_p
                aligned_s = "-" + aligned_s
                i -= 1
            elif trace_matrix[i][j] == 3:
                aligned_p = "-" + aligned_p
                aligned_s = seq_vec[j] + aligned_s
                j -= 1
            # print("aligned_p:",aligned_p)
            # print("aligned_s:",aligned_s)

        # #重新计算块长
        # if start_s == block_size+1: #master 0-blocksize
        #     block_size +=1

        # if start_s < block_size and end_s <= block_size:
        #     print("end bc already tracing all subseq")
        #     continued = 0

        #左移一块，更新索引
        start_s = start_s - block_size 
        end_s = start_s + block_size - 1
        i_subseq = int(start_s/block_size)

        if start_s < 0:
            print("end bc already tracing all subseq")
            continued = 0

        # 结束循环，否则更新回溯起始点坐标
        if trace_matrix[x_current][y_current] == 0:
            # print("end bc trace_matrix[i][j] == 0")
            continued = 0
        elif trace_matrix[x_current][y_current] == 1:#diag
            y = block_size - 1
            x = x_current - 1
        elif trace_matrix[x_current][y_current] == 2:#up
            y = y_current
            x = x_current - 1
            # print("end bc i reach boundary")
            # continued = 0 #?
        elif trace_matrix[x_current][y_current] == 3:#left
            y = block_size - 1
            x = x_current
        # if trace_matrix[i][j] == 0:
        #     # print("end bc trace_matrix[i][j] == 0")
        #     continued = 0
        # elif trace_matrix[i][j] == 1:#diag
        #     y = block_size - 1
        #     x = x_current - 1
        # elif trace_matrix[i][j] == 2:#up
        #     y = y_current
        #     x = x_current - 1
        #     # print("end bc i reach boundary")
        #     # continued = 0 #?
        # elif trace_matrix[i][j] == 3:#left
        #     y = block_size - 1
        #     x = x_current
        # print("相对坐标(x,y):",(x,y))

        aligned_p_s.append(aligned_p)
        aligned_s_s.append(aligned_s)

        # n += 1  # 测试用
        # print("next_start_s",start_s)
        # print("next_end_s",end_s)

        # print(aligned_p)
        # print(aligned_s)

    # 拼接alignment
    aligned_p_all = ""
    aligned_s_all = ""
    len_align = len(aligned_s_s)
    for i in range(len_align):
        aligned_p_all += aligned_p_s[len_align - i - 1]
        aligned_s_all += aligned_s_s[len_align - i - 1]
    # print("end traceback")
    # print(aligned_p_all)
    # print(aligned_s_all)

    return aligned_p_all, aligned_s_all

#测试用
# left_vector = [1,6,4,2,1]
# up_vector = [4,9,2]
# left_vector = np.zeros((10,),dtype = int)
# up_vector = np.zeros((8,),dtype = int)

# right, bottom,topK =  fill_matrix(left_vector, up_vector, 0,"TGTTACGG", "GGTTGACTA",9,0)
# dict = {"value":13,"i_subseq":0,"xy":(7,1)}
# dict = {"value":8,"i_subseq":0,"xy":(7,0)}

# dict = {"value":13,"i_subvec":0,"xy":(6,5)}
# trace_back(dict,0,7,"./","./")

# dict = {"value":13,"i_subvec":1,"xy":(6,5)}
# trace_back(dict,4,7,"./","./",0)
