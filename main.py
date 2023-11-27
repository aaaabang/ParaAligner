from mpi4py import MPI
import numpy as np
import copy

MPI.Init
comm = MPI.COMM_WORLD
node_num = comm.Get_size() # size
rank = comm.Get_rank() 

def Smith_Waterman_Par(chainS:str, lenR:int, chainD:str, lenC:int, rule:np.ndarray) -> None:
    # Spliting tasks
    if rank == 0:
        chainS = chainS 
        chainD = chainD 
        rules0 = rule # match=3,mismatch=-3,sparse=-2
        chainR = np.array([char for char in chainS])
        chainC_send = np.array([char for char in chainD])
    else:
        chainS = None
        chainD = None
        rules0 = np.array([0,0,0])
        chainR = np.array(['A']*lenR)
        chainC_send = None
    
    chainC = np.array(['A']*int(lenC/node_num))
    comm.Bcast(rules0, 0)
    comm.Bcast(chainR, root=0)
    comm.Scatter(chainC_send, chainC, root=0)
    chainR = np.concatenate((np.array(['?']),chainR))
    chainC = np.concatenate((np.array(['?']),chainC))
    # print(f"Rank {rank}: ChainR: {chainR}; ChainC: {chainC}. ")
    
    # Initialization
    lenR = len(chainR)
    lenC = len(chainC)
    matrix = np.zeros((lenR, lenC), dtype=int)
    # print(mat)
    comm.Barrier() # is this necessary, or somewhere else?

    # Filling matrix
    for r in range(1,lenR):
        left_cell = np.empty(1,int)
        right_cell = np.empty(1,int)
        if rank != 0:
            comm.Recv(buf=left_cell, source=rank-1, tag=r)
            matrix[r][0] = copy.deepcopy(left_cell[0])

        for c in range(1,lenC):
            if chainR[r] == chainC[c]: # match=3,mismatch=-3,sparse=-2
                matrix[r][c] = max(matrix[r-1][c] + rules0[2], # sparse, negative
                                   matrix[r][c-1] + rules0[2], 
                                   matrix[r-1][c-1] + rules0[0], # match, positive
                                   0)
            else: 
                matrix[r][c] = max(matrix[r-1][c] + rules0[2], 
                                   matrix[r][c-1] + rules0[2], 
                                   matrix[r-1][c-1] + rules0[1], # mismatch, negative
                                   0)
            
            if c == lenC-1 and rank != node_num - 1:
                right_cell = copy.deepcopy(matrix[r][c])
                comm.Isend(buf=right_cell, dest=rank+1, tag=r)

    print(f"Rank {rank}:")
    print(matrix)     
    # gather_buf = np.ascontiguousarray(matrix[:, 1:])
    # print(gather_buf)

    # all_matrix = None
    # if rank == 0:
    #     all_matrix = np.empty((lenR, (lenC-1) * comm.Get_size()), dtype=int)

    # comm.Gather(gather_buf, all_matrix, 0)

    # if rank == 0: 
    #     print(all_matrix)
    return 

import argparse

def read_file(file_path):

    try:
        with open(file_path, 'r') as file:
            content = file.read()
            return content
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Parallel Smith-Waterman Algorithm')

    parser.add_argument('-database', type=str, help='Path to the sequence file')
    parser.add_argument('-pattern', type=str, help='Path to the pattern file')
    

    parser.add_argument('--k', type=int, default=1, 
                        help='top-k best alignment you want to get')

    args = parser.parse_args()

    database_file = args.database
    pattern_file = args.pattern
    top_k = args.k

    chainS = read_file(database_file)
    chainD = read_file(pattern_file)
    # chainS = "GGTTGACTA" # The one that going to be "batch"
    # chainD = "TGTTACGG" # The one that is fixed.
    rule = np.array([3,-3,-2])
    Smith_Waterman_Par(chainS, len(chainS), chainD, len(chainD), rule) # Assuming the Column number is int times of nodes.
    # mpiexec -n 4 python .\SW_FillMatrix.py
