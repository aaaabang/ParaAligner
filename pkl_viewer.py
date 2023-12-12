import pickle

with open('backup/col_0_172.pkl', 'rb') as f:
    print(pickle.load(f))