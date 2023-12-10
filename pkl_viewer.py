import pickle

with open('backup/col_0_3.pkl', 'rb') as f:
    print(pickle.load(f))