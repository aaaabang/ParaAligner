import pickle

with open('backup/col_0_8.pkl', 'rb') as f:
    print(pickle.load(f))