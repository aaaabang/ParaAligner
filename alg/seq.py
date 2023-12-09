import os

def write_str(seq, file_path):
    with open(file_path, 'wb') as f:
        f.write(seq)


def read_str(file_path, begin_ind, end_ind):
    with open(file_path, 'rb') as file:
        file.seek(begin_ind)
        data = file.read(end_ind - begin_ind)
        return data.decode('utf-8')


def get_str_length(file_path):
    return os.stat(file_path).st_size

print(read_str("data/patterns/test_fna", 0, 10))