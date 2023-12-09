from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from Bio.SeqIO import write


def write_fna(seq, file_path, id='', description=''):
    sequence = Seq(seq)
    seq_record = SeqRecord(sequence, id=id, description=description)
    write(seq_record, file_path, "fasta")


def read_fna(file_path, begin_ind, end_ind):
    if begin_ind >= end_ind:
        raise ValueError("The start index must be less than the end index.")

    sequence = ''
    with open(file_path, 'rb') as file:
        # Skip the header line that starts with '>'
        line = file.readline()
        
        # DEBUG: the file pointer moves to the next line, i.e starting from the 2nd line
        # while line.startswith(b'>'):
        #     line = file.readline()

        # Move to the start index position
        file.seek(begin_ind, 1)  # Move from the current position

        # Read data of a specific length
        chars_read = 0
        while chars_read < end_ind - begin_ind:
            char = file.read(1)
            if char in [b'\n', b'\r']:
                continue  # Ignore newline and carriage return characters
            sequence += char.decode('utf-8')  # Decode as a string
            chars_read += 1

    return sequence


# print("p:",read_fna("data/patterns/small.fna", 0, 5))
# print("da", read_fna("data/databases/covid1.fna", 0, 5))

def get_fna_length(file_path):
    #TODO
    return 10
