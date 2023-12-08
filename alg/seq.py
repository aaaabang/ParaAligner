from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from Bio.SeqIO import write


def write_fna(seq, file_path, id='', description=''):
    sequence = Seq(seq)
    seq_record = SeqRecord(sequence, id=id, description=description)
    write(seq_record, file_path, "fasta")


def read_fna(file_path, begin_ind, end_ind):
    raise NotImplementedError
    # TODO

def get_fna_len(file_path):
    #TODO
    #return the lenghth of the database
    pass
