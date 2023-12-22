import os

def convert_fna_to_txt(input_file, output_file):
    try:
        with open(input_file, 'r') as fna_file:
            sequence = ''.join(line.strip() for line in fna_file if not line.startswith('>'))  # Concatenate lines not starting with '>'

        with open(output_file, 'w') as txt_file:
            txt_file.write(sequence)  # Write the concatenated sequence to the output file

        print(f"Converted {input_file} to {output_file}")

    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

def process_all_fna_files():
    fna_dir = 'fna_db'
    txt_dir = 'txt_db'

    # Create txt_db directory if it doesn't exist
    if not os.path.exists(txt_dir):
        os.makedirs(txt_dir)

    for filename in os.listdir(fna_dir):
        if filename.endswith('.fna'):
            input_path = os.path.join(fna_dir, filename)
            output_path = os.path.join(txt_dir, filename.replace('.fna', '.txt'))
            convert_fna_to_txt(input_path, output_path)

process_all_fna_files()
