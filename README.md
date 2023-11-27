# SmithWaterman_Par

### Set up
Before running SmithWaterman_Par, ensure that you have Python 3.6 or a higher version installed and configured on your system.

Next, run the following command to install dependency packages (in the root directory of this repo):

```
pip install -r requirements.txt
```

### Command
Users can run the SmithWaterman_Par tool with the following command:

```
python src/main.py -database <path of database file> -pattern <path of pattern file> --k <top-k alignments> -output <path of output file>
```

### Demo

We provide a simple example to demonstrate the use of SmithWaterman_Par. The `input/database.txt` file gives the sequences and `input/pattern.txt` file gives the pattern of alignment(s).

To run this demo, just run the following command:

```
./run_script.sh  
```
