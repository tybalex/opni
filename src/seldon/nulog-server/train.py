# Standard Library
import os
import sys

# Third Party
from NuLogParser import LogParser

# log_file='BGL_2k.log'
# log_format= '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>'
# filters= '([ |:|\(|\)|=|,])|(core.)|(\.{2,})'

if len(sys.argv) < 2:
    print("specify the log_file to train!...")
    sys.exit(-1)
log_file = sys.argv[1]
print("training file : " + str(log_file))
# log_file='sockshop.txt'

log_format = "<Content>"
filters = '([ |:|\(|\)|\[|\]|\{|\}|"|,|=])'
k = 50  # was 50 ## tunable, top k predictions
nr_epochs = 1
num_samples = 0

input_dir = "input/"  # The input directory of log file
output_dir = "output/"  # The output directory of parsing results
indir = os.path.join(input_dir, os.path.dirname(log_file))
log_file = os.path.basename(log_file)


parser = LogParser(
    indir=indir, outdir=output_dir, filters=filters, k=k, log_format=log_format
)
texts = parser.load_data(log_file)
if "vocab.txt" in os.listdir(output_dir):
    print("train with exists model and vocab...")
    parser.tokenizer.load_vocab(output_dir)
    tokenized = parser.tokenize_data(texts, isTrain=True)
else:
    print("train from scratch...")
    tokenized = parser.tokenize_data(texts, isTrain=True)
parser.tokenizer.save_vocab(output_dir)
parser.train(tokenized, nr_epochs=nr_epochs, num_samples=num_samples, lr=0.0001)
