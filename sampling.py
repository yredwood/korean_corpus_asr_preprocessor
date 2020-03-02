import os
import numpy as np

def pad_zero_str(d):
    d = str(d)
    while len(d) < 3:
        d = '0' + d
    return d

np.random.seed(0)
output_dir = 'output_sampled_01'
input_dir = 'output_preprocessed_01'

fnames = os.listdir(input_dir)
namu_corpus = [i for i in fnames if 'namu' in i]
news_corpus = [i for i in fnames if 'naver_news' in i] 

print (len(namu_corpus))
print (len(news_corpus))

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_num_sen = 10000

# each 100,000
def process_set(corpus, prefix):
    sen_per_file = 100000 // len(corpus)
    output_strings = []
    for fname in corpus:
        with open(os.path.join(input_dir, fname), 'r') as f:
            lines = f.readlines()

        rnd_idx = np.random.choice(len(lines), size=sen_per_file)
        output_strings += [lines[ri] for ri in rnd_idx]

    for i in range(len(output_strings) // output_num_sen + 1):
        splits = output_strings[output_num_sen*i:output_num_sen*(i+1)]
        output_fname = '{}_{}.txt'.format(prefix, pad_zero_str(i))
        with open(os.path.join(output_dir, output_fname), 'w') as f:
            f.writelines(splits)

process_set(namu_corpus, 'wiki')
process_set(news_corpus, 'news')
