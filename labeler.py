import os
from shutil import copyfile
import pickle
import pdb
from utils import _Getch

input_dir = './output_preprocessed_01'
output_dir = './output_examined_01'

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

init_flist = os.listdir(input_dir)
init_flist = [i for i in init_flist if i.endswith('.txt')]

log_fname = './logs/log_{}.pkl'.format(input_dir.split('/')[-1])
if not os.path.exists('logs'):
    os.makedirs('logs')


'''
log.txt contains 
1. init_flist (it must be consistent)
2. current fname, line_number
'''

if not os.path.exists(log_fname):
    current_fname, line_number = init_flist[0], 0
    os.system('cp -r {}/* {}'.format(input_dir, output_dir))
    bookmarks = {}
    for fname in init_flist:
        bookmarks[fname] = 0
else:
    with open(log_fname, 'rb') as f:
        load_state = pickle.load(f)

    assert load_state['init_flist'] == '|'.join(init_flist)
    current_fname, line_number = load_state['last_state'].split(',')
    line_number = int(line_number)
    bookmarks = load_state['bookmarks']


def save_log(fname, index, bookmarks, lines):
    # save bookmark, and last state
    iflist = '|'.join(init_flist).strip()
    last_state = ','.join([fname, str(index)])

    output_dict = {'init_flist': iflist,
            'last_state': last_state,
            'bookmarks': bookmarks
    }
    
    with open(log_fname, 'wb') as f:
        pickle.dump(output_dict, f)

    with open(os.path.join(output_dir, fname), 'w') as f:
        f.writelines(lines)


get_ch = _Getch()
i = init_flist.index(current_fname)
index = line_number
while True:
    fname = init_flist[i]
    print ('Starts from {}'.format(fname))
    
    with open(os.path.join(output_dir, fname), 'r') as f:
        lines = f.readlines()

    print ('{:5d} / {:5d} |  {}'.format(index, len(lines), lines[index]))
    while True:
        key_input = get_ch()

        if key_input == 's':
            save_log(fname, index, bookmarks, lines)
            print ('Saved')
            continue
        elif key_input == 'j':
            index -= 1
        elif key_input == 'l':
            index += 1
            if index == len(lines):
                index = 0
        elif key_input == ' ':
            if lines[index].startswith('*'):
                lines[index] = lines[index][1:]
            else:
                lines[index] = '*' + lines[index]
        elif key_input == 'q':
            exit()
        elif key_input == '.':
            save_log(fname, index, bookmarks, lines)
            i += 1
            if i == len(init_flist):
                print ('ERROR: end of documents')
                i -= 1
                continue
            bookmarks[fname] = index
            index = bookmarks[init_flist[i]]
            break
        elif key_input == ',':
            save_log(fname, index, bookmarks, lines)
            i -= 1
            if i < 0:
                print ('ERROR: start of documents')
                i += 1
                continue
            bookmarks[fname] = index
            index = bookmarks[init_flist[i]]
            break
        else:
            continue

        print ('{:5d} / {:5d} |  {}'.format(index, len(lines), lines[index]))
