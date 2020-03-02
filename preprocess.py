import os
import re
from tqdm import tqdm
from joblib import Parallel, delayed

import pdb


# ============ args ===============
num_cpu = 30
#root_dir = '../naver_news/sentences'
#root_dir = '../namuwiki/output'
root_dir = './sample_corpus'
output_dir = './output_preprocessed_01'
output_form = 0 # 0: ()/(), 1: vocal transcription
# =================================

def argsort(seq):
    return sorted(range(len(seq)), key=seq.__getitem__)

def prc(text):
    def get_cond(c):
        cond1 = (ord(c) >= 44032 and ord(c) <= 55203) # kor char
        cond2 = ord(c) >= ord('0')  and ord(c) <= ord('9')
        cond3 = c in ['.', ',', ' ', '-']
        if cond1 or cond2 or cond3:
            return True
        return False

    spc_cnt = 0
    for t in text:
        if not get_cond(t):
            return False
        if t == ' ':
            spc_cnt += 1
    if spc_cnt < 4:
        return False

    return True


def read_num_kr(numbers):
    numbers = numbers.replace(',', '')
    if '.' not in numbers:
        numbers = int(numbers)
        if numbers >= 100:
            return read_num_ch(str(numbers))
        unit0 = ['한', '두', '세', '네', '다섯', '여섯', '일곱', '여덟', '아홉']
        unit1 = ['열', '스물', '서른', '마흔', '쉰', '예순', '일흔', '여든', '아흔']
        num0 = numbers % 10
        num1 = numbers % 100 // 10

        output = []
        if num1 > 0:
            output.append(unit1[num1-1])
        if num0 > 0:
            output.append(unit0[num0-1])

        out = ' '.join(output)
        if out == '스물':
            out = '스무'
        return out
    else:
        return read_num_ch(numbers)


def read_base(numbers):
    units = [''] + list('십백천')
    nums = '일이삼사오육칠팔구'
    result = []
    i = 0
        
    # decimal point
    try:
        numbers = int(numbers)
        while numbers > 0:
            numbers, r = divmod(numbers, 10)
            if r > 0:
                if r-1==0 and i > 0:
                    result.append(units[i])
                else:
                    result.append(nums[r-1] + units[i])
            i+=1
        return ''.join(result[::-1])
            
    except:
        print (numbers)
        return '_'

def read_10000(numbers):
    numbers = int(numbers)
    a, b = [read_base(x) for x in divmod(numbers, 10000)]
    if a:
        return a + '만' + b
    else:
        return b

def read_num_ch(numbers):
    numbers = numbers.replace(',', '')
    if '.' not in numbers:
        numbers = int(numbers)
        a, b = [read_10000(x) for x in divmod(numbers, 100000000)]
        if a:
            return a + '억' + b
        else:
            if len(b)==0:
                return '영'
            return b
    else: 
        nums = numbers.split('.')
        _n = '영일이삼사오육칠팔구'
        if len(nums) == 2:
            num1, num2 = nums[0], nums[1]
            # 공 instead of 영??
            out1 = read_num_ch(num1) 
            out2= ''.join([_n[int(i)] for i in list(num2)])
            return out1 + '점' + out2
        else:
            outs = []
            for n in nums:
                outs.append(''.join([_n[int(i)] for i in list(n)]))
            return '점'.join(outs)

                

def read_num_ch_phones(numbers):
    # read plain text when '-' in text
    nums = numbers.split('-')
    _n = '공일이삼사오육칠팔구'
    outs = []
    for n in nums:
        outs.append(''.join([_n[int(i)] for i in list(n)]))
    return ' '.join(outs)

def read_num_ch_bus(numbers, connector):
    nums = numbers.split('-')
    outs = []
    for n in nums:
        outs.append(read_num_ch(n))
    return connector.join(outs)


#print (read_num_ch('100000.010'))
#print (read_num_ch('2.56'))
#exit()

problem_postfix = '번개대' # depends on the context

chinese_postfix = '년점승패억만회원배편층화위세일석기여분단사조백천십호강초'
korean_postfix = '살명달병권'

chinese_long_postfix = ['라운드', '시즌', '학년', '달러']
korean_long_postfix = ['마리', '그루', '사람', '시간']

special_days = {
    '4.19': '사일구',
    '5.18': '오일팔',
    '5.16': '오일육',
    '3.1': '삼일',
    '샬케 04': '샬케 공사',

}


def find_numbers(text):
    org_text = text
    trans_dict = {}

    def text_paren_and_add_list(text, m, num):
        # really slower than direct indexing, but i dont care
        text = text[:m.start()+i*2] + '(' + m.group() + ')' \
                + text[m.end()+i*2:]
        t = '/({})'.format(num)
        trans_dict['(' + m.group() + ')'] =  t
        return text

    # 1. process long postfixes
    # chinese
    _string = '|'.join(['(([\\d]+,)*([\\d]+\.)*[\\d]+{})'.format(_pf) for _pf in chinese_long_postfix])
    p = re.compile(_string)
    for i, m in enumerate(p.finditer(text)):
        
        pf = [_pf for _pf in chinese_long_postfix if _pf in m.group()][0]
        numbers = m.group()[:-len(pf)]
        num = read_num_ch(numbers)
    
    # korean
    _string = '|'.join(['(([\\d]+,)*([\\d]+\.)*[\\d]+{})'.format(_pf) for _pf in korean_long_postfix])
    p = re.compile(_string)
    for i, m in enumerate(p.finditer(text)):
        
        pf = [_pf for _pf in korean_long_postfix if _pf in m.group()][0]
        numbers = m.group()[:-len(pf)]
        num = read_num_kr(numbers)
    
    # special days
#    _string = '|'.join(['({})'.format(_pf) for _pf in special_days.keys()])
#    p = re.compile(_string)
#    for i, m in enumerate(p.finditer(text)):
#
#        for key, item in special_days.keys():
#            if key in m.group():


    # chinese style reading
    p = re.compile('([\\d]+,)*([\\d]+\.)*[\\d]+[{}]'.format(chinese_postfix))
    for i, m in enumerate(p.finditer(text)):
        numbers = m.group()[:-1]
        num = read_num_ch(numbers)
        text = text_paren_and_add_list(text, m, num + m.group()[-1])

    # korean style reading
    p = re.compile('([\\d]+,)*([\\d]+\.)*[\\d]+[{}]'.format(korean_postfix))
    for i, m in enumerate(p.finditer(text)):
        numbers = m.group()[:-1]
        num = read_num_kr(numbers)
        text = text_paren_and_add_list(text, m, num + m.group()[-1])
    
    # special case 1: month
    p = re.compile('[\d]+[월]')
    for i, m in enumerate(p.finditer(text)):
        numbers = m.group()[:-1]
        num = read_num_ch(numbers)
        num = num.replace('육', '유')
        if num == '십':
            num = '시'
        text = text_paren_and_add_list(text, m, num + m.group()[-1])

    # special case 2: ***-***-*** or more
    re_2 = '[\d]+(-[\d]+)+-[\d]+'
    ymd_re = '[\d][\d][\d][\d]-[\d][\d]-[\d][\d]'
    p = re.compile(re_2)
    ymd_p = re.compile(ymd_re)
    for i, m in enumerate(p.finditer(text)):
        _k = ymd_p.search(m.group())
        if not _k:
            # phone number style
            num = read_num_ch_phones(m.group())
            text = text_paren_and_add_list(text, m, num)
        else:
            # date: let's not do this because we have enough
            pass
        
    # special case 3: ***-*** only two: then 다시?
    #re_3 = '(?!-)' + '[\d]+-[\d]+' + '(?!-)'
    re_3 = '[\d]+-[\d]+'
    p = re.compile(re_3)
    for i, m in enumerate(p.finditer(text)):
        # too much case sensitive
        if '버스' in text or '번' in text[m.end()+i*2:m.end()+i*2+2]:
            num = read_num_ch_bus(m.group(), ' 다시 ')
            text = text_paren_and_add_list(text, m, num)
        elif '시즌' in text[m.end()+i*2:m.end()+3+i*2]:
            num = read_num_ch_bus(m.group(), ' ')
            text = text_paren_and_add_list(text, m, num)
        else:
#            print (m, '|', text)
#            pdb.set_trace()
            pass

    # special case 4: n 번째
    re_4 = '[\d]+번째'
    p = re.compile(re_4)
    for i, m in enumerate(p.finditer(text)):
        numbers = m.group()[:-2]
        num = read_num_kr(numbers)
        if num == '한':
            num = '첫'
        text = text_paren_and_add_list(text, m, num + m.group()[-2:])

    
    
    paren_style_text = text[:]
    vocal_style_text = text[:]
    for key, item in trans_dict.items():
        paren_style_text = paren_style_text.replace(key, key+item)
        vocal_style_text = vocal_style_text.replace(key, item[2:-1])

    re_last = '[\d_]'
    p = re.compile(re_last)
    out = p.search(vocal_style_text)
    if out is not None:
        return None


    if output_form == 0:
        return paren_style_text
    else:
        return vocal_style_text

#        # output form 1: (...)/(...)
#        for key, item in trans_dict.items():
#            text = text.replace(key, key + item)
#    elif output_form == 1:
#        # output form 2: only vocal transcription
#        for key, item in trans_dict.items():
#            _item = item[2:-1] # remove paren
#            text = text.replace(key, _item)


def makedirs(d):
    if not os.path.exists(d):
        os.makedirs(d)


makedirs(output_dir)
filelist = os.listdir(root_dir)

#for i, fname in enumerate(filelist):
#    with open(os.path.join(root_dir, fname), 'r') as f:
#        lines = f.readlines()
#
#    output_lines = []
#    for line in lines:
#        if prc(line.strip()):
#            processed = find_numbers(line)
#            if processed is not None:
#                output_lines.append(processed)
#    
#    print ('fname: {:15s} | {:4d} / {:4d}'.format(fname, i, len(filelist)))
#    with open(os.path.join(output_dir, fname), 'w') as f:
#        f.writelines(output_lines)

# multiprocess
def single_file_process(fname):
    with open(os.path.join(root_dir, fname), 'r') as f:
        lines = f.readlines()

    output_lines = []
    for line in lines:
        if prc(line.strip()):
            processed = find_numbers(line)
            if processed is not None:
                output_lines.append(processed)

    with open(os.path.join(output_dir, fname), 'w') as f:
        f.writelines(output_lines)

Parallel(n_jobs=num_cpu)(
        delayed(single_file_process)(fname) for fname in tqdm(filelist)
)


#
