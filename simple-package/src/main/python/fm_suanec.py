# load model from xlearn_text_model
def load_model(_path = ""):
    print 'reading the model file'
    modelf = open(_path,"r").readlines()
    feature_size = (len(modelf) -1) >> 1
    _w0 = float(modelf[0])
    print 'w0 read done.'
    _w = [] 
    for i in modelf[1:feature_size+1]:
        _w.append(float(i.strip()))
    print 'w read done.'
    _v = []
    for i in modelf[feature_size+1:]:
        if(len(_v) / float(len(_w)) % 0.01 < 1e-12):
            print 'v {fi} % read start'.format(fi = len(_v) / float(len(_w)) * 100 )
        vt = map(lambda vi : float(vi), i.strip().split(' '))
        _v.append(vt)
    print 'v read done.'
    return _w0,_w,_v

# load model from weifm_text_model
def load_weifm(_path = ""):
    print 'reading the model file'
    modelc = open(_path,"r").readlines()
    feature_size = int(filter(lambda line : line.startswith("nr_feature"), modelc)[0].split(" ")[-1])
    nr_factor = int(filter(lambda line : line.startswith("nr_factor"), modelc)[0].split(" ")[-1])
    w_index = int(filter(lambda x : 'w\n' == x[1], enumerate(modelc))[0][0])
    v_index = int(filter(lambda x : 'v\n' == x[1], enumerate(modelc))[0][0])
    _w0 = float(filter(lambda line : line.startswith("bias"), modelc)[0].split(" ")[-1])
    print 'w0 read done.'
    modelw = modelc[w_index+1:v_index]
    modelv = modelc[v_index+1:]
    _w = [0.] * feature_size
    for i in modelw:
        w_split = i.split(":")
        w_cur_idx = int(w_split[0].strip())
        _w[w_cur_idx -1] = float(i.split(":")[1].strip())
    print 'w read done.'
    assert len(_w) == feature_size
    assert len([i for i in _w if i != 0 ]) == len(modelw)
    print_batch = int(feature_size * 0.1)
    _v = []
    for i in modelv:
        if(len(_v) % print_batch == 0):
            print 'v {fi} % read start'.format(fi = int(len(_v) / print_batch) * 10 )
        vt = map(lambda vi : float(vi), i.split(":")[1].strip().split(' '))
        _v.append(vt)
    assert len(_v) == feature_size
    assert reduce(lambda x,y : x and y, map(lambda vi : len(vi) == nr_factor, _v))
    print 'v read done.'
    return _w0,_w,_v
# (w0,w,v) = load_weifm(_path)

# load sample from libsvm one sample one line
def load_sample(_path = ''):
  return map(
    lambda line: map(
      lambda kv : kv.strip().split(":"),
      line.strip().split(',')[1:]),
    open(_path,"r").readlines()
    )
def load_sample(_path = ''):
  return map(
    lambda line: map(
      lambda kv : kv.strip().split(":"),
      line.strip().split(' ')[1:]),
    open(_path,"r").readlines()
    )
def load_label(_path = ''):
    return [float(line.strip().split(' ')[0]) for line in open(_path,"r").readlines()]

def rst_auc(_sample_path = '', _model_path = ''): 
    model_path = _model_path
    sample_path = _sample_path
    samples = load_sample(sample_path)
    labels = load_label(sample_path)
    (w0, w, v) = load_weifm(model_path)
    rst = [fm_calc(xi, w, v, w0) for xi in samples]
    aucIn = zip(rst,labels)
    return auc(aucIn)

        
# v : featureRange * rank
def fm_calcV(_x = [], _v = []):
    rst = 0.
    for f in range(0,len(_v[0])):
      (ret1, ret2) = (0.0, 0.0)
      def v_row(_x = 0, _v = []) :
           return float(_v[_x][f])
      for index, value in _x:
         vtemp = float(v_row(int(index)-1, _v)) * float(value)
         ret1 += vtemp
         ret2 += vtemp ** 2
      rst += 0.5 * (ret1 ** 2 - ret2)
    return rst

# w : featureRange * 1
def fm_calcW(_x = [], _w = []):
  _wx = 0.
  for i in _x:
      index = int(i[0]) -1
      value = float(i[1])
      _wx += _w[index] * value
  return _wx

# sigmoid
def sigmoid(_x = 0.):
    import math 
    # print _x
    return 1.0 / (1.0 + math.exp(-_x));

# auc with a formular I don't know how to work..
# Params is a list with tuple contains (precision, label)
# Return a float auc value
# 
def auc_only_distinct(_pr_label_tuple_seq = []):
  _pr_label_tuple_seq.sort(key=lambda pair : pair[0])
  positive_sample_count = sum([1 for pair in _pr_label_tuple_seq if pair[1] == 1 ])
  negtive_sample_count = len(_pr_label_tuple_seq) - positive_sample_count
  sigma = 1.0 * \
    sum(
      [ pair[0] for pair in
        zip(range(1, 1 + len(_pr_label_tuple_seq)), _pr_label_tuple_seq)
        if pair[1][1] == 1])
  return (sigma - (positive_sample_count + 1) * positive_sample_count / 2) / positive_sample_count / negtive_sample_count

# auc with Wilcoxon-Mann-Witney Test and dynamic programming
# same as auc_only_distinct when precision is distinct and same precision all is postive sample
# is different from auc_only_distinct when same precision but not all positive sample
# Params is a list with tuple contains (precision, label)
# Return a float auc value
# 
def auc(_pr_label_tuple_seq = []):
  _pr_label_tuple_seq.sort(key=lambda pair : pair[0])
  _pr_seq = [ i[0] for i in _pr_label_tuple_seq ]
  _pr_rank_tuple_dict = {}
  _pr_rank_tuple_seq = zip(_pr_seq, range(1, len(_pr_label_tuple_seq)+1))
  for _pr_rank_tuple in _pr_rank_tuple_seq:
    precision = _pr_rank_tuple[0]
    rank = _pr_rank_tuple[1]
    if(_pr_rank_tuple_dict.has_key(precision)):
         _rank_count_tuple = _pr_rank_tuple_dict[precision]
         old_count = _rank_count_tuple[1]
         new_count = old_count + 1
         rank = (_rank_count_tuple[0] * old_count + rank) / new_count
         _pr_rank_tuple_dict[precision] = (rank, new_count)
    else:
         _pr_rank_tuple_dict[precision] = (rank * 1.0, 1)
  positive_sample_count = sum([1 for pair in _pr_label_tuple_seq if pair[1] == 1 ])
  negtive_sample_count = len(_pr_label_tuple_seq) - positive_sample_count
  sigma = 1.0 * sum([ _pr_rank_tuple_dict.get(pair[0])[0] for pair in _pr_label_tuple_seq if pair[1] == 1])
  return (sigma - (positive_sample_count + 1) * positive_sample_count / 2) / positive_sample_count / negtive_sample_count


def resolveSigmoid(_r = 0.):
  import math
  r = (1.0 / _r) - 1.0
  return -math.log(r)

# FM_predict
def fm_calc(_x = [], _w = [], _v = [], _w0 = 0.):
    return sigmoid(fm_calcW(_x, _w) + float(_w0) + fm_calcV(_x, _v))

def libsvm_minus_one(_sample = ''):
    parts = _sample.strip().split(' ')
    label = parts[0]
    features = parts[1:]
    def index_minus(_kvpair = ''):
        _pair = _kvpair.split(":")
        _index = str(int(_pair[0])-1)
        _value = _pair[1]
        return ':'.join([_index,_value])
    features_minused = map(lambda xpart : index_minus(xpart), features)
    return label + " " + " ".join(features_minused)

if __name__ == '__main__':
     import sys
     print 'model_path sample_path rst_path '
     model_path = sys.argv[1]
     sample_path = sys.argv[2]
     # print rst_auc(sample_path,model_path)
     print rst_auc(_model_path=model_path, _sample_path=sample_path)
     # exit(0)
     if(len(sys.argv) > 3): 
         rst_path = sys.argv[3]
         (w0, w, v) = load_weifm(model_path)
         samples = load_sample(sample_path)
         rst = map(lambda xi : fm_calc(xi, w, v, w0), samples)
         with open(rst_path,'w') as writer:
             map(lambda line : writer.write(str(line) + "\n"),rst)
