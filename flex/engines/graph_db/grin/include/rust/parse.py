import os
from pathlib import Path

def get_func_name(line):
    return line.split('(')[0].strip().split(' ')[-1].strip()

def get_macro_name(line):
    return ('one', [('yes', line.split(' ')[1].strip())])

def parse_expr(line):
    line = line.strip()
    assert(line.startswith('#if '))
    line = line[4:]
    defs = line.split()
    res = []
    rel = ''
    for d in defs:
        if d.startswith('!'):
            res.append(('not', d[1+8: -1]))
        elif d.startswith('defined'):
            res.append(('yes', d[8: -1]))
        elif d == '&&':
            assert(rel == '' or rel == 'and')
            rel = 'and'
        elif d == '||':
            assert(rel == '' or rel == 'or')
            rel = 'or'
        else:
            assert False, f'unknown: {d}'
    assert(rel != '')
    return (rel, res)

def parse(path):
    res = {}
    macros = []
    prefix = ''
    with open(path) as f:
        for _line in f:
            if _line.strip().endswith('\\'):
                prefix += _line.strip()[:-1]
                continue
            line = prefix + _line
            prefix = ''
            if line.startswith(('GRIN_', 'void', 'bool', 'size_t', 'const', 'int', 'long long int',
                                'unsigned int', 'unsigned long long int', 
                                'float', 'double', 'const char*', 'struct')):
                func_name = get_func_name(line)
                res[func_name] = macros.copy()
            elif line.startswith('#ifdef'):
                assert(len(macros) == 0)
                macro_name = get_macro_name(line)
                macros.append(macro_name)
            elif line.startswith('#endif'):
                assert(len(macros) <= 1)
                if len(macros) == 1:
                    macros = macros[:-1]
            elif line.startswith('#if '):
                assert(len(macros) == 0)
                macro_name = parse_expr(line)
                macros.append(macro_name)
    return res

def to_rust(deps):
    if len(deps) == 0:
        return ''
    assert(len(deps) == 1)
    one_foramt = '#[cfg(feature = \"{}\")]'
    yes_format = 'feature = \"{}\"'
    not_format = 'not(feature = \"{}\")'
    all_format = '#[cfg(all({}))]'
    any_format = '#[cfg(any({}))]'

    deps = deps[0]
    if deps[0] == 'one':
        assert(len(deps[1]) == 1)
        assert(deps[1][0][0] == 'yes')
        return one_foramt.format(deps[1][0][1].lower())
    elif deps[0] == 'and':
        conds = [not_format.format(d[1].lower()) if d[0] == 'not' else yes_format.format(d[1].lower()) for d in deps[1]]
        return all_format.format(", ".join(conds))
    elif deps[0] == 'or':
        conds = [not_format.format(d[1].lower()) if d[0] == 'not' else yes_format.format(d[1].lower()) for d in deps[1]]
        return any_format.format(", ".join(conds))
    else:
        assert False, f'unknown: {deps}'
 
def snake_to_camel(s):
    if s.startswith(('GRIN_DATATYPE_', 'GRIN_DIRECTION_', 'GRIN_ERROR_CODE_', 'GRIN_FEATURES_ENABLE')):
        return s.upper()
    return ''.join([w.capitalize() for w in s.split('_')])

def snake_to_camel_line(line):
    segs = line.split(' ')
    return ' '.join([snake_to_camel(s) if s.startswith('GRIN_') and s.find('NULL') == -1 else s for s in segs])

def static_replace(line):
    replaces = {
        '::std::os::raw::c_uint': 'u32',
        '::std::os::raw::c_int': 'i32',
        '::std::os::raw::c_ulonglong': 'u64',
        '::std::os::raw::c_longlong': 'i64',
        'Copy, Clone': 'Copy, Clone, PartialEq',
    }
    for k in replaces:
        line = line.replace(k, replaces[k])
    return line


def rewrite(file, r, strip=7):
    with open(file) as f:
        lines = f.readlines()
    externc_flag = True
    need_ending_line = True
    parts = [[], [], [], []]
    p = 0
    for i, line in enumerate(lines):
        if i < strip:
            continue
        line = snake_to_camel_line(line)
        line = static_replace(line)
        if line.startswith('extern '):
            if externc_flag:
                p += 1
                parts[p].append('extern "C" {')
                externc_flag = False
            continue
        if line.startswith('}'):
            if i < len(lines) - 1:
                if externc_flag:
                    parts[p].append('}')
                else:
                    parts[p].append('')
            else:
                need_ending_line = False
                parts[p].append('}')
            continue
        if line.find('pub fn') != -1:
            func_name = line
            func_name = func_name[func_name.find('pub fn')+7:]
            func_name = func_name.split('(')[0]
            if func_name in r and r[func_name]:
                parts[p].append(f'    {r[func_name]}')
            parts[p].append('    #[allow(unused)]')
        if line.find('pub type') != -1:
            func_name = line
            func_name = func_name[func_name.find('pub type')+9:]
            func_name = func_name.split(' ')[0]
            if func_name in r and r[func_name]:
                parts[p].append(f'{r[func_name]}')
                parts[p].append('#[allow(unused)]')
        if line.find('RUST_KEEP') != -1:
            macro_name = line[line.find('GRIN'):line.find('RUST_KEEP')-3].lower()
            if need_ending_line:
                parts[p][-1] = '}'
            p += 1
            segs = line.split('RUST_KEEP')
            for s in segs[1:]:
                parts[p].append(s[1:s.find(';')+1])
            break
        if line.find('pub type GrinGraph') != -1:
            p += 1
        parts[p].append(line[:-1])
    return parts


def parse_to_rs(path, dst, predefine, strip=7):
    r = {}
    r |= parse(path / predefine)
    for f in path.glob('include/**/*.h'):
        r |= parse(f)
    for k in r:
        r[k] = to_rust(r[k])
    return rewrite(f'{dst}.rs', r, strip=strip)

def get_features(path, storage):
    macros = []
    with open(path / f'storage/{storage}/predefine.h') as f:
        lines = f.readlines()
    for line in lines:
        if line.startswith('#define') and line.find('GRIN_NULL') == -1:
            macros.append(line[8:].strip().lower())
    return macros

def parse_to_toml(path, storages):
    features = {}
    for s in storages:
        features[f'grin_features_enable_{s}'] = get_features(path, s)
    with open(path / 'template/predefine.h') as f:
        lines = f.readlines()
    macros = []
    for line in lines:
        if line.startswith('#define') and line.find('GRIN_NULL') == -1:
            macros.append(line[8:].strip().lower())
    with open('Cargo.toml', 'w') as f:
        f.write('[package]\n')
        f.write(f'name = \"grin\"\n')
        f.write('version = \"0.1.1\"\n')
        f.write('authors = [\"dijie\"]\n')
        f.write('\n')
        f.write('[dependencies]\n')
        f.write('cfg-if = \"0.1\"\n\n')
        f.write('[features]\n')
        for k in macros:
            f.write(f'{k} = []\n')
        for feat in features:
            f.write(f'{feat} = {features[feat]}\n')

def bindgen(src, dst):
    os.system(f'bindgen {src} -o {dst}.rs --no-layout-tests -- -I"../include" -I".."')

def all(path):
    src = 'grin_all.h'
    dst = 'grin_all'
    predefine = 'template/predefine.h'
    bindgen(src, dst)
    return parse_to_rs(path, dst, predefine)

def v6d(path):
    src = 'grin_v6d.h'
    dst = 'grin_v6d'
    predefine = 'storage/v6d/predefine.h'
    bindgen(src, dst)
    return parse_to_rs(path, dst, predefine, strip=50)

def merge(partss):
    with open('grin.rs', 'w') as outfile:
        # write allparts 0
        outfile.write('\n'.join(partss['all'][0]))
        outfile.write('\n')
        # write every parts 1 & 3
        outfile.write('cfg_if::cfg_if! {\n')
        first = True
        for k in partss:
            if k != 'all':
                if first:
                    first = False
                    outfile.write('    if')
                else:
                    outfile.write(' elif')
                outfile.write(f' #[cfg(feature = \"grin_features_enable_{k}\")]')
            else:
                outfile.write(' else ')
            outfile.write('{\n')
            outfile.write('\n'.join([f'        {x}' for x in partss[k][1] + partss[k][3]]))
            outfile.write('\n    }')
        outfile.write('\n}\n')
        # write allparts 2
        outfile.write('\n'.join(partss['all'][2]))
        outfile.write('\n')
        

if __name__ == '__main__':
    path = Path('..')
    allparts = all(path)
    v6dparts = v6d(path)
    merge({'v6d': v6dparts, 'all': allparts})
    parse_to_toml(path, ['v6d'])