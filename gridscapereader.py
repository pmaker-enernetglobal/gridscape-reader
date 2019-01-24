#! /usr/bin/env python3
#
# gridscapereader.py - read data from gridscape over the VPN or locally or by magic
#
'''
gridscapereader.py - read data from one or more gridscape runs and iterations.

'''

import numpy as np
import pandas as pd
import urllib.request
import json
import io
from os.path import expanduser
from fnmatch import fnmatch
import re

# all variables kept in the whiteboard
#
# TODO: this should be merged with names.py eventually
#

wb = dict()

def gs_add(nm, val):
    global wb
    wb[nm] = val
    globals()[nm] = val # hack to make global variables

def gs_read(job, iter=None):
    '''read gridscape job/iteration into wb[]''' 
    gs_get(job, iter)

def gs_names(pat='*'):
    global wb
    r = []
    for v in wb:
        if fnmatch(v, pat):
            r.append(v)
    return sorted(r)

def gs(nm):
    global wb
    return wb[nm]

def gs_show():
    '''show the whiteboard'''
    for k in sorted(wb):
        try:
            print(k.ljust(32), str(round(wb[k][0], 2)) + ', ..')
        except:
            print(k.ljust(32), wb[k])

def gs_get(job, iter=None):
    try:
        niter = gs_parse_bayes_log(job)
    except Exception as e:
        print('// failed to read bayes_log.csv', e)
        exit(0)
    try:
        gs_parse_design(gs_get_json(job, 'design.json'))
    except Exception as e:
        print(e)
        print('failed to read design.json')

    for iter in range(niter):
        try:
            gs_csv(job, iter)
        except:
            # pass
            print('failed to read csv files')
    update_statistics()
    
def update_statistics():
    for v in gs_names():
        try:
            gs_add(v + '_min', gs(v).min())
            gs_add(v + '_mean', gs(v).mean())
            gs_add(v + '_max', gs(v).max())
            gs_add(v + '_sum', gs(v).sum())
        except:
            pass

# 
def gs_getfile(job, fn):
    file_prefix = 'file:' + expanduser('~') + '/Downloads/'
    http_prefix = 'http://gridscape-backend.internal.enernetglobal.com:8100/jobs/'
    
    file_url = file_prefix + 'job-' + str(job) + '-results/' + fn
    http_url = http_prefix + str(job) + \
        '/results/file?name=' + fn
    for url in  [file_url, http_url]:
        print('gs_getfile', url)
        try:
            with urllib.request.urlopen(url) as response:
                data = response.read()
            return data
        except Exception as e:
            print('  failed with:', e)
            pass
    return None

def csv_map(n):
    csv_map = {
        'Total Load [kW]' : 'LoadP',
        'Total Grid [kW]' : 'GridP',
        'Total Solar Conn [kW]' : 'PvP',
        'Total Solar Core [kW]' : 'PvAvailP',
        'Total Solar Slack [kW]' : 'PvSpillP',
        'All Generators [kW]' : 'GenP',
        'All Generators [L]' : 'GenFuelLperh'
    }
    
    if n in csv_map:
        return csv_map[n]
    else:
        re_map = [
            (r'genset([0-9]+).*\[kW\]', 'Gen', 'P'),
            (r'genset([0-9]+).*\[status\]', 'Gen', 'St'),
            (r'genset([0-9]+).*\[L\]', 'Gen', 'FuelLperh'),
        ]
        for pat, pre, post in re_map:
            r = re.match(pat, n)
            if r != None:
                return pre + r.group(1) + post
    return None

def gs_parse_design(d):
    '''parse the data for a design'''
    # toplevel just map thru with CamelCase names
    for k in d:
        if type(d[k]) == dict:
            pass # print(k, d[k])
        else:
            # print(k, d[k])
            gs_add(lower_to_name(k), d[k])

    # but things like generator parameters are a bit harder
    for n in d['electrical_view']['nodes']:
        if fnmatch(n, 'genset*'):
            cid = d['electrical_view']['nodes'][n] \
                ['properties']['component']['component_id']
            # print(d['price_components'][str(cid)])
            minp = d['price_components'][str(cid)]['min_load']
            maxp = d['price_components'][str(cid)]['prime__kw']
            # print(n, cid, minp, maxp)
            gs_add('Gen' + n[6:] + 'MaxPPa', maxp)
            gs_add('Gen' + n[6:] + 'MinPPa', minp) 
    
def gs_csv(job, itr=None):
    # print('* get_csv', job, itr)
    if itr != None:
        fn = 'results/annual_hourly_ops_iteration_' + str(itr) + '.csv'
    else:
        fn = 'results/annual_hourly_ops.csv'
    df = pd.read_table(io.BytesIO(gs_getfile(job, fn)), sep=',')
    for c in df.columns:
        if csv_map(c) != None:
            wb[csv_map(c)] = df[c]
            if itr != None:
                gs_add('Gs' + str(job) + 'Iter' + str(itr) + csv_map(c), df[c])
            else:
                gs_add('Gs' + str(job) + csv_map(c), df[c])
        else:
            pass # print('ignoring', c)

    
def gs_get_json(job, jfn):
    return json.loads(gs_getfile(job, jfn))

def lower_to_name(nm):
    '''return a name from a lower_case name

    >>> lower_to_name('gen1_p')
    'Gen1P'
    '''
    u = True
    r = ''
    for c in nm:
        if c == '_' or c == '.':
            u = True
        else:
            if u:
                r += c.upper()
            else:
                r += c
            u = False
    return r

def gs_get_j(job, jfn):
    d = gs_get_json(job, jfn)
    for k in d:
        # print(lower_to_name(k), type(d[k]), sep='\t')
        if type(d[k]) in [str, float, int]:
            print(lower_to_name(k), d[k], sep='\t')
        if type(d[k]) == dict and False:
            print(lower_to_name(k), d[k], sep='\t')
            
def gs_parse_bayes_log(job):
    try:
        df = pd.read_table(io.BytesIO(gs_getfile(job, 'bayes_log.csv')), sep=',')
        df.fillna(-1, inplace=True) # replace nans by -1
    except Exception as e:
        print('** failed to read bayes_log.csv for job', job)
        print(e)
        return 0
    
    print(df.columns)
    cmap = {
        'inventory.inventory.solar1.properties.size.pv_kw': 'PvMaxPPa',
        're_percent': 'PvPenPu', # its in Pu not %
        'p_irr': 'SysIrrPu',
        'ppa_target': 'SysPpaDpere',
        # 3 is a wolherism below, fix it
        'inventory.inventory.batt3.properties.batt_kw': 'EssMaxPPa',
        'inventory.inventory.batt3.properties.batt_kwh': 'EssMaxEPa',
    }
    for ct in cmap:
        for p in df.columns:
            if p == ct:
                for iter, v in enumerate(df[p]):
                    gs_add('Gs' + str(job) + 'Iter' + str(iter) + cmap[ct],
                          v)
    return df.shape[0] # count of rows including NaN

if __name__ == '__main__':
    gs_get(1588, 4)
    gs_show()
    exit(1)


    import matplotlib.pyplot as plt
    # plt.scatter(gs('GenFuelLperh'), gs('LoadP'))
    # plt.scaterr(gs('PvSpillP'), gs('LoadP'), color='red',
    #            alpha=0.2, marker='o', fillstyle='none')
    when = np.arange(0, 365 * 24)
    for v in gs_names('Gen*P') + ['LoadP']:
        print('plotting', v)
        plt.plot(when[0:7*24], gs(v)[0:7*24])
    plt.show()
    
