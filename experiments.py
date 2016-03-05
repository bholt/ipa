#!/usr/bin/env python
# -*- coding: utf-8 -*-
import dataset
import sys
import itertools as it
import os
import re
import socket
import time
import json

from os.path import abspath, dirname, realpath
from os import environ as env
import requests

#########################
# External dependencies
import signal
import colors  # ansicolors
import sh

import honeycomb
from util import *
import swarm



K = 1024

LIVE = {'_out': sys.stdout, '_err': sys.stderr}


#########################

def notify_slack(msg):
    if 'IPA_SLACK_WEBHOOK' not in env:
        print '<slack notifications disabled>'
        return
    
    url = env['IPA_SLACK_WEBHOOK']
    data = {
        'username': 'Experiments',
        'icon_emoji': ':shipit:',
        'text': msg
    }
    requests.post(url, headers={'content-type': 'application/json'}, data=json.dumps(data))


def hostname():
    return socket.getfqdn()


def on_sampa_cluster():
    return 'sampa' in hostname()


def slurm_nodes():
    return [
        n.replace('n', 'i') for n in
        sh.scontrol.show.hostname(env['SLURM_NODELIST']).split()
    ]


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[str(name[:-1])] = str(x)

    flatten(y)
    return out


def count_records(table, ignore=None, valid='total_time is not null', **params):
    if ignore is None:
        ignore = []
    ignore.append('sqltable')
    remain = {k: params[k] for k in params if k not in ignore}

    def query(q):
        print '#>', q
        return [dict(x) for x in DB.query(q)]

    def cmp(k, v):
        if type(v) == float:
            return "ABS({0}-{1})/{1} < 0.0001".format(k, v)
        elif type(v) == str and len(v) == 0:
            return "({0} = '' or {0} is null)".format(k)
        else:
            return "{0} = '{1}'".format(k, v)

    cond = ' and '.join([cmp(k, v) for k, v in remain.items()])

    try:
        tname = table.table.name
        r = query('SELECT count(*) as ct FROM %s WHERE %s and %s' % (tname, valid, cond))
        return r[0]['ct']
    except Exception as e:
        puts_err("error with query: " + str(e))
        return 0


def cartesian(**params):
    return [dict(p) for p in it.product(*[zip(it.repeat(k), v) for k, v in params.items()])]


#################################################################################

blockade = sh.sudo.blockade

# Tasks to run before running any jobs
def before_all():

    print note("> creating up-to-date docker image")
    # (suppress 'sending build context' messages)
    for line in sh.sbt("docker:publish", _iter=True):
        if not re.match(r"^\[info\] (Sending build context.*|.*Preparing|.*Waiting|.*Layer already exists)", strip_ansi(line)):
            print line,

    print note("> initializing cluster")
    swarm.compose(["down"])
    swarm.swarm.pull("bholt/owl", **LIVE)
    swarm.compose(["up", "-d"])
    swarm.compose(["scale", "owl=1", "cass=3"])

    def check(container):
        o = sh.grep(swarm.swarm.logs(container, _piped=True), "listening for CQL clients", _ok_code=[0,1])
        return len(o) > 0

    sys.stdout.write("Waiting for Cassandra to finish launching")
    while not check(honeycomb.cass(3)):
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(1)
    print "done"

    # time.sleep(5) # make sure Cassandra has finished initializing
    # blockade.status(**LIVE)
    # swarm.reservations(['-Dipa.replication.factor=3'], {'experiments': True})


def run(logfile, *args, **flags):
    # convert ipa_* flags to java properties & add to args
    # ipa_retwis_initial_users -> ipa.retwis.initial.users
    props = [
        "-D{}={}".format(k.replace('_','.'), flags[k])
        for k in flags if k.startswith('ipa_')
    ]

    args = list(args)
    args.extend(props)

    if 'honeycomb_mode' in flags:
        mode = flags['honeycomb_mode']
        honeycomb.configure(mode, quiet=True)

    swarm.reservations(props, {'experiments': True})

    # hack to invoke as a shell script because something chokes on some values...
    invoke = ["sh",  "-c", "exec bin/owl {}".format(" ".join(args))]

    puts(colored.magenta("@ " + now()))
    try:
        cmd = swarm.owl_exec(*invoke, _timeout=60*5, _iter=True)
        puts("> #{colored.blue(' '.join(cmd.cmd))}")
        for o in cmd:
            logfile.write(o)
            if opt.verbose:
                print o, # w/o extra newline

        puts("#{colored.black('>')} exit code: #{colored.red(cmd.exit_code)}")

        # filter out extra finagle junk from stderr
        filtered = ''.join([ line for line in cmd.stderr.split('\n') if 'inagle' not in line ])

        # flatten & clean up metrics a bit
        metrics = {
            re.sub(r"owl\.\w+\.", "", k).replace(".","_"): v
            for k, v in flatten_json(json.loads(filtered)).items()
        }

        flags.update(metrics)
        print pretty_json(flags)
        table.insert(flags)

    except KeyboardInterrupt:
        puts_err("cancelled experiments")
        sys.exit()
    except sh.TimeoutException:
        puts_err("job exceeded time limit")
    except sh.ErrorReturnCode_1:
        puts_err("job exited with '1', skipping...")


def run_retwis():
    nexp = 0
    for trial in range(1, opt.target+1):
        if not opt.dry:
            print '---------------------------------\n# starting trial', trial
        elif opt.dry and trial > 1:
            # only need to do one 'trial' to get all the counts if doing dry run
            continue
        for a in cartesian(
            ipa_version               = [version],
            ipa_output_json           = ['true'],

            ipa_replication_factor    = [3],
            ipa_reset                 = ['false'],
            ipa_retwis_generate       = ['true'],

            ipa_duration              = [60],
            ipa_zipf                  = ['1.0'],

            ipa_retwis_initial_users  = [100],
            ipa_retwis_initial_tweets = [10],

            ipa_concurrent_requests   = [16, 128, 512, 2*K, 4*K, 8*K, 32*K],

            ipa_consistency           = ['strong', 'weak'],

            honeycomb_mode = ['uniform', 'slowpoke', 'slowflat']
        ):
            ct = count_records(table, ignore=[],
                               valid='meters_retwis_op_count is not null', **a)
            puts(colored.black("→")+colored.cyan('count:')+colored.yellow(ct))
            if opt.dry:
                continue
            if ct < trial:
                run(log, *['-main', 'owl.All'] , **a)
                nexp += 1
    return nexp


class RawMix:
    def __init__(self, add, contains, size):
        self.add = str(add)
        self.contains = str(contains)
        self.size = str(size)

    def merge_into(self, a):
        a['ipa_rawmix_mix_add']      = self.add
        a['ipa_rawmix_mix_contains'] = self.contains
        a['ipa_rawmix_mix_size']     = self.size


class RawMixCounter:
    def __init__(self, read, incr):
        self.read = read
        self.incr = incr

    def merge_into(self, a):
        a['ipa_rawmix_counter_mix_read'] = self.read
        a['ipa_rawmix_counter_mix_incr'] = self.incr


def run_rawmix(log, datatype):
    nexp = 0

    containers = swarm.containers_str()

    mixes = {
        'set': {
            'no_size': RawMix(add=0.2, contains=0.8, size=0.0),
            'default': RawMix(add=0.3, contains=0.6, size=0.1)
        },
        'counter': {
            'no_size': RawMixCounter(read=0.8, incr=0.2),
            'default': RawMixCounter(read=0.8, incr=0.2)
        }
    }

    main_class = 'owl.RawMix'
    if datatype == 'counter':
        main_class = 'owl.RawMixCounter'

    for trial in range(1, opt.target+1):
        if not opt.dry:
            print '---------------------------------\n# starting trial', trial
        elif opt.dry and trial > 1:
            # only need to do one 'trial' to get all the counts if doing dry run
            continue
        for a in cartesian(
            datatype = [datatype],
            ipa_version               = [version],
            ipa_output_json           = ['true'],

            ipa_replication_factor    = [3],
            ipa_reset                 = ['false'],

            ipa_duration              = [60],
            ipa_zipf                  = ['0.6'],

            ipa_concurrent_requests   = [128, 512, 2*K, 4*K],

            ipa_bound = ['tolerance:0.1', 'tolerance:0.05', 'consistency:strong', 'consistency:weak', 'consistency:weakwrite', 'latency:50ms', 'latency:10ms'],
            # ipa_bound = ['tolerance:0.1', 'tolerance:0.05'],
            ipa_lease_period = ['200ms'],
            # ipa_bound = ['consistency:strong', 'consistency:weak', 'latency:50ms', 'latency:10ms'],
            honeycomb_mode = ['normal', 'slowpoke_flat', 'google', 'amazon'],
            mix = ['no_size']

        ):
            a['containers'] = containers

            mixes[datatype][a['mix']].merge_into(a)

            ct = count_records(table, ignore=['containers'],
                               valid='out_actual_time_length is not null', **a)
            puts(colored.black("→ ")+colored.cyan('count:')+colored.yellow(ct))

            if opt.dry:
                continue
            if ct < trial:
                run(log, *['-main', main_class] , **a)
                nexp += 1
    return nexp


if __name__ == '__main__':
    global opt
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-t', '--target', type=int, default=1)
    parser.add_argument('-m', '--mode', type=str, default='owl')
    parser.add_argument('-d', '--type', type=str, default='set')
    parser.add_argument('-f', '--failures', type=int, default=0)
    parser.add_argument('-n', '--machines', type=str, default="")
    parser.add_argument('--manual', type=str, default=None)
    parser.add_argument('--dry', dest='dry', action='store_true', default=False)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False)

    if '--' in sys.argv:
        args_to_parse = sys.argv[1:sys.argv.index('--')]
        opt = parser.parse_args(args_to_parse)
        manual = sys.argv[sys.argv.index('--')+1:]
    else:
        opt = parser.parse_args()
        manual = None

    SRC = abspath(dirname(realpath(__file__)))
    os.chdir(SRC)

    if on_sampa_cluster():
        MACHINES = slurm_nodes()
    else:
        MACHINES = hostname()

    print 'machines:', MACHINES

    DB = dataset.connect(fmt("mysql:///claret?read_default_file=#{env['HOME']}/.my.cnf"))
    table = DB['ipa_' + opt.mode]

    if manual:
        run(table, sys.stdout, ' '.join(manual),
            machines=','.join(MACHINES))
        exit(0)
    elif not opt.dry:
        # startup
        before_all()
        # print note('skipping before_all()')

    puts("SRC: #{SRC}")
    log = open(SRC + '/experiments.log', 'w')

    tag = sh.git.describe().stdout
    version = re.match(r"([a-zA-Z0-9._]+)(-.*)?", tag).group(1)

    if opt.mode == 'owl':
        n = run_retwis()
    elif opt.mode == 'rawmix':
        n = run_rawmix(log, opt.type)

    notify_slack(fmt("Finished #{n} experiments. :success:"))
