#!/usr/bin/env python
import yaml
from util import *
from swarm import swarm

config = yaml.load(open('honeycomb.yml','r'))

def cass(num):
    """ Translate number (from network config) to container name. """
    return config['prefix'] + str(num)


def configure(mode, quiet=False):
    settings = config['modes'][mode]

    if not quiet:
        puts(colored.green('>>> configuring'))
        puts(yaml.dump({mode: settings}))

    for name, commands in settings.items():
        for cmd in commands:
            swarm("exec", cass(name),
                  "tc", "qdisc", "replace", "dev", "eth0", "root", "netem", *cmd.split())


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('mode', help='Configure network with specified mode.', type=str, nargs=1)
    parser.add_argument('--quiet', help='Suppress debug output', default=False, action='store_true')
    opt = parser.parse_args()

    mode = opt.mode[0]
    if mode in config['modes']:
        configure(mode, opt.quiet)
    else:
        puts("unknown network: {mode}")
