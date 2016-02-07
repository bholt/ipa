#!/usr/bin/env python
import yaml
from util import *
from swarm import swarm

config = yaml.load(open('honeycomb.yml','r'))

def container(num):
    """ Translate number (from network config) to container name. """
    return config['prefix'] + str(num)


def configure(network):
    puts(colored.green('>>> configuring'))
    settings = config['networks'][network]
    puts(yaml.dump({network: settings}))
    for name, commands in settings.items():
        for cmd in commands:
            swarm("exec", container(name),
                  "tc", "qdisc", "replace", "dev", "eth0", "root", "netem", *cmd.split())


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('network', help='Command to execute, one of: ', type=str, nargs=1)
    opt = parser.parse_args()

    network = opt.network[0]
    if network in config['networks']:
        configure(network)
    else:
        puts("unknown network: {network}")
