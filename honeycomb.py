#!/usr/bin/env python
import yaml
from util import *
from swarm import swarm

config = yaml.load(open('honeycomb.yml','r'))

def container(num):
    """ Translate number (from network config) to container name. """
    return config['prefix'] + str(num)


def configure(network):
    settings = config['networks'][network]
    puts(colored.blue(network+": ") + colored.black(str(settings)))
    for name, latency in settings.items():
        swarm("exec", container(name),
              "tc", "qdisc", "replace", "dev", "eth0", "root", "netem", "delay", latency)


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
