#!/usr/bin/env python
import yaml
from util import *
from swarm import swarm, swarm_exec

config = yaml.load(open('honeycomb.yml','r'))


def cass(num):
    """ Translate number (from network config) to container name. """
    return config['prefix'] + str(num)


def configure(mode, quiet=False):
    settings = config['modes'][mode]
    yamlout = yaml.dump({mode: settings})

    if not quiet:
        puts(colored.green('>>> configuring'))
        puts(yamlout)

    for name, commands in settings.items():
        out = yaml.dump({mode: commands})
        swarm_exec(cass(name))("sh", "-c", fmt("echo '#{out}' > /honeycomb.yml"))
        for cmd in commands:
            swarm_exec(cass(name))(
                "tc", "qdisc", "replace", "dev", "eth0", "root", "netem", *cmd.split())


def cass_nodes():
    ps = swarm.ps()
    return [ l.split()[-1] for l in ps.split('\n') if 'owl_cass_' in l ]


def cmd_set(extra=None, opt=None):
    mode = opt.mode[0]
    if mode in config['modes']:
        configure(mode, opt.quiet)
    else:
        puts("unknown network: {mode}")


def cmd_status(extra=None, opt=None):
    print pretty_yaml({
        node: yaml.load(swarm_exec(node)("cat", "/honeycomb.yml").stdout)
        for node in cass_nodes()
    })


def cmd_modes(extra=None, opt=None):
    print pretty_yaml({
                          node: yaml.load(swarm_exec(node)("cat", "/honeycomb.yml").stdout)
                          for node in cass_nodes()
                          })


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()

    subparsers = parser.add_subparsers(help='Commands help')
    commands = {}

    def add_command(command, callback):
        subp = subparsers.add_parser(command, add_help=False)
        subp.set_defaults(command=command)
        commands[command] = callback
        return subp

    set_args = add_command('set', cmd_set)
    set_args.add_argument('mode', type=str, nargs=1,
                          help='Configure network with specified mode.')
    set_args.add_argument('--quiet', default=False, action='store_true',
                          help='Suppress debug output')

    add_command('status', cmd_status)

    opt, extra = parser.parse_known_args()
    if opt.command in commands:
        commands[opt.command](extra, opt)
    else:
        print 'invalid command'
        parser.print_help()
