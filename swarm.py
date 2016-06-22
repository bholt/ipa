#!/usr/bin/env python
import sh
import sys
from sh import sudo, ssh, docker
import time
from util import *
from os.path import expanduser as expand
from argparse import ArgumentParser, Namespace

MASTER = 'platypus'
AGENTS = ['sloth', 'rhinoceros']

DOCKER_PORT = 2376
SWARM_PORT = 4000
BRIDGE = 'swarm'

CONSUL = '10.100.1.17'
CONSUL_PORT = 8500
CONSUL_LOG = '/var/log/consul'

NETWORK = 'owl'

hosts = [MASTER] + AGENTS
machines = [ ssh.bake(host) for host in hosts ]

consul = "/homes/sys/bholt/bin/consul"

swarm_url = fmt("tcp://#{MASTER}:#{SWARM_PORT}")

swarm  = docker.bake(host=swarm_url)
master = ssh.bake(MASTER)


LIVE = {'_out': sys.stdout, '_err': sys.stderr}


def docker(host):
    return sh.docker.bake("--host=tcp://{}:{}".format(host, DOCKER_PORT))


def on(host):
    return ssh.bake(host)


def start(args=None, opt=None):
    # start Consul key/value store for service discovery
    # on(MASTER).sudo(fmt("sh -c 'rm -rf /scratch/consul; nohup /homes/sys/bholt/bin/consul agent -server -bootstrap -data-dir /scratch/consul -node=master -bind=#{CONSUL} -client #{CONSUL} >#{CONSUL_LOG} 2>&1 &'"))
    cmd = fmt("--name=consul -d --net=host -p 8400:8400 -p 8500:8500 -p 8600:53/udp progrium/consul -server -bootstrap -node=master -bind=#{CONSUL} -client #{CONSUL}")
    on(MASTER).docker.run(cmd.split())

    time.sleep(4)
    
    for ssh in machines:
        
        # create new bridge network so our docker can run alongside the normal one
        if ssh.ip.link.show(BRIDGE, _ok_code=[0,1]).exit_code == 1:
            ssh.sudo.brctl.addbr(BRIDGE)
            ssh.sudo.ip.addr.add('10.20.30.1/24', 'dev', BRIDGE)
            ssh.sudo.ip.link.set('dev', 'swarm', 'up')
        
        # start docker daemon on remote host, headless via 'nohup', output to logfile
        ssh("sudo sh -c 'nohup docker daemon -H tcp://0.0.0.0:{dp} --exec-root=/var/run/docker.swarm --graph=/var/lib/docker.swarm --pidfile=/var/run/docker.swarm.pid --bridge={b} --cluster-advertise=ens1:{dp} --cluster-store=consul://{c}:{cp} >{log} 2>&1 &'".format(dp=DOCKER_PORT, c=CONSUL, cp=CONSUL_PORT, log="/var/log/docker.swarm", b=BRIDGE))
    
    time.sleep(1)
    # start Swarm manager
    nodelist = ','.join(["{}:{}".format(h, DOCKER_PORT) for h in hosts])
    docker(MASTER).run("--name=swarm", "-d", "--publish={}:2375".format(SWARM_PORT), "swarm:1.1.0", "--debug", "manage", "nodes://{}".format(nodelist))

    #swarm.network.create('--driver=overlay', NETWORK)


def stop(args=None, opt=None):
    
    for host in hosts:
        try:
            containers = docker(host).ps("-aq").stdout.split()
            if len(containers) > 0:
                docker(host).stop(*containers)
                docker(host).rm(*containers)
        except sh.ErrorReturnCode_1:
            puts("#{colored.yellow('[warning]')} no docker running on #{host}")
        
        on(host).sudo.pkill("-f", "[d]ocker.*tcp://", _ok_code=[0,1])
    
    on(MASTER).sh(c="docker stop consul; docker rm consul", _ok_code=[0,1])


def status(args=None, opt=None):
    print swarm.info()


def env(args=None, opt=None):
    puts("alias swarm='docker -H tcp://#{MASTER}:#{SWARM_PORT}'")
    puts("export DOCKER_HOST='tcp://#{MASTER}:#{SWARM_PORT}'")


def compose(args=None, opt=None):
    """ Invoke docker-compose command using Swarm. """
    puts(">>> docker-compose #{' '.join(args)} (with --host=#{swarm_url})")
    sh.docker_compose(*args, _ok_code=[0,1], _env={'DOCKER_HOST': swarm_url}, **LIVE)


def swarm_exec(node):
    return swarm.bake("exec", "-i", node)

owl_exec = swarm_exec("owl_owl_1")


def containers(filter):
    ps = swarm.ps()
    return [ line.split()[-1] for line in ps.split('\n') if filter in line ]


def add_keys(args=None, opt=None):
    if 'cass' in opt.containers:
        cons = containers('owl_cass')
    elif 'all' in opt.containers:
        cons = containers('owl_')
    else:
        cons = opt.containers

    for c in cons:
        puts(colored.magenta("[#{c}] ", bold=True) + "add keys")
        ex = swarm_exec(c)
        ex.sh(c='mkdir -p ~/.ssh')
        ex.sh(c='cat > ~/.ssh/id_rsa.pub', _in=open(expand("~/.ssh/id_rsa.pub")))
        ex.sh(c='cat > ~/.ssh/id_rsa; chmod go-r ~/.ssh/id_rsa', _in=open(expand("~/.ssh/id_rsa")))
        ex.sh(c='cat >> ~/.ssh/config', _in=open(expand("~/.ssh/bistromath.config")))
        ex.sh(c='cat >> ~/.bashrc', _in="up(){ pushd /src >/dev/null; rsync -a bistromath:~/sync/owl . --exclude=target/ --exclude=.idea/; popd >/dev/null; };\n")


def cass(args=None, opt=None):
    if args[0] == 'grep':
        for node in containers('owl_cass'):
            puts(colored.magenta("[#{node}]", bold=True))
            for line in swarm_exec(node).egrep(*args[1:], _ok_code=[0,1], _iter=True):
                pattern = args[1]
                puts(re.sub(pattern, "#{colored.red(pattern)}", line.strip()))
    elif args[0] == 'exec':
        for node in containers('owl_cass'):
            puts(colored.magenta("\n[#{node}] ", bold=True) + ' '.join(args))
            swarm_exec(node)(*args[1:], _ok_code=[0,1], **LIVE)


def reservation_server_ready(container):
    return swarm_exec(container).sh(c="grep '\[ready\]' /opt/docker/service.log | wc -l").stdout.strip() == '1'

def reservations(args=None, opt=None):
    opt = opt or Namespace()
    m = {'experiments': False, 'verbose': True}
    if type(opt) == dict:
        m.update(opt)
    else:
        m.update(opt.__dict__)
    opt = m

    args = args or []
    args = ' '.join(args)

    verbose = opt['verbose']
    cons = containers('owl_cass')

    for c in cons:
        swarm_exec(c).sh(c="cat /dev/null > /opt/docker/service.log; pkill -f ipa.ReservationServer", _ok_code=[0,1,143])
        swarm_exec(c).sh(c="pkill -f 'java -Dipa'", _ok_code=[0,1,143])

        if opt['experiments']:
            # when running as experiment, run with whatever's built-in to the image
            swarm("exec", "-d", c, "bash", "-c", fmt("exec bin/owl -main ipa.ReservationServer #{args} >/opt/docker/service.log 2>&1"))
        else:
            puts(colored.magenta("[#{c}] ", bold=True) + "ipa.ReservationServer #{args}")
            script = fmt('source ~/.bashrc; up; cd /src/owl; exec sbt "run-main ipa.ReservationServer" #{args} >/opt/docker/service.log 2>&1')
            o = swarm("exec", "-d", c, "bash", "-c", script)

    # wait for all reservations to be started
    for c in cons:
        if verbose: puts(colored.magenta("[#{c}] ", bold=True), newline=False, flush=True)
        while not reservation_server_ready(c):
            time.sleep(0.5)
            if verbose: puts(".", newline=False, flush=True)
        if verbose: puts("ready")


def owl_sbt(args=None, opt=None):
    cmd = args[0]
    remain = ' '.join(args[1:])
    swarm_exec("owl_owl_1").bash(c=fmt("source ~/.bashrc; up; cd /src/owl; sbt '#{cmd}' #{remain}"), _ok_code=[0,1], **LIVE)

def owl_sbt_res(args=None, opt=None):
    reservations(args[1:], opt)
    owl_sbt(args, opt)


def containers_str(prefix='/owl_'):
    containers = [ l.split()[-1] for l in swarm.ps().split('\n') if prefix in l ]
    cmap = {c: n for n, c in [c.split('/') for c in containers]}
    return yaml.safe_dump(cmap).strip()


if __name__ == '__main__':
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(help='Commands help')
    commands = {}

    def add_command(command, callback):
        subp = subparsers.add_parser(command, add_help=False)
        subp.set_defaults(command=command)
        commands[command] = callback
        return subp

    add_command('start', start)
    add_command('stop', stop)
    add_command('up', start)  # alias
    add_command('down', stop) # alias

    add_command('status', status)
    add_command('env', env)
    add_command('compose', compose)

    cmd = add_command('add_keys', add_keys)
    cmd.add_argument('containers', type=str, nargs='+',
                      help='Names of containers to add keys to (or "all" or "cass").')

    add_command('cass', cass)
    add_command('reservations', reservations)
    add_command('owl_sbt', owl_sbt)
    add_command('owl_sbt_res', owl_sbt_res)

    opt, extra = parser.parse_known_args()
    if opt.command in commands:
        commands[opt.command](extra, opt)
    else:
        print 'invalid command'
        parser.print_help()
