import ipaddress
import json
import os
import pathlib
import subprocess
import sys
import time

out = pathlib.Path(sys.argv[1]).resolve()
image = sys.argv[2]
suffix = 'relay-spike-' + str(os.getpid())
network = 'supacode-' + suffix
router = 'router-' + suffix
node = 'node-' + suffix
driver = 'driver-' + suffix
token = 'isolated-spike-directory-token'
owned = []


def call(args, check=True):
    result = subprocess.run(args, text=True, capture_output=True, timeout=30)
    if check and result.returncode:
        raise RuntimeError(result.stderr)
    return result


def start(name, executable, env, alias, mounts=()):
    args = ['docker', 'run', '-d', '--name', name, '--network', network,
            '--network-alias', alias, '--memory', '768m', '--cpus', '2',
            '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges']
    for value in env:
        args.extend(['-e', value])
    for source, destination in mounts:
        args.extend(['-v', str(source) + ':' + destination + ':ro'])
    if name == node:
        args.extend(['--user', str(os.getuid()) + ':' + str(os.getgid())])
    args.extend(['--entrypoint', executable, image])
    call(args)
    if name not in owned:
        owned.append(name)


def metrics():
    response = call(['docker', 'exec', router, '/router', 'metrics'], check=False)
    try:
        return json.loads(response.stdout)
    except json.JSONDecodeError:
        return {}


def wait_ready():
    end = time.monotonic() + 15
    while time.monotonic() < end:
        data = metrics()
        if data.get('directoryStreams') == 1 and any(n['ready'] for n in data.get('nodes', [])):
            return data
        time.sleep(.1)
    raise RuntimeError('isolated directory did not become ready')


def smoke():
    return call(['docker', 'run', '--rm', '--network', network, '--entrypoint', '/probe',
                 image, '-mode', 'smoke', '-url', 'ws://router:8080', '-n', '2'], check=False)


def workload(label, extra):
    args = ['docker', 'run', '--name', driver, '--network', network, '--memory', '1g',
            '--cpus', '3', '--entrypoint', '/userbench', image, '-url', 'ws://router:8080',
            '-setup-rate', '200', '-warmup', '1s', '-duration', '3s'] + extra
    if label.startswith('tls-path'):
        args = ['docker','run','--name',driver,'--network',network,'--memory','1g','--cpus','3',
                '-v',str(out/'certs'/'cert.pem')+':/etc/ssl/certs/ca-certificates.crt:ro',
                '--entrypoint','/pathbench',image,'-direct','wss://node:8080','-routed','ws://router:8080',
                '-payloads','1024,65536','-clients','1,32','-warmup','1s','-duration','2s']
    stdout = (out / ('linux-' + label + '.jsonl')).open('w')
    stderr = (out / ('linux-' + label + '.stderr')).open('w')
    samples = (out / ('linux-' + label + '-resources.jsonl')).open('w')
    process = subprocess.Popen(args, stdout=stdout, stderr=stderr)
    owned.append(driver)
    limit = time.monotonic() + 150
    try:
        while process.poll() is None:
            if time.monotonic() > limit:
                raise RuntimeError('workload exceeded local budget')
            sample = {'wallTime': time.time(), 'routerMetrics': metrics()}
            for name in [router, node, driver]:
                raw = call(['docker', 'exec', name, '/topology', 'memory'], check=False)
                try:
                    sample[name] = json.loads(raw.stdout)
                except json.JSONDecodeError:
                    pass
            samples.write(json.dumps(sample) + '\n')
            samples.flush()
            time.sleep(.75)
        if process.returncode:
            raise RuntimeError('workload failed: ' + str(process.returncode))
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=10)
        call(['docker', 'logs', driver], check=False)
        call(['docker', 'rm', '-f', driver], check=False)
        owned.remove(driver)
        stdout.close()
        stderr.close()
        samples.close()
    print(json.dumps({'workload': label, 'completed': True}), flush=True)


(out / ('lab-manifest-' + str(os.getpid()) + '.json')).write_text(json.dumps({
    'image': image, 'containers': {'router': router, 'node': node, 'driver': driver},
    'network': network, 'componentMemoryLimit': '768m', 'componentCPUs': 2,
    'driverMemoryLimit': '1g', 'driverCPUs': 3, 'goMemoryTarget': '512MiB',
    'routerPerIPLimit': 10000, 'directoryUsesTLS': False, 'nodeUpstreamUsesTLS': True,
    'publicDriverUsesTLS': False, 'argv': sys.argv}, indent=2))

router_env = ['ROUTER_ADDR=0.0.0.0:8080', 'ROUTER_PRIVATE_ADDR=0.0.0.0:9090',
              'ROUTER_DIRECTORY_TOKEN=' + token, 'ROUTER_MAX_CONNS=10000',
              'ROUTER_MAX_CONNS_PER_IP=10000', 'ROUTER_ADMISSION_RATE=100000',
              'GOMEMLIMIT=512MiB']
try:
    ids = call(['docker', 'network', 'ls', '-q']).stdout.split()
    existing = json.loads(call(['docker', 'network', 'inspect'] + ids).stdout)
    used = [ipaddress.ip_network(c['Subnet']) for n in existing for c in (n['IPAM'].get('Config') or []) if c.get('Subnet')]
    subnet = next(str(ipaddress.ip_network('10.253.' + str(i) + '.0/24')) for i in range(1, 255)
                  if not any(ipaddress.ip_network('10.253.' + str(i) + '.0/24').overlaps(n) for n in used if n.version == 4))
    call(['docker', 'network', 'create', '--internal', '--subnet', subnet, network])
    start(router, '/router', router_env, 'router')
    start(node, '/topology', ['RELAY_ADDR=0.0.0.0:8080', 'RELAY_PRIVATE_ADDR=0.0.0.0:9090',
          'RELAY_NODE_ID=node', 'RELAY_ROUTERS=http://router:9090',
          'RELAY_ADVERTISE_URL=https://node:8080', 'RELAY_DIRECTORY_TOKEN=' + token,
          'RELAY_MAX_HOSTS=8000', 'RELAY_MAX_CLIENTS=8000', 'RELAY_ADMISSION_RATE=100000',
          'GOMEMLIMIT=512MiB', 'SPIKE_TLS_CERT=/certs/cert.pem', 'SPIKE_TLS_KEY=/certs/key.pem'],
          'node', [(out / 'certs', '/certs')])
    ready = wait_ready()
    failed = smoke()
    (out / 'container-roots-baseline.json').write_text(json.dumps({
        'ready': ready, 'probeExit': failed.returncode, 'stdout': failed.stdout, 'stderr': failed.stderr}))
    if failed.returncode == 0:
        raise RuntimeError('baseline unexpectedly trusts fixture root')
    call(['docker', 'rm', '-f', router])
    start(router, '/router', router_env, 'router',
          [(out / 'certs' / 'cert.pem', '/etc/ssl/certs/ca-certificates.crt')])
    wait_ready()
    succeeded = smoke()
    (out / 'container-roots-candidate.json').write_text(json.dumps({
        'probeExit': succeeded.returncode, 'stdout': succeeded.stdout, 'stderr': succeeded.stderr}))
    if succeeded.returncode:
        raise RuntimeError(succeeded.stderr)
    print(json.dumps({'containerRoots': 'baseline fails; supplied roots pass',
                      'directoryReadyDespiteBadDataPath': True}), flush=True)
    if len(sys.argv)>3 and sys.argv[3]=='smoke':
        pass
    elif len(sys.argv)>3 and sys.argv[3].startswith('path'):
        for run in range(3): workload('tls-'+sys.argv[3]+'-'+str(run),[])
    elif len(sys.argv)>3 and sys.argv[3]=='sustained':
        workload('sustained', ['-users', '600', '-rates', '10', '-warmup', '5s', '-duration', '30s'])
    else:
        workload('idle', ['-idle', '-users', '100,500,1000', '-rates', '1'])
        workload('traffic', ['-users', '100,300,600', '-rates', '2,10'])
finally:
    for name in list(reversed(owned)):
        logs = call(['docker', 'logs', name], check=False)
        (out / (name + '.log')).write_text(logs.stdout + logs.stderr)
        call(['docker', 'rm', '-f', name], check=False)
    call(['docker', 'network', 'rm', network], check=False)
