#!/usr/bin/env python3
"""Upgrade a disposable controller and two agents, then restore their backups.

Use server and odin with the owner's proof approval. No system service is changed.
"""

import argparse
import hashlib
import json
from pathlib import Path
import runpy
import secrets
import subprocess
import uuid

shared = runpy.run_path(str(Path(__file__).with_name('proof-m1.py')))
remote, wait = shared['remote'], shared['wait']
HOSTS, START, STOP, WRITE, IMAGE = (shared[key] for key in ('HOSTS', 'START', 'STOP', 'WRITE', 'IMAGE'))
STATE_HASHES = r'''
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
print(json.dumps({str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for directory in ('controller', 'agent') for p in (root / directory).rglob('*') if p.is_file()}))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--old-release', required=True, type=Path)
    parser.add_argument('--new-release', required=True, type=Path)
    parser.add_argument('--evidence', required=True, type=Path)
    args = parser.parse_args()
    versions = {name: json.loads((directory / 'release.json').read_text())['version']
                for name, directory in (('old', args.old_release), ('new', args.new_release))}
    if versions['old'] == versions['new']:
        parser.error('Use two distinct release versions.')
    args.evidence.mkdir(mode=0o700)
    root = '/tmp/synod-upgrade-T23-' + uuid.uuid4().hex
    fixture = 'synod-upgrade-' + uuid.uuid4().hex[:20]
    project = root + '/project'
    password, marker = secrets.token_hex(24), 'upgrade-state-' + secrets.token_hex(16)
    created, ids, baseline, drills = [], {}, {}, []
    proof = {'passed': False, 'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
             'remote_directory': root, 'versions': versions, 'checks': []}
    controller = [root + '/synod', 'server', '--config', root + '/controller/controller.json',
                  '--listen', '0.0.0.0:18443', '--agent-listen', '0.0.0.0:18444']
    phase, failure = 'prepare', None

    def request(*arguments):
        result = json.loads(remote('server', ['python3', '-c',
            'import json,subprocess,sys; r=subprocess.run(sys.argv[1:],capture_output=True,text=True); print(json.dumps({"code":r.returncode,"output":r.stdout}))',
            root + '/synod', '--json', '--attempts', '1',
            '--api-url', 'https://' + HOSTS['server'][3] + ':18443', '--ca-file', root + '/controller/api-ca.crt',
            '--token-file', root + '/controller/admin.token', *arguments]))
        value = json.loads(result['output'])
        if result['code']:
            raise RuntimeError('CLI ' + arguments[0] + ': ' + value.get('error', {}).get('code', 'unknown'))
        return value

    def ready():
        try:
            return request('installation', 'get')['installation']
        except (RuntimeError, ValueError):
            return None

    def terminal(operation):
        value = request('operation', 'get', operation)['operation']
        return value if value['status'] in ('succeeded', 'failed', 'cancelled') else None

    def backup(app, destination):
        value = request('backup', app, '--destination', destination, '--method', 'live', '--wait=false')['operation']
        value = wait(lambda: terminal(value['id']), 'Backup did not finish', seconds=300)
        if value['status'] != 'succeeded' or value['result'].get('warning_codes'):
            raise RuntimeError('Backup failed or reported warnings: ' + value.get('failure_code', ''))
        return value['result']['snapshot_id']

    try:
        for host, (_, _, arch, _) in HOSTS.items():
            if remote(host, ['ss', '-H', '-ltn', 'sport = :18443 or sport = :18444 or sport = :18445']).strip():
                raise RuntimeError('Proof ports are occupied on ' + host)
            if remote(host, ['docker', 'image', 'inspect', IMAGE, '--format', '{{.Architecture}}']).decode().strip() != arch:
                raise RuntimeError('Pinned fixture image is absent on ' + host)
            baseline[host] = {container: remote(host, ['docker', 'inspect', '--format', '{{.State.Running}} {{.State.StartedAt}}', container])
                              for container in remote(host, ['docker', 'ps', '-q', '--no-trunc']).decode().split()}
            remote(host, ['mkdir', '-m', '700', root])
            created.append(host)
            for name, directory in (('old', args.old_release), ('new', args.new_release)):
                remote(host, ['mkdir', '-m', '700', root + '/' + name])
                for asset in ('install.sh', 'release.json', 'SHA256SUMS', 'synod-linux-' + arch):
                    remote(host, ['python3', '-c', WRITE, root + '/' + name + '/' + asset, '600'], (directory / asset).read_bytes())
            remote(host, ['sh', root + '/old/install.sh', '--version', versions['old'], '--from-dir', root + '/old', '--install-dir', root])
        print('Installed the previous release on both hosts. Starting disposable services.', flush=True)
        phase = 'prepare old installation'
        remote('server', ['mkdir', '-m', '700', project])
        compose = {'services': {'app': {'image': IMAGE, 'container_name': fixture, 'network_mode': 'none',
                   'command': ['sh', '-c', 'while :; do sleep 1; done'], 'volumes': [project + ':/config:ro']}}}
        remote('server', ['python3', '-c', WRITE, project + '/compose.json', '600'], json.dumps(compose).encode())
        remote('server', ['python3', '-c', WRITE, project + '/state', '600'], marker.encode())
        remote('server', ['docker', 'compose', '-p', fixture, '-f', project + '/compose.json', 'up', '-d'])
        started = remote('server', ['docker', 'inspect', '--format', '{{.State.StartedAt}}', fixture])
        remote('server', [root + '/synod', 'server', 'init', '--dir', root + '/controller', '--address', HOSTS['server'][3], '--systemd-dir', root + '/no-systemd'])
        remote('server', ['python3', '-c', START, root, 'controller', *controller])
        installation = wait(ready, 'Controller did not start')['installation_id']
        for host in HOSTS:
            token = request('node', 'join-token')['join_token']
            remote(host, [root + '/synod', 'agent', 'join', token, '--dir', root + '/agent', '--systemd-dir', root + '/no-systemd',
                          '--destination-listen', '0.0.0.0:18445', '--destination-address', HOSTS[host][3] + ':18445'])
            ids[host] = remote(host, ['cat', root + '/agent/node-id']).decode().strip()
            remote(host, ['python3', '-c', START, root, 'agent', root + '/synod', 'agent', 'run', '--config', root + '/agent/agent.json'])
        wait(lambda: len([n for n in request('node', 'list')['nodes'] if n['connected_at'] and n['agent_version'] == versions['old']]) == 2, 'Old agents did not connect')
        for node in ids.values():
            request('node', 'approve', node)
        app = wait(lambda: next((a for a in request('app', 'list').get('apps', []) if a['node_id'] == ids['server'] and a['definition']['name'] == fixture), None), 'Fixture was not discovered')
        remote('server', ['python3', '-c', WRITE, root + '/credentials.json', '600'], json.dumps({'password': password}).encode())
        destination = request('destination', 'add', 'upgrade-proof', '--type', 'device', '--device', ids['odin'], '--path', root + '/repository', '--credentials-file', root + '/credentials.json')['destination']['id']
        wait(lambda: len([d for d in request('destination', 'list')['destinations']['delivery'] if d['current']]) == 2, 'Destination was not delivered')
        preview = request('protect', app['id'], '--destination', destination, '--method', 'live', '--at', '03:00')
        request('protect', app['id'], '--destination', destination, '--method', 'live', '--at', '03:00', '--confirm', str(preview['protection']['revision']))
        before_snapshot = backup(app['id'], destination)
        approvals = {n['id']: n['approved_at'] for n in request('node', 'list')['nodes']}
        proof.update(installation_id=installation, nodes=ids, source_app_id=app['id'], before_snapshot=before_snapshot)
        phase = 'stop Synod and upgrade'
        for host in HOSTS:
            remote(host, ['python3', '-c', STOP, root, 'agent'])
        remote('server', ['python3', '-c', STOP, root, 'controller'])
        saved = {host: json.loads(remote(host, ['python3', '-c', STATE_HASHES, root])) for host in HOSTS}
        for host in HOSTS:
            remote(host, ['sh', root + '/new/install.sh', '--version', versions['new'], '--from-dir', root + '/new', '--install-dir', root, '--upgrade'])
            expected = hashlib.sha256((args.old_release / ('synod-linux-' + HOSTS[host][2])).read_bytes()).hexdigest()
            actual = remote(host, ['sha256sum', root + '/synod.previous']).decode().split()[0]
            if actual != expected:
                raise RuntimeError('Previous binary was not preserved on ' + host)
            if json.loads(remote(host, ['python3', '-c', STATE_HASHES, root])) != saved[host]:
                raise RuntimeError('Upgrade changed installation state on ' + host)
            if versions['new'] not in remote(host, [root + '/synod', '--version']).decode():
                raise RuntimeError('Wrong installed version on ' + host)
        proof['checks'].append('Both verified upgrades kept every controller and agent state file unchanged and preserved the old binary')
        print('Both upgrades preserved all state. Restarting the same controller and agents.', flush=True)
        phase = 'restart upgraded installation'
        remote('server', ['python3', '-c', START, root, 'controller', *controller])
        current = wait(ready, 'Upgraded controller did not start')
        if current['installation_id'] != installation or current['version'] != versions['new']:
            raise RuntimeError('Controller identity or running version differs')
        for host in HOSTS:
            remote(host, ['python3', '-c', START, root, 'agent', root + '/synod', 'agent', 'run', '--config', root + '/agent/agent.json'])
        wait(lambda: len([n for n in request('node', 'list')['nodes'] if n['connected_at'] and n['agent_version'] == versions['new']]) == 2, 'Upgraded agents did not connect')
        wait(lambda: len([d for d in request('destination', 'list')['destinations']['delivery'] if d['current']]) == 2, 'Destination settings were not acknowledged after restart')
        if {n['id']: n['approved_at'] for n in request('node', 'list')['nodes']} != approvals:
            raise RuntimeError('Upgrade changed node identities or approval')
        policy = next(a for a in request('app', 'status')['protection']['apps'] if a['app']['id'] == app['id'])
        if not policy['enabled'] or policy['settings']['destinations'] != [destination] or policy['settings']['method'] != 'live':
            raise RuntimeError('Upgrade changed protection settings')
        proof['checks'].append('The new controller and both agents reported the new version with the same installation, node IDs, approval, and protection settings')
        phase = 'read old snapshot and make new backup'
        request('file', 'get', app['id'], project + '/state', '--snapshot', before_snapshot, '--destination', destination, '--via', ids['odin'], '--output', root + '/old-restored-state')
        if remote('server', ['cat', root + '/old-restored-state']).decode() != marker:
            raise RuntimeError('Old snapshot content differs')
        after_snapshot = backup(app['id'], destination)
        proof['after_snapshot'] = after_snapshot
        phase = 'restore upgraded backup on arm64'
        drill = request('restore', app['id'], '--to', ids['odin'], '--destination', destination, '--snapshot', after_snapshot, '--drill', '--wait=false')['operation']
        drills.append(drill['id'])
        container = wait(lambda: remote('odin', ['docker', 'ps', '-q', '--filter', 'label=io.synod.restore.operation=' + drill['id']]).decode().strip(), 'Restored container did not start', seconds=300)
        if remote('odin', ['docker', 'exec', container, 'cat', '/config/state']).decode() != marker:
            raise RuntimeError('Restored app data differs')
        complete = wait(lambda: terminal(drill['id']), 'Drill did not finish')
        if complete['status'] != 'succeeded':
            raise RuntimeError('Drill failed: ' + complete.get('failure_code', ''))
        proof['checks'].append('The upgraded arm64 agent read the old snapshot and restored the new backup with exact app data')
        if remote('server', ['docker', 'inspect', '--format', '{{.State.StartedAt}}', fixture]) != started:
            raise RuntimeError('The upgrade restarted the app')
        for host, containers in baseline.items():
            for container, state in containers.items():
                if remote(host, ['docker', 'inspect', '--format', '{{.State.Running}} {{.State.StartedAt}}', container]) != state:
                    raise RuntimeError('An existing container changed on ' + host)
        proof['checks'].append('The source app and every pre-existing container kept their start times')
        proof['passed'] = True
    except (RuntimeError, OSError, ValueError, KeyError, StopIteration) as error:
        failure = phase + ': ' + str(error)
        proof['failure'] = failure
    finally:
        cleanup = []
        if 'server' in created:
            try:
                if remote('server', ['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=' + fixture]).strip():
                    remote('server', ['docker', 'compose', '-p', fixture, '-f', project + '/compose.json', 'down', '--volumes'])
            except RuntimeError as error:
                cleanup.append(str(error))
        for host in reversed(created):
            try:
                remote(host, ['python3', '-c', STOP, root, 'agent'])
                for operation in drills:
                    label = 'io.synod.restore.operation=' + operation
                    containers = remote(host, ['docker', 'ps', '-aq', '--filter', 'label=' + label]).decode().split()
                    if containers:
                        remote(host, ['docker', 'rm', '-fv', *containers])
                    volumes = remote(host, ['docker', 'volume', 'ls', '-q', '--filter', 'label=' + label]).decode().split()
                    if volumes:
                        remote(host, ['docker', 'volume', 'rm', *volumes])
                remote(host, ['python3', '-c', STOP, root, 'controller'])
            except RuntimeError as error:
                cleanup.append(str(error))
            for role in ('controller', 'agent'):
                try:
                    log = remote(host, ['python3', '-c', 'import pathlib,sys; p=pathlib.Path(sys.argv[1]); sys.stdout.buffer.write(p.read_bytes() if p.exists() else b"")', root + '/' + role + '.log'])
                    if any(value.encode() in log for value in (password, marker)):
                        cleanup.append('Private data found in ' + host + ' ' + role + ' log; log was not copied')
                    else:
                        (args.evidence / (host + '-' + role + '.log')).write_bytes(log)
                except RuntimeError as error:
                    cleanup.append(str(error))
        if not cleanup:
            for host in created:
                try:
                    remote(host, ['python3', '-c', 'import shutil,sys; shutil.rmtree(sys.argv[1])', root])
                except RuntimeError as error:
                    cleanup.append(str(error))
        proof['cleanup_errors'] = cleanup
        if cleanup:
            proof['passed'] = False
        (args.evidence / 'result.json').write_text(json.dumps(proof, indent=2) + '\n')
    if failure or cleanup:
        raise SystemExit('Upgrade proof incomplete. Read ' + str(args.evidence / 'result.json'))
    print('Upgrade and restore proof passed. Evidence: ' + str(args.evidence / 'result.json'), flush=True)


if __name__ == '__main__':
    main()
