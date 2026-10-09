#!/usr/bin/env python3
"""Verify a pinned Actions artifact before extracting assets or an updater binary.

No service, database, policy, or current-pointer mutations. The operator must
obtain this verifier from reviewed, signed source, not execute it from an
unverified candidate archive. Also used by the explicit promotion workflow.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import stat
import subprocess
import tarfile
import time
import zipfile

MAX_BYTES = 4 * 1024**3
WORKFLOW = '.github/workflows/release.yml'


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    checksum = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(65536), b''):
            checksum.update(chunk)
    return checksum.hexdigest()


def root_protected(path):
    path = Path(path).absolute()
    for item in (path, *path.parents):
        metadata = item.lstat()
        require(metadata.st_uid == 0 and not metadata.st_mode & 0o022
                and not stat.S_ISLNK(metadata.st_mode), 'bootstrap path is not root-protected')


def gh_json(gh, *args):
    result = subprocess.run([gh, *args], capture_output=True, timeout=120, check=False)
    require(result.returncode == 0, 'GitHub verification command failed')
    require(len(result.stdout) <= 1024 * 1024, 'GitHub verification response too large')
    return json.loads(result.stdout)


def validate_pin(pin):
    required = {'version', 'commit_sha', 'tag_object_sha', 'run_id', 'run_attempt',
                'artifact_id', 'artifact_sha256', 'artifact_bytes'}
    require(required <= pin.keys() <= required | {'manifest_sha256', 'bundle_sha256'}, 'invalid pin fields')
    require(re.fullmatch(r'v[A-Za-z0-9._-]{1,199}', pin['version']), 'invalid candidate tag')
    for name in ('commit_sha', 'tag_object_sha'):
        require(re.fullmatch(r'[0-9a-f]{40}', pin[name]), 'invalid pinned source digest')
    for name in ('artifact_sha256', 'manifest_sha256', 'bundle_sha256'):
        if name in pin:
            require(re.fullmatch(r'[0-9a-f]{64}', pin[name]), 'invalid pinned asset digest')
    for name in ('run_id', 'run_attempt', 'artifact_id', 'artifact_bytes'):
        require(type(pin[name]) is int and pin[name] > 0, 'invalid pinned artifact identity')
    require(pin['artifact_bytes'] <= MAX_BYTES, 'artifact exceeds limit')


def verify_origin(gh, repo, pin, target):
    require(re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo), 'invalid repository')
    validate_pin(pin)
    base = f'repos/{repo}'
    ref = gh_json(gh, 'api', f'{base}/git/ref/tags/{pin["version"]}')
    require(ref['object'] == {'type': 'tag', 'sha': pin['tag_object_sha'],
        'url': f'https://api.github.com/{base}/git/tags/{pin["tag_object_sha"]}'}, 'signed tag object mismatch')
    tag = gh_json(gh, 'api', f'{base}/git/tags/{pin["tag_object_sha"]}')
    require(tag['verification']['verified'] is True and tag['verification']['reason'] == 'valid'
            and tag['tag'] == pin['version'] and tag['object']['type'] == 'commit'
            and tag['object']['sha'] == pin['commit_sha'], 'signed tag verification failed')
    run = gh_json(gh, 'api', f'{base}/actions/runs/{pin["run_id"]}')
    for name, expected in {'id': pin['run_id'], 'run_attempt': pin['run_attempt'],
                          'head_sha': pin['commit_sha'], 'head_branch': pin['version'],
                          'path': WORKFLOW, 'event': 'workflow_dispatch',
                          'status': 'completed', 'conclusion': 'success'}.items():
        require(run.get(name) == expected, f'candidate run {name} mismatch')
    require(run['repository']['full_name'] == repo and run['head_repository']['full_name'] == repo,
            'candidate repository mismatch')
    artifact = gh_json(gh, 'api', f'{base}/actions/artifacts/{pin["artifact_id"]}')
    name = 'candidate-publication' if target == 'publication' else f'release-{target}'
    require(artifact['id'] == pin['artifact_id'] and artifact['name'] == name
            and artifact['expired'] is False and artifact['size_in_bytes'] == pin['artifact_bytes']
            and artifact['digest'] == 'sha256:' + pin['artifact_sha256']
            and artifact['workflow_run']['id'] == pin['run_id']
            and artifact['workflow_run']['head_sha'] == pin['commit_sha'], 'candidate artifact mismatch')


def download(gh, repo, pin, path):
    command = [gh, 'api', f'repos/{repo}/actions/artifacts/{pin["artifact_id"]}/zip']
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, 'stdout')
    selector.register(process.stderr, selectors.EVENT_READ, 'stderr')
    counts = {'stdout': 0, 'stderr': 0}
    deadline = time.monotonic() + 1800
    try:
        with path.open('xb') as output:
            while selector.get_map():
                require(time.monotonic() < deadline, 'candidate download timeout')
                for key, _ in selector.select(timeout=1):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    counts[key.data] += len(chunk)
                    limit = pin['artifact_bytes'] if key.data == 'stdout' else 1024 * 1024
                    require(counts[key.data] <= limit, 'candidate download exceeds bound')
                    if key.data == 'stdout':
                        output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        require(process.wait(timeout=10) == 0, 'candidate download failed')
        require(counts['stdout'] == pin['artifact_bytes'] and digest(path) == pin['artifact_sha256'],
                'candidate ZIP digest mismatch')
    finally:
        selector.close()
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        process.stdout.close()
        process.stderr.close()


def expected_names(version, target):
    targets = ('x86_64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu') if target == 'publication' else (target,)
    names = {f'ai-image-factory-{version}-{arch}{suffix}'
             for arch in targets for suffix in ('.manifest.json', '.tar.gz')}
    if target == 'publication':
        names.add('install-release')
    return names


def extract_assets(archive, directory, names):
    with zipfile.ZipFile(archive) as zipped:
        entries = zipped.infolist()
        require(len(entries) == len(names) and {item.filename for item in entries} == names,
                'candidate archive member mismatch')
        for item in entries:
            kind = stat.S_IFMT(item.external_attr >> 16)
            bound = MAX_BYTES if item.filename.endswith('.tar.gz') else 8 * 1024 * 1024
            require(kind in (0, stat.S_IFREG) and 0 < item.file_size <= bound,
                    'candidate archive member type or size rejected')
            with zipped.open(item) as source, (directory / item.filename).open('xb') as output:
                shutil.copyfileobj(source, output, 65536)


def verify_certificate(results, repo, pin):
    expected = {'runInvocationURI': f'https://github.com/{repo}/actions/runs/{pin["run_id"]}/attempts/{pin["run_attempt"]}',
                'buildSignerURI': f'https://github.com/{repo}/{WORKFLOW}@refs/tags/{pin["version"]}',
                'buildSignerDigest': pin['commit_sha'], 'sourceRepositoryDigest': pin['commit_sha'],
                'sourceRepositoryRef': f'refs/tags/{pin["version"]}', 'runnerEnvironment': 'github-hosted',
                'buildTrigger': 'workflow_dispatch', 'issuer': 'https://token.actions.githubusercontent.com'}
    require(isinstance(results, list) and any(all(
        item.get('verificationResult', {}).get('signature', {}).get('certificate', {}).get(key) == value
        for key, value in expected.items()) for item in results), 'candidate certificate invocation mismatch')


def verify_assets(gh, repo, pin, directory, names):
    for name in sorted(names):
        path = directory / name
        results = gh_json(gh, 'attestation', 'verify', str(path), '--repo', repo,
                          '--signer-workflow', f'{repo}/{WORKFLOW}', '--signer-digest', pin['commit_sha'],
                          '--source-ref', f'refs/tags/{pin["version"]}', '--source-digest', pin['commit_sha'],
                          '--deny-self-hosted-runners', '--format', 'json')
        verify_certificate(results, repo, pin)
    for name in sorted(name for name in names if name.endswith('.manifest.json')):
        path = directory / name
        manifest = json.loads(path.read_text())
        bundle = directory / name.replace('.manifest.json', '.tar.gz')
        require(manifest['release_version'] == pin['version'] and manifest['commit_sha'] == pin['commit_sha'],
                'candidate manifest source mismatch')
        require(bundle.stat().st_size == manifest['bundle_bytes'] and digest(bundle) == manifest['bundle_sha256'],
                'candidate bundle mismatch')
        if 'manifest_sha256' in pin:
            require(digest(path) == pin['manifest_sha256'] and digest(bundle) == pin['bundle_sha256'],
                    'candidate asset differs from operator pin')


def extract_updater(directory, pin, target):
    prefix = directory / f'ai-image-factory-{pin["version"]}-{target}'
    manifest = json.loads(Path(str(prefix) + '.manifest.json').read_text())
    require(manifest['target_triple'] == target, 'candidate architecture mismatch')
    binaries = [item for item in manifest['files'] if item['path'] == 'bin/updated']
    require(len(binaries) == 1 and binaries[0]['mode'] == 0o755, 'updater missing from verified manifest')
    binary = binaries[0]
    output = directory / 'verified-updated'
    with tarfile.open(str(prefix) + '.tar.gz', 'r:gz') as bundle:
        members = [item for item in bundle if item.name == 'bin/updated']
        require(len(members) == 1 and members[0].isreg() and members[0].size == binary['bytes'],
                'candidate updater member invalid')
        with bundle.extractfile(members[0]) as source, output.open('xb') as destination:
            shutil.copyfileobj(source, destination, 65536)
    require(digest(output) == binary['sha256'], 'candidate updater digest mismatch')
    with output.open('rb') as stream:
        header = stream.read(20)
    machine = {'x86_64-unknown-linux-gnu': 62, 'aarch64-unknown-linux-gnu': 183}[target]
    require(len(header) == 20 and header[:6] == b'\x7fELF\x02\x01'
            and int.from_bytes(header[18:20], 'little') == machine, 'candidate updater ELF mismatch')
    output.chmod(0o755)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', required=True)
    parser.add_argument('--pin', type=Path, required=True)
    parser.add_argument('--target', choices=['publication', 'x86_64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu'], required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--extract-updater', action='store_true')
    args = parser.parse_args()
    require(not args.extract_updater or args.target != 'publication', 'bootstrap requires one architecture')
    gh = os.environ.get('AIF_UPDATE_GH_EXECUTABLE') or shutil.which('gh')
    require(gh is not None and Path(gh).is_absolute() and os.access(gh, os.X_OK),
            'existing absolute executable GitHub CLI required')
    if args.extract_updater:
        require(os.geteuid() == 0, 'bootstrap extraction requires root')
        root_protected(Path(gh).resolve())
        root_protected(args.pin)
        root_protected(args.output.parent)
    require(args.pin.stat().st_size <= 4096, 'candidate pin is too large')
    pin = json.loads(args.pin.read_text())
    verify_origin(gh, args.repo, pin, args.target)
    args.output.mkdir(mode=0o700, parents=False, exist_ok=False)
    archive = args.output / 'candidate.zip'
    download(gh, args.repo, pin, archive)
    names = expected_names(pin['version'], args.target)
    extract_assets(archive, args.output, names)
    verify_assets(gh, args.repo, pin, args.output, names)
    updater = extract_updater(args.output, pin, args.target) if args.extract_updater else None
    print(json.dumps({'verified': True, 'source': 'actions_candidate', 'run_id': pin['run_id'],
                      'run_attempt': pin['run_attempt'], 'artifact_id': pin['artifact_id'],
                      'assets': {name: digest(args.output / name) for name in sorted(names)},
                      'updater': str(updater) if updater else None}))


if __name__ == '__main__':
    main()
