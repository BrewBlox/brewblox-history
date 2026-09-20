from pathlib import Path

from invoke import Context, task

ROOT = Path(__file__).parent.resolve()
IMAGE = 'ghcr.io/brewblox/brewblox-history'


@task
def testclean(ctx: Context):
    """
    Cleans up leftover test containers.
    Container cleanup is normally done in test fixtures.
    This is skipped if debugged tests are stopped halfway.
    """
    result = ctx.run('docker ps -aq --filter "name=pytest"', hide='stdout')
    containers = result.stdout.strip().replace('\n', ' ')
    if containers:
        ctx.run(f'docker rm -f {containers}')


@task
def build(ctx: Context):
    """
    Builds the sdist and requirements file consumed by the Dockerfile.
    """
    with ctx.cd(ROOT):
        ctx.run('rm -rf dist')
        ctx.run('uv build --sdist')
        ctx.run('uv export --no-hashes --no-dev --no-emit-project -o dist/requirements.txt')


@task(pre=[build])
def image(ctx: Context, tag='local', push=False):
    with ctx.cd(ROOT):
        ctx.run(f'docker build --load -t {IMAGE}:{tag} .')
        if push:
            ctx.run(f'docker push {IMAGE}:{tag}')


@task(pre=[build])
def buildx(ctx: Context, tag='local', push=False, platform='linux/amd64,linux/arm/v7,linux/arm64/v8'):
    # Without --push, a multi-platform build has nowhere to go and is discarded
    push_flag = '--push' if push else ''
    with ctx.cd(ROOT):
        ctx.run(f'docker buildx build --platform {platform} {push_flag} -t {IMAGE}:{tag} .')
