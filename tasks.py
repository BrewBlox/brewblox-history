from pathlib import Path

from invoke import Context, task

ROOT = Path(__file__).parent.resolve()
IMAGE = 'ghcr.io/brewblox/brewblox-history'


@task
def testclean(ctx: Context) -> None:
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
def image(ctx: Context, tag: str = 'local', push: bool = False) -> None:
    with ctx.cd(ROOT):
        ctx.run(f'docker build --load -t {IMAGE}:{tag} .')
        if push:
            ctx.run(f'docker push {IMAGE}:{tag}')


@task
def buildx(
    ctx: Context,
    tag: str = 'local',
    push: bool = False,
    platform: str = 'linux/amd64,linux/arm/v7,linux/arm64/v8',
) -> None:
    # Without --push, a multi-platform build has nowhere to go and is discarded
    push_flag = '--push' if push else ''
    with ctx.cd(ROOT):
        ctx.run(f'docker buildx build --platform {platform} {push_flag} -t {IMAGE}:{tag} .')
