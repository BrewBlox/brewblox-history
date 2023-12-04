from pathlib import Path

from invoke import Context, task

ROOT = Path(__file__).parent.resolve()


@task
def testclean(ctx: Context):
    """
    Cleans up leftover test containers.
    Container cleanup is normally done in test fixtures.
    This is skipped if debugged tests are stopped halfway.
    """
    ctx.run('docker rm -f $(docker ps -aq --filter "name=pytest")')


@task
def build(ctx: Context):
    with ctx.cd(ROOT):
        ctx.run('rm -rf dist')
        ctx.run('poetry build --format sdist')
        ctx.run('poetry export --without-hashes -f requirements.txt -o dist/requirements.txt')


@task(pre=[build])
def local_docker(ctx: Context, tag='local'):
    with ctx.cd(ROOT):
        ctx.run(f'docker build -t ghcr.io/brewblox/brewblox-history:{tag} .')
