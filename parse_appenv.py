import argparse
import shlex
import sys


def parse_cmd_args(raw_args: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser()
    # Every argument maps to a ServiceConfig field
    parser.add_argument('-n', '--name')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--debugger', action='store_true')

    parser.add_argument('--mqtt-protocol')
    parser.add_argument('--mqtt-host')
    parser.add_argument('--mqtt-port')

    parser.add_argument('--redis-host')
    parser.add_argument('--redis-port')

    parser.add_argument('--victoria-protocol')
    parser.add_argument('--victoria-host')
    parser.add_argument('--victoria-port')
    parser.add_argument('--victoria-path')
    parser.add_argument('--victoria-timeout')

    parser.add_argument('--dense-protocol')
    parser.add_argument('--dense-host')
    parser.add_argument('--dense-port')
    parser.add_argument('--dense-path')
    parser.add_argument('--dense-retention')
    parser.add_argument('--dense-margin')

    parser.add_argument('--history-topic')
    parser.add_argument('--datastore-topic')

    parser.add_argument('--ranges-interval')
    parser.add_argument('--metrics-interval')
    parser.add_argument('--minimum-step')

    parser.add_argument('--query-duration-default')
    parser.add_argument('--query-desired-points')

    parser.add_argument('--sparse-interval')
    parser.add_argument('--downsample-lag')
    parser.add_argument('--downsample-interval')
    parser.add_argument('--downsample-chunk')
    parser.add_argument('--downsample-max-lag')
    parser.add_argument('--query-latency')
    parser.add_argument('--csv-chunk-dense')
    parser.add_argument('--csv-chunk-sparse')
    parser.add_argument('--follow-up-step-max')

    return parser.parse_known_args(raw_args)


if __name__ == '__main__':
    args, unknown = parse_cmd_args(sys.argv[1:])
    if unknown:
        print(f'WARNING: ignoring unknown CMD arguments: {unknown}', file=sys.stderr)
    output = [
        f'brewblox_history_{k}={shlex.quote(str(v))}' for k, v in vars(args).items() if v is not None and v is not False
    ]
    print(*output, sep='\n')
