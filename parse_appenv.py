import argparse
import shlex
import sys


def parse_cmd_args(raw_args: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name')
    parser.add_argument('--debug', action='store_true')

    parser.add_argument('--redis-url')
    parser.add_argument('--victoria-url')
    parser.add_argument('--history-topic')
    parser.add_argument('--datastore-topic')
    parser.add_argument('--ranges-interval')
    parser.add_argument('--metrics-interval')
    parser.add_argument('--minimum-step')

    return parser.parse_known_args(raw_args)


if __name__ == '__main__':
    args, unknown = parse_cmd_args(sys.argv[1:])
    if unknown:
        print(f'WARNING: ignoring unknown CMD arguments: {unknown}', file=sys.stderr)
    output = [f'brewblox_{k}={shlex.quote(str(v))}'
              for k, v in vars(args).items()
              if v is not None and v is not False]
    print(*output, sep='\n')
