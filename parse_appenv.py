import argparse
import shlex
import sys


def parse_cmd_args(raw_args: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name')
    parser.add_argument('--debug', action='store_true')

    parser.add_argument('--ranges-interval')
    parser.add_argument('--metrics-interval')
    parser.add_argument('--redis-url')
    parser.add_argument('--victoria-url')
    parser.add_argument('--history-topic')
    parser.add_argument('--datastore-topic')
    parser.add_argument('--minimum-step')

    args, _ = parser.parse_known_args(raw_args)
    return args


if __name__ == '__main__':
    args = vars(parse_cmd_args(sys.argv[1:]))
    output = [f'brewblox_{k}={shlex.quote(str(v))}'
              for k, v in args.items()
              if v is not None and v is not False]
    print(*output, sep='\n')
