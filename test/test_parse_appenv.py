"""
Tests parse_appenv
"""

from brewblox_history.models import ServiceConfig
from parse_appenv import parse_cmd_args


def test_args_match_config():
    # Every CMD argument must map to a ServiceConfig field:
    # unknown settings would otherwise be ignored silently
    args, unknown = parse_cmd_args([])
    assert unknown == []
    assert set(vars(args)) <= set(ServiceConfig.model_fields)


def test_parse_values():
    args, unknown = parse_cmd_args(['--victoria-host', 'db', '--victoria-timeout', '2m', '--debug', '--bogus'])
    assert unknown == ['--bogus']
    assert args.victoria_host == 'db'
    assert args.victoria_timeout == '2m'
    assert args.debug is True
    assert args.debugger is False

    config = ServiceConfig(_env_file=None, **{k: v for k, v in vars(args).items() if v is not None and v is not False})
    assert config.victoria_host == 'db'
    assert config.victoria_timeout.total_seconds() == 120
