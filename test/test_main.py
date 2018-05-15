"""
Tests brewblox_history.__main__
"""

from brewblox_history import __main__ as main

TESTED = main.__name__


def test_main(mocker, sys_args):
    mocker.patch.object(main.service.sys, 'argv', sys_args)
    mocker.patch(TESTED + '.service.run')
    main.main()
