"""
Tests brewblox_history.__main__
"""

from brewblox_history import app as main

TESTED = main.__name__


def test_main(event_loop, mocker, sys_args):
    def mock_run(app, coro):
        event_loop.run_until_complete(coro)

    mocker.patch.object(main.service.sys, 'argv', sys_args)
    mocker.patch(TESTED + '.service.run_app', mock_run)
    main.main()
