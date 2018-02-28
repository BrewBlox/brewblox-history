"""
Tests brewblox_history.__main__
"""

from brewblox_history import __main__ as main

TESTED = main.__name__


def test_main(mocker):
    service_mock = mocker.patch(TESTED + '.service')
    events_mock = mocker.patch(TESTED + '.events')
    influx_mock = mocker.patch(TESTED + '.influx')

    main.main()

    assert service_mock.create.call_count == 1
    assert service_mock.furnish.call_count == 1
    assert service_mock.run.call_count == 1
    assert events_mock.setup.call_count == 1
    assert influx_mock.setup.call_count == 1
