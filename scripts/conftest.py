import pytest

# Load the integration tests plugin, which contains the common
# fixtures. It would be nice to just put this line in the test module
# itself, and do away with this conftest.py file entirely, but sadly
# that doesn't seem to work: it looks like that appraoch loads the
# plugin too late for the pytest_addoption in integration.py to take
# effect
pytest_plugins = [ "minidaqapp.integration_tests.integration" ]
