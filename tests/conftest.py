"""Package-wide test configuration."""
# external
from _pytest.config import Config


def pytest_configure(config: Config) -> None:
    """Pytest configuration hook."""
    config.addinivalue_line("markers", "slow: mark as a slow test.")
