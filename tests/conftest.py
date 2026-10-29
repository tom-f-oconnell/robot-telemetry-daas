from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def msgs_dir(repo_root: Path) -> Path:
    return repo_root / "contracts" / "msgs"


@pytest.fixture(scope="session")
def registry_yml(repo_root: Path) -> Path:
    return repo_root / "contracts" / "registry.yml"


@pytest.fixture(scope="session")
def msg_registry(msgs_dir: Path):
    from rtdaas.contracts.msgspec import MsgRegistry

    return MsgRegistry.load_dir(msgs_dir)
