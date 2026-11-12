"""Spec bundles ship in the package (``formal/`` is mapped to ``valkey_fuzzer/formal``) and are found by name."""

import os
import re
import shutil
from pathlib import Path

import pytest
from setuptools import find_packages

from valkey_fuzzer.spec import SpecBundleError, find_bundle

ROOT = Path(__file__).resolve().parents[1]


def test_pyproject_lists_every_package_and_the_bundles():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    block = re.search(r"^\[tool\.setuptools\]\npackages = \[(.*?)\]", text, flags=re.MULTILINE | re.DOTALL)
    listed = sorted(re.findall(r'"([^"]+)"', block.group(1)))
    assert listed == sorted([*find_packages(where=str(ROOT), include=["valkey_fuzzer*"]), "valkey_fuzzer.formal"])


def test_the_data_files_of_the_components_ship_with_them():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    shipped = re.search(r'^"valkey_fuzzer\.components" = \[(.*)\]$', text, flags=re.MULTILINE).group(1)
    patterns = re.findall(r'"([^"]+)"', shipped)
    components = ROOT / "valkey_fuzzer" / "components"
    data = [path for path in components.rglob("*") if path.is_file() and path.suffix in (".yaml", ".json")]
    assert len(data) > 20
    for path in data:
        assert any(path.match(pattern) for pattern in patterns), path


def test_bundles_are_found_by_directory_or_by_protocol_name_on_the_spec_path(tmp_path, monkeypatch):
    fixtures = ROOT / "tests" / "fixtures" / "spec"
    monkeypatch.delenv("VALKEY_FUZZER_SPEC_PATH", raising=False)
    assert find_bundle(str(fixtures / "register")) == fixtures / "register"
    assert find_bundle("cluster-election") == ROOT / "formal" / "cluster-election"
    with pytest.raises(SpecBundleError, match=r"no spec bundle named 'register'"):
        find_bundle("register")
    with pytest.raises(SpecBundleError, match="no spec bundle at"):
        find_bundle(str(tmp_path / "missing"))
    # The search path comes before the shipped bundles.
    shutil.copytree(fixtures / "register", tmp_path / "cluster-election")
    monkeypatch.setenv("VALKEY_FUZZER_SPEC_PATH", f"{tmp_path}{os.pathsep}{fixtures}")
    assert find_bundle("register") == fixtures / "register"
    assert find_bundle("cluster-election") == tmp_path / "cluster-election"
    # A protocol name is never a directory of the current one; a path is.
    monkeypatch.delenv("VALKEY_FUZZER_SPEC_PATH")
    monkeypatch.chdir(tmp_path)
    assert find_bundle("cluster-election") == ROOT / "formal" / "cluster-election"
    assert find_bundle("./cluster-election") == tmp_path / "cluster-election"
