import importlib

def test_import():
    pkg = importlib.import_module('pytho_spark')
    assert dir(pkg)
    assert pkg.__name__ == 'pytho_spark'
