import importlib

def test_transform_module_has_a_public_entrypoint():
    module = importlib.import_module("whoisdrift.kernel")
    assert callable(getattr(module, "process", None))
    result = module.process({"id": "sample", "name": "item", "value": 7, "status": "active", "ts": "t1"})
    assert isinstance(result, dict)
