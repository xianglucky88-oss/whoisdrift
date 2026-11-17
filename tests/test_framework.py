from whoisdrift.framework import content_hash, dedup

def test_record_identity_is_stable():
    first = {"id": "1", "v": 2}
    same = {"id": "1", "v": 2}
    other = {"id": "1", "v": 3}
    assert content_hash(first) == content_hash(same)
    values, duplicate_count = dedup([first, same, other])
    assert len(values) == 2
    assert duplicate_count == 1
