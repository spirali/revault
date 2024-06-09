from revault.database import Database
from revault.entry import AnnounceResult
from revault import Key


def test_db_announce():
    db = Database("sqlite:///:memory:")
    db.init()

    key = Key("test", 1, {"x": 10}, 0)

    r = db.get_or_announce_entry(key)
    assert len(r) == 3
    assert isinstance(r[1], int)
    assert r[0] == AnnounceResult.COMPUTE_HERE
    assert r[2] is None

    r2 = db.get_or_announce_entry(key)
    assert r2 == (AnnounceResult.COMPUTING_ELSEWHERE, r[1], None)

    r2 = db.get_or_announce_entry(key)
    assert r2 == (AnnounceResult.COMPUTING_ELSEWHERE, r[1], None)

    db.finish_entry(r[1], "Hello", {})

    r2 = db.get_or_announce_entry(key)
    assert r2 == (AnnounceResult.FINISHED, r[1], "Hello")

    r2 = db.get_or_announce_entry(key)
    assert r2 == (AnnounceResult.FINISHED, r[1], "Hello")

    key2 = Key("test", 1, {"x": 11}, 0)
    t = db.get_or_announce_entry(key2)
    assert t[0] == AnnounceResult.COMPUTE_HERE
    assert t[2] is None
    assert t[1] != r[1]

    t2 = db.get_or_announce_entry(key2)
    assert t2 == (AnnounceResult.COMPUTING_ELSEWHERE, t[1], None)

    db.cancel_entry(t[1])

    t = db.get_or_announce_entry(key2)
    assert t[0] == AnnounceResult.COMPUTE_HERE
    assert t[2] is None
    assert t[1] != r[1]
