import time
import client


local_client = client.CustomCache(host="127.0.0.1", port=6379, db=0)


def test_get_set():
    local_client.set("abv", "cde")
    ret = local_client.get("abv")
    assert ret == b"cde"


def test_delete():
    local_client.set("abv", "cde")
    ret = local_client.delete("abv")
    assert ret == 1


def test_ttl():
    local_client.set("abv", "cde", px=200)
    time.sleep(0.3)
    ret = local_client.get("abv")
    assert ret is None


def test_mtsadd_srem():
    ret = local_client.mtsadd("abc", "a")
    assert ret == 1

    # check duplicate
    ret = local_client.mtsadd("abc", "a")
    assert ret == 0

    ret = local_client.mtsmembers("abc")
    assert b"a" in ret

    # multiple
    ret = local_client.mtsadd("abc", "b", "c", "d")
    assert ret == 3

    ret = local_client.mtsmembers("abc")
    assert b"b" in ret
    assert b"a" in ret
    assert b"c" in ret
    assert b"d" in ret

    ret = local_client.mtsrem("abc", "a", "b", "c", "d")
    assert ret == 4

    ret = local_client.mtsmembers("abc")
    assert len(ret) == 0
