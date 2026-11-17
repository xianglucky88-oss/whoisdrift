from whoisdrift.store import Store

def test_store_identity_and_checkpoint():
    con = Store().connect()
    st = Store(con)
    st.reset()
    try:
        record = {"id": "sample", "ts": "t1", "v": 1}
        assert st.upsert(record) is True
        assert st.upsert(record) is False
        st.set_watermark("checkpoint")
        assert st.watermark() == "checkpoint"
    finally:
        st.reset()
        con.close()
