from calibre_core.run_scope import belongs_to_run, message_run_scope


def test_historical_raw_messages_are_isolated_by_run_id():
    matching = {
        "run_id": "replay-a",
        "source_mode": "HISTORICAL_REPLAY",
    }
    other = {
        "run_id": "replay-b",
        "source_mode": "HISTORICAL_REPLAY",
    }

    assert belongs_to_run(matching, "replay-a")
    assert not belongs_to_run(other, "replay-a")


def test_replay_evidence_and_sink_envelopes_keep_nested_run_scope():
    output = {
        "event": {
            "provenance": {
                "run_id": "replay-b",
                "source_mode": "HISTORICAL_REPLAY",
            }
        }
    }

    assert message_run_scope(output) == ("replay-b", "HISTORICAL_REPLAY")
    assert not belongs_to_run(output, "replay-a")
    assert belongs_to_run(output, "replay-b")


def test_verdict_run_scope_is_recovered_from_session_instance_id():
    verdict = {
        "decision_instance_id": "session#race:replay-b|driver:1|contract-x",
    }

    assert message_run_scope(verdict) == ("replay-b", None)
    assert not belongs_to_run(verdict, "replay-a")
    assert belongs_to_run(verdict, "replay-b")


def test_live_feed_is_not_filtered_by_an_ingestion_instance_run_id():
    live = {"run_id": "producer-instance-1", "source_mode": "LIVE"}

    assert belongs_to_run(live, "backend-live-1")

