"""
Pub/Sub -> PostgreSQL

Stores GCS events in file_events.

Ack only after successful DB commit.
If insert fails, Pub/Sub retries and eventually DLQ handles it.
"""

import json
import os

from google.cloud import pubsub_v1

from common import db, log

SUBSCRIPTION = os.environ["PUBSUB_SUBSCRIPTION"]


def handle(msg):

    attrs = msg.attributes

    event_type = attrs.get("eventType")

    log.info(
        "EVENT=%s OBJECT=%s GENERATION=%s",
        event_type,
        attrs.get("objectId"),
        attrs.get("objectGeneration")
    )


    if event_type == "OBJECT_FINALIZE":
        operation = "CREATE"

    elif event_type in ("OBJECT_DELETE", "OBJECT_ARCHIVE"):
        operation = "DELETE"

    else:
        msg.ack()
        return

    bucket = attrs["bucketId"]
    object_name = attrs["objectId"]
    generation = attrs.get("objectGeneration", "0")

    try:
        payload = json.loads(msg.data or b"{}")
        size_bytes = int(payload.get("size", 0))
    except Exception:
        size_bytes = 0

    event_id = f"{bucket}:{object_name}:{generation}:{operation}"

    conn = None

    try:

        conn = db()

        with conn:
            with conn.cursor() as cur:

                cur.execute(
                    """
                    INSERT INTO event_dedupe
                    (
                        bucket_name,
                        object_name,
                        generation,
                        operation
                    )
                    VALUES
                    (
                        %s,
                        %s,
                        %s,
                        %s
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        bucket,
                        object_name,
                        generation,
                        operation
                    )
                )

                if cur.rowcount > 0:

                    cur.execute(
                        """
                        INSERT INTO file_events
                        (
                            event_id,
                            bucket_name,
                            object_name,
                            generation,
                            size_bytes,
                            operation,
                            status
                        )
                        VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            'NEW'
                        )
                        ON CONFLICT (event_id) DO NOTHING
                        """,
                        (
                            event_id,
                            bucket,
                            object_name,
                            generation,
                            size_bytes,
                            operation
                        )
                    )

        msg.ack()

    except Exception:

        log.exception(
            "insert failed for %s/%s",
            bucket,
            object_name
        )

        msg.nack()

    finally:

        if conn:
            try:
                conn.close()
            except Exception:
                pass


def main():

    subscriber = pubsub_v1.SubscriberClient()

    flow_control = pubsub_v1.types.FlowControl(
        max_messages=int(
            os.getenv(
                "MAX_INFLIGHT",
                "500"
            )
        )
    )

    log.info(
        "listening on %s",
        SUBSCRIPTION
    )

    future = subscriber.subscribe(
        SUBSCRIPTION,
        callback=handle,
        flow_control=flow_control
    )

    future.result()


if __name__ == "__main__":
    main()