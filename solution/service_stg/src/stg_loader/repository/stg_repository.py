from datetime import datetime

from lib.pg import PgConnect


class StgRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def order_events_insert(self,
                            object_id: int,
                            object_type: str,
                            sent_dttm: datetime,
                            payload: str
                            ) -> None:

        with self._db.connection() as c:
            c.cursor().execute(
                    """
                        INSERT INTO stg.order_events (object_id, object_type, payload, sent_dttm) VALUES
                        (%(object_id)s, %(object_type)s, %(payload)s, %(sent_dttm)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET object_type = EXCLUDED.object_type, payload = EXCLUDED.payload, sent_dttm = EXCLUDED.sent_dttm;
                    """,
                    {
                        'object_id': object_id,
                        'object_type': object_type,
                        'sent_dttm': sent_dttm,
                        'payload': payload,
                    }
            )