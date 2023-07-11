from datetime import datetime
from logging import getLogger
from typing import Optional, Any

log = getLogger(__name__)


class ETLTaskManager:
    def __init__(self, db_connection: Any = None):
        self.db_connection = db_connection

    def create_new_etl(
        self,
        run_id: str,
        run_created_dttm: datetime,
        run_dag_name: str,
        run_finished_dttm: Optional[datetime] = None,
        run_processed_dttm: Optional[datetime] = None,
    ) -> None:
        query = """
            INSERT INTO etl_registry
            (
                run_id,
                run_status_code,
                run_created_dttm,
                run_finished_dttm,
                run_processed_dttm,
                run_dag_name,
            )
            VALUES (%s, %s, %s, %s, %s, %s);
        """  # noqa: WPS323
        params = (
            (
                run_id,
                "R",
                run_created_dttm,
                run_finished_dttm,
                run_processed_dttm,
                run_dag_name,
            ),
        )

        # TODO: impl db logic here ...
        log.info(f"DB CONN:\n {self.db_connection}")
        log.info(f"PARAMS:\n {params}")
        log.info(f"SQL:\n {query}")

    def finish_etl(self, run_id: str, status_code: str) -> None:
        created_dttm = datetime.now()

        query = """
            UPDATE etl_registry
            SET run_processed_dttm = %s,
                run_finished_dttm = %s,
                run_status_code = %s
            WHERE run_id = %s
        """  # noqa: WPS323

        params = (
            created_dttm,
            created_dttm,
            status_code,
            run_id,
        )

        # TODO: impl db logic here ...
        log.info(f"DB CONN:\n {self.db_connection}")
        log.info(f"PARAMS:\n {params}")
        log.info(f"SQL:\n {query}")
