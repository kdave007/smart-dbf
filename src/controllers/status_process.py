from tracemalloc import start
from src.models.pending_server_sql import ServerPendingSQL

class StatusProcess:
    def __init__(self) -> None:
        self.pending_sql = ServerPendingSQL()

    def _get_waiting_line(self, field_id, table_name, version, date_range):

        start_date = date_range.get('from')
        end_date = date_range.get('to')

        results = self.pending_sql.select_by_update_date_queued(field_id, table_name, version.get('id'), start_date, end_date)

        print(f"results {len(results)} : {results}")