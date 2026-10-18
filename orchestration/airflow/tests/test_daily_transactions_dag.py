import ast
from pathlib import Path
import unittest


class DailyTransactionsDagSourceTest(unittest.TestCase):
    def test_dag_is_parseable_and_has_required_controls(self):
        dag_path = Path(__file__).parents[1] / "dags" / "daily_transactions.py"
        source = dag_path.read_text(encoding="utf-8")
        ast.parse(source)

        for required in (
            'dag_id="banking_daily_transactions"',
            'schedule="0 1 * * *"',
            "catchup=False",
            "max_active_runs=1",
            "retry_exponential_backoff",
            "data_interval_start",
            "check_source_partition >> run_java_daily_batch >> validate_aggregates >> publish_report_metadata",
        ):
            self.assertIn(required, source)

        self.assertNotIn("change-me-warehouse", source)
        self.assertNotIn("change-me-writer", source)


if __name__ == "__main__":
    unittest.main()
