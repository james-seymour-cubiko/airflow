import asyncio
from typing import List

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from airflow.exceptions import AirflowException

class WaitForDagRunTrigger(BaseTrigger):
    def __init__(
        self,
        dag_id: str,
        run_id: str,
        allowed_states: List[DagRunState],
        failed_states: List[DagRunState],
        poke_interval: int = 60,
    ):
        self.dag_id = dag_id
        self.run_id = run_id
        self.allowed_states = allowed_states
        self.failed_states = failed_states
        self.poke_interval = poke_interval

    def serialize(self):
        return (
            "airflow.triggers.wait_for_dagrun.WaitForDagRunTrigger",
            {
                "dag_id": self.dag_id,
                "run_id": self.run_id,
                "poke_interval": self.poke_interval,
                "allowed_states": self.allowed_states,
                "failed_states": self.failed_states,
            },
        )

    async def run(self):
        dag_run = DagRun.find(dag_id=self.dag_id, run_id=self.run_id)[0]

        while True:
            self.log.info(
                "Waiting for %s on %s to become allowed state %s ...",
                self.dag_id,
                dag_run.execution_date,
                self.allowed_states,
            )
            await asyncio.sleep(self.poke_interval)

            dag_run.refresh_from_db()
            state = dag_run.state
            if state in self.failed_states:
                raise AirflowException(
                    f"{self.dag_id}_{self.run_id} failed with failed states {state}"
                )
            if state in self.allowed_states:
                yield TriggerEvent(
                    f"{self.dag_id}_{self.run_id} finished with allowed state {state}"
                )
