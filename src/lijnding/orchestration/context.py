from __future__ import annotations
import datetime
from datetime import timezone
import traceback
from typing import Optional, TYPE_CHECKING

from sqlalchemy.orm import Session

from ..core.context import Context
from .models import Run, StageRun, RunStatus, StageStatus, create_db_session

if TYPE_CHECKING:
    from ..core.pipeline import Pipeline
    from ..core.stage import Stage


class OrchestrationContext(Context):
    """
    A context that records pipeline run history to a database.
    """

    def __init__(self, db_session: Optional[Session] = None, **kwargs):
        super().__init__(**kwargs)
        self.db_session = db_session or create_db_session()
        self.current_run: Optional[Run] = None
        self.current_stage_run: Optional[StageRun] = None
        self.failed_stage_index: Optional[int] = None

    def on_run_start(self, pipeline: "Pipeline"):
        """
        Creates a new Run record in the database and sets its status to RUNNING.
        """
        self.current_run = Run(
            pipeline_name=pipeline.name,
            status=RunStatus.RUNNING,
            start_time=datetime.datetime.now(timezone.utc),
        )
        self.db_session.add(self.current_run)
        self.db_session.commit()
        self.logger.info("Orchestration: Run started.", run_id=self.current_run.id)

    def on_stage_start(self, stage: "Stage", index: int):
        """
        Creates a new StageRun record for the current run with a status of RUNNING.
        """
        if not self.current_run:
            return

        stage_run = StageRun(
            run_id=self.current_run.id,
            stage_name=stage.name,
            stage_index=index,
            status=StageStatus.RUNNING,
            start_time=datetime.datetime.now(timezone.utc),
        )
        self.db_session.add(stage_run)
        self.db_session.commit()
        self.current_stage_run = stage_run
        self.logger.info(
            "Orchestration: Stage started.",
            run_id=self.current_run.id,
            stage_run_id=stage_run.id,
            stage_name=stage.name,
            stage_index=index,
        )

    def on_stage_error(self, stage: "Stage", exception: Exception):
        """
        Marks the given stage as FAILED in the database.
        This is called by runners when they catch an exception.
        """
        if not self.current_run:
            return

        # Find the StageRun that corresponds to the stage that failed.
        # This assumes stage names are unique within a pipeline.
        stage_run = (
            self.db_session.query(StageRun)
            .filter_by(run_id=self.current_run.id, stage_name=stage.name)
            .first()
        )

        if stage_run:
            stage_run.status = StageStatus.FAILED
            stage_run.end_time = datetime.datetime.now(timezone.utc)
            self.failed_stage_index = stage_run.stage_index
            self.db_session.commit()
            self.logger.error(
                "Orchestration: Stage failed (reported by runner).",
                run_id=self.current_run.id,
                stage_run_id=stage_run.id,
                stage_name=stage.name,
                exception=str(exception),
            )

    def on_run_finish(self, pipeline: "Pipeline", exception: Optional[Exception] = None):
        """
        Finalizes the run. The `on_stage_error` hook is responsible for marking
        the specific failed stage. This method just cleans up the rest.
        """
        if not self.current_run:
            self.logger.warning("Orchestration: on_run_finish called without a run.")
            return

        try:
            final_run_status = RunStatus.FAILED if exception else RunStatus.COMPLETED

            # The on_stage_error hook should have already marked the failed stage.
            # We just clean up any other stages that were left in a RUNNING state.
            running_stages = (
                self.db_session.query(StageRun)
                .filter_by(run_id=self.current_run.id, status=StageStatus.RUNNING)
                .all()
            )

            for stage_run in running_stages:
                # If the run failed, any stage with an index greater than the failed
                # stage should be marked as SKIPPED. Otherwise, it completed.
                if self.failed_stage_index is not None and stage_run.stage_index > self.failed_stage_index:
                    stage_run.status = StageStatus.SKIPPED
                else:
                    stage_run.status = StageStatus.COMPLETED

                stage_run.end_time = datetime.datetime.now(timezone.utc)
                self.logger.info(
                    "Orchestration: Stage finished.",
                    run_id=self.current_run.id,
                    stage_run_id=stage_run.id,
                    stage_name=stage_run.stage_name,
                )

            self.current_run.status = final_run_status
            self.current_run.end_time = datetime.datetime.now(timezone.utc)
            self.db_session.commit()
            self.logger.info("Orchestration: Run finished.", run_id=self.current_run.id, status=final_run_status.value)

        except Exception as db_exc:
            self.logger.critical(
                "Orchestration: CRITICAL - Failed to update database on run finish.",
                run_id=self.current_run.id,
                error=str(db_exc),
                original_exception=str(exception)
            )
            self.db_session.rollback()
        finally:
            self.db_session.close()
