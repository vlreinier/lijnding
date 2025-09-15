import pytest
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from lijnding.core import Pipeline, stage, Context
from lijnding.orchestration.context import OrchestrationContext
from lijnding.orchestration.models import Run, StageRun, RunStatus, StageStatus, Base

# Fixture for an in-memory database session
@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


# Simple stages for testing
@stage(name="stage_a")
def stage_a(context: Context, item: int) -> int:
    return item + 1

@stage(name="stage_b")
def stage_b(context: Context, item: int) -> int:
    return item * 2

@stage(name="failing_stage")
def failing_stage(context: Context, item: int) -> int:
    raise ValueError("This stage is designed to fail")


def test_successful_run_is_recorded(db_session):
    """
    Tests that a successful pipeline run creates the correct records
    in the database with COMPLETED status.
    """
    # Arrange
    pipeline = Pipeline([stage_a, stage_b], name="successful_pipeline")
    orchestration_context = OrchestrationContext(db_session=db_session)

    # Act
    pipeline.collect(data=[1, 2, 3], context=orchestration_context)

    # Assert
    # Verify Run record
    run = db_session.query(Run).one()
    assert run.pipeline_name == "successful_pipeline"
    assert run.status == RunStatus.COMPLETED
    assert run.start_time is not None
    assert run.end_time is not None

    # Verify StageRun records
    stage_runs = db_session.query(StageRun).order_by(StageRun.stage_index).all()
    assert len(stage_runs) == 2

    # Stage A
    assert stage_runs[0].stage_name == "stage_a"
    assert stage_runs[0].stage_index == 0
    assert stage_runs[0].status == StageStatus.COMPLETED
    assert stage_runs[0].run_id == run.id
    assert stage_runs[0].start_time is not None
    assert stage_runs[0].end_time is not None

    # Stage B
    assert stage_runs[1].stage_name == "stage_b"
    assert stage_runs[1].stage_index == 1
    assert stage_runs[1].status == StageStatus.COMPLETED
    assert stage_runs[1].run_id == run.id
    assert stage_runs[1].start_time is not None
    assert stage_runs[1].end_time is not None


def test_failed_run_is_recorded(db_session):
    """
    Tests that a failed pipeline run is correctly marked as FAILED,
    and the stage that caused the failure is also marked FAILED.
    """
    # Arrange
    pipeline = Pipeline([stage_a, failing_stage, stage_b], name="failed_pipeline")
    orchestration_context = OrchestrationContext(db_session=db_session)

    # Act
    with pytest.raises(ValueError, match="This stage is designed to fail"):
        pipeline.collect(data=[1, 2, 3], context=orchestration_context)

    # Assert
    # Verify Run record
    run = db_session.query(Run).one()
    assert run.pipeline_name == "failed_pipeline"
    assert run.status == RunStatus.FAILED
    assert run.start_time is not None
    assert run.end_time is not None

    # Verify StageRun records
    stage_runs = db_session.query(StageRun).order_by(StageRun.stage_index).all()
    assert len(stage_runs) == 3

    # Stage A (should be completed)
    assert stage_runs[0].stage_name == "stage_a"
    assert stage_runs[0].stage_index == 0
    assert stage_runs[0].status == StageStatus.COMPLETED

    # Failing Stage (should be failed)
    assert stage_runs[1].stage_name == "failing_stage"
    assert stage_runs[1].stage_index == 1
    assert stage_runs[1].status == StageStatus.FAILED

    # Stage B (should be marked as SKIPPED because it never ran)
    assert stage_runs[2].stage_name == "stage_b"
    assert stage_runs[2].stage_index == 2
    assert stage_runs[2].status == StageStatus.SKIPPED
