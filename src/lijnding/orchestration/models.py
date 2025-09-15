import datetime
from datetime import timezone
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Enum as SQLAlchemyEnum,
)
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
import enum


class RunStatus(enum.Enum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class StageStatus(enum.Enum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


Base = declarative_base()


class Run(Base):
    __tablename__ = "runs"

    id = Column(Integer, primary_key=True)
    pipeline_name = Column(String, nullable=False)
    status = Column(SQLAlchemyEnum(RunStatus), nullable=False)
    start_time = Column(DateTime, default=lambda: datetime.datetime.now(timezone.utc))
    end_time = Column(DateTime)

    stages = relationship("StageRun", back_populates="run", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Run(id={self.id}, pipeline_name='{self.pipeline_name}', status='{self.status}')>"


class StageRun(Base):
    __tablename__ = "stage_runs"

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("runs.id"), nullable=False)
    stage_name = Column(String, nullable=False)
    stage_index = Column(Integer, nullable=False)
    status = Column(SQLAlchemyEnum(StageStatus), nullable=False)
    start_time = Column(DateTime, default=lambda: datetime.datetime.now(timezone.utc))
    end_time = Column(DateTime)

    run = relationship("Run", back_populates="stages")

    def __repr__(self):
        return f"<StageRun(id={self.id}, stage_name='{self.stage_name}', status='{self.status}')>"


def create_db_session(db_url="sqlite:///:memory:"):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()
