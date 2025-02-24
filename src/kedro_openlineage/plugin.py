from __future__ import annotations

import datetime as dt
import typing as t

import structlog
from kedro.framework.context import KedroContext
from kedro.framework.hooks import hook_impl
from kedro.io.core import CatalogProtocol
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    Dataset,
    Job,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.uuid import generate_new_uuid

PRODUCER = "kedro.org"

logger = structlog.get_logger()


class RunParams(t.TypedDict):
    session_id: str
    project_path: str
    env: str
    kedro_version: str
    tags: list[str] | None
    from_nodes: list[str] | None
    to_nodes: list[str] | None
    node_names: list[str] | None
    from_inputs: list[str] | None
    to_outputs: list[str] | None
    load_versions: list[str] | None
    extra_params: dict[str, t.Any] | None
    pipeline_name: str
    namespace: str | None
    runner: str


class OpenLineageKedroHook:
    @hook_impl
    def after_context_created(self, context: KedroContext):
        logger.debug("Creating OpenLineage client")
        self._client = OpenLineageClient()

    @hook_impl
    def before_pipeline_run(
        self, run_params: RunParams, pipeline: Pipeline, catalog: CatalogProtocol
    ) -> None:
        # A Job is a process that consumes or produces Datasets.
        # This is abstract, and can map to different things
        # in different operational contexts.
        # For example, a job could be a task in a workflow orchestration system.
        self._job = Job(
            namespace="kedro",
            name=run_params["pipeline_name"] or "__default__",
        )

        # A Run is an instance of a Job that represents one of its occurrences in time.
        self._run = Run(
            # It has to be a UUID, so this won't work
            # runId=run_params["session_id"],
            runId=str(generate_new_uuid()),
        )

        logger.debug("Emitting OpenLineage run event")
        run_event = RunEvent(
            eventType=RunState.START,
            eventTime=dt.datetime.now().isoformat(),
            run=self._run,
            job=self._job,
            producer=PRODUCER,
        )

        self._client.emit(run_event)

    @hook_impl
    def before_node_run(
        self,
        node: Node,
        catalog: CatalogProtocol,
        inputs: dict[str, str],
    ) -> None:
        # A Dataset is an abstract representation of data.
        # Build list of inputs as OpenLineage datasets
        inputs = [
            Dataset(
                namespace="kedro",
                name=name,
            )
            for name in inputs
        ]

        logger.debug("Emitting OpenLineage run event")
        self._client.emit(
            RunEvent(
                eventType=RunState.RUNNING,
                eventTime=dt.datetime.now().isoformat(),
                run=self._run,
                job=self._job,
                producer=PRODUCER,
                inputs=inputs,
            )
        )

    @hook_impl
    def after_node_run(
        self,
        node: Node,
        catalog: CatalogProtocol,
        inputs: dict[str, str],
        outputs: dict[str, str],
    ) -> None:
        # Build list of outputs as OpenLineage datasets
        outputs = [
            Dataset(
                namespace="kedro",
                name=name,
            )
            for name in outputs
        ]

        logger.debug("Emitting OpenLineage run event")
        self._client.emit(
            RunEvent(
                eventType=RunState.RUNNING,
                eventTime=dt.datetime.now().isoformat(),
                run=self._run,
                job=self._job,
                producer=PRODUCER,
                outputs=outputs,
            )
        )

    @hook_impl
    def after_pipeline_run(
        self,
        run_params: RunParams,
        run_result: dict[str, t.Any],
        pipeline: Pipeline,
        catalog: CatalogProtocol,
    ) -> None:
        logger.debug("Emitting OpenLineage run event")
        self._client.emit(
            RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=dt.datetime.now().isoformat(),
                run=self._run,
                job=self._job,
                producer=PRODUCER,
            )
        )

    @hook_impl
    def on_pipeline_error(
        self,
        error: Exception,
        run_params: RunParams,
        pipeline: Pipeline,
        catalog: CatalogProtocol,
    ) -> None:
        logger.debug("Emitting OpenLineage run event")
        self._client.emit(
            RunEvent(
                eventType=RunState.FAIL,
                eventTime=dt.datetime.now().isoformat(),
                run=self._run,
                job=self._job,
                producer=PRODUCER,
            )
        )


hooks = OpenLineageKedroHook()
