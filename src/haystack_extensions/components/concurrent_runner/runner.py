from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, NamedTuple, Optional
from haystack import Pipeline
from haystack import component
from haystack.core.component import Component


class NamedComponent(NamedTuple):
    name: str
    component: Component


class NamedPipeline(NamedTuple):
    name: str
    pipeline: Pipeline


@component
class ConcurrentComponentRunner:
    """
    This component allows you to run multiple components concurrently in a thread pool.
    """

    def __init__(self, named_components: List[NamedComponent],  executor: Optional[ThreadPoolExecutor | None] = None):
        """
        :param named_components: List of NamedComponent instances
        :param executor: ThreadPoolExecutor instance if not provided a new one will be created with default values
        """
        if type(named_components) != list or any(
            [type(named_component) != NamedComponent for named_component in named_components]
        ):
            raise ValueError("named_components must be a list of NamedComponent instances")

        names = [named_component.name for named_component in named_components]
        if len(names) != len(set(names)):
            raise ValueError("All components must have unique names")

        input_types: Dict = {}
        for named_component in named_components:
            input_types[named_component.name] = {}
            socket_dict = named_component.component.__haystack_input__._sockets_dict
            
            for key, value in socket_dict.items():
                input_types[named_component.name][key] = value.type

        component.set_input_types(self, **input_types)

        output_types: Dict = {}
        for named_component in named_components:
            output_types[named_component.name] = {}

            socket_dict = named_component.component.__haystack_output__._sockets_dict

            for key, value in socket_dict.items():
                output_types[named_component.name][key] = value.type

        component.set_output_types(self, **output_types)

        self.components = named_components
        self.executor = executor

    def run(self, **inputs):
        if self.executor is None:
            with ThreadPoolExecutor() as executor:
                final_results = self._run_in_executor(executor, inputs)
        else:
            final_results = self._run_in_executor(self.executor, inputs)

        return {named_component.name: result for named_component, result in zip(self.components, final_results)}

    def _run_in_executor(self, executor, inputs):
        results = executor.map(lambda c: c[0].component.run(**inputs[c[1]]), zip(self.components, inputs.keys()))
        return [result for result in results]


@component
class ConcurrentPipelineRunner:
    """
    This component allows you to run multiple pipelines concurrently in a thread pool.
    """

    def __init__(self, named_pipelines: List[NamedPipeline], executor: Optional[ThreadPoolExecutor | None] = None):
        if type(named_pipelines) != list or any(
            [type(named_pipeline) != NamedPipeline for named_pipeline in named_pipelines]
        ):
            raise ValueError("named_components must be a list of NamedComponent instances")

        names = [named_pipeline.name for named_pipeline in named_pipelines]
        if len(names) != len(set(names)):
            raise ValueError("All components must have unique names")

        for named_pipeline in named_pipelines:
            component.set_input_type(self, named_pipeline.name, {named_pipeline.name: Dict[str, Any]})

        output_types = {}
        for named_pipeline in named_pipelines:
            output_types[named_pipeline.name] = Dict[str, Any]
        self.pipelines = named_pipelines
        self.executor = executor

    def run(self, **inputs):
        if self.executor is None:
            with ThreadPoolExecutor() as executor:
                final_results = self._run_in_executor(executor, inputs)
        else:
            final_results = self._run_in_executor(self.executor, inputs)

        return {named_pipeline.name: result for named_pipeline, result in zip(self.pipelines, final_results)}

    def _run_in_executor(self, executor: ThreadPoolExecutor, inputs: Dict[str, Any]):
        results = executor.map(lambda c: c[0].pipeline.run(data=inputs[c[1]]), zip(self.pipelines, inputs.keys()))
        return [result for result in results]
