from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, NamedTuple, Optional
from haystack import Pipeline
from haystack import component
from haystack.core.component import Component
from haystack.core.component.sockets import Sockets
from haystack.core.component import InputSocket, OutputSocket
from copy import deepcopy

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
        
        self._create_input_types(named_components)
        self._create_output_types(named_components)

        self.executor = executor
        self.components = named_components


    def _create_input_types(self, named_components: List[NamedComponent]):
        if not hasattr(self, "__haystack_input__"):
            self.__haystack_input__ = Sockets(self, {}, InputSocket)
        
        for named_component in named_components:
            socket_dict = deepcopy(named_component.component.__haystack_input__._sockets_dict)

            for name, value in socket_dict.items():
                value.name = f"{named_component.name}_{name}"
                self.__haystack_input__[f"{named_component.name}_{name}"] = value
    
    def _create_output_types(self, named_components: List[NamedComponent]):
        if not hasattr(self, "__haystack_output__"):
            self.__haystack_output__ = Sockets(self, {}, OutputSocket)
        
        for named_component in named_components:
            socket_dict = deepcopy(named_component.component.__haystack_output__._sockets_dict)
            
            for name, value in socket_dict.items():
                value.name = f"{named_component.name}_{name}"
                self.__haystack_output__[f"{named_component.name}_{name}"] = value

    def run(self, **inputs):
        if self.executor is None:
            with ThreadPoolExecutor() as executor:
                final_results = self._run_in_executor(executor, inputs)
        else:
            final_results = self._run_in_executor(self.executor, inputs)

        outputs = {}
        for named_component, result in zip(self.components, final_results):
            for key, value in result.items():
                outputs[f"{named_component.name}_{key}"] = value

        return outputs

    def _run_in_executor(self, executor, inputs):

        def _get_real_input(component_name, inputs):
            real_input = {}
            for key, value in inputs.items():
                if key.startswith(component_name):
                    real_input[key.replace(f"{component_name}_", "")] = value
            return real_input

        results = executor.map(lambda c: c.component.run(**_get_real_input(c.name, inputs)), self.components)
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
            raise ValueError("named_pipelines must be a list of NamedPipeline instances")

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
