from typing import Any, Dict, List, NamedTuple, Tuple
from haystack import Pipeline
import time

from haystack import component
from haystack.core.component import Component

from concurrent.futures import ThreadPoolExecutor

class NamedComponent(NamedTuple):
    name: str
    component: Component

@component
class ConcurrentComponentRunner:
    """
    This component allows you to run multiple components concurrently in a thread pool.
    """

    def __init__(self, named_components: List[NamedComponent]):
        if type(named_components) != list or any([type(named_component) != NamedComponent for named_component in named_components]):
            raise ValueError("named_components must be a list of NamedComponent instances")
        
        names = [named_component.name for named_component in named_components]
        if len(names) != len(set(names)):
            raise ValueError("All components must have unique names")

        for named_component in named_components: 
            socket_dict = named_component.component.__haystack_input__._sockets_dict
            for key, value in socket_dict.items():
                component.set_input_type(self, named_component.name, {key: value.type})
        
        output_types = {}
        for named_component in named_components: 
            output_types[named_component.name] = {}

            socket_dict = named_component.component.__haystack_output__._sockets_dict

            for key, value in socket_dict.items():
                output_types[named_component.name][key] = value.type
                
        component.set_output_types(self, **output_types)

        self.components = named_components

    def run(self, **inputs):
        final_results = [] 

        with ThreadPoolExecutor() as executor:
            results = executor.map(lambda c: c[0].component.run(**inputs[c[1]]), zip(self.components, inputs.keys()))

            for result in results:
                final_results.append(result)

        return {named_component.name: result for named_component, result in zip(self.components, final_results)}