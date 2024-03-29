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
    A component that runs multiple components in parallel
    """

    def __init__(self, named_components: List[NamedComponent]):
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