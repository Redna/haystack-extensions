import time
from typing import Callable, Dict, List
from haystack import Pipeline, component
import pytest
from haystack_extensions.components.concurrent_runner.runner import ConcurrentComponentRunner, NamedComponent, NamedPipeline, ConcurrentPipelineRunner
from haystack.core.component import Component


@component
class SimpleComponent:
    def __init__(self, wait_time: float, callback: Callable) -> None:
        self.wait_time = wait_time
        self.callback = callback
        
    @component.output_types(number=int)
    def run(self, increment: int, number:int = 5) -> int:
        time.sleep(self.wait_time)
        self.callback(self)
        return {"number": number + increment}
    
@component
class ComplexInputOutputComponent:
    @component.output_types(number=int, as_list=List[int], as_dict_of_lists={str: List[int]})
    def run(self, some_dict: Dict[str, List[int]], increment: int, number:int = 5) -> int:
        return {"number": number + increment, "as_list": [number + increment], "as_dict_of_lists": {key: [number + increment] for key in some_dict.keys()}} 



class TestConcurrentComponentRunner:
    def test_concurrent_component_runner(self):
        component_call_stack = []
        def callback(component):
            component_call_stack.append(component)

        named_components = [
            NamedComponent(name="component1", component=SimpleComponent(wait_time=0.05, callback=callback)),
            NamedComponent(name="component2", component=SimpleComponent(wait_time=0.09, callback=callback)),
            NamedComponent(name="component3", component=SimpleComponent(wait_time=0.01, callback=callback)),
        ]

        runner = ConcurrentComponentRunner(named_components)

        pipe = Pipeline()
        pipe.add_component("concurrent_runner", runner)

        results = pipe.run(data={"concurrent_runner": {
                                    "component1": {"increment": 1},
                                    "component2": {"increment": 2, "number": 10}, 
                                    "component3": {"increment": 3, "number": 20}
                                    }
                                })
        
        assert results == {'concurrent_runner': {'component1': {'number': 6}, 'component2': {'number': 12}, 'component3': {'number': 23}}}
        
        assert len(component_call_stack) == 3
        assert component_call_stack[0] == named_components[2].component
        assert component_call_stack[1] == named_components[0].component
        assert component_call_stack[2] == named_components[1].component

    def test_concurrent_runner_raises_error_on_wrong_input(self):
        component = ("component", SimpleComponent(wait_time=0.1, callback=lambda x: None))

        with pytest.raises(ValueError):    
            ConcurrentComponentRunner([component])

    def test_same_component_name_raises_error(self):
        component1 = NamedComponent("component", SimpleComponent(wait_time=0.1, callback=lambda x: None))
        component2 = NamedComponent("component", SimpleComponent(wait_time=0.1, callback=lambda x: None))
        with pytest.raises(ValueError):
            ConcurrentComponentRunner([component1, component2])

    def test_complex_input_output_working(self):
        component = NamedComponent("component", ComplexInputOutputComponent())
        runner = ConcurrentComponentRunner([component])
        pipeline = Pipeline()
        pipeline.add_component("concurrent_runner", runner)

        results = pipeline.run(data={"concurrent_runner": {
                                        "component": {
                                            "some_dict": {
                                                "a": [1, 2, 3]
                                            }, 
                                            "increment": 1
                                            }
                                        }
                                    })
        
        assert results == {'concurrent_runner': {
                                'component': {
                                    'number': 6, 
                                    'as_list': [6], 
                                    'as_dict_of_lists': {
                                        'a': [6]
                                        }
                                    }
                                }
                            }

    
    @pytest.mark.parametrize("input", [("component", SimpleComponent(wait_time=0.1, callback=lambda x: None)),
                                        {"component": SimpleComponent(wait_time=0.1, callback=lambda x: None)}])
    def test_concurrent_runner_raises_error_on_wrong_input(self, input):
        component = ("component", SimpleComponent(wait_time=0.1, callback=lambda x: None))

        with pytest.raises(ValueError):    
            ConcurrentComponentRunner([component])

    def test_default_values_are_respected(self):
        component = NamedComponent("component", SimpleComponent(wait_time=0.1, callback=lambda x: None))
        runner = ConcurrentComponentRunner([component])
        pipeline = Pipeline()
        pipeline.add_component("concurrent_runner", runner)

        results = pipeline.run(data={"concurrent_runner": {"component": {"increment": 1}}})
        assert results == {'concurrent_runner': {'component': {'number': 6}}}

class TestConcurrentPipelineRunner:

    def test_concurrent_pipeline_runner(self):
        component_call_stack = []

        def callback(component):
            component_call_stack.append(component)
        
        simple_component_1 = SimpleComponent(wait_time=0.09, callback=callback)
        pipeline1 = Pipeline()
        pipeline1.add_component("simple_component", simple_component_1)

        simple_component_2 = SimpleComponent(wait_time=0.02, callback=callback)
        pipeline2 = Pipeline()
        pipeline2.add_component("simple_component", simple_component_2)


        concurrent_pipeline_runner = ConcurrentPipelineRunner([NamedPipeline("pipeline1", pipeline1), 
                                                                NamedPipeline("pipeline2", pipeline2)])
        
        overall_pipeline = Pipeline()

        overall_pipeline.add_component("concurrent_pipeline_runner", concurrent_pipeline_runner)

        results = overall_pipeline.run(data={"concurrent_pipeline_runner": {
                                                "pipeline1": {
                                                    "simple_component": {
                                                        "increment": 1
                                                    }
                                                },
                                                "pipeline2": {
                                                    "simple_component": {
                                                        "increment": 2,
                                                        "number": 10
                                                    }
                                                }
                                            }
                                        })

        assert results == {'concurrent_pipeline_runner': {
                                'pipeline1': {'simple_component': {'number': 6}},
                                'pipeline2': {'simple_component': {'number': 12}}
                            }}
        assert len(component_call_stack) == 2
        assert component_call_stack[0] == simple_component_2
        assert component_call_stack[1] == simple_component_1

        
    @pytest.mark.parametrize("input", [("pipeline", Pipeline(), {"pipeline": Pipeline()})])
    def test_concurrent_pipeline_runner_raises_error_on_wrong_input(self, input):
        pipeline = Pipeline()
        with pytest.raises(ValueError):
            ConcurrentPipelineRunner([("pipeline", [input])])

    def test_same_pipeline_name_raises_error(self):
        pipeline1 = NamedPipeline("pipeline", Pipeline())
        pipeline2 = NamedPipeline("pipeline", Pipeline())
        with pytest.raises(ValueError):
            ConcurrentPipelineRunner([pipeline1, pipeline2])