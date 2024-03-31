# Haystack 2.x Custom Extensions

Welcome to the repository for custom extensions of [Haystack](https://github.com/deepset-ai/haystack), version 2.0 and onwards. This collection primarily aims at enhancing concurrent components to optimize I/O-bound tasks. Furthermore, it addresses the concept of subpipelines, offering comprehensive solutions within this space.

## Quick Start Guide

To get started with developing or creating new integrations, you will need to use `hatch`. Please visit [this link](https://hatch.pypa.io/latest/install/#installation) and follow the installation instructions tailored to your operating system and platform.

The integrations within this repository are designed to be self-contained. Thus, the initial step in working on an integration involves navigating (`cd`) into the corresponding folder. For instance, if you wish to execute the test suite for the Chroma document store, simply run the following command from the root of the repository:

```sh
$ hatch run test
```

Hatch will take care of setting up an isolated Python environment and run the tests.

## Installation

Run: 
```
$ pip install haystack-extensions
```


## Usage

### Concurrent Components
Below a code snipped demonstrating how to run components multithreaded in another component: 


```python
import time
from haystack import component

from haystack_extensions.components.concurrent_runner.runner import (
    ConcurrentComponentRunner,
    NamedComponent
)


@component
class SimplePrintStringWithWaitComponent:
    """
    A component that prints a string and waits for a given number of seconds
    """

    def __init__(self, wait_time: int = 4):
        self.wait_time = wait_time

    @component.output_types(text=str)
    def run(self, text: str) -> str:
        time.sleep(self.wait_time)
        print(text, " <<== waited for", self.wait_time, "seconds")
        return {"text": text}


if __name__ == "__main__":
    comp1 = SimplePrintStringWithWaitComponent(wait_time=10)
    comp2 = SimplePrintStringWithWaitComponent(wait_time=3)
    comp3 = SimplePrintStringWithWaitComponent(wait_time=5)

    p = Pipeline()
    p.add_component("multithreaded_component", multithreaded_component)

    result = p.run(data={ "multithreaded_component": {
                    "one": {"text": "Hello"}, 
                    "two": {"text": "World"}, 
                    "three": {"text": "!"}
                }
            })

    print(result)
```

This will lead to the following output and result: 

**Console output:**
```
World  <<== waited for 3 seconds
!  <<== waited for 5 seconds
Hello  <<== waited for 10 seconds
```

**Result:**
```python
result = {'multithreaded_component': {'one': {'text': 'Hello'}, 'two': {'text': 'World'}, 'three': {'text': '!'}}}
```

### Subpipelines

```python
import time
from haystack import component, Pipeline

from haystack_extensions.components.concurrent_runner.runner import (
    NamedPipeline,
    ConcurrentPipelineRunner
)


@component
class SimplePrintStringWithWaitComponent:
    """
    A component that prints a string and waits for a given number of seconds
    """

    def __init__(self, wait_time: int = 4):
        self.wait_time = wait_time

    @component.output_types(text=str)
    def run(self, text: str) -> str:
        time.sleep(self.wait_time)
        print(text, " <<== waited for", self.wait_time, "seconds")
        return {"text": text}

if __name__ == "__main__":
    comp1 = SimplePrintStringWithWaitComponent(wait_time=10)
    comp2 = SimplePrintStringWithWaitComponent(wait_time=3)

    pipeline1 = Pipeline()
    pipeline1.add_component("simple_component", comp1)

    pipeline2 = Pipeline()
    pipeline2.add_component("simple_component", comp2)

    concurrent_pipeline_runner = ConcurrentPipelineRunner([NamedPipeline("pipeline1", pipeline1), NamedPipeline("pipeline2", pipeline2)])

    overall_pipeline = Pipeline()

    overall_pipeline.add_component("concurrent_pipeline_runner", concurrent_pipeline_runner)

    results = overall_pipeline.run(
        data={
            "concurrent_pipeline_runner": {
                "pipeline1": {"simple_component": {"text": "Hello"}},
                "pipeline2": {"simple_component": {"text": "World"}},
            }
        }
    )

    print(results)
```

This will lead to the following output and result: 

**Console output:**
```
World  <<== waited for 3 seconds
Hello  <<== waited for 10 seconds
```

**Result:**
```python
result = {'concurrent_pipeline_runner': {'pipeline1': {'simple_component': {'text': 'Hello'}}, 'pipeline2': {'simple_component': {'text': 'World'}}}}
```
