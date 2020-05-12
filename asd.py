import time
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.trace.samplers import ProbabilitySampler
from opencensus.trace.tracer import Tracer

# TODO: replace the all-zero GUID with your instrumentation key.
tracer = Tracer(
    exporter=AzureExporter(
        connection_string='InstrumentationKey=f523a71f-1d49-40ca-9128-773cb12218af'),
    sampler=ProbabilitySampler(1.0),
)

def valuePrompt():
    with tracer.span(name="MyFirstSpan") as span:
        time.sleep(1)
        with tracer.span(name="MySecondSpan") as span2:
            time.sleep(2)

def main():
    valuePrompt()

if __name__ == "__main__":
    main()

