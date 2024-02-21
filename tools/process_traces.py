import json
from datetime import timedelta

def extractTagValue(data, tag):
    if "attributes" in data:
        for x in data["attributes"]:
            if "key" in x and x["key"] == tag and "value" in x and "stringValue" in x["value"]:
                return x["value"]["stringValue"]
        raise KeyError("Provided tag not found in attributes")
    else:
        raise KeyError("Attributes not found")


class Span:
    def __init__(self, data: dict) -> None:
        self.__span__ = data

        self.name:str = data["name"]
        self.traceId:str = data["traceId"]
        self.spanId:str = data["spanId"]
        self.parentSpanId:str = data["parentSpanId"]

        self.start:int = int(data["startTimeUnixNano"])
        self.end:int = int(data["endTimeUnixNano"])

        self.sessionId:str = extractTagValue(data, "SessionId")
        self.taskId:str = extractTagValue(data, "TaskId")
        self.messageId:str = extractTagValue(data, "MessageId")
        self.ownerPodId:str = extractTagValue(data, "OwnerPodId")
        self.ownerPodName:str = extractTagValue(data, "OwnerPodName")
    
        self.microseconds: int = (self.end - self.start)/1000
        self.duration:timedelta = timedelta(microseconds = self.microseconds)

    def __str__(self) -> str:
        return f"{self.name} {self.traceId} -> {self.duration}"





def main():
    tasks = set()
    aggreg : dict[str, timedelta] = dict()

    with open("terraform/logs/traces.json") as f:
        for line in f.readlines():
            data = json.loads(line)

            for rspan in data["resourceSpans"]:
                for sspan in rspan["scopeSpans"]:
                    for span in sspan["spans"]:
                        if span["name"].startswith("TaskHandler."):
                            s = Span(span)
                            tasks.add(s.taskId)
                            t = aggreg.get(s.name, timedelta())
                            t = t + s.duration
                            aggreg[s.name] = t

    n = len(tasks)
    for k, v in aggreg.items():
        print(f"avg {k} time {v/n}")


if __name__ == "__main__":
    main()