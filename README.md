# JTempo
Automate python for Jmeter and Grafana tempo without using log_result.jtl

## How it works
JTempo finds time void Tempo traces and correlates them with JMeter results automatically, without relying on a `log_result.jtl` file.

## Configuration
Before running, set the following configuration variables:

| Variable | Example Value | Description |
|---|---|---|
| `TEMPO_BASE_URL` | `"XXXXXX:3200"` | Base URL of your Grafana Tempo instance (host:port) |
| `JMETER_GRAPH_FILE` | `"graph.js"` | Path to the JMeter result graph file |

Example:
```python
TEMPO_BASE_URL = "XXXXXX:3200"
JMETER_GRAPH_FILE = "graph.js"
```
