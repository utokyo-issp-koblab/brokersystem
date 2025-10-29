# brokersystem

This is an UNOFFICIAL fork of [brokersystem · PyPI](https://pypi.org/project/brokersystem/).

# Broker System

Python module for connecting to a broker server as an agent. The broker server must have the following REST interfaces.

- /api/v1/agent/msgbox

  Message box for receiving requests from clients. Messages must be returned as a list.

- /api/v1/agent/report

  An interface to return results to the client, receiving results as JSON and interpreting the results with keys starting with @ as properties.

- /api/v1/agent/config

  Interface for receiving agent settings.

- /upload

  Interface for uploading a file.

### Version history

- 0.2.0 Add file transfer functionality. Still $\alpha$ version.
- 0.1.0 $\alpha$ version
