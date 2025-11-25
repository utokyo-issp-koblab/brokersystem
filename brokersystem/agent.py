import importlib
import inspect
import io
import json
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import timedelta
from os import PathLike
from pathlib import Path
from types import ModuleType
from typing import Any, Literal

import pandas as pd
import PIL.Image
import requests
from logzero import logger

from . import __version__


class ValueTemplate:
    def __init__(self, unit: str | None = None):
        self.type = self.__class__.__name__.lower()
        self.unit = unit
        self.constraint_dict = dict[str, Any]()
        self.item_type = None

    def set_constraint(self, key: str, value: Any):
        if value is not None:
            self.constraint_dict[key] = value

    @property
    def format_dict(self) -> dict[str, Any]:
        format = {"@type": self.type, "@unit": self.unit}
        if self.item_type == "input":
            format.update(
                {"@necessity": "required", "@constraints": self.constraint_dict}
            )
        if self.item_type == "output":
            format.update({})
        return format

    def cast(self, value: Any):
        return value

    def format_for_output(
        self, value: Any, uploader: Callable[[str, bytes], dict[str, Any]]
    ):
        format_dict = {"@type": self.type, "@unit": self.unit}
        return value, format_dict

    def set_item_type(self, item_type: str):
        self.item_type = item_type

    @classmethod
    def guess_from_value(
        cls,
        value: Any,
        unit_callback_func: Callable[[str], str | None] | None = None,
        key: str | None = None,
    ):
        unit = None
        if isinstance(value, (int, float)):
            if unit_callback_func is not None and key is not None:
                unit = unit_callback_func(key)
            return Number(value, unit=unit)
        if isinstance(value, str):
            return String(value)

        if (
            isinstance(value, pd.DataFrame)
            or (isinstance(value, list) and all([isinstance(x, dict) for x in value]))
            or (
                isinstance(value, dict)
                and all([isinstance(x, list) for x in value.values()])
            )
        ):
            if not isinstance(value, pd.DataFrame):
                value = pd.DataFrame(value)
            unit_dict = dict[str, str | None]()
            for column in value.columns:
                if unit_callback_func is not None and column is not None:
                    unit_dict[column] = unit_callback_func(column)
                else:
                    unit_dict[column] = None
            graph = None
            if len(value.columns) >= 2:
                graph = {"x": value.columns[0], "y": value.columns[1]}
            return Table(unit_dict=unit_dict, graph=graph)

        if isinstance(value, list):
            return Choice(value)

        assert False, f"Cannot find a matching value template... {value}:{type(value)}"

    @classmethod
    def from_dict(cls, format_dict: dict[str, Any], unit_dict: dict[str, str | None]):
        match format_dict["@type"]:
            case "number":
                template = Number()
            case "string":
                template = String()
            case "choice":
                template = Choice(format_dict["@constraints"]["choices"])
            case "table":
                unit_dict = {key: unit_dict[key] for key in format_dict["@table"]}
                graph = None
                if "@repr" in format_dict and format_dict["@repr"]["type"] == "graph":
                    graph = {
                        "x": format_dict["@repr"]["key_x"],
                        "y": format_dict["@repr"]["key_y"],
                    }
                template = Table(unit_dict=unit_dict, graph=graph)
            case _ as unknown_type:
                raise NotImplementedError(f"Unknown type: {unknown_type}")
        if "@constraints" in format_dict:
            for key, value in format_dict["@constraints"].items():
                template.set_constraint(key, value)
        if "@unit" in format_dict:
            template.unit = format_dict["@unit"]

        return template


class Number(ValueTemplate):
    def __init__(
        self,
        value: int | float | None = None,
        unit: str | None = None,
        min: int | float | None = None,
        max: int | float | None = None,
    ):
        super().__init__(unit)
        self.set_constraint("default", value)
        self.set_constraint("min", min)
        self.set_constraint("max", max)

    def cast(self, value: Any):
        try:
            value = int(value)
        except:
            value = float(value)
        assert not (
            "min" in self.constraint_dict and value < self.constraint_dict["min"]
        )
        assert not (
            "max" in self.constraint_dict and value > self.constraint_dict["max"]
        )
        return value


class Range(ValueTemplate):
    def __init__(
        self,
        range_min: int | float | None = None,
        range_max: int | float | None = None,
        unit: str | None = None,
    ):
        super().__init__(unit)
        self.set_constraint("default", {"min": range_min, "max": range_max})


class String(ValueTemplate):
    def __init__(self, string: str | None = None):
        super().__init__()
        self.set_constraint("default", string)


class File(ValueTemplate):
    def __init__(self, file_type: str):
        super().__init__()
        if file_type in ["png", "jpeg", "gif"]:
            self.type = "image"
        self.file_type = file_type
        self.set_constraint("file_type", file_type)

    def format_for_output(
        self,
        value: bytes | PathLike[str] | str,
        uploader: Callable[[str, bytes], dict[str, Any]],
    ):
        if isinstance(value, bytes | bytearray | memoryview):
            binary_data = value
        else:
            try:
                path = Path(value)
            except Exception as e:
                raise TypeError(
                    f"a File value must be a file path but got: {value} (type: {type(value)}); {e}"
                )
            match self.type:
                case "image":
                    match self.file_type:
                        case "png" | "jpeg":
                            img = PIL.Image.open(path)
                            output = io.BytesIO()
                            img.save(output, format=self.file_type)
                            binary_data = output.getvalue()
                        case "gif":
                            with path.open("rb") as f:
                                binary_data = f.read()
                        case _ as unknown_type:
                            raise NotImplementedError(
                                f"Unknown image file type: {unknown_type}"
                            )
                case "file":
                    with path.open("rb") as f:
                        binary_data = f.read()
                case _ as unknown_type:
                    raise NotImplementedError(f"Unknown file type: {unknown_type}")

        response = uploader(self.file_type, binary_data)
        logger.debug("upload result: %s", response)
        assert "file_id" in response
        value = response["file_id"]

        return value, self.format_dict


class Choice(ValueTemplate):
    def __init__(self, choices: Iterable[int | float | str], unit: str | None = None):
        super().__init__(unit)
        self.set_constraint("choices", list(choices))

    def cast(self, value: Any):
        try:
            value = int(value)
        except:
            pass
        try:
            value = float(value)
        except:
            pass
        assert value in self.constraint_dict["choices"]
        return value


class Table(ValueTemplate):
    def __init__(
        self,
        unit_dict: Mapping[str, str | None],
        graph: Mapping[str, Any] | None = None,
    ):
        super().__init__(unit=None)
        self.unit_dict = dict(unit_dict)
        self.graph = graph

    def cast(self, value: Any):
        return value

    @property
    def format_dict(self):
        format_dict = super().format_dict
        format_dict["@table"] = list(self.unit_dict.keys())
        format_dict["_unit_dict"] = self.unit_dict
        if self.graph is not None:
            format_dict["@repr"] = {
                "type": "graph",
                "key_x": self.graph["x"],
                "key_y": self.graph["y"],
            }
        return format_dict

    def format_for_output(self, value, uploader):
        format_dict = self.format_dict
        if isinstance(value, pd.DataFrame):
            value = value.to_dict(orient="list")
        elif (
            isinstance(value, list) and all([isinstance(x, dict) for x in value])
        ) or (
            isinstance(value, dict)
            and all([isinstance(x, list) for x in value.values()])
        ):
            value = pd.DataFrame(value).to_dict(orient="list")
        else:
            raise Exception(
                "The return value for table should be given by a list of dict, a dict of list or pandas DataFrame"
            )

        keys = list(value.keys())
        if self.graph is None and len(keys) >= 2:
            format_dict["@repr"] = {"type": "graph", "key_x": keys[0], "key_y": keys[1]}

        return value, format_dict


class TemplateContainer:
    def __init__(self, item_type: str):
        self._container_dict = dict[str, ValueTemplate]()
        self._value_keys = set[str]()
        self._table_keys = set[str]()
        self._item_type = item_type

    def __setattr__(self, key: str, value: Any):
        if key.startswith("_"):
            super().__setattr__(key, value)
            return
        if not isinstance(value, ValueTemplate):
            logger.exception(
                f"You can only set ValueTemplate object(such as Number or Table) for input, condition and output: {key} ({type(value)})",
            )
            return
        self._container_dict[key] = value
        if isinstance(value, Table):
            self._table_keys.add(key)
        else:
            self._value_keys.add(key)
        value.set_item_type(self._item_type)

    def _get_template(self):
        match self._item_type:
            case "input":
                format_keys = ["@type", "@unit", "@necessity", "@constraints"]
            case "output":
                format_keys = ["@type", "@unit", "@repr"]
            case "condition":
                format_keys = ["@type", "@unit", "@value"]
            case _ as unknown_type:
                raise NotImplementedError(f"Unknown item type: {unknown_type}")
        template_dict = dict[str, Any](**{fkey: {} for fkey in format_keys})

        template_dict["@keys"] = list(self._value_keys | self._table_keys)
        if len(self._value_keys) > 0:
            template_dict["@value"] = list(self._value_keys)

        if len(self._table_keys) > 0:
            template_dict["@table"] = {}  # will be given by Table instance

        for key, template in self._container_dict.items():
            for fkey, fvalue in template.format_dict.items():
                if fkey == "_unit_dict":
                    for k, v in fvalue.items():
                        assert (
                            k not in template_dict["@unit"]
                            and k not in template_dict["@type"]
                        ), f"Duplicated key error: {k}"
                        template_dict["@unit"][k] = v
                        template_dict["@type"][k] = "number"
                else:
                    assert (
                        key not in template_dict[fkey]
                    ), f"Duplicated key error: {key}"
                    template_dict[fkey][key] = fvalue
        return template_dict

    def _load(self, config_dict: Mapping[str, Any]):
        for key in config_dict["@keys"]:
            format_dict = {
                prop_key: config_dict[prop_key][key]
                for prop_key in config_dict.keys()
                if prop_key.startswith("@")
                and isinstance(config_dict[prop_key], dict)
                and key in config_dict[prop_key]
            }
            setattr(
                self, key, ValueTemplate.from_dict(format_dict, config_dict["@unit"])
            )

    def __contains__(self, key: str):
        return key in self._container_dict

    def __getitem__(self, key: str) -> ValueTemplate:
        return self._container_dict[key]


class ValueContainer:
    def __init__(self, value_dict: Mapping[str, Any]):
        self._container_dict = dict(value_dict)

    def __getattr__(self, key: str):
        if key.startswith("_"):
            raise AttributeError(f"{key}")
        return self._container_dict[key]

    def __getitem__(self, key: str):
        return self._container_dict[key]


class AgentInterface:
    def __init__(self):
        self.secret_token = ""
        self.name: str | None = None
        self.charge = 10000
        self.convention = ""
        self.description = ""
        self.input = TemplateContainer("input")
        self.condition = TemplateContainer("condition")
        self.output = TemplateContainer("output")

    def prepare(self, func_dict: Mapping[str, Callable[..., Any]]):
        self.func_dict = dict(func_dict)
        if "make_config" not in self.func_dict and (
            self.secret_token == "" or self.name is None
        ):
            logger.exception("Secret_token and name should be specified.")
            return
        if "job_func" not in self.func_dict:
            logger.exception(
                "job execution function is required: Prepare a decorated function with @job_func."
            )
            return
        self.make_config()

    def make_config(self, for_registration: bool = False):
        if "make_config" in self.func_dict:
            self.func_dict["make_config"]()
            if self.name is None:
                logger.exception("Agent's name should be set in config function")
        config_dict = dict[str, Any]()
        for item_type in ["input", "condition", "output"]:
            config_dict[item_type] = getattr(
                getattr(self, item_type), "_get_template"
            )()

        config_dict["charge"] = self.charge
        if for_registration:
            config_dict["module_version"] = (
                __version__  # pyright: ignore[reportUndefinedVariable]
                if "__version__" in globals()
                else "-1.0.0"
            )
            config_dict["convention"] = self.convention
            config_dict["description"] = self.description

        return config_dict

    def has_func(self, func_name: str) -> bool:
        return func_name in self.func_dict

    def validate(self, input_dict: dict[str, Any]):
        template_dict = self.input._get_template()
        msg = "ok"
        for key in template_dict["@keys"]:
            if key in input_dict:
                try:
                    value = self.input[key].cast(input_dict[key])
                    template_dict[key] = value
                except (AssertionError, TypeError):
                    msg = "need_revision"
        return msg, template_dict

    def cast(self, key: str, input_value: Any):
        if key not in self.input:
            logger.exception(f"{key} is not registered as input.")
            raise Exception
        return self.input[key].cast(input_value)

    def format_for_output(
        self,
        result_dict: dict[str, Any],
        uploader: Callable[[str, bytes], dict[str, Any]],
    ):
        output_dict = {
            "@unit": {},
            "@type": {},
            "@option": {},
            "@repr": {},
            "@keys": [],
        }

        for key, value in result_dict.items():
            if key not in self.output:
                continue
            output_value, format_dict = self.output[key].format_for_output(
                value, uploader
            )
            if output_value is None:
                continue
            output_dict[key] = output_value
            output_dict["@keys"].append(key)
            ### manage type specific properties
            if format_dict["@type"] == "table":
                if "@table" not in output_dict:
                    output_dict["@table"] = {}
                table_keys = list(format_dict["_unit_dict"].keys())
                output_dict["@unit"].update(format_dict["_unit_dict"])
                output_dict["@type"].update({k: "number" for k in table_keys})

            elif format_dict["@type"] in ["image", "file"]:
                if "@file" not in output_dict:
                    output_dict["@file"] = {}
            else:
                if "@value" not in output_dict:
                    output_dict["@value"] = []
                output_dict["@value"].append(key)

            for fk, fv in format_dict.items():
                if not fk.startswith("_"):
                    output_dict[fk][key] = fv
        # remove empty property keys

        return output_dict


class DummyJob:
    _to_show_i_am_job_class = True

    def __init__(self, request_params: Any, config: dict[str, str]):
        class DummyAgentInterface:
            def __init__(self):
                self.secret_token = config["secret_token"]

        class DummyAgent:
            def __init__(self):
                if config is not None:
                    self.broker_url = config["broker_url"]
                    self.interface = DummyAgentInterface()

        self._agent = DummyAgent()

    def report(
        self,
        msg: str | None = None,
        progress: float | None = None,
        result: dict[str, Any] | None = None,
    ):
        logger.info(f"[DUMMY JOB] REPORT: {msg}, {progress}, {result}")

    def __getitem__(self, key: str):
        return 0

    def __contains__(self, key: str):
        return True


class Job:
    _to_show_i_am_job_class = True

    def __init__(self, agent: "Agent", negotiation_id: str, request: dict[str, Any]):
        self._agent = agent
        self._negotiation_id = negotiation_id
        self._request = request
        self._result_dict = {}
        self._status = "init"
        self.id = negotiation_id

    def report(
        self,
        msg: str | None = None,
        progress: float | None = None,
        result: dict[str, Any] | None = None,
    ):
        payload = dict[str, Any](
            negotiation_id=self._negotiation_id, status=self._status
        )
        if msg is not None:
            payload["msg"] = msg
        if progress is not None:
            payload["progress"] = progress
        if result is not None:
            assert isinstance(
                result, dict
            ), "Result should be given as a dict: {result_key: result_value}"
            self._result_dict.update(result)
            payload["result"] = self._agent.interface.format_for_output(
                self._result_dict, self._agent.upload
            )

        self._agent.post("report", payload)

    def msg(self, msg: str):
        self.report(msg=msg)

    def progress(self, progress: float, msg: str | None = None):
        self.report(progress=progress, msg=msg)

    def periodic_report(
        self,
        estimated_time: timedelta | float | None = None,
        interval: timedelta = timedelta(seconds=2),
        callback_func: Callable[..., str] | None = None,
        **kwargs: Any,
    ):
        if "start_time" not in kwargs:
            kwargs["start_time"] = time.perf_counter()
        if self._status not in ["done", "error"]:
            msg = None
            if callback_func is not None:
                kwargs["callback_func"] = callback_func
                msg = callback_func(self, **kwargs)
            if estimated_time is not None:
                if not isinstance(estimated_time, timedelta):
                    estimated_time = timedelta(seconds=estimated_time)
                kwargs["estimated_time"] = estimated_time
                self.progress(
                    (time.perf_counter() - kwargs["start_time"])
                    / estimated_time.total_seconds(),
                    msg=msg,
                )
            elif msg is not None:
                self.msg(msg)
            timer = threading.Timer(
                interval.total_seconds(), self.periodic_report, kwargs=kwargs
            )
            timer.daemon = True
            timer.start()

    def __getitem__(self, key: str):
        return self._agent.interface.cast(key, self._request[key])

    def __contains__(self, key: str):
        return key in self._request

    def _set_status(self, status: str):
        self._status = status


class Agent:
    RESTART_INTERVAL_CRITERIA = 30
    HEARTBEAT_INTERVAL = 2
    _automatic_built_agents = dict[str, "Agent"]()

    def __init__(self, broker_url: str):
        self._to_show_i_am_agent_instance = True
        self.broker_url = broker_url
        self.access_token = None
        self.agent_funcs = {}
        self.running = False
        self.interface = AgentInterface()

    def run(self, _automatic: bool = False):
        if self.running:
            return
        self.interface.prepare(self.agent_funcs)
        self.auth = self.interface.secret_token
        self.polling_interval = 0
        self.last_heartbeat = time.perf_counter()
        if self.register_config():
            self.running = True
            threading.Thread(target=self.connect, daemon=True).start()
            # threading.Thread(target=self.heartbeat).start()
            if not _automatic:
                logger.info(
                    f"Agent {self.interface.name} has started. Press return to quit."
                )
                try:
                    input("")
                finally:
                    self.goodbye()

    @classmethod
    def start(cls):
        agent_list = list[Agent]()
        try:
            for agent in cls._automatic_built_agents.values():
                agent.run(_automatic=True)
                agent_list.append(agent)
            logger.info("Press return to quit.")
            input("")
        finally:
            for agent in agent_list:
                agent.goodbye()

    def goodbye(self):
        self.running = False

    def register_config(self):
        for _ in range(5):
            response = self.post(
                "config",
                self.interface.make_config(for_registration=True),
                basic_auth=True,
            )
            if "status" in response and response["status"] == "ok":
                logger.info(
                    f"Agent {self.interface.name} has successfully connected to the broker system!"
                )
                self.access_token = response["token"]
                logger.debug(f"TOKEN {self.access_token}")
                return True
            elif "status" in response and response["status"] == "error":
                logger.exception(response["error_msg"])
                break
            else:
                logger.warning("Cannot connect to the broker system.")
            time.sleep(3)
        logger.exception("Stop try connecting to the broker system.")
        return False

    def heartbeat(self):
        restart_timer = None
        while self.running:
            if (
                time.perf_counter() - self.last_heartbeat
                > self.RESTART_INTERVAL_CRITERIA
            ):
                if restart_timer is None:
                    restart_timer = time.perf_counter()
                elif (
                    time.perf_counter() - restart_timer
                    >= self.RESTART_INTERVAL_CRITERIA
                ):
                    restart_timer = None
                    logger.info("Automatic reconnection...")
                    threading.Thread(target=self.connect, daemon=True).start()
            else:
                restart_timer = None
            time.sleep(self.HEARTBEAT_INTERVAL)

    def connect(self):
        while self.running:
            self.last_heartbeat = time.perf_counter()
            try:
                messages = self.check_msgbox()
                for message in messages:
                    threading.Thread(
                        target=self.process_message, args=[message], daemon=True
                    ).start()
            except:
                logger.exception("Error occured during the message processing")
            time.sleep(self.polling_interval)

    def check_msgbox(self) -> list[Any]:
        response = self.get("msgbox")
        if len(response) > 0:
            messages = response["messages"]
            assert isinstance(
                messages, list
            ), f"Response messages was not a list: {response=}"
            return messages
        return []

    def process_message(self, message: Mapping[str, Any]):
        try:
            logger.debug(f"Message: {message}")
            if "msg_type" not in message or "body" not in message:
                logger.exception(f"Wrong message format: {message}")
                return
            if message["msg_type"] == "negotiation_request":
                self.process_negotiation_request(message["body"])
            if message["msg_type"] == "negotiation":
                self.process_negotiation(message["body"])
            if message["msg_type"] == "contract":
                self.process_contract(message["body"])
        except:
            logger.exception(f"Error occured during process_message; {message=}")

    def process_negotiation_request(self, body: Mapping[str, Any]):
        negotiation_id = body["negotiation_id"]
        response = self.interface.make_config()
        self.post(
            "negotiation/response",
            {
                "msg": "need_revision",
                "negotiation_id": negotiation_id,
                "response": response,
            },
        )

    def process_negotiation(self, body: Mapping[str, Any]):
        negotiation_id = body["negotiation_id"]
        response = self.interface.make_config()
        request = body["request"]
        msg, input_response = self.interface.validate(request)
        response["input"] = input_response
        if msg == "ok" and self.interface.has_func("negotiation"):
            msg, response = self.interface.func_dict["negotiation"](request, response)
            if msg not in ["ok", "need_revision", "ng"]:
                logger.exception(
                    f"Negotiation func in {self.interface.name} returns a wrong msg: should be one of 'ok', 'need_revision' or 'ng'"
                )

        if msg == "ok" and self.interface.has_func("charge_func"):
            response["charge"] = round(
                self.interface.func_dict["charge_func"](input_response)
            )

        self.post(
            "negotiation/response",
            {"msg": msg, "negotiation_id": negotiation_id, "response": response},
        )

    def process_contract(self, msg: Mapping[str, Any]):
        st = time.perf_counter()
        negotiation_id = msg["negotiation_id"]
        request = msg["request"]
        job = Job(self, negotiation_id, request)
        try:
            self.post("contract/accept", {"negotiation_id": negotiation_id})
            job._set_status("running")
            job.msg(f"{self.interface.name} starts running...")

            result = self.interface.func_dict["job_func"](job)
            logger.debug(f"JOB RETURN VALUE: {result}")
            if result is None:
                result = {}
            job._set_status("done")
            job.report(msg="Job done.", progress=1, result=result)
        except:
            logger.exception(f"Error occured during the job execution; {msg=}")
            job._set_status("error")
            job.report(msg="Error occured during the job.", progress=-1)
        logger.debug(f"{negotiation_id} took {time.perf_counter()-st:.3f} s.")

    def header(self, basic_auth: bool = False, upload: bool = False):
        if upload:
            headers = {}
        else:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if basic_auth:
            headers.update({"authorization": f"Basic {self.auth}"})
        elif self.access_token:
            headers.update({"authorization": f"Token {self.access_token}"})

        return headers

    def post(
        self, uri: str, payload: dict[str, Any], basic_auth: bool = False
    ) -> dict[str, Any]:
        try:
            response = requests.post(
                f"{self.broker_url}/api/v1/agent/{uri}",
                json=payload,
                headers=self.header(basic_auth),
            )
        except requests.exceptions.ConnectionError:
            return {}

        if response.status_code == 401 and not basic_auth:
            assert self.register_config()
            return self.post(uri, payload)
        elif response.status_code != 200:
            logger.exception(f"{response.status_code}: {uri} {payload}")
            return {}
        obj = response.json()
        assert isinstance(obj, dict), f"Response was not a dict: {obj}"
        assert all(
            isinstance(k, str) for k in obj.keys()
        ), f"Some of response keys were not str: {obj.keys()}"
        return obj

    def get(self, uri: str, basic_auth: bool = False) -> dict[str, Any]:
        try:
            response = requests.get(
                f"{self.broker_url}/api/v1/agent/{uri}", headers=self.header(basic_auth)
            )
        except requests.exceptions.ConnectionError:
            if self.polling_interval < 10:
                self.polling_interval += 1
            return {}

        if response.status_code == 401 and not basic_auth:
            assert self.register_config()
            return self.get(uri)
        elif response.status_code != 200:
            logger.exception(response.status_code)
            if self.polling_interval < 10:
                self.polling_interval += 1
            return {}
        self.polling_interval = 0
        obj = response.json()
        assert isinstance(obj, dict), f"Response was not a dict: {obj}"
        assert all(
            isinstance(k, str) for k in obj.keys()
        ), f"Some of response keys were not str: {obj.keys()}"
        return obj

    def upload(self, file_type: str, binary_data: bytes) -> dict[str, Any]:
        file_name = "__file_name__"
        mime_dict = {
            "png": "image/png",
            "gif": "image/gif",
            "jpeg": "image/jpeg",
            "csv": "text/csv",
            "pdf": "application/pdf",
            "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        }
        assert file_type in mime_dict
        files = {file_type: (file_name, binary_data, mime_dict[file_type])}
        try:
            response = requests.post(
                f"{self.broker_url}/api/v1/agent/upload",
                headers=self.header(upload=True),
                files=files,
            )
        except requests.exceptions.ConnectionError:
            logger.exception("Cannot connect to the broker system for upload")
            return {}
        return response.json()

    def register_func(self, func_name: str, func: Callable[..., Any]):
        self.agent_funcs[func_name] = func

    ##### wrapper functions #####
    def config(self, func: Callable[[], None]):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        self.register_func("make_config", wrapper)
        return wrapper

    def negotiation(
        self,
        func: Callable[
            [dict[str, Any], dict[str, Any]],
            tuple[Literal["ok", "need_revision", "ng"], dict[str, Any]],
        ],
    ):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        self.register_func("negotiation", wrapper)
        return wrapper

    def charge_func(self, func: Callable[[dict[str, Any]], int]):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        self.register_func("charge_func", wrapper)
        return wrapper

    def job_func(self, func: Callable[..., dict[str, Any] | None]):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        self.register_func("job_func", wrapper)
        return wrapper

    @classmethod
    def _automatic_run(cls):
        agent_list = list[Agent]()
        for agent in cls._automatic_built_agents.values():
            agent.run(_automatic=True)
            agent_list.append(agent)
        return agent_list

    @classmethod
    def _is_automatic_mode(cls):
        return len(cls._automatic_built_agents) > 0

    @classmethod
    def make(cls, name: str, **agent_kwargs: Any):
        def make_func(func: Callable[..., dict[str, Any] | None]):
            # automatic_flag = "_automatic" in agent_kwargs and agent_kwargs["_automatic"]
            agent = AgentConstructor.make(name=name, job_func=func, kwargs=agent_kwargs)
            # agent.run(_automatic=automatic_flag)

            def wrapper(*args, **kwargs):
                logger.warning(
                    "Direct call of the agent job function for the start up will be disabled in future. Change it to Agent.start().",
                    stack_info=True,
                )
                cls.start()
                return agent

            cls._automatic_built_agents[name] = agent
            return wrapper

        return make_func

    @classmethod
    def add_config(cls, broker_url: str | None = None, secret_token: str | None = None):
        config_dict = dict[str, str]()
        if broker_url is not None:
            config_dict["broker_url"] = broker_url
        if secret_token is not None:
            config_dict["secret_token"] = secret_token

        def add_config_func(func: Callable[..., Any]):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            wrapper._additional_config = (  # pyright: ignore[reportFunctionMemberAccess]
                config_dict
            )
            wrapper._original_keys = (  # pyright: ignore[reportFunctionMemberAccess]
                list(inspect.signature(func).parameters)
            )
            return wrapper

        return add_config_func

    ##### interface for agent interface #####
    @property
    def input(self):
        return self.interface.input

    @property
    def output(self):
        return self.interface.output

    @property
    def condition(self):
        return self.interface.condition

    @property
    def secret_token(self):
        return self.interface.secret_token

    @secret_token.setter
    def secret_token(self, token: str):
        self.interface.secret_token = token

    @property
    def name(self):
        return self.interface.name

    @name.setter
    def name(self, name: str):
        self.interface.name = name

    @property
    def convention(self):
        return self.interface.convention

    @convention.setter
    def convention(self, convention: str):
        self.interface.convention = convention

    @property
    def description(self):
        return self.interface.description

    @description.setter
    def description(self, description: str):
        self.interface.description = description

    @property
    def charge(self):
        return self.interface.charge

    @charge.setter
    def charge(self, charge: int):
        self.interface.charge = charge


class AgentConstructor:
    def __init__(
        self,
        name: str,
        job_func: Callable[..., dict[str, Any] | None],
        kwargs: Mapping[str, Any],
    ):
        self.name = name
        self.job_func = job_func
        self.kwargs = dict(kwargs)
        self.config = dict[str, Any]()
        self.config_changed = False
        self.agent = None

    @classmethod
    def make(
        cls,
        name: str,
        job_func: Callable[..., dict[str, Any] | None],
        kwargs: Mapping[str, Any],
    ):
        constructor = cls(name, job_func, kwargs)
        constructor.load_config()
        constructor.config["name"] = name
        constructor.fill_config()
        if constructor.config_changed:
            constructor.dump_config()
        if not ("skip_config" in kwargs and kwargs["skip_config"]):
            constructor.investigate_job_func()
            constructor.dump_config()

        constructor.agent = Agent(constructor.config["broker_url"])

        constructor.agent.register_func("make_config", constructor.make_config_func)
        constructor.agent.register_func("job_func", constructor.agent_job_func)
        return constructor.agent

    def investigate_job_func(self):
        input_params = self.fill_input_params()
        if self.config_changed:
            self.dump_config()
        if "job" in self.job_func_keys:
            input_params["job"] = DummyJob(
                input_params,
                {
                    "broker_url": self.config["broker_url"],
                    "secret_token": self.config["secret_token"],
                },
            )
        result = self.job_func(**input_params)
        if result is None:
            result = {}
        self.fill_output_format(result)

    def fill_input_params(self):
        if "input" not in self.config:
            self.config["input"] = {"@keys": []}
        if "condition" not in self.config:
            self.config["condition"] = {"@keys": []}
        container = TemplateContainer("input")
        updated = False
        keys_to_remove = set(self.config["input"]["@keys"])
        default_value_dict = dict[str, Any]()
        for key in self.job_func_keys:
            if key in ["job"]:
                continue
            if key in keys_to_remove:
                keys_to_remove.discard(key)
            if key not in self.config["input"]["@keys"]:
                if not updated:
                    print("")
                    print("--- Input ---")
                updated = True
                setattr(container, key, self.ask_input_format(key))
                if container[key].type in ["number", "string"]:
                    default_value_dict[key] = container[key].constraint_dict["default"]
                elif container[key].type in ["choice"]:
                    default_value_dict[key] = container[key].constraint_dict["choices"][
                        0
                    ]
                self.config_changed = True
            else:
                if "default" in self.config["input"]["@constraints"][key]:
                    default_value_dict[key] = self.config["input"]["@constraints"][key][
                        "default"
                    ]
                elif "choices" in self.config["input"]["@constraints"][key]:
                    default_value_dict[key] = self.config["input"]["@constraints"][key][
                        "choices"
                    ][0]
        if updated:
            self.merge_config_and_container("input", container)
        self.remove_unused_keys("input", keys_to_remove)
        return default_value_dict

    def merge_config_and_container(self, item_type: str, container: TemplateContainer):
        template_dict = container._get_template()
        for key, value in template_dict.items():
            assert key.startswith("@"), "Property keys should start with @"
            if isinstance(value, dict):
                if key not in self.config[item_type]:
                    self.config[item_type][key] = {}
                self.config[item_type][key].update(value)
            elif isinstance(value, list):
                if key not in self.config[item_type]:
                    self.config[item_type][key] = []
                s = set(self.config[item_type][key])
                self.config[item_type][key] = list(s.union(value))

    def remove_unused_keys(self, item_type: str, keys_to_remove: set[str]):
        if len(keys_to_remove) == 0:
            return
        for property_key in self.config[item_type]:
            for key in keys_to_remove:
                print(item_type, property_key, self.config[item_type][property_key])
                if isinstance(self.config[item_type][property_key], dict):
                    if key in self.config[item_type][property_key]:
                        del self.config[item_type][property_key][key]
                if isinstance(self.config[item_type][property_key], list):
                    s = set(self.config[item_type][property_key])
                    s.discard(key)
                    self.config[item_type][property_key] = list(s)

    def fill_output_format(self, result: dict[str, Any]):
        if "output" not in self.config:
            self.config["output"] = {"@keys": []}
        updated = False
        keys_to_remove = set(self.config["output"]["@keys"])
        container = TemplateContainer("output")
        for key, value in result.items():
            if key in keys_to_remove:
                keys_to_remove.discard(key)
            if key not in self.config["output"]["@keys"] or (
                isinstance(value, pd.DataFrame)
                and any(
                    [c not in self.config["output"]["@unit"] for c in value.columns]
                )
            ):
                if not updated:
                    print("")
                    print("--- Output ---")
                updated = True
                setattr(
                    container,
                    key,
                    ValueTemplate.guess_from_value(
                        value, unit_callback_func=self.ask_output_format, key=key
                    ),
                )
        if updated:
            self.merge_config_and_container("output", container)
        self.remove_unused_keys("output", keys_to_remove)

    def ask_input_format(self, key: str):
        while True:
            value_type = input(
                f"Select the value type of \"{key}\" ('n':Number, 's': String, 'c': Choice):"
            )
            if value_type in ["n", "s", "c"]:
                break
        unit = None
        if value_type in ["n", "c"]:
            unit = input(f'--> Unit of "{key}" (empty if none):')
            if len(unit) == 0:
                unit = None
        if value_type == "n":
            default_value = float(input(f'--> Default value of "{key}":'))
            print(
                f"== {key}: {default_value} {'('+unit+')' if unit is not None else ''} =="
            )
            print("")
            return Number(value=default_value, unit=unit)
        elif value_type == "c":
            choices = input(f'--> Choices separated by a comma of "{key}":')
            choices = [c.strip() for c in choices.split(",")]
            print(f"== {key}: {choices} {'('+unit+')' if unit is not None else ''}")
            print("")
            return Choice(choices=choices, unit=unit)
        elif value_type == "s":
            default_value = input(f'--> Default value of "{key}":')
            print(
                f"== {key}: {default_value} {'('+unit+')' if unit is not None else ''} =="
            )
            print("")
            return String(string=default_value)

    def ask_output_format(self, key: str):
        unit = input(f'Unit of "{key}" (empty if none) > ')
        print("")
        return unit

    def load_config(self):
        self.config_changed = False
        try:
            with self.config_path.open() as f:
                self.config = json.load(f)
        except:
            self.config = {}

    def fill_config(self):
        broker_url = self.fill_config_item("broker_url")
        assert broker_url.startswith("http"), "broker_url should be properly specified."
        secret_token = self.fill_config_item("secret_token")
        assert len(secret_token) > 0, "secret_token should be set"
        charge = self.fill_config_item("charge", int)
        assert charge > 0, "charge should be larger than 0"
        _ = self.fill_config_item("description")

    def fill_config_item(self, key: str, astype: type = str):
        if hasattr(self.job_func, "_additional_config") and key in getattr(
            self.job_func, "_additional_config"
        ):
            self.config[key] = getattr(self.job_func, "_additional_config")[key]
        elif key in self.kwargs:
            if key in self.config and self.config[key] == astype(self.kwargs[key]):
                return self.config[key]
            self.config[key] = astype(self.kwargs[key])
            self.config_changed = True
        else:
            user_input = input(f"{key}:")
            self.config[key] = astype(user_input)
            self.config_changed = True
        return self.config[key]

    def make_config_func(self):
        assert self.agent is not None, "Agent has not been properly prepared."
        keys = ["name", "secret_token", "charge", "description"]
        for key in keys:
            setattr(self.agent, key, self.config[key])

        item_types = ["input", "output", "condition"]
        for item_type in item_types:
            getattr(self.agent, item_type)._load(self.config[item_type])

    def agent_job_func(self, job: Job):
        assert self.agent is not None, "Agent has not been properly prepared."
        input_params = self.job_to_params(job)
        if "estimated_time" in self.kwargs:
            job.periodic_report(self.kwargs["estimated_time"])
        result = self.job_func(**input_params)
        if result is None:
            result = {}
        return result

    def job_to_params(self, job: Job):
        input_keys = self.job_func_keys
        params = {key: job[key] for key in input_keys if key in job}
        if "job" in input_keys:
            params["job"] = job
        return params

    def dump_config(self):
        self.config_path.parent.mkdir(exist_ok=True)

        with self.config_path.open("w") as f:
            json.dump(self.config, f)
        self.config_changed = False

    @property
    def job_func_keys(self):
        if hasattr(self.job_func, "_original_keys"):
            return getattr(self.job_func, "_original_keys")
        return list(inspect.signature(self.job_func).parameters)

    @property
    def config_path(self):
        return Path(f"./configs/{self.name}.json")


class Broker:
    def __init__(
        self,
        job: Job | None = None,
        broker_url: str | None = None,
        auth: str | None = None,
    ):
        if job is None:
            assert broker_url is not None and auth is not None
            self.broker_url = broker_url
            self.auth = auth
        elif isinstance(job, Job) or hasattr(job, "_to_show_i_am_job_class"):
            self.broker_url = job._agent.broker_url
            self.auth = job._agent.interface.secret_token
        else:
            raise TypeError

    def ask(self, agent_id: str, request: dict[str, Any]) -> dict[str, Any]:
        response = self.negotiate(agent_id, request)
        if "negotiation_id" not in response:
            raise Exception(f"Cannot communicate with Agent {agent_id}")

        negotiation_id = response["negotiation_id"]
        response = self.contract(negotiation_id)
        if "status" not in response:
            raise Exception(f"Server error")
        if response["status"] == "error":
            raise Exception(
                f"Cannot make a contract with Agent {agent_id}: {response['error_msg']}"
            )
        result = self.get_result(negotiation_id)

        return result

    def negotiate(self, agent_id: str, request: dict[str, Any]):
        response = self.post("negotiate", {"agent_id": agent_id, "request": request})
        return response

    def contract(self, negotiation_id: str):
        response = self.post("contract", {"negotiation_id": negotiation_id})
        return response

    def get_result(self, negotiation_id: str):
        msg = ""
        while True:
            response = self.get(f"result/{negotiation_id}")
            assert len(response) > 0, "Bad response error"
            if response["msg"] != msg:
                msg = response["msg"]
                logger.debug(msg)
            if response["status"] in ["done", "error"]:
                break
            time.sleep(1)
        if response["status"] == "error":
            raise Exception(f"Error response from agent: {response['msg']}")
        return response

    @property
    def header_auth(self) -> dict[str, str]:
        return {"authorization": f"Basic {self.auth}"}

    @property
    def header(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self.header_auth,
        }
        return headers

    def post(self, uri: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = requests.post(
                f"{self.broker_url}/api/v1/client/{uri}",
                json=payload,
                headers=self.header,
            )
        except requests.exceptions.ConnectionError:
            logger.exception("Connection error on post request")
            raise
        if response.status_code != 200:
            logger.exception(response.status_code)
            return {}
        obj = response.json()
        assert isinstance(obj, dict), f"Response was not a dict: {obj}"
        assert all(
            isinstance(k, str) for k in obj.keys()
        ), f"Some of response keys were not str: {obj.keys()}"
        return obj

    def get(self, uri: str) -> dict[str, Any]:
        try:
            response = requests.get(
                f"{self.broker_url}/api/v1/client/{uri}", headers=self.header
            )
        except requests.exceptions.ConnectionError:
            logger.exception(f"Connection error on get request; {uri=}")
            return {}

        if response.status_code != 200:
            logger.exception(
                f"Not 200: {response.status_code=}; {uri=}, {response.content=}"
            )
            return {}
        obj = response.json()
        assert isinstance(obj, dict), f"Response was not a dict: {obj}"
        assert all(
            isinstance(k, str) for k in obj.keys()
        ), f"Some of response keys were not str: {obj.keys()}"
        return obj

    def get_file(self, uri: str) -> requests.Response:
        try:
            response = requests.get(
                f"{self.broker_url}/api/v1/client/{uri}", headers=self.header_auth
            )
        except requests.exceptions.ConnectionError:
            logger.exception(f"Connection error on get request; {uri=}")
            raise Exception("Connection error")

        if response.status_code != 200:
            logger.exception(
                f"Not 200: {response.status_code=}; {uri=}, {response.content=}"
            )
            raise Exception("Not 200")
        return response


class AgentManager:
    WATCHING_INTERVAL = 5

    def __init__(self, dir_path: PathLike[str] | str = Path(".")):
        self.dir_path = Path(dir_path)
        self.agents = dict[Path, dict[str, Any]]()
        self.start_watching_loop()

    def start_watching_loop(self):
        while True:
            try:
                self.dog_watching()
                time.sleep(self.WATCHING_INTERVAL)
            except KeyboardInterrupt:
                logger.info("Automatic agent runner closing...")
                break
        for k in self.agents.keys():
            self.stop(k)

    def dog_watching(self):
        exisiting_files = set(self.dir_path.glob("*.py"))
        current_files = set(self.agents.keys())
        new_files = exisiting_files - current_files
        removed_files = current_files - exisiting_files
        ongoing_files = current_files & exisiting_files
        for file in new_files:
            self.load(file)
        for file in ongoing_files:
            self.check_for_update(file)
        for file in removed_files:
            self.stop(file)
            self.agents.pop(file)

    def run_module(self, module: ModuleType) -> Agent | list[Agent] | None:
        def is_agent(obj):
            return hasattr(obj, "_to_show_i_am_agent_instance")

        def is_automatic_agent(obj):
            return (
                hasattr(obj, "_is_automatic_mode")
                and getattr(obj, "_is_automatic_mode")()
            )

        for key in module.__dir__():
            if is_agent(getattr(module, key)):
                agent = getattr(module, key)
                assert isinstance(
                    agent, Agent
                ), f"Not an Agent instance; {module=}; {key=}; {agent=}"
                agent.run(_automatic=True)
                return agent
            elif is_automatic_agent(getattr(module, key)):
                agent_class = getattr(module, key)
                return getattr(agent_class, "_automatic_run")()
        return None

    def load(self, file: Path):
        self.agents[file] = {"mtime": file.stat().st_mtime}
        try:
            module_name = file.as_posix().replace(".py", "").replace("/", ".")
            module = importlib.import_module(module_name)
            self.agents[file]["module"] = module
            agent = self.run_module(module)
            if agent is not None:
                self.agents[file]["agent"] = agent
                if isinstance(agent, list):
                    logger.info(
                        f"Agent {[x.interface.name for x in agent]} has started!"
                    )
                else:
                    logger.info(f"Agent {agent.interface.name} has started!")
        except:
            logger.exception(f"load failed: {file=}")

    def check_for_update(self, file: Path):
        try:
            mtime = file.stat().st_mtime
            if mtime == self.agents[file]["mtime"]:
                return
            self.agents[file]["mtime"] = mtime
            if "module" not in self.agents[file]:
                self.load(file)
                return
            if "agent" in self.agents[file]:
                self.stop(file)
                time.sleep(Agent.HEARTBEAT_INTERVAL)

            module = importlib.reload(self.agents[file]["module"])
            self.agents[file]["module"] = module
            agent = self.run_module(module)
            if agent is not None:
                self.agents[file]["agent"] = agent
                if isinstance(agent, list):
                    logger.info(
                        f"Agent {[x.interface.name for x in agent]} has started!"
                    )
                else:
                    logger.info(f"Agent {agent.interface.name} has started!")
        except:
            logger.exception(f"check_for_update failed: {file=}")

    def stop(self, file: Path):
        try:
            if "agent" in self.agents[file]:
                if isinstance(self.agents[file]["agent"], list):
                    for agent in self.agents[file]["agent"]:
                        agent.goodbye()
                else:
                    self.agents[file]["agent"].goodbye()
        except:
            logger.exception(f"stop failed: {file=}")


def batch_run():
    AgentManager()


def main():
    from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser

    parser = ArgumentParser(
        prog="brokersystem",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("command", choices=["batch_run"], help="Command to run")
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {__version__}"
    )
    args = parser.parse_args()
    if args.command == "batch_run":
        batch_run()


if __name__ == "__main__":
    main()
