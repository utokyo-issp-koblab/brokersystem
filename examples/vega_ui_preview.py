"""Example agent that provides a Vega UI preview spec.

This demonstrates `agent.ui_preview`, which is sent as part of the agent config
and can be rendered by the broker UI during negotiation.

Run:
  export BROKER_URL="http://..."
  export AGENT_ID="..."
  export AGENT_SECRET="..."
  python examples/vega_ui_preview.py

Notes:
- The broker UI should treat `agent.ui_preview` as untrusted input and render it
  in a sandbox (e.g., a sandboxed iframe).
- Do not embed secrets in the spec.
"""

from __future__ import annotations

import math
import os

from brokersystem import Agent, Job, JsonDict, Number


def require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Missing required env var: {key}")
    return value


VEGA_SPEC: JsonDict = {
    "$schema": "https://vega.github.io/schema/vega/v6.json",
    "width": 400,
    "height": 200,
    "padding": 0,
    "autosize": "none",
    "background": "white",
    # The broker UI injects current form values into signals of the same name.
    "signals": [
        {"name": "taper_angle", "value": 10.0},
        {"name": "thickness", "value": 5.0},
        {"name": "hole_diameter", "value": 5.0},
        {"name": "length_per_pixel", "value": 0.1},
        {"name": "abf_y0", "value": 60},
        {"name": "x_thickness", "update": "width - 20"},
        {"name": "arrow_size", "value": 5},
        {
            "name": "d1_um",
            "update": "hole_diameter - 2 * thickness * tan((taper_angle / 2) * 3.141592653589793 / 180)",
        },
        {"name": "abf_thickness_px", "update": "thickness / length_per_pixel"},
        {"name": "abf_d0_px", "update": "hole_diameter / length_per_pixel"},
        {"name": "abf_d1_px", "update": "d1_um / length_per_pixel"},
        {"name": "x_left0", "update": "0"},
        {"name": "x_right0", "update": "width"},
        {"name": "x_left1", "update": "width / 2 - abf_d0_px / 2"},
        {"name": "x_right1", "update": "width / 2 + abf_d0_px / 2"},
        {"name": "x_left2", "update": "width / 2 - abf_d1_px / 2"},
        {"name": "x_right2", "update": "width / 2 + abf_d1_px / 2"},
        {"name": "y0", "update": "abf_y0"},
        {"name": "y1", "update": "abf_y0 + abf_thickness_px"},
        {"name": "y_boundary_dim", "update": "min(height - 26, y1 + 18)"},
        {"name": "y_boundary_tick", "update": "min(height - 10, y1 + 24)"},
        {"name": "y_boundary_label", "update": "min(height - 4, y1 + 30)"},
        {
            "name": "abf_left_path",
            "update": "'M ' + x_left0 + ' ' + y0 + ' L ' + x_left1 + ' ' + y0 + ' L ' + x_left2 + ' ' + y1 + ' L ' + x_left0 + ' ' + y1 + ' Z'",
        },
        {
            "name": "abf_right_path",
            "update": "'M ' + x_right0 + ' ' + y0 + ' L ' + x_right1 + ' ' + y0 + ' L ' + x_right2 + ' ' + y1 + ' L ' + x_right0 + ' ' + y1 + ' Z'",
        },
    ],
    "marks": [
        {
            "type": "text",
            "encode": {
                "enter": {
                    "font": {"value": "Helvetica"},
                    "fontSize": {"value": 18},
                    "fontWeight": {"value": "bold"},
                    "fill": {"value": "black"},
                    "align": {"value": "left"},
                    "baseline": {"value": "top"},
                },
                "update": {
                    "x": {"value": 8},
                    "y": {"value": 6},
                    "text": {"value": "Target design"},
                },
            },
        },
        {
            "type": "path",
            "encode": {
                "enter": {"fill": {"value": "skyblue"}},
                "update": {"path": {"signal": "abf_left_path"}},
            },
        },
        {
            "type": "path",
            "encode": {
                "enter": {"fill": {"value": "skyblue"}},
                "update": {"path": {"signal": "abf_right_path"}},
            },
        },
        {
            "type": "rect",
            "encode": {
                "enter": {"fill": {"value": "orange"}},
                "update": {
                    "x": {"value": 0},
                    "y": {"signal": "y1"},
                    "width": {"signal": "width"},
                    "height": {"signal": "max(0, height - y1)"},
                },
            },
        },
        {
            "type": "rule",
            "encode": {
                "enter": {
                    "stroke": {"value": "black"},
                    "strokeDash": {"value": [3, 3]},
                },
                "update": {
                    "x": {"signal": "x_left1"},
                    "y": {"signal": "y0 - 30"},
                    "y2": {"signal": "y0"},
                },
            },
        },
        {
            "type": "rule",
            "encode": {
                "enter": {
                    "stroke": {"value": "black"},
                    "strokeDash": {"value": [3, 3]},
                },
                "update": {
                    "x": {"signal": "x_right1"},
                    "y": {"signal": "y0 - 30"},
                    "y2": {"signal": "y0"},
                },
            },
        },
        {
            "type": "rule",
            "encode": {
                "enter": {
                    "stroke": {"value": "black"},
                    "strokeDash": {"value": [1, 1]},
                },
                "update": {
                    "x": {"signal": "x_left1"},
                    "x2": {"signal": "x_right1"},
                    "y": {"signal": "y0 - 20"},
                },
            },
        },
        {
            "type": "text",
            "encode": {
                "enter": {
                    "font": {"value": "Helvetica"},
                    "fontSize": {"value": 16},
                    "align": {"value": "center"},
                    "baseline": {"value": "middle"},
                },
                "update": {
                    "x": {"signal": "width / 2"},
                    "y": {"signal": "y0 - 30"},
                    "fill": {"value": "black"},
                    "text": {"signal": "format(hole_diameter, '.3~f') + ' µm'"},
                },
            },
        },
        {
            "type": "rule",
            "encode": {
                "enter": {
                    "stroke": {"value": "black"},
                    "strokeDash": {"value": [1, 1]},
                },
                "update": {
                    "x": {"signal": "x_thickness"},
                    "y": {"signal": "y0"},
                    "y2": {"signal": "y1"},
                },
            },
        },
        {
            "type": "rule",
            "encode": {
                "enter": {"stroke": {"value": "black"}},
                "update": {
                    "x": {"signal": "x_thickness"},
                    "y": {"signal": "y0"},
                    "x2": {"signal": "x_thickness + arrow_size"},
                    "y2": {"signal": "y0 + arrow_size"},
                },
            },
        },
        {
            "type": "rule",
            "encode": {
                "enter": {"stroke": {"value": "black"}},
                "update": {
                    "x": {"signal": "x_thickness"},
                    "y": {"signal": "y0"},
                    "x2": {"signal": "x_thickness - arrow_size"},
                    "y2": {"signal": "y0 + arrow_size"},
                },
            },
        },
        {
            "type": "rule",
            "encode": {
                "enter": {"stroke": {"value": "black"}},
                "update": {
                    "x": {"signal": "x_thickness"},
                    "y": {"signal": "y1"},
                    "x2": {"signal": "x_thickness + arrow_size"},
                    "y2": {"signal": "y1 - arrow_size"},
                },
            },
        },
        {
            "type": "rule",
            "encode": {
                "enter": {"stroke": {"value": "black"}},
                "update": {
                    "x": {"signal": "x_thickness"},
                    "y": {"signal": "y1"},
                    "x2": {"signal": "x_thickness - arrow_size"},
                    "y2": {"signal": "y1 - arrow_size"},
                },
            },
        },
        {
            "type": "rule",
            "encode": {
                "enter": {
                    "stroke": {"value": "black"},
                    "strokeDash": {"value": [3, 3]},
                },
                "update": {
                    "x": {"signal": "x_left2"},
                    "y": {"signal": "y1"},
                    "y2": {"signal": "y_boundary_tick"},
                },
            },
        },
        {
            "type": "rule",
            "encode": {
                "enter": {
                    "stroke": {"value": "black"},
                    "strokeDash": {"value": [3, 3]},
                },
                "update": {
                    "x": {"signal": "x_right2"},
                    "y": {"signal": "y1"},
                    "y2": {"signal": "y_boundary_tick"},
                },
            },
        },
        {
            "type": "rule",
            "encode": {
                "enter": {
                    "stroke": {"value": "black"},
                    "strokeDash": {"value": [1, 1]},
                },
                "update": {
                    "x": {"signal": "x_left2"},
                    "x2": {"signal": "x_right2"},
                    "y": {"signal": "y_boundary_dim"},
                },
            },
        },
        {
            "type": "text",
            "encode": {
                "enter": {
                    "font": {"value": "Helvetica"},
                    "fontSize": {"value": 16},
                    "align": {"value": "center"},
                    "baseline": {"value": "middle"},
                },
                "update": {
                    "x": {"signal": "width / 2"},
                    "y": {"signal": "y_boundary_label"},
                    "fill": {"value": "black"},
                    "text": {"signal": "format(d1_um, '.3~f') + ' µm'"},
                },
            },
        },
        {
            "type": "text",
            "encode": {
                "enter": {
                    "font": {"value": "Helvetica"},
                    "fontSize": {"value": 16},
                    "align": {"value": "right"},
                    "baseline": {"value": "middle"},
                },
                "update": {
                    "x": {"signal": "width - 25"},
                    "y": {"signal": "y0 + abf_thickness_px / 2 + 6"},
                    "fill": {"value": "black"},
                    "text": {"signal": "format(thickness, '.3~f') + ' µm'"},
                },
            },
        },
        {
            "type": "text",
            "encode": {
                "enter": {
                    "font": {"value": "Helvetica"},
                    "fontSize": {"value": 16},
                    "fontWeight": {"value": "bold"},
                    "fill": {"value": "blue"},
                },
                "update": {
                    "x": {"value": 20},
                    "y": {"signal": "y0 + abf_thickness_px / 2 + 6"},
                    "text": {"value": "ABF"},
                },
            },
        },
        {
            "type": "text",
            "encode": {
                "enter": {
                    "font": {"value": "Helvetica"},
                    "fontSize": {"value": 16},
                    "fontWeight": {"value": "bold"},
                    "fill": {"value": "brown"},
                },
                "update": {
                    "x": {"value": 20},
                    "y": {"signal": "(y1 + height) / 2 + 6"},
                    "text": {"value": "Copper"},
                },
            },
        },
    ],
}


def build_agent() -> Agent:
    broker_url = require_env("BROKER_URL")
    agent_id = require_env("AGENT_ID")
    agent_secret = require_env("AGENT_SECRET")

    agent = Agent(broker_url)

    @agent.config
    def make_config() -> None:
        agent.name = "vega-ui-preview-agent"
        agent.secret_token = f"{agent_id}:{agent_secret}"
        agent.description = "Example agent that includes a Vega UI preview spec"
        agent.charge = 1

        agent.input.taper_angle = Number(10.0, min=0.0, max=90.0, step=0.5, unit="deg")
        agent.input.thickness = Number(5.0, min=0.1, max=200.0, step=0.1, unit="µm")
        agent.input.hole_diameter = Number(5.0, min=0.1, max=200.0, step=0.1, unit="µm")

        agent.output.d1_um = Number(unit="µm")

        # UI preview during negotiation
        agent.ui_preview = {"type": "vega", "spec": VEGA_SPEC}

    @agent.job_func
    def job_func(job: Job) -> JsonDict:
        taper_angle = float(job["taper_angle"])
        thickness = float(job["thickness"])
        hole_diameter = float(job["hole_diameter"])
        d1_um = hole_diameter - 2.0 * thickness * math.tan(
            (taper_angle / 2.0) * math.pi / 180.0
        )
        return {"d1_um": d1_um}

    return agent


if __name__ == "__main__":
    build_agent().run()
