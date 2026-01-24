import pandas as pd

from brokersystem.agent import Choice, File, Number, String, Table, ValueTemplate


def test_guess_from_value_number() -> None:
    template = ValueTemplate.guess_from_value(3)
    assert isinstance(template, Number)


def test_guess_from_value_string() -> None:
    template = ValueTemplate.guess_from_value("hello")
    assert isinstance(template, String)


def test_guess_from_value_choice() -> None:
    template = ValueTemplate.guess_from_value(["a", "b"])
    assert isinstance(template, Choice)
    assert template.constraint_dict["choices"] == ["a", "b"]


def test_guess_from_value_table_from_dataframe() -> None:
    df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
    template = ValueTemplate.guess_from_value(df)
    assert isinstance(template, Table)
    assert template.format_dict["@repr"]["type"] == "graph"


def test_table_format_for_output_from_list_of_dict() -> None:
    table = Table(unit_dict={"x": "m", "y": "s"})
    value, fmt = table.format_for_output([{"x": 1, "y": 2}], lambda *_: {})
    assert value == {"x": [1], "y": [2]}
    assert fmt["@table"] == ["x", "y"]


def test_file_format_for_output_uploads_bytes() -> None:
    uploader_calls = []

    def fake_uploader(file_type: str, data: bytes):
        uploader_calls.append((file_type, data))
        return {"file_id": "file-123.png"}

    file_template = File("png")
    value, fmt = file_template.format_for_output(b"data", fake_uploader)

    assert uploader_calls == [("png", b"data")]
    assert value == "file-123.png"
    assert fmt["@type"] == "image"
