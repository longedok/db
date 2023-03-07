#!/usr/bin/env python
import subprocess
import pytest
import os


def run_script(commands):
    pipe = subprocess.Popen(
        ["./target/debug/db", "test.dat"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )

    out, _ = pipe.communicate("\n".join(commands))

    return out.split("\n")


@pytest.fixture(autouse=True)
def remove_db():
    try:
        os.remove("./test.dat")
    except FileNotFoundError:
        pass


def test_insert_retrieve():
    result = run_script([
        "insert 1 user1 person1@example.com",
        "select",
        ".exit",
    ])

    assert result == [
        "db > Executed.",
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db > ",
    ]


def test_prints_error_when_table_full():
    script = []
    for i in range(1401):
        script.append(f"insert {i} user{i} person{i}@example.com")
    script.append(".exit")
    result = run_script(script)

    assert result[-2] == "db > Error: Table full."


def test_insert_max_length():
    long_username = "a"*32
    long_email = "a"*255

    result = run_script([
        f"insert 1 {long_username} {long_email}",
        "select",
        ".exit",
    ])

    assert result == [
        "db > Executed.",
        f"db > (1, {long_username}, {long_email})",
        "Executed.",
        "db > ",
    ]


def test_print_error_when_strings_too_long():
    long_username = "a"*33
    long_email = "a"*256

    result = run_script([
        f"insert 1 {long_username} {long_email}",
        "select",
        ".exit",
    ])

    print(result)

    assert result == [
        "db > String is too long.",
        "db > Executed.",
        "db > ",
    ]


def test_print_error_when_id_negative():
    result = run_script([
        f"insert -1 longedok foo@bar.com",
        "select",
        ".exit",
    ])

    print(result)

    assert result == [
        "db > ID must be positive.",
        "db > Executed.",
        "db > ",
    ]


def test_keeps_data_after_closing_connection():
    result1 = run_script([
        "insert 1 user1 person1@example.com",
        ".exit"
    ])

    assert result1 == [
        "db > Executed.",
        "db > ",
    ]

    result2 = run_script([
        "select",
        ".exit"
    ])

    assert result2 == [
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db > ",
    ]

