import array
import os
import random
import string
import time
from typing import Generator
from uuid import uuid4

import psycopg2
import pytest
import ulid
from pytest_docker_tools import container, fetch
from pytest_docker_tools.wrappers import Container
from uuid_extensions import uuid7

POSTGRES_DB = "testdb"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "password"

# Number of inserts to perform in the test, read from ENV if set otherwise default to 10
INSERT_COUNT = int(os.getenv("INSERT_COUNT", 100))
SELECT_COUNT = int(os.getenv("SELECT_COUNT", 10))
postgres_image = fetch(repository="postgres:latest")  # type: ignore

postgres_container: Container = container(
    scope="session",
    image="{postgres_image.id}",
    environment={
        "POSTGRES_DB": POSTGRES_DB,
        "POSTGRES_USER": POSTGRES_USER,
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
    },
    ports={
        "5432/tcp": None,
    },
)  # type: ignore


class SlidingSample:
    def __init__(self, size) -> None:
        self.size = size
        self.data = list()

    def append(self, item):
        if len(self.data) < self.size:
            self.data.append(item)
        elif random.random() > 0.5:
            index = int(random.random() * self.size)
            self.data[index] = item

        pass

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)


@pytest.fixture
def postgres_url(postgres_container: Container) -> str:
    # Wait for the container to be ready
    while not postgres_container.status == "running":
        time.sleep(5)

    ip, port = postgres_container.get_addr("5432/tcp")
    return f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{ip}:{port}/{POSTGRES_DB}"


def generate_random_string():
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=10))


@pytest.fixture(autouse=True)
def cleanup(postgres_url: str) -> Generator[None, None, None]:
    # Run test
    yield

    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS child;")
    cur.execute("DROP TABLE IF EXISTS parent;")
    conn.commit()
    cur.close()
    conn.close()


def create_tables_with_serial_pk(conn, cur):
    cur.execute("CREATE TABLE parent (id serial PRIMARY KEY, data text);")
    cur.execute(
        "CREATE TABLE child (id serial PRIMARY KEY, parent_id int REFERENCES parent(id), data text);"
    )
    conn.commit()


def create_tables_with_bytea_ulid_pk(conn, cur):
    cur.execute("CREATE TABLE parent (id BYTEA PRIMARY KEY, data text);")
    cur.execute(
        "CREATE TABLE child (id BYTEA PRIMARY KEY, parent_id BYTEA REFERENCES parent(id), data text);"
    )
    conn.commit()


def create_tables_with_uuidv7_pk(conn, cur):
    cur.execute("CREATE TABLE parent (id UUID PRIMARY KEY, data text);")
    cur.execute(
        "CREATE TABLE child (id UUID PRIMARY KEY, parent_id UUID REFERENCES parent(id), data text);"
    )
    conn.commit()


def create_tables_with_uuidv4_pk(conn, cur):
    cur.execute("CREATE TABLE parent (id UUID PRIMARY KEY, data text);")
    cur.execute(
        "CREATE TABLE child (id UUID PRIMARY KEY, parent_id UUID REFERENCES parent(id), data text);"
    )
    conn.commit()


@pytest.mark.benchmark(
    group="insert",
)
def test_serial_pk_insert(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_serial_pk(conn, cur)

    ids = SlidingSample(SELECT_COUNT)

    # Benchmark the insert
    @benchmark
    def insert():
        for i in range(INSERT_COUNT):
            ids.append(i)
            cur.execute(
                f"INSERT INTO parent (data) VALUES ('{generate_random_string()}');"
            )
        conn.commit()

    cur.close()
    conn.close()


@pytest.mark.benchmark(
    group="select",
)
def test_serial_pk_select(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_serial_pk(conn, cur)

    ids = SlidingSample(SELECT_COUNT)

    for i in range(INSERT_COUNT):
        cur.execute(
            f"INSERT INTO parent (data) VALUES ('{generate_random_string()}') RETURNING id;"
        )
        ids.append(cur.fetchone()[0])  # type: ignore

    conn.commit()

    @benchmark
    def select():
        result = []
        for id in ids:
            cur.execute(f"SELECT * FROM parent WHERE id = {id};")
            result.append(cur.fetchall())
        conn.commit()
        return result

    assert len(select) == SELECT_COUNT  # type: ignore

    cur.close()
    conn.close()


@pytest.mark.benchmark(group="relation")
def test_serial_pk_parent_child_insert(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_serial_pk(conn, cur)

    @benchmark
    def insert():
        for i in range(INSERT_COUNT):
            cur.execute(
                f"INSERT INTO parent (data) VALUES ('{generate_random_string()}') RETURNING id;"
            )
            parent_id = cur.fetchone()[0]  # type: ignore
            cur.execute(
                f"INSERT INTO child (parent_id, data) VALUES ({parent_id}, '{generate_random_string()}');"
            )
        conn.commit()

    cur.close()
    conn.close()


@pytest.mark.benchmark(
    group="insert",
)
def test_bytea_ulid_pk_insert(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_bytea_ulid_pk(conn, cur)

    # Benchmark the insert
    @benchmark
    def insert():
        for i in range(INSERT_COUNT):
            cur.execute(
                f"INSERT INTO parent (id, data) VALUES ('{ulid.ulid()}','{generate_random_string()}');"
            )
        conn.commit()

    cur.close()
    conn.close()


@pytest.mark.benchmark(
    group="select",
)
def test_bytea_ulid_pk_select(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_bytea_ulid_pk(conn, cur)

    ids = SlidingSample(SELECT_COUNT)

    for i in range(INSERT_COUNT):
        id = ulid.ulid()
        cur.execute(
            f"INSERT INTO parent (id, data) VALUES ('{id}','{generate_random_string()}');"
        )
        ids.append(id)

    conn.commit()

    @benchmark
    def select():
        result = []
        for id in ids:
            cur.execute(f"SELECT * FROM parent WHERE id = '{id}';")
            result.append(cur.fetchall())
        conn.commit()
        return result

    assert len(select) == SELECT_COUNT  # type: ignore

    cur.close()
    conn.close()


@pytest.mark.benchmark(group="relation")
def test_bytea_ulid_pk_parent_child_insert(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_bytea_ulid_pk(conn, cur)

    @benchmark
    def insert():
        for i in range(INSERT_COUNT):
            parent_id = ulid.ulid()
            cur.execute(
                f"INSERT INTO parent (id, data) VALUES ('{parent_id}', '{generate_random_string()}');"
            )
            child_id = ulid.ulid()
            cur.execute(
                f"INSERT INTO child (id, parent_id, data) VALUES ('{child_id}', '{parent_id}', '{generate_random_string()}');",
            )
        conn.commit()

    cur.close()
    conn.close()


@pytest.mark.benchmark(
    group="insert",
)
def test_uuid_uuidv7_pk_insert(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_uuidv7_pk(conn, cur)

    # Benchmark the insert
    @benchmark
    def result():
        for i in range(INSERT_COUNT):
            cur.execute(
                f"INSERT INTO parent (id, data) VALUES ('{uuid7(as_type='str')}','{generate_random_string()}');"
            )
        conn.commit()

    cur.execute("SELECT * FROM parent lIMIT 10;")
    x = cur.fetchall()  # type: ignore
    print(x)
    cur.close()
    conn.close()


@pytest.mark.benchmark(
    group="select",
)
def test_uuid_uuidv7_pk_select(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_uuidv7_pk(conn, cur)

    ids = SlidingSample(SELECT_COUNT)

    for i in range(INSERT_COUNT):
        id = uuid7(as_type="str")
        cur.execute(
            f"INSERT INTO parent (id, data) VALUES ('{id}','{generate_random_string()}');"
        )
        ids.append(id)

    conn.commit()

    @benchmark
    def select():
        result = []
        for id in ids:
            cur.execute(f"SELECT * FROM parent WHERE id = '{id}';")
            result.append(cur.fetchall())
        conn.commit()
        return result

    assert len(select) == SELECT_COUNT  # type: ignore

    cur.close()
    conn.close()


@pytest.mark.benchmark(group="relation")
def test_uuidv7_pk_parent_child_insert(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_uuidv7_pk(conn, cur)

    @benchmark
    def insert():
        for i in range(INSERT_COUNT):
            parent_id = uuid7(as_type="str")
            cur.execute(
                f"INSERT INTO parent (id, data) VALUES ('{parent_id}', '{generate_random_string()}');"
            )
            child_id = uuid7(as_type="str")
            cur.execute(
                f"INSERT INTO child (id, parent_id, data) VALUES ('{child_id}', '{parent_id}', '{generate_random_string()}');",
            )
        conn.commit()

    cur.close()
    conn.close()


@pytest.mark.benchmark(
    group="insert",
)
def test_uuid_uuidv4_pk_insert(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_uuidv4_pk(conn, cur)

    # Benchmark the insert
    @benchmark
    def result():
        for i in range(INSERT_COUNT):
            cur.execute(
                f"INSERT INTO parent (id, data) VALUES ('{str(uuid4())}','{generate_random_string()}');"
            )
        conn.commit()

    cur.execute("SELECT * FROM parent lIMIT 10;")
    x = cur.fetchall()  # type: ignore
    print(x)
    cur.close()
    conn.close()


@pytest.mark.benchmark(
    group="select",
)
def test_uuid_uuidv4_pk_select(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_uuidv4_pk(conn, cur)

    ids = SlidingSample(SELECT_COUNT)

    for i in range(INSERT_COUNT):
        id = str(uuid4())
        cur.execute(
            f"INSERT INTO parent (id, data) VALUES ('{id}','{generate_random_string()}');"
        )
        ids.append(id)

    conn.commit()

    @benchmark
    def select():
        result = []
        for id in ids:
            cur.execute(f"SELECT * FROM parent WHERE id = '{id}';")
            result.append(cur.fetchall())
        conn.commit()
        return result

    assert len(select) == SELECT_COUNT  # type: ignore

    cur.close()
    conn.close()


@pytest.mark.benchmark(group="relation")
def test_uuidv4_pk_parent_child_insert(benchmark, postgres_url: str) -> None:
    conn = psycopg2.connect(postgres_url)
    cur = conn.cursor()

    create_tables_with_uuidv4_pk(conn, cur)

    @benchmark
    def insert():
        for i in range(INSERT_COUNT):
            parent_id = str(uuid4())
            cur.execute(
                f"INSERT INTO parent (id, data) VALUES ('{parent_id}', '{generate_random_string()}');"
            )
            child_id = str(uuid4())
            cur.execute(
                f"INSERT INTO child (id, parent_id, data) VALUES ('{child_id}', '{parent_id}', '{generate_random_string()}');",
            )
        conn.commit()

    cur.close()
    conn.close()
