#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2021 The Matrix.org Foundation C.I.C.
# Copyright (C) 2023 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
# Originally licensed under the Apache License, Version 2.0:
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# [This file includes modifications made by New Vector Limited]
#
#
from contextlib import contextmanager
from typing import Generator
from unittest import mock

from twisted.internet.testing import MemoryReactor

from synapse.app.generic_worker import GenericWorkerServer
from synapse.server import HomeServer
from synapse.storage.database import (
    DatabasePool,
    LoggingDatabaseConnection,
    PsycopgDatabasePool,
    TwistedDatabasePool,
)
from synapse.storage.prepare_database import PrepareDatabaseException, prepare_database
from synapse.storage.schema import SCHEMA_VERSION
from synapse.types import JsonDict
from synapse.util.clock import Clock

from tests.unittest import HomeserverTestCase


def fake_listdir(filepath: str) -> list[str]:
    """
    A fake implementation of os.listdir which we can use to mock out the filesystem.

    Args:
        filepath: The directory to list files for.

    Returns:
        A list of files and folders in the directory.
    """
    if filepath.endswith("full_schemas"):
        return [str(SCHEMA_VERSION)]

    return ["99_add_unicorn_to_database.sql"]


class WorkerSchemaTests(HomeserverTestCase):
    def make_homeserver(self, reactor: MemoryReactor, clock: Clock) -> HomeServer:
        hs = self.setup_test_homeserver(homeserver_to_use=GenericWorkerServer)
        return hs

    def default_config(self) -> JsonDict:
        conf = super().default_config()

        # Mark this as a worker app.
        conf["worker_app"] = "yes"
        conf["instance_map"] = {"main": {"host": "127.0.0.1", "port": 0}}

        return conf

    @contextmanager
    def _get_connection(
        self, db_pool: DatabasePool
    ) -> Generator[LoggingDatabaseConnection]:
        """
        Grab a database connection. Different pools require different methods. Use the
        context manager pattern to ensure closing the connection after.
        """
        if isinstance(db_pool, PsycopgDatabasePool):
            conn = self.get_success_or_raise(db_pool._db_pool.connection().__aenter__())
            yield LoggingDatabaseConnection(
                conn=conn,
                engine=db_pool.engine,
                default_txn_name="tests",
                server_name="test_server",
            )
            self.get_success_or_raise(
                db_pool._db_pool.connection().__aexit__(None, None, None)
            )
        else:
            assert isinstance(db_pool, TwistedDatabasePool)
            db_conn = LoggingDatabaseConnection(
                conn=db_pool._db_pool.connect(),
                engine=db_pool.engine,
                default_txn_name="tests",
                server_name="test_server",
            )
            yield db_conn
            db_conn.close()

    def test_rolling_back(self) -> None:
        """Test that workers can start if the DB is a newer schema version"""
        db_pool = self.hs.get_datastores().main.db_pool
        with self._get_connection(db_pool) as db_conn:
            cur = db_conn.cursor()
            cur.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION + 1,))

            db_conn.commit()

            prepare_database(db_conn, db_pool.engine, self.hs.config)

    def test_not_upgraded_old_schema_version(self) -> None:
        """Test that workers don't start if the DB has an older schema version"""
        db_pool = self.hs.get_datastores().main.db_pool
        with self._get_connection(db_pool) as db_conn:
            cur = db_conn.cursor()
            cur.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION - 1,))

            db_conn.commit()

            with self.assertRaises(PrepareDatabaseException):
                prepare_database(db_conn, db_pool.engine, self.hs.config)

    def test_not_upgraded_current_schema_version_with_outstanding_deltas(self) -> None:
        """
        Test that workers don't start if the DB is on the current schema version,
        but there are still outstanding delta migrations to run.
        """
        db_pool = self.hs.get_datastores().main.db_pool
        with self._get_connection(db_pool) as db_conn:
            # Set the schema version of the database to the current version
            cur = db_conn.cursor()
            cur.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))

            db_conn.commit()

            # Path `os.listdir` here to make synapse think that there is a migration
            # file ready to be run.
            # Note that we can't patch this function for the whole method, else Synapse
            # will try to find the file when building the database initially.
            with mock.patch("os.listdir", mock.Mock(side_effect=fake_listdir)):
                with self.assertRaises(PrepareDatabaseException):
                    # Synapse should think that there is an outstanding migration file due to
                    # patching 'os.listdir' in the function decorator.
                    #
                    # We expect Synapse to raise an exception to indicate the master process
                    # needs to apply this migration file.
                    prepare_database(db_conn, db_pool.engine, self.hs.config)
