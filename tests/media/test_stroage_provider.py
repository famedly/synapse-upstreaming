import os

from twisted.internet.testing import MemoryReactor

from synapse.media._base import FileInfo
from synapse.media.filepath import MediaFilePaths
from synapse.media.storage_provider import (
    FileStorageProviderBackend,
)
from synapse.server import HomeServer
from synapse.util.clock import Clock

from tests import unittest
from tests.test_utils import SMALL_PNG, SMALL_PNG_SHA256, read, write


class FileStorageProviderBackendTestCase(unittest.HomeserverTestCase):
    """`FileStorageProviderBackend.store_file` with content-addressed paths."""

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.primary = self.mktemp()
        self.backup = self.mktemp()
        hs.config.media.media_store_path = self.primary
        self.backend = FileStorageProviderBackend(hs, self.backup)
        self.rel_path = MediaFilePaths(self.primary).local_media_filepath_rel(
            SMALL_PNG_SHA256
        )
        write(os.path.join(self.primary, self.rel_path), SMALL_PNG)

    def test_existing_sha256_backup_is_not_copied(self) -> None:
        """A backup at a SHA-256 path already holds these bytes, so it is kept."""
        backup_path = os.path.join(self.backup, self.rel_path)
        write(backup_path, b"existing backup")

        file_info = FileInfo(None, "media_id", sha256=SMALL_PNG_SHA256)
        self.get_success(self.backend.store_file(self.rel_path, file_info))

        self.assertEqual(read(backup_path), b"existing backup")

    def test_missing_sha256_backup_is_copied(self) -> None:
        file_info = FileInfo(None, "media_id", sha256=SMALL_PNG_SHA256)
        self.get_success(self.backend.store_file(self.rel_path, file_info))

        self.assertEqual(read(os.path.join(self.backup, self.rel_path)), SMALL_PNG)

    def test_existing_backup_without_sha256_is_overwritten(self) -> None:
        """Without a SHA-256 the path is not content-addressed, so it is copied."""
        backup_path = os.path.join(self.backup, self.rel_path)
        write(backup_path, b"existing backup")

        self.get_success(
            self.backend.store_file(self.rel_path, FileInfo(None, "media_id"))
        )

        self.assertEqual(read(backup_path), SMALL_PNG)
