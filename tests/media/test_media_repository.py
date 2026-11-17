import os
from io import BytesIO
from typing import Any, BinaryIO
from unittest.mock import AsyncMock, Mock, patch

from matrix_common.types.mxc_uri import MXCUri
from parameterized import parameterized

from twisted.internet import defer
from twisted.internet.testing import MemoryReactor
from twisted.web.resource import Resource

from synapse.media._base import FileInfo, Responder
from synapse.media.media_storage import (
    _MEDIA_TEMP_FILE_PREFIX,
    FileResponder,
)
from synapse.rest import admin
from synapse.rest.client import login, media
from synapse.server import HomeServer
from synapse.storage.databases.main.media_repository import RemoteMedia
from synapse.types import JsonDict, UserID
from synapse.util.clock import Clock
from synapse.util.stringutils import random_string

from tests import unittest
from tests.test_utils import (
    SMALL_PNG,
    SMALL_PNG_SHA256,
    get_temp_files_from_directory,
    read,
    write,
)

OTHER_SHA256 = "0" * 64


class _MediaRepoTestCase(unittest.HomeserverTestCase):
    """A homeserver with a local media store, a storage provider and
    a mocked client for remote download."""

    servlets = [
        media.register_servlets,
        login.register_servlets,
        admin.register_servlets,
    ]

    def default_config(self) -> JsonDict:
        config = super().default_config()
        config["media_storage_providers"] = [
            {
                "module": "synapse.media.storage_provider.FileStorageProviderBackend",
                "store_local": True,
                "store_remote": True,
                "store_synchronous": True,
                "config": {"directory": self._media_storage_provider_path},
            }
        ]
        return config

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        self.creator = self.register_user("creator", "testpass")
        self.creator_tok = self.login("creator", "testpass")
        self.media_repo = hs.get_media_repository()
        self.filepaths = self.media_repo.filepaths
        self.media_repo.client = Mock()
        self.media_repo.client.download_media = self._download

    def create_resource_dict(self) -> dict[str, Resource]:
        resources = super().create_resource_dict()
        resources["/_matrix/media"] = self.hs.get_media_repository_resource()
        return resources

    async def _download(
        self, server_name: str, media_id: str, output_stream: BinaryIO, **kwargs: Any
    ) -> tuple[int, dict[bytes, list[bytes]]]:
        output_stream.write(SMALL_PNG)
        return len(SMALL_PNG), {b"Content-Type": [b"image/png"]}

    def _download_remote_file(self, origin: str, media_id: str) -> str:
        # Federation download will be covered in the test_federation_media.py
        remote_media = self.get_success(
            self.media_repo._download_remote_file(
                origin, media_id, 20_000, Mock(), "127.0.0.1"
            )
        )
        return remote_media.filesystem_id

    def _get_remote_media_impl(
        self, server_name: str, media_id: str
    ) -> tuple[Responder | None, RemoteMedia]:
        return self.get_success(
            self.media_repo._get_remote_media_impl(
                server_name=server_name,
                media_id=media_id,
                max_timeout_ms=1000,
                download_ratelimiter=AsyncMock(),
                ip_address="127.0.0.1",
                use_federation_endpoint=False,
                allow_authenticated=True,
            )
        )

    def _create_or_update_content(
        self, content: bytes = SMALL_PNG, media_id: str | None = None
    ) -> str:
        mxc = self.get_success(
            self.media_repo.create_or_update_content(
                "image/png",
                None,
                BytesIO(content),
                len(content),
                UserID.from_string(self.creator),
                media_id=media_id,
            )
        )
        return mxc.media_id

    def _store_local_media(
        self,
        media_id: str,
        sha256: str | None = None,
    ) -> None:
        self.get_success(
            self.store.store_local_media(
                media_id=media_id,
                media_type="image/png",
                time_now_ms=self.clock.time_msec(),
                upload_name=None,
                media_length=len(SMALL_PNG),
                user_id=UserID.from_string(self.creator),
                sha256=sha256,
                quarantined_by=None,
            )
        )

    def _store_cached_remote_media(
        self,
        server_name: str,
        media_id: str,
        upload_name: str | None,
        filesystem_id: str,
        sha256: str | None = None,
    ) -> None:
        self.get_success(
            self.store.store_cached_remote_media(
                origin=server_name,
                media_id=media_id,
                media_type="image/png",
                time_now_ms=self.clock.time_msec(),
                upload_name=upload_name,
                media_length=len(SMALL_PNG),
                filesystem_id=filesystem_id,
                sha256=sha256,
            )
        )

    def _generate_thumbnails(
        self,
        server_name: str | None,
        media_id: str,
        file_id: str,
        origin_sha256: str | None = None,
    ) -> None:
        self.get_success(
            self.media_repo._generate_thumbnails(
                server_name=server_name,
                media_id=media_id,
                media_type="image/png",
                file_id=file_id,
                origin_sha256=origin_sha256,
            )
        )

    def _create_legacy_local_media(self, sha256: str | None) -> MXCUri:
        """Store local media and its thumbnails at its media-ID path, with or without
        `sha256` record."""
        media_id = random_string(24)
        write(self.filepaths.local_media_filepath(media_id), SMALL_PNG)
        self._store_local_media(media_id, sha256)
        self._generate_thumbnails(None, media_id, media_id, sha256)
        return MXCUri(server_name=self.hs.config.server.server_name, media_id=media_id)

    def _create_legacy_remote_media(
        self, server_name: str, sha256: str | None
    ) -> MXCUri:
        """Store remote media and its thumbnails at its media-ID path, with or without
        `sha256` record"""
        media_id = random_string(24)
        write(self.filepaths.remote_media_filepath(server_name, media_id), SMALL_PNG)
        self._store_cached_remote_media(
            server_name, media_id, "other_server_media", media_id, sha256
        )
        self._generate_thumbnails(server_name, media_id, media_id, sha256)
        return MXCUri(server_name=server_name, media_id=media_id)

    def _create_remote_media_sha256(self, server_name: str) -> str:
        """Creates remote media object with sha256 path."""
        media_id = random_string(24)
        upload_name = "other_server_media"
        expected_path = self.media_repo.filepaths.remote_media_filepath(
            server_name, SMALL_PNG_SHA256
        )

        write(expected_path, SMALL_PNG)
        self.assertTrue(
            os.path.exists(expected_path), f"File does not exist: {expected_path}"
        )

        self.get_success(
            self.store.store_cached_remote_media(
                origin=server_name,
                media_id=media_id,
                media_type="image/png",
                media_length=len(SMALL_PNG),
                time_now_ms=self.clock.time_msec(),
                upload_name=upload_name,
                filesystem_id=media_id,
                sha256=SMALL_PNG_SHA256,
            )
        )
        remote_media = self.get_success(
            self.media_repo.store.get_cached_remote_media(server_name, media_id)
        )
        assert remote_media is not None
        self.assertEqual(remote_media.upload_name, upload_name)
        self.assertEqual(remote_media.sha256, SMALL_PNG_SHA256)
        return media_id

    def _assert_file_path_and_content(
        self, responder: Responder | None, path: str
    ) -> None:
        """Assert that `responder` serves `SMALL_PNG` from the file at `path`, and
        close it."""
        assert isinstance(responder, FileResponder)
        with responder:
            self.assertEqual(responder.open_file.name, path)
            self.assertEqual(responder.open_file.read(), SMALL_PNG)

    def _make_temp_file(self, name: str, mtime_s: float) -> str:
        path = os.path.join(self._media_store_path, name)
        write(path, b"")
        os.utime(path, (mtime_s, mtime_s))
        return path


class MediaRepositoryTestCase(_MediaRepoTestCase):
    def test_create_or_update_content_works_with_sha256_path(
        self,
    ) -> None:
        """New media is stored only under its SHA-256 path, not its media-ID path,
        and stays there when it is updated."""
        # Confirm that the sha256 path does not exist yet
        expected_path = self.media_repo.filepaths.local_media_filepath(SMALL_PNG_SHA256)
        self.assertFalse(os.path.exists(expected_path))

        # Create a new media with sha256 path
        media_id = self._create_or_update_content()
        self.assertTrue(os.path.exists(expected_path))
        # Make sure the media is only created with sha256 path and not using the media_id path
        old_path = self.media_repo.filepaths.local_media_filepath(media_id)
        self.assertFalse(os.path.exists(old_path))

        # Update the media with the sha256 path
        self.assertEqual(self._create_or_update_content(media_id=media_id), media_id)
        self.assertTrue(
            os.path.exists(
                self.media_repo.filepaths.local_media_filepath(SMALL_PNG_SHA256)
            )
        )
        self.assertFalse(
            os.path.exists(self.media_repo.filepaths.local_media_filepath(media_id))
        )

    @parameterized.expand(
        [
            (True,),  # Legacy media with its SHA-256 recorded.
            (False,),  # Legacy media without its SHA-256 recorded.
        ]
    )
    def test_create_or_update_content_works_with_media_id_path(
        self, with_sha256: bool
    ) -> None:
        """`create_or_update_content` can handle media with legacy media_id path,
        whether it has `sha256` record or not. Its media-ID file is kept and
        neither moved nor removed."""
        # Create media with media_id path
        mxc_uri = self._create_legacy_local_media(
            SMALL_PNG_SHA256 if with_sha256 else None
        )
        media_id = mxc_uri.media_id

        # Update the media with the media_id path
        self.assertEqual(self._create_or_update_content(media_id=media_id), media_id)
        self.assertTrue(
            os.path.exists(self.media_repo.filepaths.local_media_filepath(media_id))
        )

    def test_lock_blocks_new_media_from_being_stored(self) -> None:
        """New media waits for the SHA-256 lock before its file is moved onto the
        SHA-256 path and its row is inserted. Here the test holds the lock in
        place of a delete, and checks that the upload neither writes the file
        nor finishes until the lock is released."""
        # Hold the lock that a delete of this SHA-256 file would take.
        lock = self.media_repo._lock_sha256_files(None, SMALL_PNG_SHA256)
        self.get_success(lock.__aenter__())
        # Start the upload without waiting for it, since it blocks on the lock.
        d = defer.ensureDeferred(
            self.media_repo.create_or_update_content(
                "image/png",
                None,
                BytesIO(SMALL_PNG),
                len(SMALL_PNG),
                UserID.from_string(self.creator),
            )
        )
        # Let the reactor run, so the upload gets as far as it can.
        self.pump()
        # The upload is still waiting for the lock, so its file has not been
        # moved onto the SHA-256 path yet.
        self.assertNoResult(d)
        sha256_path = self.filepaths.local_media_filepath(SMALL_PNG_SHA256)
        self.assertFalse(os.path.exists(sha256_path))

        # Once the lock is released, the upload finishes.
        self.get_success(lock.__aexit__(None, None, None))
        mxc = self.get_success(d)
        self.assertTrue(os.path.exists(sha256_path))
        media_info = self.get_success(self.store.get_local_media(mxc.media_id))
        assert media_info is not None
        self.assertEqual(media_info.sha256, SMALL_PNG_SHA256)

    def test_get_remote_media_impl_with_sha256_path_cache_hit(self) -> None:
        """On a cache hit, `_get_remote_media_impl` serves remote media from its
        SHA-256 path."""
        server_name = "other_server.com"
        media_id = self._create_remote_media_sha256(server_name)
        responder, remote_media = self._get_remote_media_impl(server_name, media_id)

        self._assert_file_path_and_content(
            responder,
            self.filepaths.remote_media_filepath(server_name, SMALL_PNG_SHA256),
        )
        self.assertEqual(remote_media.media_id, media_id)
        self.assertEqual(remote_media.media_origin, server_name)
        self.assertEqual(remote_media.sha256, SMALL_PNG_SHA256)

    def test_get_remote_media_impl_with_sha256_path_cache_miss(self) -> None:
        """On a cache miss, `_get_remote_media_impl` downloads the media, stores it
        only under its SHA-256 path, records the SHA-256 and generates thumbnails
        under the SHA-256 path."""
        server_name = "other_server.com"
        media_id = random_string(24)
        responder, media_info = self._get_remote_media_impl(server_name, media_id)
        self._assert_file_path_and_content(
            responder,
            self.filepaths.remote_media_filepath(server_name, SMALL_PNG_SHA256),
        )
        self.assertEqual(media_info.media_id, media_id)
        self.assertEqual(media_info.media_origin, server_name)
        self.assertEqual(media_info.sha256, SMALL_PNG_SHA256)

        remote_media = self.get_success(
            self.store.get_cached_remote_media(server_name, media_id)
        )
        assert remote_media is not None
        self.assertEqual(remote_media.sha256, SMALL_PNG_SHA256)

        # The file is only saved under the SHA-256 path.
        self.assertEqual(
            read(self.filepaths.remote_media_filepath(server_name, SMALL_PNG_SHA256)),
            SMALL_PNG,
        )
        self.assertFalse(
            os.path.exists(
                self.filepaths.remote_media_filepath(
                    server_name, remote_media.filesystem_id
                )
            )
        )
        # Thumbnails are generated under the SHA-256 path.
        self.assertTrue(
            os.listdir(
                self.filepaths.remote_media_thumbnail_dir(server_name, SMALL_PNG_SHA256)
            )
        )
        thumbnail = self.get_success(
            self.store.get_remote_media_thumbnail(
                server_name, media_id, 1, 1, "image/png"
            )
        )
        assert thumbnail is not None

    @parameterized.expand(
        [
            (SMALL_PNG_SHA256,),  # Legacy remote media with SHA-256 record.
            (None,),  # Legacy remote media without SHA-256 record.
        ]
    )
    def test_get_remote_media_impl_with_media_id_path_cache_hit(
        self, sha256: str | None
    ) -> None:
        """On a cache hit, `_get_remote_media_impl` serves remote media stored
        before deduplication from its media-ID path, without downloading it again."""
        server_name = "other_server.com"
        media_id = self._create_legacy_remote_media(server_name, sha256).media_id
        with patch.object(
            self.media_repo.client, "download_media", side_effect=self._download
        ) as download:
            responder, remote_media = self._get_remote_media_impl(server_name, media_id)

        download.assert_not_called()
        self._assert_file_path_and_content(
            responder, self.filepaths.remote_media_filepath(server_name, media_id)
        )
        self.assertEqual(remote_media.media_id, media_id)
        self.assertEqual(remote_media.sha256, sha256)

    @parameterized.expand(
        [
            (SMALL_PNG_SHA256,),  # Legacy remote media with SHA-256 record.
            (None,),  # Legacy remote media without SHA-256 record.
        ]
    )
    def test_get_remote_media_impl_with_media_id_path_cache_miss(
        self, sha256: str | None
    ) -> None:
        """If the file of remote media is missing, `_get_remote_media_impl`
        downloads it again. The row already exists, so the download is kept
        under its SHA-256 path if the row has its `sha256` record. If not,
        the `sha256` is fed by this new download."""
        server_name = "other_server.com"
        media_id = self._create_legacy_remote_media(server_name, sha256).media_id
        media_id_path = self.filepaths.remote_media_filepath(server_name, media_id)
        # Prepare cache miss
        os.remove(media_id_path)
        with patch.object(
            self.media_repo.client, "download_media", side_effect=self._download
        ) as download:
            responder, remote_media = self._get_remote_media_impl(server_name, media_id)
        download.assert_called_once()
        self._assert_file_path_and_content(
            responder,
            self.filepaths.remote_media_filepath(server_name, SMALL_PNG_SHA256),
        )
        self.assertEqual(remote_media.media_id, media_id)
        self.assertEqual(remote_media.filesystem_id, media_id)
        self.assertEqual(remote_media.sha256, SMALL_PNG_SHA256)
        self.assertFalse(os.path.exists(media_id_path))

        row = self.get_success(
            self.store.get_cached_remote_media(server_name, media_id)
        )
        assert row is not None
        self.assertEqual(row.filesystem_id, media_id)
        self.assertEqual(row.sha256, SMALL_PNG_SHA256)

    def test_remote_downloads_share_file_only_within_origin(self) -> None:
        """Remote media with the same content from the same origin shares one file.
        Media from another origin gets its own file.
        """
        # Two media IDs from `a.org` and one from `b.org`, all with the same
        # content. Note that `b.org` reuses media ID "1".
        self._download_remote_file("a.org", "1")
        self._download_remote_file("a.org", "2")
        self._download_remote_file("b.org", "1")

        # Each origin has its own copy at its SHA-256 path.
        for server_name in ("a.org", "b.org"):
            self.assertTrue(
                os.path.exists(
                    self.filepaths.remote_media_filepath(server_name, SMALL_PNG_SHA256)
                )
            )
        # Both media from `a.org` share one file: only the SHA-256 file exists
        # under its content dir, and there are no media-ID files.
        files = [
            name
            for _, _, names in os.walk(
                os.path.join(self._media_store_path, "remote_content", "a.org")
            )
            for name in names
        ]
        self.assertEqual(len(files), 1)

    @parameterized.expand(
        [
            (  # The winner has the same content: its row references the file.
                SMALL_PNG_SHA256,
                True,
            ),
            (  # The winner has other content: nothing references the file.
                OTHER_SHA256,
                False,
            ),
        ]
    )
    def test_remote_download_race(self, winner_sha256: str, file_kept: bool) -> None:
        """If another worker recorded the media first, the download fails with an
        `IntegrityError`, and the downloaded SHA-256 file is only kept when a row
        references it. No temporary file is left behind."""
        self.get_success(
            self.store.store_cached_remote_media(
                origin="remote.org",
                media_id="abc",
                media_type="image/png",
                media_length=len(SMALL_PNG),
                time_now_ms=self.clock.time_msec(),
                upload_name=None,
                filesystem_id="winner",
                sha256=winner_sha256,
            )
        )
        failure = self.get_failure(
            self.media_repo._download_remote_file(
                "remote.org", "abc", 20_000, Mock(), "127.0.0.1"
            ),
            Exception,
        )
        self.assertIsInstance(
            failure.value, self.store.database_engine.module.IntegrityError
        )
        self.assertEqual(
            os.path.exists(
                self.filepaths.remote_media_filepath("remote.org", SMALL_PNG_SHA256)
            ),
            file_kept,
        )
        self.assertEqual(get_temp_files_from_directory(self._media_store_path), [])

    @parameterized.expand(
        [
            (SMALL_PNG_SHA256,),  # Legacy local media with SHA-256 record.
            (None,),  # Legacy local media without SHA-256 record.
        ]
    )
    def test_remove_local_media_from_disk_with_media_id_path(
        self, sha256: str | None
    ) -> None:
        """`_remove_local_media_from_disk` removes the media with media_id path
        whether or not its SHA-256 is recorded."""
        # Generate 2 media with media_id path
        media1_mxc = self._create_legacy_local_media(sha256)
        media1_id = media1_mxc.media_id
        media2_mxc = self._create_legacy_local_media(None)
        media2_id = media2_mxc.media_id

        removed_media_id, total = self.get_success(
            self.media_repo._remove_local_media_from_disk([media1_id, media2_id])
        )
        self.assertEqual(removed_media_id, [media1_id, media2_id])
        self.assertEqual(total, 2)
        self.assertFalse(
            os.path.exists(self.media_repo.filepaths.local_media_filepath(media1_id))
        )
        self.assertFalse(
            os.path.exists(self.media_repo.filepaths.local_media_filepath(media2_id))
        )

    def test_remove_local_media_from_disk_with_sha256_path(self) -> None:
        """`_remove_local_media_from_disk` removes the SHA-256 file once all the
        media sharing it are removed."""
        local_content_dir = os.path.abspath(
            os.path.join(self._media_store_path, "local_content")
        )

        def local_content_files() -> list[str]:
            return [
                os.path.join(root, name)
                for root, _, names in os.walk(local_content_dir)
                for name in names
            ]

        # Generate 2 media with sha256 path with the same image.
        media1_id = self._create_or_update_content()
        media2_id = self._create_or_update_content()
        # There should be 2 rows in the table but only 1 media stored in filesystem.
        self.assertEqual(
            local_content_files(),
            [self.media_repo.filepaths.local_media_filepath(SMALL_PNG_SHA256)],
        )
        # Now remove both media and check that the sha256 file is also removed.
        removed_media_id, total = self.get_success(
            self.media_repo._remove_local_media_from_disk([media1_id, media2_id])
        )
        self.assertEqual(removed_media_id, [media1_id, media2_id])
        self.assertEqual(total, 2)
        self.assertEqual(local_content_files(), [])

    def test_delete_local_media_keeps_sha256_file_referenced_by_legacy_media(
        self,
    ) -> None:
        """Media file is not deleted when other media with media_id path can
        reference the same SHA-256 file."""
        media_id = self._create_or_update_content()
        legacy_media_id = self._create_legacy_local_media(SMALL_PNG_SHA256).media_id
        sha256_path = self.filepaths.local_media_filepath(SMALL_PNG_SHA256)
        self.assertEqual(
            self.get_success(self.media_repo.delete_local_media_ids([media_id])),
            ([media_id], 1),
        )
        # Even though the media with `media_id` is deleted, the SHA-256 file is kept.
        self.assertTrue(os.path.exists(sha256_path))

        # The legacy media reads the SHA-256 file first.
        responder = self.get_success(
            self.media_repo.media_storage.fetch_media(
                FileInfo(None, legacy_media_id, sha256=SMALL_PNG_SHA256)
            )
        )
        self._assert_file_path_and_content(responder, sha256_path)

    def test_delete_local_media_keeps_shared_thumbnail_files(self) -> None:
        """The SHA-256 file and thumbnails are kept while other local media
        references them, and removed with the last one."""
        first = self._create_or_update_content()
        second = self._create_or_update_content()
        sha256_path = self.filepaths.local_media_filepath(SMALL_PNG_SHA256)
        thumbnail_dir = self.filepaths.local_media_thumbnail_dir(SMALL_PNG_SHA256)

        self.assertEqual(
            self.get_success(self.media_repo.delete_local_media_ids([first])),
            ([first], 1),
        )
        self.assertTrue(os.path.exists(sha256_path))
        self.assertTrue(os.path.exists(thumbnail_dir))

        self.assertEqual(
            self.get_success(self.media_repo.delete_local_media_ids([second])),
            ([second], 1),
        )
        self.assertFalse(os.path.exists(sha256_path))
        self.assertFalse(os.path.exists(thumbnail_dir))

    def test_delete_old_remote_media_keeps_shared_sha256_file(self) -> None:
        """`delete_old_remote_media` always removes the media_id path file, but only
        removes the SHA-256 file and thumbnails with the last media of the same
        origin that references them."""
        # old_media's file is at its media-ID path, but its SHA-256 is recorded
        # and its thumbnails are under the SHA-256 path.
        old_media_id = self._create_legacy_remote_media(
            "remote.org", SMALL_PNG_SHA256
        ).media_id
        legacy_path = self.filepaths.remote_media_filepath("remote.org", old_media_id)
        # An hour later, new_media with the same content is stored at the SHA-256
        # path.
        self.reactor.advance(3600)
        self._create_remote_media_sha256("remote.org")
        sha256_path = self.filepaths.remote_media_filepath(
            "remote.org", SMALL_PNG_SHA256
        )
        # `delete_old_remote_media` should delete only old_media.
        result = self.get_success(
            self.media_repo.delete_old_remote_media(self.clock.time_msec() - 1)
        )
        self.assertEqual(result, {"deleted": 1})
        # The media-ID file of old_media is removed, but the SHA-256 file is kept as
        # new_media still references it.
        self.assertFalse(os.path.exists(legacy_path))
        self.assertTrue(os.path.exists(sha256_path))

        # Delete all the remaining media: "new".
        result = self.get_success(
            self.media_repo.delete_old_remote_media(self.clock.time_msec() + 1)
        )
        self.assertEqual(result, {"deleted": 1})
        # No media references the SHA-256 any more, so the file and its
        # thumbnails are removed.
        self.assertFalse(os.path.exists(sha256_path))
        self.assertFalse(
            os.path.exists(
                self.filepaths.remote_media_thumbnail_dir(
                    "remote.org", SMALL_PNG_SHA256
                )
            )
        )

    def test_lock_blocks_delete_of_sha256_file(self) -> None:
        """Deleting media waits for the SHA-256 lock before removing the file.

        New media with the same content takes the same lock while it moves its
        file onto the SHA-256 path and inserts its row. Without the lock, a delete
        could check for references just before that row is inserted, and then
        remove the file the new media uses.

        Here the test holds the lock in place of an upload, and checks that the
        delete neither removes the file nor finishes until the lock is released.
        """
        # The only media stored at the SHA-256 path.
        media_id = self._create_or_update_content()
        sha256_path = self.filepaths.local_media_filepath(SMALL_PNG_SHA256)

        # Hold the lock that an upload of the same content would take.
        lock = self.media_repo._lock_sha256_files(None, SMALL_PNG_SHA256)
        self.get_success(lock.__aenter__())

        # Start the delete without waiting for it, since it blocks on the lock.
        d = defer.ensureDeferred(self.media_repo.delete_local_media_ids([media_id]))
        # Let the reactor run, so the delete gets as far as it can.
        self.pump()
        # The delete is still waiting for the lock, so the file is not removed
        # yet.
        self.assertNoResult(d)
        self.assertTrue(os.path.exists(sha256_path))

        # Once the lock is released, the delete finishes.
        self.get_success(lock.__aexit__(None, None, None))
        self.assertEqual(self.get_success(d), ([media_id], 1))

        # No media references the SHA-256 any more, so the file is removed.
        self.assertFalse(os.path.exists(sha256_path))

    @parameterized.expand(
        [
            # Local file in use by a local row. Should be kept.
            (None, None, True),
            # Local file with no row. Should be removed.
            (None, "", False),
            # Remote file in use by a row of the same origin. Should be kept.
            ("a.org", "a.org", True),
            # Remote file only used by a row of another origin. Should be removed.
            ("a.org", "b.org", False),
        ]
    )
    def test_remove_sha256_files_if_unused(
        self,
        origin: str | None,
        reference: str | None,
        kept: bool,
    ) -> None:
        """The SHA-256 file and thumbnails are only removed when no media references
        the SHA-256. For remote media, only media of the same origin counts."""
        if reference is None:
            self._store_local_media("media_id", SMALL_PNG_SHA256)
        elif reference:
            self._store_cached_remote_media(
                reference, "media_id", None, "file_id", SMALL_PNG_SHA256
            )
        media_path = self.media_repo.media_storage.media_filepath(
            origin, SMALL_PNG_SHA256
        )
        thumbnail_dir = self.media_repo.media_storage.thumbnail_dirpath(
            origin, SMALL_PNG_SHA256
        )
        write(media_path, SMALL_PNG)
        write(os.path.join(thumbnail_dir, "32-32-image-png-scale"), b"thumbnail")

        self.get_success(
            self.media_repo._remove_sha256_files_if_unused(origin, SMALL_PNG_SHA256)
        )

        self.assertEqual(os.path.exists(media_path), kept)
        self.assertEqual(os.path.exists(thumbnail_dir), kept)

    def test_remove_media_files_from_disk(self) -> None:
        """`_remove_media_files_from_disk` returns whether the media file is gone
        from disk."""
        # No file exists at the path: nothing to remove, so it counts as removed.
        self.assertTrue(
            self.get_success(
                self.media_repo._remove_media_files_from_disk(None, "missingmedia")
            )
        )
        # Put a directory at the media path to make the removal fail: `os.remove`
        # raises an error other than "file not found" for a directory, so it
        # returns False.
        os.makedirs(self.filepaths.local_media_filepath("directorymedia"))
        self.assertFalse(
            self.get_success(
                self.media_repo._remove_media_files_from_disk(None, "directorymedia")
            )
        )
        # The file and its thumbnail directory exist: both are removed.
        media_id = self._create_legacy_local_media(None).media_id
        media_path = self.filepaths.local_media_filepath(media_id)
        thumbnail_dir = self.filepaths.local_media_thumbnail_dir(media_id)
        self.assertTrue(
            self.get_success(
                self.media_repo._remove_media_files_from_disk(None, media_id)
            )
        )
        self.assertFalse(os.path.exists(media_path))
        self.assertFalse(os.path.exists(thumbnail_dir))

    def test_stale_temp_files_are_cleaned_up(self) -> None:
        """The hourly job removes media temporary files, with either the current or
        the old prefix, once they are older than 24 hours. Newer temporary files and
        other files are kept."""
        start_time = self.clock.time()
        stale = self._make_temp_file(_MEDIA_TEMP_FILE_PREFIX + "stale", start_time)
        legacy = self._make_temp_file("tmpstale", start_time)
        fresh = self._make_temp_file(
            _MEDIA_TEMP_FILE_PREFIX + "fresh", start_time + 30 * 3600
        )
        other = self._make_temp_file("not-a-temp-file", start_time)

        # Not removed while they are at most 24 hours old.
        self.reactor.advance(24 * 3600)
        self.assertTrue(os.path.exists(stale))

        self.reactor.advance(2 * 3600)
        self.assertFalse(os.path.exists(stale))
        self.assertFalse(os.path.exists(legacy))
        self.assertTrue(os.path.exists(fresh))
        self.assertTrue(os.path.exists(other))


class ThumbnailTestCase(_MediaRepoTestCase):
    """Tests for generating, storing and fetching thumbnails."""

    def _get_thumbnail(self, media_id: str) -> bytes:
        channel = self.make_request(
            "GET",
            f"/_matrix/client/v1/media/thumbnail/test/{media_id}"
            "?width=32&height=32&method=scale",
            shorthand=False,
            access_token=self.creator_tok,
        )
        self.assertEqual(channel.code, 200, channel.result)
        return channel.result["body"]

    def test_local_thumbnails_with_sha256_stored_under_sha256_path(self) -> None:
        """Thumbnails of new local media, with its SHA-256 recorded, are stored
        under its SHA-256 path, not its media-ID path."""
        media_id = self._create_or_update_content()

        self.assertTrue(
            self.get_success(self.store.get_local_media_thumbnails(media_id))
        )
        self.assertTrue(
            os.listdir(self.filepaths.local_media_thumbnail_dir(SMALL_PNG_SHA256))
        )
        self.assertFalse(
            os.path.exists(self.filepaths.local_media_thumbnail_dir(media_id))
        )

    def test_local_thumbnails_without_sha256_stored_under_media_id_path(
        self,
    ) -> None:
        """Thumbnails of local media with media_id path, without its
        SHA-256 recorded, are stored under its media-ID path."""
        media_id = self._create_legacy_local_media(None).media_id

        self.assertTrue(
            self.get_success(self.store.get_local_media_thumbnails(media_id))
        )
        self.assertTrue(os.listdir(self.filepaths.local_media_thumbnail_dir(media_id)))
        self.assertFalse(
            os.path.exists(self.filepaths.local_media_thumbnail_dir(SMALL_PNG_SHA256))
        )

    def test_remote_thumbnails_with_sha256_stored_under_sha256_path(self) -> None:
        """Thumbnails of new remote media, with its SHA-256 recorded, are stored
        under the SHA-256 path of its origin."""
        server_name = "remote.org"
        media_id = self._create_remote_media_sha256(server_name)
        self._generate_thumbnails(server_name, media_id, media_id, SMALL_PNG_SHA256)

        self.assertTrue(
            self.get_success(
                self.store.get_remote_media_thumbnails(server_name, media_id)
            )
        )
        self.assertTrue(
            os.listdir(
                self.filepaths.remote_media_thumbnail_dir(server_name, SMALL_PNG_SHA256)
            )
        )
        self.assertFalse(
            os.path.exists(
                self.filepaths.remote_media_thumbnail_dir(server_name, media_id)
            )
        )

    def test_remote_thumbnails_without_sha256_stored_under_media_id_path(
        self,
    ) -> None:
        """Thumbnails of remote media stored in media_id path, without its
        SHA-256 recorded, are stored under its media-ID path."""
        server_name = "remote.org"
        media_id = self._create_legacy_remote_media(server_name, None).media_id

        self.assertTrue(
            self.get_success(
                self.store.get_remote_media_thumbnails(server_name, media_id)
            )
        )
        self.assertTrue(
            os.listdir(self.filepaths.remote_media_thumbnail_dir(server_name, media_id))
        )
        self.assertFalse(
            os.path.exists(
                self.filepaths.remote_media_thumbnail_dir(server_name, SMALL_PNG_SHA256)
            )
        )

    def test_same_sha256_copies_thumbnail_rows(self) -> None:
        """Media with the same content reuses the thumbnail rows of earlier media,
        without decoding the image again."""
        first = self._create_or_update_content()

        with patch(
            "synapse.media.media_repository.Thumbnailer",
            side_effect=AssertionError("Thumbnails must be copied"),
        ):
            second = self._create_or_update_content()

        first_rows = self.get_success(self.store.get_local_media_thumbnails(first))
        second_rows = self.get_success(self.store.get_local_media_thumbnails(second))
        self.assertTrue(first_rows)
        self.assertCountEqual(first_rows, second_rows)

    def test_media_id_original_with_sha256_gets_sha256_thumbnails(self) -> None:
        """Thumbnails of media stored in media_id path, with its SHA-256
        recorded, are generated from its media-ID file and stored under the SHA-256
        path. The original is not moved."""
        media_id = self._create_legacy_local_media(SMALL_PNG_SHA256).media_id

        self.assertTrue(
            os.listdir(self.filepaths.local_media_thumbnail_dir(SMALL_PNG_SHA256))
        )
        self.assertFalse(
            os.path.exists(self.filepaths.local_media_thumbnail_dir(media_id))
        )
        self.assertTrue(os.path.exists(self.filepaths.local_media_filepath(media_id)))
        self.assertFalse(
            os.path.exists(self.filepaths.local_media_filepath(SMALL_PNG_SHA256))
        )

    @parameterized.expand(
        [
            # Original media with a SHA-256: moved from a temporary file.
            (SMALL_PNG_SHA256,),
            # Original media without a SHA-256: written in place.
            (None,),
        ]
    )
    def test_store_thumbnail_returns_kept_path(self, sha256: str | None) -> None:
        """`_store_thumbnail` returns the path where the thumbnail is kept, not a
        temporary file."""
        media_id = random_string(24)
        self._store_local_media(media_id, sha256)

        path, _ = self.get_success(
            self.media_repo._store_thumbnail(
                None,
                media_id,
                media_id,
                False,
                32,
                32,
                "scale",
                "image/png",
                BytesIO(b"thumbnail"),
                sha256,
            )
        )

        self.assertEqual(
            os.path.dirname(path),
            self.filepaths.local_media_thumbnail_dir(sha256 or media_id),
        )
        self.assertEqual(read(path), b"thumbnail")
        self.assertEqual(get_temp_files_from_directory(self._media_store_path), [])

    def test_remote_thumbnail_rows_copied_only_within_origin(self) -> None:
        """Remote media with the same content from the same origin reuses the
        thumbnail rows of earlier media. Each origin keeps its own thumbnails."""
        for origin, media_id in (("a.org", "1"), ("a.org", "2"), ("b.org", "3")):
            file_id = self._download_remote_file(origin, media_id)
            self.get_success(
                self.media_repo._generate_thumbnails(
                    origin,
                    media_id,
                    file_id,
                    "image/png",
                    origin_sha256=SMALL_PNG_SHA256,
                )
            )

        # Each origin keeps its own SHA-256 file and thumbnails.
        for origin in ("a.org", "b.org"):
            self.assertTrue(
                os.listdir(
                    self.filepaths.remote_media_thumbnail_dir(origin, SMALL_PNG_SHA256)
                )
            )
        self.assertCountEqual(
            self.get_success(self.store.get_remote_media_thumbnails("a.org", "1")),
            self.get_success(self.store.get_remote_media_thumbnails("a.org", "2")),
        )

    def test_sha256_thumbnail_is_preferred(self) -> None:
        """If a thumbnail exists under both paths, the SHA-256 one is served.
        `_file_info_to_path` decides the path to use"""
        media_id = random_string(24)
        self._store_local_media(media_id, SMALL_PNG_SHA256)
        self.get_success(
            self.store.store_local_thumbnail(
                media_id, 32, 32, "image/png", "scale", len(b"thumbnail")
            )
        )
        for identifier, content in (
            (SMALL_PNG_SHA256, b"thumbnail"),
            (media_id, b"legacy"),
        ):
            write(
                self.filepaths.local_media_thumbnail(
                    identifier, 32, 32, "image/png", "scale"
                ),
                content,
            )
        self.assertEqual(self._get_thumbnail(media_id), b"thumbnail")
