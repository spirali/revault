from typing import Any, Callable, Iterable, Tuple

import sqlalchemy as sa
from sqlalchemy.sql.expression import func
from datetime import datetime

from .key import Key
from .entry import AnnounceResult, EntryId, Entry


# def _set_sqlite_pragma(dbapi_connection, _connection_record):
#     cursor = dbapi_connection.cursor()
#     cursor.execute("PRAGMA foreign_keys=ON")
#     cursor.close()


class Database:
    def __init__(self, url):
        engine = sa.create_engine(url)
        # if "sqlite" in engine.dialect.name:
        #     sa.event.listen(engine, "connect", _set_sqlite_pragma)
        self.url = url
        metadata = sa.MetaData()
        self.entries = sa.Table(
            "entries",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("name", sa.String(80)),
            sa.Column("version", sa.Integer),
            sa.Column("config_key", sa.String(56)),  # 56 = hexdigest of sha224
            sa.Column("replica", sa.Integer),
            sa.Column("config", sa.PickleType),
            sa.Column("result", sa.PickleType),
            sa.Column(
                "start_date",
                sa.DateTime(timezone=True),
                server_default=sa.sql.func.now(),
            ),
            sa.Column(
                "finish_date",
                sa.DateTime(timezone=True),
                nullable=True,
            ),
            sa.Column("run_info", sa.JSON),
            sa.UniqueConstraint("name", "version", "config_key", "replica"),
            # sa.Column("computation_time", sa.Integer(), nullable=True),
            # sa.Index("key_idx", "key"),
        )

        # self.deps = sa.Table(
        #     "deps",
        #     metadata,
        #     sa.Column(
        #         "source_id",
        #         sa.Integer(),
        #         sa.ForeignKey("entries.id", ondelete="cascade"),
        #     ),
        #     sa.Column(
        #         "target_id",
        #         sa.Integer(),
        #         sa.ForeignKey("entries.id", ondelete="cascade"),
        #     ),
        #     sa.UniqueConstraint("source_id", "target_id"),
        # )

        # self.blobs = sa.Table(
        #     "blobs",
        #     metadata,
        #     sa.Column("entry_id", sa.ForeignKey("entries.id", ondelete="cascade"), primary_key=True),
        #     sa.Column("name", sa.String, nullable=False, primary_key=True),
        #     sa.Column("data", sa.LargeBinary, nullable=False),
        #     sa.Column("mime", sa.String(255), nullable=False),
        #     sa.Column("repr", sa.String(85), nullable=True),
        # )

        self.metadata = metadata
        self.engine = engine

    def load_entry(self, key: Key) -> Entry | None:
        c = self.entries.c
        with self.engine.connect() as conn:
            select = (
                sa.select(c.id, c.result)
                .where(c.name == key.name)
                .where(c.version == key.version)
                .where(c.config_key == key.config_key)
                .where(c.replica == key.replica)
            )
            r = conn.execute(select).one_or_none()
            if r is not None:
                return Entry(entry_id=r[0], key=key, result=r[1])
            else:
                return None

    def cancel_running(self):
        c = self.entries.c
        with self.engine.connect() as conn:
            stmt = sa.delete(self.entries).where(c.finish_date == None)
            conn.execute(stmt)
            conn.commit()

    def _load_keys(self, add_filter: Callable) -> list[Key]:
        c = self.entries.c
        with self.engine.connect() as conn:
            select = add_filter(
                sa.select(c.name, c.version, c.config, c.config_key, c.replica)
            )
            return [
                Key(
                    name,
                    version,
                    config,
                    replica,
                    config_key=config_key,
                )
                for name, version, config, config_key, replica in conn.execute(
                    select
                ).all()
            ]

    def load_all_keys(self) -> list[Key]:
        return self._load_keys(lambda s: s)

    def query_keys(self, name: str, version: int) -> list[Key]:
        c = self.entries.c
        return self._load_keys(
            lambda s: s.where(c.name == name).where(c.version == version)
        )

    # def load_all_refs(self):
    #     c = self.entries.c
    #     with self.engine.connect() as conn:
    #         select = sa.select(
    #             c.id, c.name, c.version, c.config, c.config_key, c.replica
    #         )
    #         return [
    #             Key(
    #                 name,
    #                 version,
    #                 config,
    #                 replica,
    #                 config_key=config_key,
    #             )
    #             for entry_id, name, version, config, config_key, replica in conn.execute(
    #                 select
    #             ).all()
    #         ]

    # def load_result(self, key: Key) -> Tuple[bool, Any]:
    #     c = self.entries.c
    #     with self.engine.connect() as conn:
    #         select = (
    #             sa.select(c.result)
    #             .where(c.name == key.name)
    #             .where(c.version == key.version)
    #             .where(c.config_key == key.config_key)
    #             .where(c.replica == key.replica)
    #         )
    #         r = conn.execute(select).one_or_none()
    #         if r is not None:
    #             return True, r[0]
    #         else:
    #             return False, None

    def get_or_announce_entry(self, key: Key) -> Tuple[AnnounceResult, EntryId, Any]:
        c = self.entries.c
        with self.engine.connect() as conn:
            try:
                stmt = (
                    sa.insert(self.entries)
                    .values(
                        name=key.name,
                        version=key.version,
                        config=key.config,
                        config_key=key.config_key,
                        replica=key.replica,
                    )
                    .returning(self.entries.c.id)
                )
                r = conn.execute(stmt).one_or_none()
                conn.commit()
                return AnnounceResult.COMPUTE_HERE, r[0], None
            except sa.exc.IntegrityError:
                select = (
                    sa.select(c.id, c.result, c.finish_date)
                    .where(c.name == key.name)
                    .where(c.version == key.version)
                    .where(c.config_key == key.config_key)
                    .where(c.replica == key.replica)
                )
                r = conn.execute(select).one()
                if r[2] is None:
                    return AnnounceResult.COMPUTING_ELSEWHERE, r[0], None
                else:
                    return AnnounceResult.FINISHED, r[0], r[1]

    def finish_entry(self, entry_id: EntryId, result: Any, run_info: dict):
        with self.engine.connect() as conn:
            stmt = (
                sa.update(self.entries)
                .where(self.entries.c.id == entry_id)
                .values(result=result, run_info=run_info, finish_date=datetime.now())
            )
            conn.execute(stmt)
            conn.commit()

    def cancel_entry(self, entry_id):
        with self.engine.connect() as conn:
            stmt = sa.delete(self.entries).where(self.entries.c.id == entry_id)
            conn.execute(stmt)
            conn.commit()

    def remove(self, key: Key):
        c = self.entries.c
        with self.engine.connect() as conn:
            stmt = (
                sa.delete(self.entries)
                .where(c.name == key.name)
                .where(c.version == key.version)
                .where(c.config_key == key.config_key)
                .where(c.replica == key.replica)
            )
            conn.execute(stmt)
            conn.commit()

    def insert_new_replica(
        self, key: Key, result: Any
    ) -> Tuple[AnnounceResult, EntryId, Any]:
        c = self.entries.c
        with self.engine.connect() as conn:
            select = (
                sa.select(func.max(c.replica))
                .where(c.name == key.name)
                .where(c.version == key.version)
                .where(c.config_key == key.config_key)
            )
            r = conn.execute(select).one()
            if r[0] is None:
                replica = 0
            else:
                replica = r[0] + 1
            stmt = sa.insert(self.entries).values(
                name=key.name,
                version=key.version,
                config=key.config,
                config_key=key.config_key,
                replica=replica,
                result=result,
                finish_date=datetime.now(),
            )
            conn.execute(stmt)
            conn.commit()
            return replica

    def init(self):
        self.metadata.create_all(self.engine)
