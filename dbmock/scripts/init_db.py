"""初始化 Doris 业务数据库"""

import logging
import os
import re
from pathlib import Path

import dotenv
import pymysql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).parents[1]
ENV_FILE = ROOT_DIR / ".env"
SQL_DIR = ROOT_DIR / "scripts" / "sql"


class DorisInitializer:
    """Doris 数据库初始化器"""

    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        self._conn_conf = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "autocommit": True,
            "charset": "utf8mb4",
        }

    @staticmethod
    def _identifier(database: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
            raise ValueError(f"Doris 数据库名无效: {database}")
        return f"`{database}`"

    def delete_db(self, db_name: str) -> None:
        identifier = self._identifier(db_name)
        try:
            with (
                pymysql.connect(**self._conn_conf) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute(f"DROP DATABASE IF EXISTS {identifier}")
            logger.info("数据库 %s 删除成功", db_name)
        except pymysql.MySQLError:
            logger.exception("数据库 %s 删除失败", db_name)
            raise

    def create_db(self, db_name: str) -> None:
        identifier = self._identifier(db_name)
        try:
            with (
                pymysql.connect(**self._conn_conf) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute(f"CREATE DATABASE {identifier}")
            logger.info("数据库 %s 创建成功", db_name)
        except pymysql.MySQLError:
            logger.exception("数据库 %s 创建失败", db_name)
            raise

    def exec_sql_file(self, db_name: str, sql_file_path: Path) -> None:
        statements = [
            statement.strip()
            for statement in sql_file_path.read_text(encoding="utf-8").split(";")
            if statement.strip()
        ]
        if not statements:
            raise ValueError(f"Doris 建表脚本为空: {sql_file_path}")
        try:
            with (
                pymysql.connect(
                    **self._conn_conf,
                    database=db_name,
                ) as connection,
                connection.cursor() as cursor,
            ):
                for statement in statements:
                    cursor.execute(statement)
            logger.info("%s 执行成功", sql_file_path.name)
        except pymysql.MySQLError:
            logger.exception("%s 执行失败", sql_file_path.name)
            raise


if __name__ == "__main__":
    dotenv.load_dotenv(ENV_FILE)
    db_name = os.environ["DB_NAME"]
    db_initializer = DorisInitializer(
        host=os.environ["DB_HOST"],
        port=int(os.environ["DB_PORT"]),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    db_initializer.delete_db(db_name)
    db_initializer.create_db(db_name)
    db_initializer.exec_sql_file(db_name, SQL_DIR / "ecommerce.sql")
