import sqlite3
import os

_nimbus_migration_table = "__nimbus__mig_"

# TODO all print statements should be converted over to logging


class Migration:
    """ A migration represents a database connection and a directory of migration files """

    def __init__(self, mig_dir: str, sql_conn: sqlite3.Connection, up=True):
        if not os.path.exists(mig_dir):
            raise ValueError(f"migration directory {mig_dir} does not exist")
        self._mig_dir = mig_dir
        self._db = sql_conn
        self._up = up
        cursor = sql_conn.cursor()
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {_nimbus_migration_table}"
            f" (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR, date DATETIME)")
        # [(ID, name, date)]
        migrations = cursor.execute(f"SELECT * FROM {_nimbus_migration_table}").fetchall()
        self._ran_migrations = set(each[1] for each in migrations)

    def __call__(self):
        self.run_migration_directory()

    def run_migration(self, sql: str, name: str) -> bool:
        cursor = self._db.cursor()
        try:
            if self._up:
                print(f"Running Migration {name}")
            else:
                print(f"Reverting Migration {name}")
            cursor.executescript(sql)
            self._db.commit()
            if self._up:
                cursor.execute(f"INSERT INTO {_nimbus_migration_table} (`name`, `date`) VALUES (?, datetime())", [name])
            else:
                cursor.execute(f"DELETE FROM {_nimbus_migration_table} WHERE `name` = ?", [name])
            self._db.commit()
        except sqlite3.Error:
            self._db.rollback()
            raise

    def run_migration_directory(self):
        try:
            for _, _, files in os.walk(self._mig_dir):
                for file in files:
                    if self._up and file.endswith(".up.sql"):
                        name = file[:-7]
                        if name not in self._ran_migrations:
                            with open(os.path.join(self._mig_dir, file)) as f:
                                sql = f.read()
                                self.run_migration(sql, name)
                    elif not self._up and file.endswith('.dn.sql'):
                        name = file[:-7]
                        if name in self._ran_migrations:
                            with open(os.path.join(self._mig_dir, file)) as f:
                                sql = f.read()
                                self.run_migration(sql, name, False)
        except sqlite3.Error as e:
            print(f"Error executing SQL for migration '{name}': {e}")
        except Exception as e:
            print(f"Unexpected error occurred opening migration directory or file: {e}")
        else:
            print("All migrations ran successfully")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Runs migrations to upgrade database state"
    )
    parser.add_argument(
        "action",
        choices=("up", "down"),
        help="whether to nimbus-migrate or revert a migration"
    )
    parser.add_argument(
        "db",
        help="the sqlite database file to nimbus-migrate",
    )
    parser.add_argument(
        "--dir",
        help="The directory containing the SQL migration files",
        default=os.path.dirname(__file__) + "/db/sqlite/migrations"
    )
    args = parser.parse_args()
    sql_conn = sqlite3.connect(args.db)
    migration = Migration(args.dir, sql_conn, up=args.action == "up")
    migration()
