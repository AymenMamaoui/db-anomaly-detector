import oracledb
from dotenv import load_dotenv
import os
import json

load_dotenv()


class SchemaParser:

    def __init__(self):
        self.conn = oracledb.connect(
            user=os.getenv("ORACLE_USER"),
            password=os.getenv("ORACLE_PASSWORD"),
            dsn=os.getenv("ORACLE_DSN")
        )
        self.cursor = self.conn.cursor()

    def get_tables(self):
        """Retourne la liste des tables du user connecté."""
        self.cursor.execute("""
                            SELECT table_name
                            FROM user_tables
                            ORDER BY table_name
                            """)
        return [row[0] for row in self.cursor.fetchall()]

    def get_columns(self, table_name):
        """Retourne les colonnes d'une table avec leurs types et nullable."""
        self.cursor.execute("""
                            SELECT column_name, data_type, nullable, data_precision, data_scale
                            FROM user_tab_columns
                            WHERE table_name = :1
                            ORDER BY column_id
                            """, [table_name])

        columns = []
        for row in self.cursor.fetchall():
            columns.append({
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "Y",
                "precision": row[3],
                "scale": row[4]
            })
        return columns

    def get_constraints(self, table_name):
        """Retourne les contraintes d'une table."""
        self.cursor.execute("""
                            SELECT c.constraint_name, c.constraint_type, cc.column_name
                            FROM user_constraints c
                                     JOIN user_cons_columns cc
                                          ON c.constraint_name = cc.constraint_name
                            WHERE c.table_name = :1
                            ORDER BY c.constraint_type
                            """, [table_name])

        constraints = []
        for row in self.cursor.fetchall():
            constraints.append({
                "name": row[0],
                "type": row[1],  # P=Primary, R=Foreign, C=Check, U=Unique
                "column": row[2]
            })
        return constraints

    def parse(self):
        """Parse le schéma complet de la DB et retourne un dict structuré."""
        schema = {}
        tables = self.get_tables()

        for table in tables:
            schema[table] = {
                "columns": self.get_columns(table),
                "constraints": self.get_constraints(table)
            }

        return schema

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    parser = SchemaParser()
    schema = parser.parse()
    parser.close()

    print(json.dumps(schema, indent=2))