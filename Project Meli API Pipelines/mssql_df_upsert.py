# Copyright 2023 Gordon D. Thompson, gord@gordthompson.com
#
# https://gist.github.com/gordthompson/be1799bd68a12be58c880bb9c92158bc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# version 1.3 - 2023-04-12

import pandas as pd
import sqlalchemy as sa


def df_upsert(data_frame, table_name, engine, schema=None, match_columns=None):
    """
    Perform an "upsert" on a SQL Server table from a DataFrame.
    Constructs a T-SQL MERGE statement, uploads the DataFrame to a
    temporary table, and then executes the MERGE.
    Parameters
    ----------
    data_frame : pandas.DataFrame
        The DataFrame to be upserted.
    table_name : str
        The name of the target table.
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy Engine to use.
    schema : str, optional
        The name of the schema containing the target table.
    match_columns : list of str, optional
        A list of the column name(s) on which to match. If omitted, the
        primary key columns of the target table will be used.
    """
    table_spec = ""
    if schema:
        table_spec += "[" + schema.replace("]", "]]") + "]."
    table_spec += "[" + table_name.replace("]", "]]") + "]"

    df_columns = list(data_frame.columns)
    if not match_columns:
        insp = sa.inspect(engine)
        match_columns = insp.get_pk_constraint(table_name, schema=schema)[
            "constrained_columns"
        ]
    columns_to_update = [col for col in df_columns if col not in match_columns]
    stmt = f"MERGE {table_spec} WITH (HOLDLOCK) AS main\n"
    stmt += f"USING (SELECT {', '.join([f'[{col}]' for col in df_columns])} FROM #temp_table) AS temp\n"
    join_condition = " AND ".join(
        [f"main.[{col}] = temp.[{col}]" for col in match_columns]
    )
    stmt += f"ON ({join_condition})\n"
    stmt += "WHEN MATCHED THEN\n"
    update_list = ", ".join(
        [f"[{col}] = temp.[{col}]" for col in columns_to_update]
    )
    stmt += f"  UPDATE SET {update_list}\n"
    stmt += "WHEN NOT MATCHED THEN\n"
    insert_cols_str = ", ".join([f"[{col}]" for col in df_columns])
    insert_vals_str = ", ".join([f"temp.[{col}]" for col in df_columns])
    stmt += f"  INSERT ({insert_cols_str}) VALUES ({insert_vals_str});"

    with engine.begin() as conn:
        data_frame.to_sql("#temp_table", conn, index=False)
        conn.exec_driver_sql(stmt)
        conn.exec_driver_sql("DROP TABLE IF EXISTS #temp_table")


if __name__ == "__main__":
    # Usage example adapted from
    # https://stackoverflow.com/a/62388768/2144390

    engine = sa.create_engine(
        "mssql+pyodbc://scott:tiger^5HHH@192.168.0.199/test"
        "?driver=ODBC+Driver+17+for+SQL+Server",
        fast_executemany=True,
    )

    # create example environment
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP TABLE IF EXISTS main_table")
        conn.exec_driver_sql(
            "CREATE TABLE main_table (id int primary key, txt nvarchar(50), status nvarchar(50))"
        )
        conn.exec_driver_sql(
            "INSERT INTO main_table (id, txt, status) VALUES (1, N'row 1 old text', N'original')"
        )
        # [(1, 'row 1 old text', 'original')]

    # DataFrame to upsert
    df = pd.DataFrame(
        [(2, "new row 2 text", "upserted"), (1, "row 1 new text", "upserted")],
        columns=["id", "txt", "status"],
    )

    df_upsert(df, "main_table", engine)
    """The MERGE statement generated for this example:
    MERGE [main_table] WITH (HOLDLOCK) AS main
    USING (SELECT [id], [txt], [status] FROM #temp_table) AS temp
    ON (main.[id] = temp.[id])
    WHEN MATCHED THEN
      UPDATE SET [txt] = temp.[txt], [status] = temp.[status]
    WHEN NOT MATCHED THEN
      INSERT ([id], [txt], [status]) VALUES (temp.[id], temp.[txt], temp.[status]);
    """

    # check results
    with engine.begin() as conn:
        print(
            conn.exec_driver_sql("SELECT * FROM main_table").all()
        )
        # [(1, 'row 1 new text', 'upserted'), (2, 'new row 2 text', 'upserted')]