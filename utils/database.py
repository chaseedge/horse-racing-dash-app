import os
import re
import json 
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import pandas as pd
from dotenv import load_dotenv

from pathlib import Path

# load .env file with DB Params
load_dotenv()


# remove commas from number strings "12,000" -> "12000"
COMMA_PAT = re.compile(r",(?=\d{3})")

class Database:
    INT_DTYPES = {"integer", "smallint", "bigint", "int"}
    FLOAT_DTYPES = {"numeric", "real", "float", "decimal", "double precision"}
    
    def __init__(self):
        self.params = dict(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PWD"),
            host=os.getenv("DB_URL"),
            autocommit=True,
        )
        self._execute_check_pattern = re.compile("(?:^update)|(?:(?:(?:create)|(?:drop)|(?:alter)"\
                                                 "|(?:truncate))\s+table)|(?:insert\s+into)|(?:delete from)", flags=re.I)
    
    def query(self, query, params=None, as_df=True):
        data = self.execute(query, params, as_dict=True)
        if as_df:
            return pd.DataFrame(data)
        return data
        
    
    def _get_col_dtypes(self, table):
        params = {}
        q = """SELECT column_name, data_type 
               FROM information_schema.columns 
               WHERE table_name = {table} """
        if "." in table:
            q += " AND table_schema = {schema};"
            schema, table = table.split(".")
            params['table'] = table
            params['schema'] = schema
        else:
            params['table'] = table
            
        with psycopg.connect(**self.params) as conn:
            with conn.cursor() as cur:
                q = sql.SQL(q).format(**params)
                cur.execute(q)
                return dict(cur.fetchall())
    
    def get_cols(self, table):
        params = {}

        q = """SELECT column_name
                FROM information_schema.columns 
                WHERE table_name = {table} """
        if "." in table:
            q += " AND table_schema = {schema};"
            schema, table = table.split(".")
            params['table'] = table
            params['schema'] = schema
        else:
            params['table'] = table
            
        with psycopg.connect(**self.params) as conn:
            q = sql.SQL(q).format(**params)
            with conn.cursor() as cur:
                cur.execute(q)
                data = cur.fetchall()
                data = [x[0] for x in data]
                return data
                    
    def get_primary_keys(self, table):
        params = {}
        q = """
        select kc.column_name
        from information_schema.table_constraints tc
        join information_schema.key_column_usage kc 
            on kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name
        where tc.constraint_type = 'PRIMARY KEY'
        and kc.ordinal_position is not null
        AND tc.table_name = {table}"""
        if "." in table:
            params['schema'], params['table'] = table.split(".")
            q += " AND tc.table_schema = {schema}"
        else:
            params['table'] = table
        
        with psycopg.connect(**self.params) as conn:
            with conn.cursor() as cur:
                q = sql.SQL(q).format(**params)
                cur.execute(q)
                return [x[0] for x in cur.fetchall()]
    
    @classmethod
    def _prep_col_val(cls, val, dtype, nan_val=None):
        """clean up the values for database entry"""
        dtype = dtype.lower()

        if pd.isna(val) or val in [cls.NULL, "", None]:
            val = cls._fill_null(val, dtype=dtype, nan_val=nan_val)
        elif dtype in cls.FLOAT_DTYPES:
            if isinstance(val, str):
                val = COMMA_PAT.sub("", val)
        elif dtype in cls.INT_DTYPES:
            if isinstance(val, str):
                val = COMMA_PAT.sub("", val)
            try:
                v = round(val, 0)
            except (ValueError, TypeError) as e:
                v = round(float(val), 0)
            assert v - round(float(val), 4) == 0, f"CASTING a float as INT for val {val}"
            val = int(v)
        elif dtype in ["character varying", "text", "character"]:
            val = str(val)
            val = (
                val.replace("\n", "  ")
                .replace("\t", "  ")
                .replace("\r", "  ")
                .replace(";", "  ")
                .replace("\.", ".")
                .replace("\\", "  ")
                .strip()
            )
        elif dtype == "date":
            # 2020-01-01 00:00:00 -> 2020-01-01
            if not isinstance(val, str):
                val = str(val)
            val = val[:10]
        return val
            
    def _prep_df(self, df, table):
        """prep df cols for insert"""
        dtypes = self._get_col_dtypes(table)
        
        df = df.astype(object)
        df = df.where((pd.notnull(df)), None)
        
        for c in df:
            if c not in dtypes:
                continue
            dt = dtypes[c]
            if dt == 'integer':
                ix = df[c].notnull()
                df.loc[ix, c] = df[ix][c].astype(int)
            elif dt in ('json','jsonb'):
                ix = df[c].notnull()
                df.loc[ix, c] = df[ix][c].apply(lambda x: json.dumps(x, default=str))
        return df
    
    @staticmethod
    def _chunk(l, n):
        for i in range(0, len(l), n): 
            yield [tuple(x) for x in l[i:i + n]]
            
    def execute(self, query, params=None, as_dict=False, flatten=False):
        """Pass params to query as (list or tuples) for %s and
        as a dict for %(param)s for named params in query"""
        
        db_params = self.params.copy()
        if as_dict:
            db_params['row_factory'] = dict_row
        fetch = self._execute_check_pattern.search(query) is None

        with psycopg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL(query), params)
                if fetch:
                    d = cur.fetchall()
                    if flatten and not as_dict:
                        out = []
                        for x in d:
                            out.extend(x)
                        return out
                    return d
            conn.commit()
            return "Success"
    
    def insert_df(self, df, table):
        """only inserts data, fails on load"""
        cols = self._get_df_db_cols(df, table)
        df = self._prep_df(df[cols], table)
        
        with psycopg.connect(**self.params) as conn:
            with conn.cursor() as cur:
                q = sql.SQL("COPY {table} ({cols}) FROM STDIN").format(**{'table': sql.Identifier(table),
                                                                 "cols": sql.SQL(', ').join(map(sql.Identifier, cols))})
                with cur.copy(q) as copy:
                    [copy.write_row(tuple(v)) for v in df[cols].values]
                    
    def _get_df_db_cols(self, df, table):
        """gets only the columns from the df that are table columns"""
        cols = []
        skip_cols = []
        tbl_cols = self.get_cols(table)
        for c in df:
            if c in tbl_cols:
                cols.append(c)
            else:
                skip_cols.append(c)
        
        if skip_cols:
            print(f"_get_df_db_cols: Skipping cols not found in {table}: {','.join(skip_cols)}")
        return cols
    
    def _batch_execute(self, query, data, batch_size=10000, table=None):
        msg = ""
        with psycopg.connect(**self.params) as conn:
            with conn.cursor() as cur:
                for tuples in self._chunk(data, batch_size):
                    try:
                        if table:
                            cur.execute("BEGIN")
                            cur.execute(sql.SQL("LOCK TABLE {table}").format(table=sql.Identifier(table)))
                        cur.executemany(query, tuples)
                        conn.commit()
                        msg = "Success"
                    except (Exception, psycopg.DatabaseError) as error:
                        conn.rollback()
                        cur.close()
                        raise error
                conn.commit()
        return msg
        
    def upsert_df(self, df, table, pkeys=None, batch_size=10000):
        if pkeys is None:
            pkeys = self.get_primary_keys(table)
        if isinstance(pkeys, str):
            pkeys = [pkeys]
        cols = self._get_df_db_cols(df, table)
        df = self._prep_df(df=df[cols], table=table)
        ex_cols = [x for x in cols if x not in pkeys]

        params = {'table': sql.Identifier(*table.split(".")),
                  "pkeys": sql.SQL(", ").join(map(sql.Identifier, pkeys)),
                  "all_cols": sql.SQL(', ').join(map(sql.Identifier, cols)),
                  "vals": sql.SQL(', ').join(sql.Placeholder() * len(cols)),
                  "conflict": sql.SQL(", ").join([
                      sql.SQL(" = ").join([
                      sql.Identifier(c), 
                      sql.Identifier("excluded",c)
                  ]) for c in ex_cols])
                 }
        
        query = sql.SQL("""INSERT INTO {table} ({all_cols}) VALUES ({vals}) 
        ON CONFLICT ({pkeys}) DO UPDATE SET {conflict}""").format(**params)

        return self._batch_execute(query=query, data=df[cols].values, batch_size=batch_size)
    
    def upsert(self, data, table, pkeys=None, batch_size=10000):
        if pkeys is None:
            pkeys = self.get_primary_keys(table)
        if isinstance(pkeys, str):
            pkeys = [pkeys]
        
        cols = self.get_cols(table)
        values = [[d.get(c, None) for c in cols] for d in data]
        ex_cols = [x for x in cols if x not in pkeys]

        params = {'table': sql.Identifier(*table.split(".")),
                  "pkeys": sql.SQL(", ").join(map(sql.Identifier, pkeys)),
                  "all_cols": sql.SQL(', ').join(map(sql.Identifier, cols)),
                  "vals": sql.SQL(', ').join(sql.Placeholder() * len(cols)),
                  "conflict": sql.SQL(", ").join([
                      sql.SQL(" = ").join([
                      sql.Identifier(c), 
                      sql.Identifier("excluded",c)
                  ]) for c in ex_cols])
                 }
        
        query = sql.SQL("""INSERT INTO {table} ({all_cols}) VALUES ({vals}) 
        ON CONFLICT ({pkeys}) DO UPDATE SET {conflict}""").format(**params)

        return self._batch_execute(query=query, data=values, batch_size=batch_size)
    
    def upsert_df_except_null(self, df, table, pkeys=None, batch_size=10000):
        """updates database with DF. When db value for column exists, it is overwritten with DF
        if DF value is not null, other wise the DB value stands"""
        
        if pkeys is None:
            pkeys = self.get_primary_keys(table)
        if isinstance(pkeys, str):
            pkeys = [pkeys]
        cols = self._get_df_db_cols(df, table)
        df = self._prep_df(df=df[cols], table=table)
        ex_cols = [x for x in cols if x not in pkeys]

        params = {'table': sql.Identifier(*table.split(".")),
                  "pkeys": sql.SQL(", ").join(map(sql.Identifier, pkeys)),
                  "all_cols": sql.SQL(', ').join(map(sql.Identifier, cols)),
                  "vals": sql.SQL(', ').join(sql.Placeholder() * len(cols)),
                  "conflict": sql.SQL(", ").join([
                      sql.SQL(" = ").join([
                      sql.Identifier(c), 
                          sql.SQL("COALESCE({})").format(
                          sql.SQL(", ").join([
                              sql.Identifier("excluded",c),
                              sql.Identifier(table,c),
                          ]))
                  ]) for c in ex_cols
                  ])}

        query = sql.SQL("""
        INSERT INTO {table} ({all_cols}) VALUES ({vals}) 
        ON CONFLICT ({pkeys}) DO UPDATE SET {conflict}""").format(**params)
        
        return self._batch_execute(query=query, data=df[cols].values, batch_size=batch_size, table=table)
    
    def upsert_df_only_null(self, df, table, pkeys=None, batch_size=10000):
        """updates database values that are null with values in DF. If db value for column exists, then do nothing, 
        else fill with DF value"""
        
        if pkeys is None:
            pkeys = self.get_primary_keys(table)
        if isinstance(pkeys, str):
            pkeys = [pkeys]
        cols = self._get_df_db_cols(df, table)
        df = self._prep_df(df=df[cols], table=table)
        ex_cols = [x for x in cols if x not in pkeys]

        params = {'table': sql.Identifier(*table.split(".")),
                  "pkeys": sql.SQL(", ").join(map(sql.Identifier, pkeys)),
                  "all_cols": sql.SQL(', ').join(map(sql.Identifier, cols)),
                  "vals": sql.SQL(', ').join(sql.Placeholder() * len(cols)),
                  "conflict": sql.SQL(", ").join([
                      sql.SQL(" = ").join([
                      sql.Identifier(c), 
                          sql.SQL("COALESCE({})").format(
                          sql.SQL(", ").join([
                              sql.Identifier(table,c),
                              sql.Identifier("excluded",c),
                          ]))
                  ]) for c in ex_cols
                  ])}

        query = sql.SQL("""INSERT INTO {table} ({all_cols}) VALUES ({vals}) 
        ON CONFLICT ({pkeys}) DO UPDATE SET {conflict}""").format(**params)
        
        return self._batch_execute(query=query, data=df[cols].values, batch_size=batch_size)
        