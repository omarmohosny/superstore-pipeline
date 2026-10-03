import os
import sys

import duckdb

con = duckdb.connect(os.environ["SUPERSTORE_DB"], read_only=True)
con.sql(sys.argv[1]).show()
