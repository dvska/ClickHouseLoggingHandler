# _ClickHouseLoggingHandler_ ➜ Sends logs to centralized ClickHouse from (Python 3) microservices
⚠️ just sample concept for specific project


## Usage
```sql
 CREATE TABLE IF NOT EXISTS Log(
    ts       DateTime DEFAULT now(),
    msec     UInt32,
    node     String,
    uid      UInt32 DEFAULT toUInt32(0),
    jobid    UInt32,
    plug     String DEFAULT '',
    level    Enum8('💥💥💥CRITICAL'=50, '⛑ERROR'=40, '🚸WARNING'=30, '💚INFO'=20, '🖤DEBUG'=10, 'NOTSET'=0),
    type     Enum8('monitor'=1, 'widget'=2, ''=0),
    pid      UInt32,
    procname String,
    file     String,
    logger   String,
    body     String
    )
    ENGINE = MergeTree()
    PARTITION BY (toYYYYMM(ts), node)
    ORDER BY (ts, node, uid);
CREATE TABLE Log_buffer AS Log ENGINE = Buffer(default, Log, 16, 10, 120, 10000, 100000, 1000000, 1000000)
    
```


```python
import logging, sys

import requests
import clickhouse_logging_handler

CLICKHOUSE_URL = 'http://your_clickhouse_server:8123'

try:
    requests.get(CLICKHOUSE_URL)
except Exception as e:
    print(f"Cannot connect to ClickHouse server at {CLICKHOUSE_URL} - {str(e)}", sys.stderr)
    
clickh_handler = clickhouse_logging_handler.ClickHouseLoggingHandler(CLICKHOUSE_URL)
logging.getLogger().addHandler(clickh_handler)

def main():
    log = logging.getLogger()

    log.debug('some debug output')
    log.critical('time to die', extra=dict(force_flush=True))  # forcibly send to ClHouse
```

