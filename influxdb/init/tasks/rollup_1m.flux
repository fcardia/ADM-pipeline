option task = {name: "rollup_1m", every: 5m, offset: 1m}

from(bucket: "farm_raw")
|> range(start: -task.every)
|> drop(columns: ["status"])
|> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
|> to(bucket: "farm_rollup", org: "farm")