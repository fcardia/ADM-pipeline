// Rollup al minuto di temperatura/umidità per zona: farm_raw -> farm_rollup (retention ∞).
// Campi: temperature_c / humidity_pct (media), *_min, *_max.
// Per cadenza oraria: cambia ogni "1m" in "1h".
option task = {name: "rollup_1m", every: 1m}

src = () =>
  from(bucket: "farm_raw")
    |> range(start: -1m)
    |> filter(fn: (r) => r._measurement == "air_quality")
    |> filter(fn: (r) => r._field == "temperature_c" or r._field == "humidity_pct")

src() |> aggregateWindow(every: 1m, fn: mean, createEmpty: false) |> to(bucket: "farm_rollup")
src() |> aggregateWindow(every: 1m, fn: min, createEmpty: false) |> map(fn: (r) => ({r with _field: r._field + "_min"})) |> to(bucket: "farm_rollup")
src() |> aggregateWindow(every: 1m, fn: max, createEmpty: false) |> map(fn: (r) => ({r with _field: r._field + "_max"})) |> to(bucket: "farm_rollup")
