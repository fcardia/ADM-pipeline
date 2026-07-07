// Task di rollup: media / minimo / massimo AL MINUTO di temperatura e umidità
// per zona. Legge farm_raw, aggrega e materializza in farm_rollup (retention ∞).
//
// Campi scritti (measurement air_quality, tag zone):
//   temperature_c      humidity_pct       (medie — nomi invariati, letti dal pannello)
//   temperature_c_min  humidity_pct_min   (minimi)
//   temperature_c_max  humidity_pct_max   (massimi)
//
// Per passare a cadenza ORARIA (più aderente alla traccia): cambia ogni "1m" in
// "1h" qui sotto e aggiorna il titolo del pannello.
option task = {name: "rollup_1m", every: 1m}

src = () =>
  from(bucket: "farm_raw")
    |> range(start: -1m)
    |> filter(fn: (r) => r._measurement == "air_quality")
    |> filter(fn: (r) => r._field == "temperature_c" or r._field == "humidity_pct")

src() |> aggregateWindow(every: 1m, fn: mean, createEmpty: false) |> to(bucket: "farm_rollup")
src() |> aggregateWindow(every: 1m, fn: min, createEmpty: false) |> map(fn: (r) => ({r with _field: r._field + "_min"})) |> to(bucket: "farm_rollup")
src() |> aggregateWindow(every: 1m, fn: max, createEmpty: false) |> map(fn: (r) => ({r with _field: r._field + "_max"})) |> to(bucket: "farm_rollup")
