# Map Embedding {.dqlh}

The EMBED-MAP operator `+$(  )(  )`{.delightql .sigil} applies a function across columns and creates
new columns from the results (rather than replacing the originals):

```{.delightql .numberLines .am }

f_to_c:(f) :- (f - 32.0)*0.5556
f_to_k:(f) :- f_to_c:(f)+273.15

?- boston_temps(*)
     |> +$(f_to_c:() as :"{@}_c")( /_temp/ )
     |> +$(f_to_k:() as :"{@}_k")( /_temp/ )
```

```sql
SELECT
  month,
  daily_max_temp,
  daily_min_temp,
  daily_avg_temp,
  (daily_max_temp - 32.0) * 0.5556 AS daily_max_temp_c,
  (daily_min_temp - 32.0) * 0.5556 AS daily_min_temp_c,
  (daily_avg_temp - 32.0) * 0.5556 AS daily_avg_temp_c,
  (daily_max_temp - 32.0) * 0.5556
  + 273.15 AS daily_max_temp_k,
  (daily_min_temp - 32.0) * 0.5556
  + 273.15 AS daily_min_temp_k,
  (daily_avg_temp - 32.0) * 0.5556
  + 273.15 AS daily_avg_temp_k
FROM boston_temps;
```

The first parentheses contain the function and an as qualifier with an
F-STRING. The F-PARAM sigil `@`{.delightql .sigil} stands in for the column name, generating
`daily_max_temp_c`, `daily_min_temp_c`, etc. The second parentheses specify the
target columns--here, all columns matching `/_temp/`{.delightql }.

Unlike **MAP-COVER**, which replaces columns in place, **EMBED-MAP** preserves the
originals and appends the transformed columns.
