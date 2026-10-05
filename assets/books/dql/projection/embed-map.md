# Map Embedding {.dqlh}

The EMBED-MAP operator `+$(  )(  )`{.delightql .sigil} applies a function across columns and creates
new columns from the results (rather than replacing the originals):

```{.delightql .numberLines .am }
tidy:(s) :- trim:(coalesce:(s, ""))
label:(s) :- upper:(tidy:(s))
```

```{.delightql .numberLines}
invoice(*)
  |> +$(tidy:() as :"tidy_{@}")( /^billing_/ )
  |> +$(label:() as :"label_{@}")( /^billing_/ )
```

```sql
SELECT
  invoice_id,
  customer_id,
  invoice_date,
  billing_address,
  billing_city,
  billing_state,
  billing_country,
  billing_postal_code,
  total,
  trim(coalesce(billing_address, '')) AS tidy_billing_address,
  trim(coalesce(billing_city, '')) AS tidy_billing_city,
  trim(coalesce(billing_state, '')) AS tidy_billing_state,
  trim(coalesce(billing_country, '')) AS tidy_billing_country,
  trim(coalesce(billing_postal_code, '')) AS tidy_billing_postal_code,
  upper(trim(coalesce(billing_address, ''))) AS label_billing_address,
  upper(trim(coalesce(billing_city, ''))) AS label_billing_city,
  upper(trim(coalesce(billing_state, ''))) AS label_billing_state,
  upper(trim(coalesce(billing_country, ''))) AS label_billing_country,
  upper(trim(coalesce(billing_postal_code, ''))) AS label_billing_postal_code
FROM invoice;
```

The first parentheses contain the function and an as qualifier with an
F-STRING. The F-PARAM sigil `@`{.delightql .sigil} stands in for the column name, generating
`tidy_billing_address`, `tidy_billing_city`, etc. The second parentheses specify the
target columns--here, all columns matching `/^billing_/`{.delightql }.

Unlike **MAP-COVER**, which replaces columns in place, **EMBED-MAP** preserves the
originals and appends the transformed columns.
