# JSON-awareness and cataloging {.dqlh}

```delightql
partner_sale(*)
  |> (sale_id, payload:{.buyer} as buyer)
  |> %(sale_id ~> max:(buyer) as buyer)
  |> (sale_id, {"buyer": buyer} as packet)
```

Today, without the max step, the buyer re-enters the packet as an object
(sale 1):

```json
{"buyer":{"email":"luisg@embraer.com.br","country":"BR"}}
```

With the max step it re-enters as a string:

```json
{"buyer":"{\"email\":\"luisg@embraer.com.br\",\"country\":\"BR\"}"}
```


This is a known limitation.
