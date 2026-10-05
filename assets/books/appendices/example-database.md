# Appendix: Example Database {#example-database .appendix .dqlh}

Every example in this reference runs against one database: Chinook, a
sample database for a small digital music store, extended with a few
tables of our own. A second, smaller database, `etl.sqlite`, sits beside
it for the examples about mounting and loading.

## Chinook {.dqlh}

Chinook is the Chinook Database, version 1.4.5, by Luis Rocha
(<https://github.com/lerocha/chinook-database>), used under the MIT
license reproduced at the end of this appendix. We changed one thing:
every table and column name is converted from PascalCase to lowercase
snake_case (`InvoiceLine.UnitPrice` becomes `invoice_line.unit_price`).
The data is unchanged.

```text
artist(artist_id, name)                                             275 rows
album(album_id, title, artist_id)                                   347
track(track_id, name, album_id, media_type_id, genre_id, composer,
      milliseconds, bytes, unit_price)                             3503
genre(genre_id, name)                                                25
media_type(media_type_id, name)                                       5
playlist(playlist_id, name)                                          18
playlist_track(playlist_id, track_id)                              8715
invoice(invoice_id, customer_id, invoice_date, billing_address,
        billing_city, billing_state, billing_country,
        billing_postal_code, total)                                 412
invoice_line(invoice_line_id, invoice_id, track_id, unit_price,
             quantity)                                             2240
customer(customer_id, first_name, last_name, company, address, city,
         state, country, postal_code, phone, fax, email,
         support_rep_id)                                             59
employee(employee_id, last_name, first_name, title, reports_to,
         birth_date, hire_date, address, city, state, country,
         postal_code, phone, fax, email)                              8
```

An album belongs to an artist; a track to an album, a genre, and a media
type; `playlist_track` joins playlists to tracks. A customer places
invoices, each with invoice lines naming tracks. Employees form a tree
through `reports_to`, and each customer has a support representative,
`support_rep_id`, among them.

## Our additions {.dqlh}

These tables are ours, added so that examples of set operators, JSON,
and recursion have real data with the shape they teach.

- `genre_2024` and `genre_2025` are yearly extracts of `genre`. The 2025
  extract stores its columns in the other order, drops Easy Listening,
  renames Rock And Roll, adds K-Pop, and loaded Jazz twice.
- `employee_2024` and `employee_2025` are yearly extracts of `employee`.
  In 2024 Jordan Hall had not yet left and Mitchell was still IT Staff;
  the 2025 extract renamed `title` to `job_title` and
  `reports_to` to `manager_id`.
- `similar_artist(artist_id, similar_id, score)` records which artists'
  fans also like which others: a directed graph with cycles and two
  separate clusters.
- `partner_sale(sale_id, received_at, payload)` holds sales from a partner
  storefront, each payload the JSON document the partner sent.

## The etl database {.dqlh}

`etl.sqlite` holds the partner's sales again, flattened to one row per
purchased track and landed in one table per load date:
`partner_sale_2025_06_30` and `partner_sale_2025_07_31`. The examples
that use it mount it first:

```delightql
mount!("etl.sqlite", "etl")(*)
etl.partner_sale_2025_06_30(*) |> (sale_id, buyer_email, track_id)
```

## Chinook license {.dqlh}

```text
Chinook Database
--------------------------------------
Copyright (c) 2008-2024 Luis Rocha

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```
