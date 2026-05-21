
## Revenue & Transactions

### Cross-mart slicer dimension: dim_currency

Page 2 has three visuals from three different marts (mart_top_merchants,
mart_tx_by_channel, mart_tx_by_category), all needing to be filtered by
a single Currency slicer.

The wide-marts design (no inter-mart relationships) meant a slicer
pointing at any single mart's `currency` column would only filter that
mart, leaving the others showing aggregated-across-all-currencies data.
This was discovered during page 2 maquetación when the merchants table
showed values 9× too large (summing all 9 currencies per merchant).

**Solution**: created `dim_currency` as a synthetic dimension table with
9 rows (the LATAM currencies in the dataset: USD, ARS, BRL, CLP, COP,
EUR, MXN, PEN, UYU). One-to-many relationships from dim_currency[currency]
to each mart's currency column. The page slicer points at dim_currency,
and filters propagate to all three marts simultaneously.

This is the standard star-schema slicer pattern applied minimally — we
don't have a full star schema in Gold (the wide marts are pre-aggregated,
which is the right choice for analytics performance and clarity), but
dim_currency provides the minimum dimensional plumbing needed for
cross-mart slicers.

Single-select enforced on the slicer with USD as default, since summing
monetary values across currencies without FX conversion is mathematically
invalid.

### DQ finding: auto-summarization in PowerBI tables

While building the top-merchants table, default PowerBI auto-summarization
(SUM) applied to numeric fields in tables produced inflated values when
the underlying mart has multi-grain (merchant × currency). Each merchant
appeared 9 times (once per currency), and PowerBI summed across them.

Symptom: Movistar shown as $7.28B total value (actual USD: $9.47M).

**Fix**: explicit "Don't summarize" on each numeric column in the table.
Standard practice for tables sourced from pre-aggregated marts —
auto-summarization should only be used with raw fact tables.

Documented as a generic PowerBI anti-pattern for this type of architecture.