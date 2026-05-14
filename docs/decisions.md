# Decisiones de modelado y negocio

> Documento vivo. Última actualización: tras EDA exhaustivo del dataset crudo
> (Día 1, bloque tarde).
>
> Este documento captura todas las decisiones de calidad de datos, modelado y
> métricas que guían la implementación de las capas Silver y Gold del pipeline.

---

## 1. Calidad de datos — hallazgos clave del EDA

### 1.1 Volumen
- **5.100 records** de customer (consigna decía ~5.000 ✓).
- 17.870 accounts | 89.470 transactions | 7.821 loans.
- Schema top-level uniforme: 24 keys idénticas en todos los records.

### 1.2 Inconsistencias categóricas masivas
Las categorías vienen sucias en 3 formas combinables:
- **Casing inconsistente** (`Retail` / `RETAIL` / `retail`).
- **Mezcla inglés/español** (`pyme` ↔ `sme`, `cerrado` ↔ `closed`, `reembolso` ↔ `refund`).
- **Whitespace** delante de valores (` credit_card`, `  investment`).

### 1.3 Nulls codificados como string
Los campos contienen `"null"`, `"N/A"`, `"NA"`, `"None"`, `""` que no son
detectados por parsers automáticos. Una función de normalización los convierte
a NULL real antes de cualquier análisis.

### 1.4 Fechas en 4 formatos coexistentes
Ningún null real — solo problemas de parsing. **El separador desambigua
totalmente el orden día/mes**, lo que hace al dataset 100% recuperable:

| Formato      | %    | Discriminador           |
|--------------|------|-------------------------|
| YYYY-MM-DD   | 82%  | 4 dígitos al inicio     |
| MM-DD-YYYY   | 6%   | Separador `-` (US)      |
| DD/MM/YYYY   | 6%   | Separador `/` (latino)  |
| YYYYMMDD     | 6%   | 8 dígitos sin separador |

Aplica a: `registration_date`, `date_of_birth`, `accounts.opened_date`,
`transactions.date`, `loans.start_date`, `loans.end_date`,
`digital_engagement.last_login_date`.

### 1.5 Numéricos como string
~3% de los registros en campos numéricos vienen como strings con 4 sub-formatos:
- Limpio: `"424926.82"` → cast directo.
- Latino: `"191286,14"` → reemplazar `,` por `.`.
- Símbolo: `"$1810568162.59"` → quitar `$`.
- Sufijo de moneda: `"404393.03 USD"` → quitar sufijo con regex.

Afecta: `accounts.balance`, `transactions.amount`, `loans.principal`,
`loans.outstanding_balance`, `loans.monthly_payment`.

### 1.6 Outliers numéricos extremos
Campos con valores aparentemente generados con escala equivocada (max ~$2 mil
millones): `balance`, `principal`, `outstanding_balance`, `monthly_payment`,
`total_limit`, `total_used`.
- **No descartar** — pueden ser legítimos en `private_banking`.
- Marcar con flag `is_outlier_<col>` por percentil 99.
- En Gold, los KPIs de tendencia central usan **mediana**, no media.

### 1.7 credit_score corrupto
**524 records (10,3%) tienen credit_score fuera del rango válido [300-850]**,
con valores absurdos como `-99` y `999999`. Probable artefacto de generación.
- En Silver: valores fuera de rango → NULL + flag `credit_score_invalid`.
- En Gold: las queries de credit_score filtran por valor válido.
- **Documentado en README como limitación que afecta business question #6.**

### 1.8 Booleans con valores mixtos
`bankruptcy_flag`, `push_notifications`, `paperless_statements` mezclan:
- bool reales (`True` / `False`),
- strings inglés (`"true"` / `"false"`),
- letras `Y` / `N`,
- español (`"si"`).

`mobile_app_registered` y `web_banking_registered` sí son bool puros.

---

## 2. Estrategia de canonicalización categórica

### 2.1 customer_segment → 4 valores

| Canónico          | Variantes a mapear                                         |
|-------------------|------------------------------------------------------------|
| `retail`          | retail, Retail, RETAIL, minorista                          |
| `premium`         | premium, Premium, PREMIUM                                  |
| `sme`             | sme, Sme, SME, pyme, PYME                                  |
| `private_banking` | private_banking, Private_Banking, PRIVATE_BANKING, banca_privada |

### 2.2 customer.status → 4 valores

| Canónico    | Variantes                                                    |
|-------------|--------------------------------------------------------------|
| `active`    | active, Active, ACTIVE, activo, Activo, ACTIVO               |
| `inactive`  | inactive, Inactive, INACTIVE, inactivo, Inactivo             |
| `suspended` | suspended, Suspended, SUSPENDED, suspendido                  |
| `closed`    | closed, Closed, CLOSED, cerrado, Cerrado                     |

### 2.3 accounts.status → 3 valores
`active` | `frozen` | `closed`
(incluye español: `congelado` → frozen, `cerrado` → closed).

### 2.4 transactions.type → 6 valores
`deposit` | `withdrawal` | `transfer` | `payment` | `refund` | `fee`
Variantes en español: `deposito`, `retiro`, `transferencia`, `pago`,
`reembolso`, `comision`.

### 2.5 transactions.status → 4 valores
`pending` | `completed` | `failed` | `reversed` (solo casings, sin español).

### 2.6 loans.type, loans.status, kyc_status, gender
Solo casings → `lower()` + `trim()` resuelve todo. Para `gender`, mapear a
`F` / `M` / `Other` / NULL.

### 2.7 Whitespace en account_type y transactions.category
Aplicar `trim()` antes de cualquier comparación.

### 2.8 Booleans

```sql
CASE
  WHEN LOWER(TRIM(col)) IN ('true','t','y','yes','si','1') THEN TRUE
  WHEN LOWER(TRIM(col)) IN ('false','f','n','no','0')      THEN FALSE
  ELSE NULL
END
```

---

## 3. Estrategia de fechas (PySpark)

Aplicar parsing en cascada con `coalesce`, del formato más restrictivo al más laxo:

```python
F.coalesce(
    F.to_date(col, "yyyy-MM-dd"),
    F.to_date(col, "yyyyMMdd"),
    F.to_date(col, "MM-dd-yyyy"),    # solo matchea con guión
    F.to_date(col, "dd/MM/yyyy"),    # solo matchea con barra
)
```

Si todos los intentos fallan → NULL + flag `<col>_parse_failed = TRUE` para
auditoría.

**No confiar en parsing automático de pandas/Spark con formatos mixtos.**

---

## 4. Estrategia de numéricos (PySpark)

```python
def parse_money(col):
    # 1. Quitar sufijos de moneda " USD", " EUR", etc.
    cleaned = F.regexp_replace(
        col.cast("string"),
        r"\s+(USD|EUR|ARS|BRL|CLP|COP|MXN|PEN|UYU)$",
        ""
    )
    # 2. Quitar símbolos de moneda
    cleaned = F.regexp_replace(cleaned, r"[\$€£]", "")
    # 3. Manejar formato latino con miles ("1.234,56") vs decimal ("191286,14")
    cleaned = F.when(
        cleaned.rlike(r"^\d{1,3}(\.\d{3})+,\d+$"),
        F.regexp_replace(F.regexp_replace(cleaned, r"\.", ""), r",", ".")
    ).otherwise(
        F.regexp_replace(cleaned, r",", ".")
    )
    return cleaned.cast("double")
```

Strings no parseables → NULL + columna de auditoría
`<col>_parse_failed boolean`.
**dbt test:** asegurar que `% de fallos < 1%`.

---

## 5. Deduplicación

### 5.1 Hallazgo (validado por EDA)
Los duplicados en arrays nested (accounts, transactions, loans) son **100%
derivados** de duplicados en customers. No existe duplicación independiente
en los arrays. Cada PK duplicada se origina porque su `customer_id` padre
aparece más de una vez (98 customers × 2 + 1 customer × 3 = 100 duplicados
totales en customers).

### 5.2 Estrategia: una sola operación cascadea limpieza

1. **Canonicalizar PRIMERO** todos los campos categóricos en customers
   (`lower()`, español→inglés, `trim()`). Necesario porque los duplicados
   difieren exactamente en casing/idioma/whitespace.

2. **Dedup en customers** con criterio:
   ```sql
   ROW_NUMBER() OVER (
     PARTITION BY customer_id
     ORDER BY
       (cantidad de campos no-null) DESC,
       registration_date DESC NULLS LAST
   ) = 1
   ```
   El primer criterio prioriza el record más completo. El segundo desempata
   por recencia. Maneja N apariciones sin lógica especial.

3. **Cascada a arrays:** explotar arrays haciendo INNER JOIN contra
   `stg_customers` deduplicado. Los IDs duplicados desaparecen
   automáticamente porque sus filas "perdedoras" del customer ya no existen.

### 5.3 Implementación en PySpark
- **Etapa 1:** canonicalización + ROW_NUMBER en bronze → `silver.stg_customers`.
- **Etapa 2:** explotar arrays con JOIN contra `stg_customers` →
  `silver.stg_accounts`, `silver.stg_transactions`, `silver.stg_loans`.
  Quedan sin duplicados de PK por construcción.

### 5.4 Verificación en dbt
Tests `unique` en `customer_id`, `account_id`, `transaction_id`, `loan_id`
deben pasar después de la dedup en cascada.

---

## 6. Tratamiento de nulls reales

| Campo                                      | % nulls | Decisión                                                        |
|--------------------------------------------|---------|-----------------------------------------------------------------|
| `relationship_manager`                     | 9,9%    | Imputar `'UNASSIGNED'` en dim_customer.                         |
| `address`                                  | 7,8%    | Conservar NULL. No bloquea análisis.                            |
| `gender`                                   | 5,4%    | Imputar `'Unknown'`.                                            |
| `accounts.credit_limit`                    | 74,6%   | **Legítimo:** solo `credit_card` lo tiene.                      |
| `transactions.description`                 | 10,2%   | Aceptable. Conservar NULL.                                      |
| `transactions.merchant`                    | 8,3%    | Aceptable. Conservar NULL.                                      |
| `transactions.category`                    | 4,9%    | Conservar NULL.                                                 |
| `loans.collateral_type`                    | 47,7%   | **Legítimo:** unsecured loans → mapear a `'unsecured'` en Gold. |
| `digital_engagement.avg_monthly_logins`    | 7,4%    | Validar hipótesis (¿coincide con no-registrados?) → imputar 0 si sí. |

---

## 7. Definiciones de métricas de negocio

### Revenue
> Revenue = `fees` + `interest_income` proporcional.
> - **Fees:** `SUM(amount) WHERE type='fee'` por customer/segment/mes.
> - **Interest income:** `(loans.outstanding_balance * loans.interest_rate / 12)`
>   por loan activo.

### Delinquency
> Un loan está delinquent si `days_past_due > 30` OR
> `status IN ('delinquent', 'default')`.
> Delinquency rate = loans delinquent / loans totales (excluyendo `paid_off`).

### Tenure
> Tenure (años) = `(today - registration_date) / 365.25`.

### Buckets de segmentación

**Age:**
> 18–25 | 26–35 | 36–45 | 46–55 | 56–65 | 65+

**Risk score (0-100):**
> low: 0–25 | medium: 26–50 | high: 51–75 | critical: 76–100

**Credit utilization (0-100):**
> low: <30 | medium: 30–70 | high: 70–90 | critical: >90

**Credit score (300-850, validados):**
> poor: 300-579 | fair: 580-669 | good: 670-739 | very_good: 740-799 | excellent: 800-850

**Days past due:**
> current: 0 | 1–30 | 31–60 | 61–90 | 90+

---

## 8. Casos especiales documentados

### 8.1 EUR en transactions
940 transacciones (1%) en EUR a pesar de ser un dataset LATAM. **Conservar** —
sirven para responder business question #19 (international transfer patterns).

### 8.2 `phone` en preferred_channel
`digital_engagement.preferred_channel` tiene valores
`mobile / atm / branch / phone / web`, mientras que `transactions.channel`
tiene `mobile / atm / branch / pos / web`. Universos distintos:
**no crear `dim_channel` única**.

### 8.3 collateral_type = "None" (string)
~48% de loans no tienen colateral. El string `"None"` se mapea a NULL real en
Silver y se traduce como `'unsecured'` en Gold para queries de portfolio.

### 8.4 Customer triplicado
1 customer (`CUST-0004728`) aparece 3 veces, mientras el resto de los
duplicados son pares. Aritmética: 98 × 2 + 1 × 3 = 199 filas → 100 duplicados
detectados por `.duplicated().sum()`. La estrategia de ROW_NUMBER por
completitud + recencia maneja N apariciones sin lógica especial.

---

## 9. Versiones e infraestructura

### Imagen base de Airflow: 2.10.5
La consigna pide "Apache Airflow 2.7+". Elegimos 2.10.5 (última 2.x estable al
momento de arrancar el proyecto) por seguridad — la imagen 2.7.3 acumula CVEs
críticos por antigüedad. Reduce drásticamente la superficie de vulnerabilidades
manteniendo compatibilidad total con la consigna.

### Usuario en runtime
El Dockerfile usa `USER root` solo durante la instalación de paquetes APT y la
descarga del driver JDBC (operaciones que requieren privilegios). El contenedor
en runtime corre como `airflow` (UID 50000), siguiendo la práctica recomendada
por la documentación oficial de Airflow para extender la imagen.

### PySpark en local mode
Spark corre en `local[*]` dentro del contenedor de Airflow (no cluster externo).
Es suficiente para los ~5k records del dataset. **Trade-off documentado:** esta
configuración no escalaría a millones de filas; en ese caso requeriría un
cluster Spark externo.


## Día 3 — PySpark setup + flatten de accounts

### 1. Decisión arquitectónica: cómo se invoca PySpark desde Airflow

**Opción elegida**: `spark-submit` ejecutado vía `BashOperator`.
**Alternativa descartada**: PySpark embebido dentro de un `PythonOperator`.

**Razones**:
- La consigna pide explícitamente "PySpark scripts live in a dedicated folder
  (e.g., spark/) and are triggered from Airflow", lo que sugiere scripts
  standalone invocables, no funciones embebidas.
- Cada script de Spark queda autocontenido y testeable localmente
  (`spark-submit spark/flatten_accounts.py` desde dentro del container,
  sin tocar Airflow). Esto aceleró fuertemente el debugging del día.
- Separación limpia entre orquestación (Airflow) y ejecución (Spark).
- El overhead de levantar una JVM por task (~10-15s) es irrelevante en
  un pipeline batch que corre 1x/día.

### 2. Configuración de la SparkSession

Centralizada en `spark/utils.py` con `get_spark_session()`:
- `master = local[*]` (parametrizable por env var `SPARK_MASTER`).
- `spark.jars`, `spark.driver.extraClassPath`, `spark.executor.extraClassPath`
  apuntando al JAR de JDBC (`/opt/spark/jars/postgresql-42.7.3.jar`).
- `spark.sql.session.timeZone = UTC` para evitar conversiones implícitas
  según la zona horaria del host. La conversión a hora local se delega
  a la capa BI.
- `spark.sql.shuffle.partitions = 4` (default 200), porque corremos en
  `local[*]` con un dataset chico y 200 particiones generan miles de
  tareas mínimas con overhead innecesario.

### 3. Manejo de credenciales en scripts de Spark

Las credenciales de Postgres se leen desde variables de entorno
(`POSTGRES_USER`, `POSTGRES_PASSWORD`, etc.), nunca hardcodeadas.

`get_jdbc_config()` usa `os.environ[...]` (no `.get()`) para usuario y
password. Si la env var no está, el script falla rápido con `KeyError`
explícito en lugar de conectar como `None` y devolver un error críptico
de Postgres segundos después.

### 4. Lectura de bronze: jsonb llega como string, parseado con `from_json`

El driver JDBC de Postgres entrega columnas `jsonb` como `text`, no como
struct nativo. No hay forma de evitar este round-trip. La lógica:

1. Leer `bronze.raw_fintech_data` por JDBC; `data` viene como string.
2. Aplicar `from_json(col("data"), customer_partial_schema)` para
   parsear a struct.
3. `explode()` sobre el array `accounts`.
4. Promover los campos del struct a columnas top-level.

### 5. Schema parcial por script

Cada script de Spark declara únicamente la porción del JSON que necesita.
`flatten_accounts.py` declara `customer_id + accounts[]` y nada más.
Los demás scripts (transactions, loans) declararán SUS schemas.

**Por qué**: evita acoplamiento implícito. Si el script de accounts
"conoce" la forma entera del customer, cuando alguien cambia transactions
este script reacciona inadvertidamente. El schema parcial declara un
contrato mínimo: "yo solo necesito esto".

### 6. Particionado de la lectura JDBC

`read_bronze` usa `partitionColumn=id`, `lowerBound=1`, `upperBound=100000`,
`numPartitions=4`.

Para 10.200 records es overkill (un solo split bastaría), pero es la
práctica correcta y se nota cuando el dataset crece. Documentado para
que sea reutilizado en los scripts de transactions y loans.

### 7. Estrategia de deduplicación

**Hallazgo**: bronze contiene 10.200 records pero solo 5.000 customers
únicos. Cada customer aparece duplicado en bronze por múltiples
`load_timestamp` (consecuencia natural de re-correr el DAG durante
desarrollo del día 2).

**No truncamos bronze**: la consigna pide preservar el raw faithfully,
y tener historial de cargas es lo correcto en una capa Bronze
profesional. La deduplicación se delega a Silver.

**Implementación**: window function en `spark/flatten_accounts.py`:

```python
Window.partitionBy("account_id").orderBy(col("load_timestamp").desc())
keep row_number() == 1
```

Se conserva la versión más reciente de cada cuenta. El mismo patrón
se aplicará en `stg_transactions` y `stg_loans`.

**Resultado**: 35.740 cuentas tras explode → 17.529 tras dedup
(eliminó exactamente la mitad, consistente con la teoría de doble carga).

**Implicancia para tests dbt**: la PK `account_id` en `silver.stg_accounts`
debe ser única. Si en runs futuros este test falla, indica que la window
function no cubrió algún caso edge — investigar antes de relajar la regla.

### 8. Modo de escritura en silver: `overwrite` con `truncate=true`

`mode=overwrite` + `truncate=true` en el JDBC writer.

**Por qué overwrite**: el contrato de un staging table es "esto refleja
la última vista de bronze". Si en el futuro queremos historial de cambios,
eso pertenece a una capa SCD2 en silver/gold, no a staging.

**Por qué truncate=true**: reusa la tabla existente en lugar de
droppearla y recrearla. Esto preserva permisos, constraints e índices
que dbt o admin pudieran haber agregado.

### 9. Limpieza de datos: separación de responsabilidades Spark / dbt

Durante el flatten de `accounts[]` detectamos inconsistencias categóricas
serias (ver hallazgos abajo). La regla de qué se limpia dónde:

- **PySpark (silver staging)**: solo limpieza sintáctica universal.
  - `trim()` en todos los strings.
  - String vacío post-trim → `NULL`.
  - Sin lógica de negocio.
- **dbt (silver models)**: normalización semántica.
  - `lower()` para unificar casing.
  - Mapeo explícito de traducciones.
  - Tests `accepted_values` para bloquear la introducción de nuevas
    variantes inadvertidas en runs futuros.

**Por qué**: la normalización semántica involucra decisiones
(¿`cerrado` y `closed` son equivalentes? sí, pero hay que defenderlo).
Tenerla en SQL versionado es auditable y queda documentada en
`schema.yml` de dbt; tenerla en código Python imperativo no.
Además, los tests de dbt convierten estas reglas en contratos
verificables automáticamente.

### 10. Hallazgos de calidad de datos en `accounts[]`

**Estructura sana**:
- 17.529 cuentas, todas con `account_id` único (dedup OK).
- 5.000 customers, 2-5 cuentas cada uno, distribución pareja.
- Cero NULLs en campos categóricos (`account_type`, `status`,
  `currency`, `branch_code`).

**`account_type` — 4 valores reales, 20 variantes en bronze**
- 5 variantes por valor, todas diferenciadas únicamente por whitespace.
- Resuelto en Spark con `trim()`: 20 → 4 valores canónicos.
- Set canónico: `savings`, `checking`, `investment`, `credit_card`.

**`status` — 3 valores reales, 12 variantes en bronze**
- Variantes por (a) casing inconsistente, (b) traducciones al español.
- El trim NO colapsa estas variantes (son decisiones semánticas).
- Familias detectadas:
  - **active** (5.877): `active`, `Active`, `ACTIVE`, `activo`
  - **frozen** (5.811): `frozen`, `Frozen`, `FROZEN`, `congelado`
  - **closed** (5.841): `closed`, `Closed`, `CLOSED`, `cerrado`
- A resolver en dbt silver con `lower()` + mapping
  `{cerrado→closed, activo→active, congelado→frozen}`.

**Divergencia consigna vs datos reales**:
- La consigna documenta `status ∈ {active, inactive, suspended, closed}`.
- Los datos contienen `{active, frozen, closed}`.
- No aparecen `inactive` ni `suspended`; sí aparece `frozen`.
- El test `accepted_values` de dbt usará el set REAL, no el documentado.
- La consigna avisa explícitamente sobre "unexpected statuses": esto es
  exactamente ese caso.

**`currency` — limpio, dominio LATAM coherente**:
- 8 valores: USD + 7 monedas locales (PEN, COP, MXN, UYU, BRL, ARS, CLP).
- Hallazgo de negocio: **~50% de las cuentas (8.734 / 17.529) están
  denominadas en USD**, consistente con la dolarización informal en la
  región (especialmente AR y UY).
- **Implicancia para Gold**: para la pregunta 2 ("total account balances
  by country"), hay que decidir si reportar en moneda nominal o convertir
  a una moneda común. Decisión a tomar en día 5-6.

**`branch_code` — limpio**:
- Patrón consistente `BR-NNN` (3 dígitos).
- Distribución pareja en el top 20 (29-36 cuentas por branch).
- Sin nulls, sin variantes raras.


## Deduplication Strategy

The pipeline applies deduplication at two layers, each handling the kind of
duplicate that's natural to its abstraction.

### Why deduplicate at all

Bronze is append-only: every DAG run inserts the full dataset again with a
fresh `load_id` and `load_timestamp`. This is intentional — bronze is meant
to be a faithful audit log of what arrived from the source, not a deduped
view. During development the DAG runs many times, so by the time we hit
silver, the same `customer_id` (and every PK nested inside it) appears in
multiple bronze rows.

Without dedup, the silver staging tables would carry those repeats forward,
breaking PK-uniqueness tests in dbt and inflating every aggregate downstream.

### Where dedup happens

**PySpark (silver staging tables) — dedup by array PK.**

Each of the three flatteners (`flatten_accounts.py`, `flatten_transactions.py`,
`flatten_loans.py`) deduplicates by the natural primary key of the array it
explodes:

| Script                       | Output table                  | Dedup key        |
|------------------------------|-------------------------------|------------------|
| `flatten_accounts.py`        | `silver.stg_accounts`         | `account_id`     |
| `flatten_transactions.py`    | `silver.stg_transactions`     | `transaction_id` |
| `flatten_loans.py`           | `silver.stg_loans`            | `loan_id`        |

The shared logic lives in `spark/utils.py::deduplicate_by_pk`, which applies
a window function partitioned by the PK and ordered by `load_timestamp DESC`,
keeping `row_number() == 1`. In plain English: **for each PK, keep the row
that came from the most recent bronze load.**

This is the right semantics for a staging table: silver should reflect the
latest known state of each entity, not its history. If we ever need the
history (SCD2-style), that lives in a dedicated dimensional model in
silver/gold, not in staging.

**dbt (silver dimensions) — dedup by customer_id.**

Customer-level dedup is *not* done in PySpark. The reason is the project's
tool roles: PySpark's job is array flattening, and the customer record itself
has no nested arrays to flatten — its flat fields are already flat in the
source JSON. So `dim_customers` is built directly in dbt, reading from
`bronze.raw_fintech_data` via a staging model that parses the `jsonb` and
applies the same "latest `load_timestamp` wins" rule using `qualify
row_number() over (partition by customer_id order by load_timestamp desc) = 1`.

This split keeps each tool doing what the project asks it to do, and avoids
materializing an intermediate `silver.stg_customers` table that would
duplicate work between the layers.

### Edge cases

- **Same `load_timestamp` for two versions of the same PK** — happens if the
  DAG fires twice in the same second. Spark picks one arbitrarily; since
  the rows are byte-identical when this occurs (same source file, same
  parsing), it doesn't matter which one wins. Documented but not guarded
  against.
- **Null PKs** — `row_number()` treats nulls as their own group and would
  keep one. We don't filter nulls in PySpark; if a null PK appears, it's an
  upstream data-quality bug that dbt's `not_null` test will catch and fail
  loudly on, which is the behavior we want.
- **Customers without loans** — `flatten_loans.py` uses `explode` (not
  `explode_outer`), so customers with empty `loans[]` produce zero rows.
  This is correct: `silver.stg_loans` is a fact table of loans, not a
  customer × loan matrix. Metrics like "% of customers with a loan" are
  built in gold via a `LEFT JOIN` from `dim_customers`.

### Sanity checks

Each flattener logs three numbers per run:
- `bronze records read` — how many rows came from `bronze.raw_fintech_data`
- `<entity> after explode` — how many rows after exploding the array
- `<entity> after dedup` / `duplicates dropped` — final count vs. dropped

In a healthy run with N bronze loads of the same dataset, `duplicates
dropped` should equal `(N-1) × <expected entity count>`. If it's higher, a
PK collision exists upstream that wasn't there before; if it's lower, a load
went partial.



## Day 6 — Silver dbt Design Decisions

### Refined Spark vs dbt responsibility split

The original split (Spark = syntactic, dbt = semantic) is refined to be more precise:

- **Spark handles array flattening** (`accounts[]`, `transactions[]`, `loans[]`):
  cardinality changes via `explode`; distributed compute semantics genuinely apply.
- **dbt handles flat fields and nested objects** (customer fields, `credit_info{}`,
  `digital_engagement{}`): cardinality is preserved 1:1 with customer; Postgres
  `jsonb` operators are both performant (5k rows) and idiomatic.
- **Deduplication follows the same logic**: array entities (accounts, txns, loans)
  are deduped in Spark as part of their explode pipeline. Customer dedup happens
  in dbt because customers are not exploded — they are extracted flat from bronze.

Rationale: the meaningful distinction is *whether explode is needed*, not
*whether dedup is needed*. Forcing customer through Spark just to dedup would
require a script that doesn't actually flatten anything, breaking the naming
convention (`flatten_*`) and introducing a fourth Spark script with no
distributed-compute justification.

### Status field divergence (correction to Day 1 EDA notes)

Earlier notes conflated two `status` fields. Distinct findings:

- **`account.status`** (`silver.stg_accounts`): contains `active / frozen / closed`.
  Diverges from the brief, which specifies `active / inactive / suspended / closed`.
  Documented divergence; `accepted_values` test reflects observed data.
- **`customer.status`** (bronze raw): contains 20 surface variants across 4
  canonical values: `active / inactive / suspended / closed`. **These 4 canonical
  values exactly match the brief.** Variants are casing chaos (`Active`, `ACTIVE`)
  plus Spanish translations (`activo`, `suspendido`, `cerrado`, `inactivo`,
  including their casing variants). 4,223 of 5,000 records (84.5%) use a
  canonical value; 777 (15.5%) need normalization.
- Normalization happens in `stg_customers.sql` via lowercase + Spanish→English
  mapping. `accepted_values` test on the resulting 4 canonical values.

### Data quality findings (from `silver.dim_customer` audit, dropped today)

The legacy `silver.dim_customer` table (origin unknown; no script generates it;
dropped today) was useful as an audit surface and surfaced three findings:

- **340 customers (6.8%) have invalid coordinates**: `lat` outside [-90, 90] or
  `lon` outside [-180, 180]. Likely generator bug in the dataset.
  Treatment: `is_geo_valid` boolean flag in `dim_customer`; `lat`/`lon` set to
  NULL when invalid. Preserves the record (no row loss) while making the
  data quality issue explicit and queryable.
- **`nationality` equals `country` in 100% of records.** Field is informationally
  redundant. Treatment: keep in `dim_customer` as a verbatim copy (in case
  downstream analysis ever differentiates), but add a `dbt_utils.expression_is_true`
  test asserting equality. If the test ever fails in a future run, that's a
  signal to revisit.
- **City names contain typos** (e.g. `Lma` for `Lima`). Treatment: deferred.
  Documented as known dataset quality issue; will not affect aggregations
  by `country` (the primary geographic dimension for business questions 2, 6, 10).

### Bucketing definitions

These cover business questions 9, 11, 21, and partially 5, 6, 7.

**Age buckets** (from `date_of_birth`, computed via `AGE()`):
- `18-25` — students / early career
- `26-35` — millennials, peak product acquisition
- `36-50` — peak earning years
- `51-65` — pre-retirement
- `65+` — retirees

Rationale: standard LATAM fintech segmentation aligned with life stages and
product affinity. Anyone under 18 in the data is treated as a data quality
issue (flagged, not bucketed).

**Tenure buckets** (from `registration_date`, computed via months difference):
- `new` — < 6 months
- `established` — 6 to 24 months
- `loyal` — > 24 months

**Risk score buckets** (from `risk_score`, 0-100 numeric):
- `low` — 0 to 30
- `medium` — 30 to 60
- `high` — 60 to 85
- `critical` — 85 to 100

Aligned with business question 9. Boundaries chosen to roughly approximate
equal-population quartiles based on EDA.

**Credit score buckets** (FICO standard):
- `poor` — 300-579
- `fair` — 580-669
- `good` — 670-739
- `very_good` — 740-799
- `excellent` — 800-850

### Naming conventions for dbt models

- `stg_*` — staging layer, one model per source table or extracted object.
  Materialization: view (cheap to rebuild, no aggregation).
- `dim_*` — dimensions in the silver layer. Materialization: table (joined
  downstream by gold models).
- `fact_*` — facts in the silver layer (transactions, loan_snapshots). Tables.
- Gold marts will use `mart_*` or `gold_*` prefix (decided in Day 6+).


### Note on legacy table cleanup

A `silver.dim_customer` table existed at the start of Day 6, origin unknown
(no Spark script generates it; likely created during Day 1 exploration or
PoC work). It was dropped manually via `DROP TABLE silver.dim_customer`
before starting dbt modeling. No reproducible script is included because the
table is not part of the pipeline — the canonical `dim_customer` is now
built by dbt and any future clean-clone setup will never produce the legacy
version.


## Day 6 — Silver dbt Design Decisions

### Refined Spark vs dbt responsibility split

The original split (Spark = syntactic, dbt = semantic) is refined to be more precise:

- **Spark handles array flattening** (`accounts[]`, `transactions[]`, `loans[]`):
  cardinality changes via `explode`; distributed compute semantics genuinely apply.
- **dbt handles flat fields and nested objects** (customer fields, `credit_info{}`,
  `digital_engagement{}`): cardinality is preserved 1:1 with customer; Postgres
  `jsonb` operators are both performant (5k rows) and idiomatic.
- **Customer deduplication lives in dbt**, not Spark. Rationale: customer is not
  an array — it's the root of each JSON record. A Spark script for customer
  would not actually flatten anything (no `explode` involved); it would only
  dedup, which Postgres handles trivially at this volume via `ROW_NUMBER()`.
  Forcing it through Spark would break the `flatten_*` naming convention and
  add a script without distributed-compute justification.

The meaningful distinction is *whether explode is needed*, not *whether dedup
is needed*.

### Customer status field — full picture

Earlier EDA notes (day 1) flagged `status` divergence but conflated two fields.
Distinct findings:

- **`account.status`** (in `silver.stg_accounts`, produced by Spark): contains
  only `active / frozen / closed`. Diverges from the brief spec which lists
  `active / inactive / suspended / closed`. Documented divergence;
  `accepted_values` test in dbt will reflect the observed three values.
- **`customer.status`** (in `bronze.raw_fintech_data`): contains 20 surface
  variants across 4 canonical values. **The 4 canonical values exactly match
  the brief**: `active / inactive / suspended / closed`. Variants are casing
  chaos (`Active`, `ACTIVE`) plus Spanish translations (`activo`, `suspendido`,
  `cerrado`, `inactivo`, with their own casing variants). 4,223 of 5,000 rows
  (84.5%) use canonical values; 777 (15.5%) require normalization.
- Normalization is implemented in macro `normalize_customer_status` (lowercase
  + Spanish→English mapping). Applied in `dim_customer.sql`.

### Data quality findings (from legacy `silver.dim_customer` audit, now dropped)

A legacy `silver.dim_customer` table was found in Postgres at the start of
day 6, origin unknown (no Spark script generates it; likely created during
day 1 exploration). It was dropped manually before starting dbt modeling.
While it was not part of the pipeline, it served as a useful audit surface
and surfaced three findings now addressed in the canonical dbt-built
`dim_customer`:

- **340 customers (6.8%) have invalid coordinates**: `lat` outside [-90, 90]
  or `lon` outside [-180, 180]. Likely a generator bug.
  Treatment: `is_geo_valid` boolean flag; `lat`/`lon` set to NULL when invalid.
  Preserves the record while making the data quality issue explicit.
- **`nationality` equals `country` in 100% of records**. Informationally
  redundant. Treatment: kept in `dim_customer` (in case downstream ever
  differentiates) with a `dbt_utils.expression_is_true` test asserting
  equality. A future test failure would be a useful signal.
- **City names contain typos** (e.g. `Lma` for `Lima`). Deferred. Aggregations
  by `country` (the primary geographic dimension for business questions 2, 6,
  10) are unaffected.

The legacy table is not reproducible from any script; no clean-clone setup
will produce it. No cleanup SQL committed.

### Bucketing definitions

These support business questions 9, 11, 21, and partially 5, 6, 7. Implemented
as reusable dbt macros (`age_bucket`, `tenure_bucket`).

**Age buckets** (macro `age_bucket(date_of_birth)`):
- `under_18` — flagged as data quality issue (banks don't onboard minors)
- `18-25` — students / early career
- `26-35` — millennials, peak product acquisition
- `36-50` — peak earning years
- `51-65` — pre-retirement
- `65+` — retirees
- `unknown` — when `date_of_birth` is NULL or unparseable

**Tenure buckets** (macro `tenure_bucket(registration_date)`):
- `new` — < 6 months
- `established` — 6 to 24 months
- `loyal` — > 24 months
- `unknown` — when `registration_date` is NULL or unparseable

**Risk score buckets** (deferred to gold layer — only used in business
question 9, no value adding to dim_customer):
- `low` (0-30), `medium` (30-60), `high` (60-85), `critical` (85-100)

**Credit score buckets** (deferred to gold layer — used in Q6, lives more
naturally with credit_info):
- `poor` (300-579), `fair` (580-669), `good` (670-739),
  `very_good` (740-799), `excellent` (800-850)

### Customer activity model placement

A `customer_summary` model written in a prior session lives in `models/gold/`.
On reflection, its grain (1 row per customer) and contents (identity +
product counts) make it a **silver-layer aggregate**, not a gold mart. It is
moved to `models/silver/agg_customer_activity.sql`. Rationale:

- Gold marts in this project will use a `mart_*` or `gold_*` prefix and have
  business-question-specific grains (customer-month, segment-month, loan,
  etc.). A per-customer count table doesn't belong with them.
- An `agg_*` prefix in silver honestly describes the model: it's an aggregate,
  not a fact (no transactional grain) and not a dimension (has measures, not
  attributes).
- Snapshot semantics: refreshed full per run, not slowly changing.

### Naming conventions for dbt models

- `stg_*` — staging, one model per source. Materialization: view.
- `dim_*` — dimensions in silver. Materialization: table.
- `fact_*` — transactional facts in silver (e.g. `fact_transactions`,
  `fact_loan_payments` if added). Materialization: table.
- `agg_*` — aggregates in silver (e.g. `agg_customer_activity`).
  Materialization: table.
- Gold marts (day 7+) — prefix to be decided, likely `mart_*` or `gold_*`.



### Generalized pattern: all customer-level categoricals need normalization

Day 6 testing surfaced that the casing chaos + Spanish translation pattern
documented for `customer.status` is NOT isolated to that field. It is a
**generator-wide pattern** affecting every categorical field in the dataset:

- `customer.status` — 20 variants, ~15.5% non-canonical (documented day 1).
- `customer.kyc_status` — 8 variants, ~7.6% non-canonical. Only casing
  variants (no Spanish translations). Normalized via `normalize_kyc_status`.
- `customer.customer_segment` — 16 variants, ~15.7% non-canonical. Both
  casing and Spanish translations. Normalized via `normalize_customer_segment`.

Implication for downstream silver models: any categorical field arriving
from bronze (transactions.channel, transactions.category, transactions.type,
transactions.status, accounts.account_type, loans.type, loans.status,
gender, etc.) must be assumed to have casing/translation variants until
empirically proven otherwise. Each gets its own `normalize_*` macro
following the same pattern (lowercase+trim, optional ES→EN CASE map,
else-passthrough so unexpected values fail tests loudly).


### Tests configured as `warn` for documented data quality issues

Some `not_null` tests are intentionally set to `severity: warn` instead of
the default error level. This captures data quality issues from the source
generator without blocking the build. Affected fields:

- `silver.stg_accounts.balance` — 486 NULLs (2.8%), uniformly distributed
  across all account_types. Confirmed not a parsing or join issue;
  comes as NULL in the bronze JSON for those records.

The pattern is: if a test fails because of generator data quality (not
because of a bug in our code), warn lets the issue stay visible in
`dbt test` output while keeping the pipeline runnable. If the dataset is
ever refreshed and the NULL rate changes significantly, we'll see it
in the warn count.

## Day 6 (continued) — Staging models, dimensions, and aggregate

### Macro architecture: normalize_casing + entity-specific specifics

Adopted "Option C" macro hierarchy:
- `normalize_casing(col)` — base macro doing `lower(trim(col))`.
- `normalize_<entity>_<field>(col)` — thin or rich wrappers using the base.

Thin wrappers (just delegate to normalize_casing): `normalize_kyc_status`,
`normalize_transaction_status`, `normalize_loan_status`, `normalize_loan_type`.
These exist for naming consistency: call sites read as
`{{ normalize_kyc_status(...) }}` (intent-revealing) rather than
`{{ normalize_casing(...) }}` (generic).

Rich wrappers (lowercase + Spanish-to-English mapping):
`normalize_customer_status`, `normalize_customer_segment`,
`normalize_account_status`, `normalize_transaction_type`,
`normalize_transaction_category`, `normalize_collateral_type`.

For `account_type` and `transaction.channel`, `normalize_casing` is applied
inline in the model (no dedicated macro) because the field has only casing
variants and creating a macro for a one-liner would be ritualistic.

### Schema separation: silver_raw (Spark) vs silver (dbt)

Day 6 discovered a name collision: Spark wrote to `silver.stg_accounts` and
dbt tried to create a view at the same fully-qualified name. dbt silently
failed to materialize, causing tests to run against the raw Spark output
instead of the normalized view.

Fix: introduced `silver_raw` schema for Spark outputs. dbt reads from
`silver_raw.stg_*` (declared as source) and writes views/tables to `silver`.

Implementation:
- Added `SPARK_TARGET_SCHEMA` env var to `.env`, `env.example`, `docker-compose.yml`.
- Created `TARGET_SCHEMA` constant in `spark/utils.py` reading from env.
- Updated 3 Spark flatten scripts to use `f"{TARGET_SCHEMA}.stg_<name>"`.
- Updated `dbt/models/sources.yml`: `schema: silver_raw`.
- Migrated existing tables with `ALTER TABLE ... SET SCHEMA silver_raw`.

The default value `silver_raw` is hardcoded in `utils.py` so the script
works even if the env var is missing; the env var allows overriding for
test environments or future production deployments.

### Date parsing centralization

Spark's `to_date("yyyy-MM-dd")` silently NULL-ed any non-ISO date. EDA
discovered 4 formats in date fields across the dataset:
- ISO (~91%): `2026-05-08`
- Compact (~3%): `20260613`
- Slash DMY (~3%): `26/04/2026`
- Dash MDY (~3%): `06-13-2024`

Decision: Spark passes dates as text; dbt's `parse_date_multi_format` macro
handles all 4 formats. Rationale: choosing which formats are valid is a
semantic decision, not a syntactic one. Per the responsibility split, dbt
owns it.

Used by: `dim_customer` (date_of_birth, registration_date), `stg_accounts`
(opened_date), `stg_transactions` (transaction_date), `stg_loans`
(start_date, end_date).

### Generalized data quality patterns

Day 6 confirmed and extended the generator's data quality patterns:
- **Casing chaos + Spanish translations** affect every categorical field
  (customer.status, customer.kyc_status, customer.customer_segment,
  account.status, transaction.type, transaction.status, transaction.category,
  loan.status, loan.type, loan.collateral_type, digital_engagement.preferred_channel).
- **Missing markers as strings** (`''`, `'NA'`, `'N/A'`, `'null'`, `'NULL'`)
  appear in text categoricals AND in numeric fields. The `safe_cast_numeric`
  macro NULL-s all 5 markers before casting.
- **Non-parseable numeric strings** (`'$78.26'`, `'89.5 USD'`, `'83,5'`)
  appear in 38 records of utilization_pct (0.4%). The safe_cast_numeric
  macro returns NULL for any string not matching `^-?[0-9]+\.?[0-9]*$`.
- **Out-of-range sentinels** in credit_score (~10%): values like 999999, 0,
  negatives. Handled via the raw + flag + validated pattern.

### Raw + flag + validated pattern

Applied consistently for fields with genuine data quality issues that
cannot be cleanly recovered:
- `dim_customer.lat/lon` + `is_geo_valid` (6.8% invalid coordinates).
- `stg_credit_info.credit_score_raw` + `is_credit_score_valid` + `credit_score` (10% out of range).
- `stg_credit_info.utilization_pct_raw` + `is_utilization_pct_valid` + `utilization_pct` (0.4% unparseable + range issues).

The pattern preserves the original value for audit, exposes a boolean for
filtering, and provides a NULL-ed validated version for aggregations.

### EUR currency in transactions

Transactions contain ~1% of activity in EUR (924 rows). EUR is NOT present
in accounts. Likely cross-border activity. Documented in stg_transactions
yaml; relevant for business question 19 (international transfer patterns).

### Tests configured as `warn` for documented data quality issues

- `stg_accounts.balance` — 486 NULLs (2.8%), uniformly distributed.
- `stg_transactions.amount` — 2619 NULLs (3.0%), uniformly distributed.

Both are generator data quality, not parsing or join issues. Warn surfaces
them without blocking the build.

### customer_summary moved from gold to silver as agg_customer_activity

Originally created in a prior session as gold.customer_summary. On
reflection during day 6, its grain (1 row per customer) and contents
(activity counts) fit a silver aggregate, not a business mart. Renamed
to agg_customer_activity in silver. Old gold model dropped (file deleted,
Postgres table dropped via CASCADE).

### dim_geography is hardcoded, not derived

7 countries from the spec (CO, UY, AR, MX, CL, PE, BR), implemented with
`VALUES` in the model. Region is hardcoded as 'LATAM' for future
extensibility. City is NOT in this dimension because of typos in
dim_customer.city (e.g., 'Lma' for 'Lima'); customer.city can be used
directly when needed.