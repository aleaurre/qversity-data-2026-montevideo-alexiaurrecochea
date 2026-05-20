# Decisiones de modelado y negocio

> Documento vivo. Última actualización: tras EDA exhaustivo del dataset crudo
> (Día 1, bloque tarde).
>
> Este documento captura todas las decisiones de calidad de datos, modelado y
> métricas que guían la implementación de las capas Silver y Gold del pipeline.

---

## 1. Calidad de datos - hallazgos clave del EDA

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
Ningún null real - solo problemas de parsing. **El separador desambigua
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
- **No descartar** - pueden ser legítimos en `private_banking`.
- ~~Marcar con flag `is_outlier_<col>` por percentil 99.~~
  **Estado Día 10:** decisión Día 1 NO implementada (verificado via
  `information_schema.columns` — 0 columnas `is_outlier*` en el warehouse).
  Los outliers están presentes en Gold sin marcar; los marts usan SUM/AVG
  estándar sin filtrar. Si Power BI necesita robustez frente a outliers,
  se puede aplicar filtrado por percentil en el dashboard, o agregar
  columnas `is_outlier_*` en una iteración futura.
- En Gold, los KPIs de tendencia central usan **mediana**, no media,
  cuando la sensibilidad a outliers importa (aplicado selectivamente
  en los marts donde corresponde).

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
| `relationship_manager`                     | 9,9%    | **Día 6: revisited.** La decisión Día 1 (imputar `'UNASSIGNED'`) NO se implementó. `dim_customer.sql` pasa el valor through tal cual viene del JSON. Cualquier consumidor downstream que necesite tratar NULL como "no asignado" debe aplicar `COALESCE(relationship_manager, 'UNASSIGNED')` en su query. |
| `address`                                  | 7,8%    | Conservar NULL. No bloquea análisis.                            |
| `gender`                                   | 5,4%    | **Día 6: revisited.** La decisión Día 1 (imputar `'Unknown'`) NO se implementó. `dim_customer.sql` pasa NULL through: `(data ->> 'gender')::text as gender`. Sin imputación. |
| `accounts.credit_limit`                    | 74,6%   | **Legítimo:** solo `credit_card` lo tiene.                      |
| `transactions.description`                 | 10,2%   | Aceptable. Conservar NULL.                                      |
| `transactions.merchant`                    | 8,3%    | Aceptable. Conservar NULL.                                      |
| `transactions.category`                    | 4,9%    | Conservar NULL.                                                 |
| `loans.collateral_type`                    | 47,7%   | **Legítimo:** unsecured loans → mapear a `'unsecured'` en Gold. |
| `digital_engagement.avg_monthly_logins`    | 7,4%    | **Día 6: revisited.** La decisión Día 1 (validar hipótesis + imputar 0) NO se implementó. `dim_digital_engagement` aplica `safe_cast_numeric(..., 'int')` y deja NULL passthrough cuando el source es NULL o no-parseable. La hipótesis "NULL coincide con `mobile_app_registered=FALSE AND web_banking_registered=FALSE`" nunca se validó formalmente. Cualquier consumidor downstream que necesite tratar NULL como 0 debe aplicar `COALESCE(avg_monthly_logins, 0)` en su query. |

**Patrón observado:** las decisiones de imputación de Día 1 (UNASSIGNED, Unknown)
no se implementaron — durante el modelado en dbt se eligió pass-through NULL
en lugar de imputación. Esto es defensible: NULL preserva la información
"dato no disponible" sin inventar valores; los consumidores downstream
(Power BI, queries ad-hoc) pueden imputar en el punto de uso si lo necesitan.

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
> *(Definición Día 1 — **superseded por Día 9 §4**: la versión final tiene
> 6 buckets incluyendo `over_limit` y `unknown`. Ver sección 4 "Business
> definitions for Gold marts" más abajo.)*
> low: <30 | medium: 30–70 | high: 70–90 | critical: >90

**Credit score (300-850, validados):**
> poor: 300-579 | fair: 580-669 | good: 670-739 | very_good: 740-799 | excellent: 800-850

**Days past due:**
> current: 0 | 1–30 | 31–60 | 61–90 | 90+

---

## 8. Casos especiales documentados

### 8.1 EUR en transactions
940 transacciones (1%) en EUR a pesar de ser un dataset LATAM. **Conservar** -
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
momento de arrancar el proyecto) por seguridad - la imagen 2.7.3 acumula CVEs
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


## PySpark setup + flatten de accounts

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
function no cubrió algún caso edge - investigar antes de relajar la regla.

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

**`account_type` - 4 valores reales, 20 variantes en bronze**
- 5 variantes por valor, todas diferenciadas únicamente por whitespace.
- Resuelto en Spark con `trim()`: 20 → 4 valores canónicos.
- Set canónico: `savings`, `checking`, `investment`, `credit_card`.

**`status` - 3 valores reales, 12 variantes en bronze**
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

**`currency` - limpio, dominio LATAM coherente**:
- 8 valores: USD + 7 monedas locales (PEN, COP, MXN, UYU, BRL, ARS, CLP).
- Hallazgo de negocio: **~50% de las cuentas (8.734 / 17.529) están
  denominadas en USD**, consistente con la dolarización informal en la
  región (especialmente AR y UY).
- **Implicancia para Gold**: para la pregunta 2 ("total account balances
  by country"), hay que decidir si reportar en moneda nominal o convertir
  a una moneda común. Decisión a tomar en día 5-6.

**`branch_code` - limpio**:
- Patrón consistente `BR-NNN` (3 dígitos).
- Distribución pareja en el top 20 (29-36 cuentas por branch).
- Sin nulls, sin variantes raras.


## Estrategia de deduplicación

El pipeline aplica deduplicación en dos capas, cada una manejando el tipo de
duplicación que es natural a su nivel de abstracción.

### Por qué deduplicar

Bronze es append-only: cada corrida del DAG inserta el dataset completo de
nuevo con un nuevo `load_id` y `load_timestamp`. Esto es intencional —
Bronze debe ser un log fiel de auditoría de lo que llegó del source, no
una vista deduplicada. Durante el desarrollo el DAG corre múltiples veces,
así que para cuando los datos llegan a Silver, el mismo `customer_id` (y
cada PK anidada adentro) aparece en múltiples filas de Bronze.

Sin deduplicación, las tablas de staging en Silver arrastrarían esos
duplicados, rompiendo los tests de unicidad de PK en dbt e inflando todo
agregado downstream.

### Dónde sucede la deduplicación

**PySpark (tablas de staging en Silver) — dedup por PK del array.**

Cada uno de los tres flatteners (`flatten_accounts.py`, `flatten_transactions.py`,
`flatten_loans.py`) deduplica por la primary key natural del array que
explota:

| Script                       | Tabla de salida               | Clave de dedup   |
|------------------------------|-------------------------------|------------------|
| `flatten_accounts.py`        | `silver.stg_accounts`         | `account_id`     |
| `flatten_transactions.py`    | `silver.stg_transactions`     | `transaction_id` |
| `flatten_loans.py`           | `silver.stg_loans`            | `loan_id`        |

La lógica compartida vive en `spark/utils.py::deduplicate_by_pk`, que aplica
una window function particionada por la PK y ordenada por `load_timestamp DESC`,
conservando `row_number() == 1`. En palabras simples: **para cada PK, se
conserva la fila que vino del load más reciente de Bronze.**

Esta es la semántica correcta para una tabla de staging: Silver debe
reflejar el último estado conocido de cada entidad, no su historial. Si
alguna vez necesitamos el historial (estilo SCD2), eso vive en un modelo
dimensional dedicado en silver/gold, no en staging.

**dbt (dimensiones en Silver) — dedup por customer_id.**

La deduplicación a nivel customer NO se hace en PySpark. La razón son los
roles de las herramientas del proyecto: el trabajo de PySpark es el array
flattening, y el record de customer en sí mismo no tiene arrays anidados
para aplanar — sus campos planos ya están planos en el JSON source. Por
eso `dim_customer` se construye directamente en dbt, leyendo de
`bronze.raw_fintech_data` vía un modelo de staging que parsea el `jsonb` y
aplica la misma regla "gana el último `load_timestamp`" usando
`qualify row_number() over (partition by customer_id order by load_timestamp desc) = 1`.

Esta separación mantiene a cada herramienta haciendo lo que el proyecto
pide, y evita materializar una tabla intermedia `silver.stg_customers` que
duplicaría trabajo entre las capas.

### Casos borde

- **Mismo `load_timestamp` para dos versiones de la misma PK** — sucede si
  el DAG dispara dos veces en el mismo segundo. Spark elige una arbitrariamente;
  como las filas son byte-idénticas cuando esto ocurre (mismo source file,
  mismo parsing), no importa cuál gana. Documentado pero no protegido.
- **PKs NULL** — `row_number()` trata los NULLs como su propio grupo y
  conservaría uno. No filtramos NULLs en PySpark; si aparece una PK NULL,
  es un bug de calidad de datos upstream que el test `not_null` de dbt va
  a catchear y fallar ruidosamente, que es el comportamiento que queremos.
- **Customers sin loans** — `flatten_loans.py` usa `explode` (no
  `explode_outer`), entonces customers con `loans[]` vacío producen cero filas.
  Esto es correcto: `silver.stg_loans` es una fact table de loans, no una
  matriz customer × loan. Métricas como "% de customers con loan" se
  construyen en Gold vía `LEFT JOIN` desde `dim_customer`.

### Sanity checks

Cada flattener loguea tres números por corrida:
- `bronze records read` — cuántas filas vinieron de `bronze.raw_fintech_data`
- `<entidad> after explode` — cuántas filas después de explotar el array
- `<entidad> after dedup` / `duplicates dropped` — conteo final vs. droppeados

En una corrida sana con N loads de Bronze del mismo dataset, `duplicates
dropped` debería ser igual a `(N-1) × <conteo esperado de entidad>`. Si es
mayor, existe una colisión de PK upstream que no estaba antes; si es menor,
un load quedó parcial.



## Decisiones de diseño de Silver dbt

### Refinamiento del split Spark vs dbt

El split original (Spark = sintáctico, dbt = semántico) se refina para ser
más preciso:

- **Spark maneja el array flattening** (`accounts[]`, `transactions[]`,
  `loans[]`): la cardinalidad cambia vía `explode`; la semántica de cómputo
  distribuido aplica genuinamente.
- **dbt maneja los campos planos y objetos anidados** (campos de customer,
  `credit_info{}`, `digital_engagement{}`): la cardinalidad se preserva 1:1
  con customer; los operadores `jsonb` de Postgres son tanto performantes
  (5k filas) como idiomáticos.
- **La deduplicación sigue la misma lógica:** las entidades array (accounts,
  transactions, loans) se deduplican en Spark como parte de su pipeline de
  explode. La dedup de customer sucede en dbt porque los customers no se
  explotan — se extraen planos desde Bronze.

**Rationale:** la distinción significativa es *si hace falta explode*, no
*si hace falta dedup*. Forzar a customer a pasar por Spark solo para
deduplicar requeriría un script que en realidad no aplana nada, rompiendo
la convención de nombres (`flatten_*`) e introduciendo un cuarto script
Spark sin justificación de cómputo distribuido.

### Divergencia del campo status (corrección a notas EDA del Día 1)

Las notas previas confundían dos campos `status`. Hallazgos distintos:

- **`account.status`** (`silver.stg_accounts`): contiene `active / frozen / closed`.
  Diverge de la consigna, que especifica `active / inactive / suspended / closed`.
  Divergencia documentada; el test `accepted_values` refleja la data observada.
- **`customer.status`** (bronze raw): contiene 20 variantes en superficie
  pero solo 4 valores canónicos: `active / inactive / suspended / closed`.
  **Estos 4 valores canónicos coinciden exactamente con la consigna.** Las
  variantes son caos de casing (`Active`, `ACTIVE`) más traducciones al
  español (`activo`, `suspendido`, `cerrado`, `inactivo`, incluyendo sus
  variantes de casing). 4.223 de 5.000 records (84,5%) usan un valor
  canónico; 777 (15,5%) requieren normalización.
- La normalización sucede en `stg_customers.sql` vía lowercase + mapping
  español→inglés. Test `accepted_values` sobre los 4 valores canónicos
  resultantes.

### Hallazgos de calidad de datos (de la auditoría de `silver.dim_customer`, droppeada hoy)

La tabla legacy `silver.dim_customer` (origen desconocido; ningún script la
genera; droppeada hoy) fue útil como superficie de auditoría y surfaceó tres
hallazgos:

- **340 customers (6,8%) tienen coordenadas inválidas:** `lat` fuera de
  [-90, 90] o `lon` fuera de [-180, 180]. Probablemente un bug del generador.
  Tratamiento: flag boolean `is_geo_valid` en `dim_customer`; `lat`/`lon` se
  setean a NULL cuando son inválidos. Preserva el record (sin pérdida de filas)
  mientras hace el issue de calidad de datos explícito y queryable.
- **`nationality` es igual a `country` en 100% de los records.** El campo es
  informacionalmente redundante. Tratamiento: se mantiene en `dim_customer`
  como copia verbatim (por si análisis downstream alguna vez los diferencia),
  pero se agrega un test `dbt_utils.expression_is_true` que asercia igualdad.
  Si el test falla en alguna corrida futura, es señal para revisitar.
- **Los nombres de ciudad tienen typos** (ej. `Lma` por `Lima`). Tratamiento:
  diferido. Documentado como issue conocido del dataset; no afecta a las
  agregaciones por `country` (la dimensión geográfica primaria para las
  business questions 2, 6, 10).

### Definiciones de bucketing

Estas cubren las business questions 9, 11, 21, y parcialmente 5, 6, 7.

**Age buckets** (desde `date_of_birth`, computado vía `AGE()`):
- `18-25` — estudiantes / inicio de carrera
- `26-35` — millennials, pico de adquisición de productos
- `36-50` — años pico de ingresos
- `51-65` — pre-jubilación
- `65+` — jubilados

**Rationale:** segmentación estándar de fintech LATAM alineada con etapas de
vida y afinidad por productos. Cualquier persona menor a 18 en la data se
trata como issue de calidad de datos (se flaggea, no se bucketea).

**Tenure buckets** (desde `registration_date`, computado vía diferencia de meses):
- `new` — < 6 meses
- `established` — 6 a 24 meses
- `loyal` — > 24 meses

**Implementación:** macro `macros/tenure_bucket.sql`. Computa el delta en meses
totales usando `extract(year from age()) * 12 + extract(month from age())`
(la forma portable en Postgres — `extract(month from age())` solo devuelve
el componente de meses [0-11], no el total).

**Risk score buckets** (desde `risk_score`, 0-100 numeric):
- `low` — 0 a 30
- `medium` — 30 a 60
- `high` — 60 a 85
- `critical` — 85 a 100

Alineado con la business question 9. Los bordes se eligieron para aproximar
aproximadamente cuartiles de población igual basado en EDA.

**Credit score buckets** (estándar FICO):
- `poor` — 300-579
- `fair` — 580-669
- `good` — 670-739
- `very_good` — 740-799
- `excellent` — 800-850

### Convenciones de nombres para modelos dbt

- `stg_*` — capa de staging, un modelo por tabla source u objeto extraído.
  Materialización: view (barato de reconstruir, sin agregación).
- `dim_*` — dimensiones en la capa Silver. Materialización: table (joineadas
  downstream por los modelos Gold).
- `fact_*` — facts en la capa Silver (transactions, loan_snapshots). Tables.
- Los marts de Gold usan prefijo `mart_*` (decidido en Día 6+).


### Nota sobre limpieza de tabla legacy

Una tabla `silver.dim_customer` existía al inicio del Día 6, de origen
desconocido (ningún script de Spark la genera; probablemente creada durante
exploración del Día 1 o trabajo PoC). Fue droppeada manualmente vía
`DROP TABLE silver.dim_customer` antes de empezar el modelado en dbt. No
se incluye script reproducible porque la tabla no es parte del pipeline —
la `dim_customer` canónica ahora la construye dbt y cualquier setup de
clean-clone futuro nunca producirá la versión legacy.


### Patrón generalizado: todos los categóricos a nivel customer necesitan normalización

El testing del Día 6 reveló que el patrón de caos de casing + traducción al
español documentado para `customer.status` NO está aislado a ese campo. Es
un **patrón generalizado del generador** que afecta a todo campo categórico
del dataset:

- `customer.status` — 20 variantes, ~15,5% no-canónico (documentado en Día 1).
- `customer.kyc_status` — 8 variantes, ~7,6% no-canónico. Solo variantes de
  casing (sin traducciones al español). Normalizado vía `normalize_kyc_status`.
- `customer.customer_segment` — 16 variantes, ~15,7% no-canónico. Tanto
  casing como traducciones al español. Normalizado vía
  `normalize_customer_segment`.

**Implicancia para los modelos Silver downstream:** cualquier campo
categórico que llegue de Bronze (transactions.channel, transactions.category,
transactions.type, transactions.status, accounts.account_type, loans.type,
loans.status, gender, etc.) debe asumirse que tiene variantes de
casing/traducción hasta probarse empíricamente lo contrario. Cada uno tiene
su propia macro `normalize_*` siguiendo el mismo patrón (lowercase+trim,
mapa CASE ES→EN opcional, else-passthrough para que valores inesperados
fallen los tests ruidosamente).


### Tests configurados como `warn` para issues de calidad de datos documentados

Algunos tests `not_null` están seteados intencionalmente con `severity: warn`
en lugar del nivel default error. Esto captura issues de calidad de datos
del generador source sin bloquear el build. Campos afectados:

- `silver.stg_accounts.balance` — 486 NULLs (2,8%), uniformemente
  distribuidos entre todos los `account_type`. Confirmado que no es un
  issue de parsing o join; viene como NULL en el JSON de Bronze para esos
  records.

**El patrón es:** si un test falla por calidad de datos del generador (no
por un bug en nuestro código), `warn` deja el issue visible en el output de
`dbt test` mientras mantiene el pipeline funcionando. Si el dataset alguna
vez se refresca y la tasa de NULLs cambia significativamente, lo veremos en
el conteo de warns.

## Modelos staging, dimensions y aggregate

### Arquitectura de macros: normalize_casing + específicos por entidad

Se adoptó la jerarquía de macros "Opción C":
- `normalize_casing(col)` — macro base que hace `lower(trim(col))`.
- `normalize_<entidad>_<campo>(col)` — wrappers thin o rich usando la base.

**Thin wrappers** (solo delegan a normalize_casing): `normalize_kyc_status`,
`normalize_transaction_status`, `normalize_loan_status`, `normalize_loan_type`.
Existen por consistencia de nombres: los call sites se leen como
`{{ normalize_kyc_status(...) }}` (revelador de intención) en lugar de
`{{ normalize_casing(...) }}` (genérico).

**Rich wrappers** (lowercase + mapping español-a-inglés):
`normalize_customer_status`, `normalize_customer_segment`,
`normalize_account_status`, `normalize_transaction_type`,
`normalize_transaction_category`, `normalize_collateral_type`.

Para `account_type` y `transaction.channel`, `normalize_casing` se aplica
inline en el modelo (sin macro dedicada) porque el campo tiene solo
variantes de casing y crear una macro para un one-liner sería ritualístico.

### Separación de schema: silver_raw (Spark) vs silver (dbt)

El Día 6 descubrió una colisión de nombres: Spark escribía a
`silver.stg_accounts` y dbt trataba de crear una view con el mismo nombre
fully-qualified. dbt fallaba silenciosamente en materializar, causando que
los tests corrieran contra el output crudo de Spark en lugar de la view
normalizada.

**Fix:** se introdujo el schema `silver_raw` para los outputs de Spark.
dbt lee de `silver_raw.stg_*` (declarado como source) y escribe views/tables
a `silver`.

**Implementación:**
- Se agregó variable de entorno `SPARK_TARGET_SCHEMA` a `.env`, `env.example`,
  `docker-compose.yml`.
- Se creó constante `TARGET_SCHEMA` en `spark/utils.py` que lee de env.
- Se actualizaron los 3 scripts de flatten de Spark para usar
  `f"{TARGET_SCHEMA}.stg_<name>"`.
- Se actualizó `dbt/models/sources.yml`: `schema: silver_raw`.
- Se migraron las tablas existentes con `ALTER TABLE ... SET SCHEMA silver_raw`.

El valor default `silver_raw` está hardcodeado en `utils.py` para que el
script funcione aún si la env var falta; la env var permite override para
ambientes de test o deployments futuros de producción.

### Centralización del parsing de fechas

El `to_date("yyyy-MM-dd")` de Spark silenciosamente NULL-eaba cualquier
fecha no-ISO. El EDA descubrió 4 formatos en los campos de fecha del dataset:
- ISO (~91%): `2026-05-08`
- Compacto (~3%): `20260613`
- Slash DMY (~3%): `26/04/2026`
- Dash MDY (~3%): `06-13-2024`

**Decisión:** Spark pasa las fechas como text; la macro `parse_date_multi_format`
de dbt maneja los 4 formatos. **Rationale:** elegir qué formatos son válidos
es una decisión semántica, no sintáctica. Según la división de responsabilidades,
dbt es dueño.

**Usado por:** `dim_customer` (date_of_birth, registration_date), `stg_accounts`
(opened_date), `stg_transactions` (transaction_date), `stg_loans` (start_date,
end_date).

### Patrones generalizados de calidad de datos

El Día 6 confirmó y extendió los patrones de calidad de datos del generador:
- **Caos de casing + traducciones al español** afectan todo campo categórico
  (customer.status, customer.kyc_status, customer.customer_segment,
  account.status, transaction.type, transaction.status, transaction.category,
  loan.status, loan.type, loan.collateral_type, digital_engagement.preferred_channel).
- **Marcadores de missing como strings** (`''`, `'NA'`, `'N/A'`, `'null'`,
  `'NULL'`) aparecen en categóricos de texto Y en campos numéricos. La macro
  `safe_cast_numeric` NULL-ea los 5 marcadores antes de castear.
- **Strings numéricos no-parseables** (`'$78.26'`, `'89.5 USD'`, `'83,5'`)
  aparecen en 38 records de utilization_pct (0,4%). La macro safe_cast_numeric
  devuelve NULL para cualquier string que no matchee `^-?[0-9]+\.?[0-9]*$`.
- **Sentinelas fuera de rango** en credit_score (~10%): valores como 999999,
  0, negativos. Manejados vía el patrón raw + flag + validated.

### Patrón raw + flag + validated

Aplicado consistentemente para campos con issues genuinos de calidad de
datos que no se pueden recuperar limpiamente:
- `dim_customer.lat/lon` + `is_geo_valid` (6,8% coordenadas inválidas).
- `stg_credit_info.credit_score_raw` + `is_credit_score_valid` + `credit_score` (10% fuera de rango).
- `stg_credit_info.utilization_pct_raw` + `is_utilization_pct_valid` + `utilization_pct` (0,4% no-parseable + issues de rango).

El patrón preserva el valor original para audit, expone un boolean para
filtrado, y provee una versión validada NULL-eada para agregaciones.

### Currency EUR en transactions

Transactions contiene ~1% de actividad en EUR (924 filas). EUR NO está
presente en accounts. Probablemente actividad cross-border. Documentado en
el yaml de stg_transactions; relevante para la business question 19 (patrones
de transferencia internacional).

### Tests configurados como `warn` para issues de calidad de datos documentados

- `stg_accounts.balance` — 486 NULLs (2,8%), uniformemente distribuidos.
- `stg_transactions.amount` — 2.619 NULLs (3,0%), uniformemente distribuidos.

Ambos son calidad de datos del generador, no issues de parsing o join. Warn
los surfacea sin bloquear el build.

### customer_summary movido de gold a silver como agg_customer_activity

Originalmente creado en una sesión previa como `gold.customer_summary`. Tras
reflexión durante el Día 6, su grain (1 fila por customer) y contenido
(conteos de actividad) calza mejor como agregado de silver, no como mart de
negocio. Renombrado a `agg_customer_activity` en silver. El modelo gold
viejo se droppeó (archivo eliminado, tabla en Postgres droppeada vía
CASCADE).

### dim_geography es hardcoded, no derivado

7 países de la consigna (CO, UY, AR, MX, CL, PE, BR), implementado con
`VALUES` en el modelo. La región está hardcodeada como 'LATAM' para
extensibilidad futura. La ciudad NO está en esta dimensión por los typos en
`dim_customer.city` (ej. `Lma` por `Lima`); `customer.city` puede usarse
directamente cuando se necesita.

### Tag v0.2.0-silver (cierre Silver layer)

```bash
git tag -a v0.2.0-silver -m "Silver layer complete: PySpark flattening + dbt cleaning, dimensions, facts, 160 tests passing (7 warns documented as generator DQ)"
git push origin main
git push origin v0.2.0-silver
```


## Diseño de Gold + primeros 3 marts

### Resumen ejecutivo del día

- Diseño de la capa Gold: 8 marts mapeados a 24 preguntas (no un mart por pregunta).
- 3 marts implementados y testeados: `mart_acquisition_trend`, `mart_account_mix`, `mart_customer_360`.
- 65 tests dbt para los 3 marts, todos PASS.
- 5 macros de bucketing creadas en Gold; 1 macro defensiva nueva (`safe_cast_boolean`).
- Refactor estructural en Silver: `stg_credit_info` y `stg_digital_engagement` renombradas a `dim_*` y materializadas como `table`.
- 5 hallazgos de data quality nuevos documentados (boolean variants, NULL balance, BETWEEN con decimales, view-lazy cast, CTE no propagado).
- Decisiones de negocio congeladas: revenue, delinquency, 5 sets de buckets.

---

### 1. Arquitectura: 8 marts cubriendo 24 preguntas

Mapeo mart → preguntas (**diseño original Día 8 — superseded por Día 9**):

| Mart (Día 8) | Grano | Preguntas |
|------|-------|-----------|
| `mart_customer_360` | 1 fila/customer | Q1, Q9, Q10, Q11, Q13, Q14, Q24 + credit profile |
| `mart_revenue_by_segment` | segment × month | Q1 (rollup) |
| `mart_transactions_summary` | category × channel × month | Q3, Q15, Q16, Q17, Q18 |
| `mart_loan_portfolio` | loan_id (con buckets) | Q4, Q5, Q8, Q23 |
| `mart_acquisition_trend` | month | Q12 |
| `mart_digital_engagement` | segment × age_bucket | Q20, Q21 |
| `mart_account_mix` | country × account_type × currency | Q2, Q22 |
| `mart_international_transfers` | currency_pair × month | Q19 |

Cobertura: 24/24. **No** crear un mart por pregunta — la consigna evalúa "reusability and clarity of metrics" y "model design quality", lo cual penaliza redundancia.

**Actualización Día 9:** durante la construcción se identificó que varios de
estos marts agrupaban preguntas con grano genuinamente diferente y diluían
la claridad. Se split-earon en versiones más específicas. Mapeo final
(implementado en `dbt/models/gold/`):

| Mart (final, 16 total) | Grano | Preguntas |
|------|-------|-----------|
| `mart_acquisition_trend` | month | Q12 |
| `mart_customer_360` | 1 fila/customer | Q1 (rollup), Q9, Q10, Q11, Q13, Q14, Q24 |
| `mart_revenue_by_segment_usd` | segment | Q1 (USD) |
| `mart_account_mix` | country × account_type × currency | Q2, Q22 |
| `mart_delinquency_by_segment` | segment | Q5 |
| `mart_credit_score_by_country` | country × score_bucket | Q6 |
| `mart_utilization_vs_delinquency` | utilization_bucket | Q7 |
| `mart_risk_buckets` | risk_bucket | Q9 |
| `mart_loan_dpd` | loan_type × dpd_bucket × currency | Q8 |
| `mart_loan_composition` | loan_type × status × currency | Q4, Q23 |
| `mart_tx_by_channel` | channel × currency | Q3, Q17, Q18 |
| `mart_tx_by_category` | category × currency | Q15 |
| `mart_tx_by_dow` | day_of_week × currency | Q16 |
| `mart_international_transfers` | origin_country × tx_currency | Q19 |
| `mart_digital_adoption_by_segment` | segment | Q20 |
| `mart_channel_preference_by_age` | age_bucket × channel | Q21 |

Cobertura final: 24/24 (23 ✅ + Q13 ⚠️ documentado con divergencia spec/dataset).

---

### 2. Fusión `mart_credit_risk` → `mart_customer_360`

Razón: el dataset es un único punto en el tiempo (no hay snapshots históricos). Dos marts con grano `customer` serían redundantes. `mart_customer_360` lleva perfil + credit_score + utilization + risk_bucket en una sola tabla. Si en el futuro hubiera snapshots, se separaría en `dim_customer` + `fct_credit_risk_snapshot`.

---

### 3. Definición de revenue (la consigna pide definirlo)

`revenue = transaction_fees + interest_income`

- **`transaction_fees`**: `SUM(amount)` en `silver.fct_transactions` donde `transaction_type = 'fee'` AND `status = 'completed'`.
- **`interest_income`**: `SUM(outstanding_balance × interest_rate / 12)` sobre loans con `status IN ('current', 'delinquent')` (loans activos generan interés; default y paid_off no).

Representa lo que el banco **gana**, no el volumen transado. El volumen se reporta aparte (`transaction_volume`) para no confundir Q1 (revenue) con Q15 (volume).

**Hallazgo de DQ relacionado:** las fees del dataset tienen distribución `status` casi uniforme (~25% en cada uno de los 4 status), sugiriendo generador sintético sin lógica de negocio. En banca real `completed` debería ser ~95%. Por eso el filtro a `completed` es crítico: sin él, revenue queda inflado 4x.

---

### 4. Definición de delinquency (la consigna pide definirlo)

Se confía en el campo `status` del dataset tal cual viene en `silver.dim_loan`/`silver.fct_loans`:
- `current` — al día
- `delinquent` — en mora
- `default` — incumplimiento
- `paid_off` — saldado

Razón: el dataset ya provee el status semánticamente. Los dbt tests verifican consistencia interna (`accepted_values`). Inconsistencias entre `status` y `days_past_due`, si existen, se documentan pero no se corrigen en Gold — el origen es la verdad.

---

### 5. Buckets analíticos: distribución entre Silver y Gold

| Dimensión | Buckets | Vive en | Justificación |
|-----------|---------|---------|---------------|
| `age_bucket` | 18-25, 26-35, 36-50, 51-65, 65+, under_18 (DQ flag), unknown | **Silver** (`dim_customer`) | Uso transversal en 3+ marts. |
| `tenure_bucket` | new (<6m), established (6-24m), loyal (>24m), unknown | **Silver** (`dim_customer`) | Uso transversal en 3+ marts. |
| `risk_bucket` | low, medium, high, critical | Gold (macro `get_risk_bucket`) | Literal Q9 → 4 niveles obligatorios. |
| `utilization_bucket` | healthy (<30%), moderate (30-70%), high (>70%) | Gold (macro `get_utilization_bucket`) | Regla FICO. |
| `credit_score_bucket` | poor (300-579), fair (580-669), good (670-739), very_good (740-799), exceptional (800-850) | Gold (macro `get_credit_score_bucket`) | Rangos FICO estándar. |
| `days_past_due_bucket` | current (0), 1-30, 31-60, 61-90, 90+ | Gold (macro `get_days_past_due_bucket`) | Buckets regulatorios de provisioning bancario. |
| `tenure_years` | decimal calculado | Gold (macro auxiliar `get_tenure_years`) | Casos donde tenure como número complementa al bucket categórico. |

**Heurística operativa para ubicar futuros buckets:**
- ¿Lo usan 3+ marts? → Silver, columna materializada en la dim.
- ¿Lo usa 1-2 marts? → Gold, vía macro.
- ¿Definición cambia frecuentemente? → Gold (cambios baratos).
- ¿Estable y uso muy general? → Silver.

**Bucket de risk DECIMAL (rangos numéricos finos):** ver hallazgo #11 abajo sobre el bug de BETWEEN.

---

### 6. División de responsabilidades por capa (refinamiento a 3 niveles)

El principio original "Spark = syntactic / dbt = semantic" se extiende a tres niveles tras Día 8:

| Capa | Responsabilidad | Ejemplos |
|------|-----------------|----------|
| **PySpark → `silver_raw`** | Syntactic cleaning | `trim()`, empty → NULL, dedup, array flattening |
| **dbt → `silver`** | Semantic normalization + dimensiones transversales | Cast de tipos, normalización casing/traducciones, derivaciones aritméticas neutrales (`age`, `tenure_months`), buckets de uso transversal, constraints |
| **dbt → `gold`** | Bucketing analítico específico + business logic | `risk_bucket`, `utilization_bucket`, etc.; revenue, delinquency, agregaciones por grano de negocio |

**Sub-división `silver_raw` vs `silver` (formalizada en Día 8):**

| Schema | Responsable | Contenido |
|--------|-------------|-----------|
| `silver_raw` | PySpark (vía JDBC) | Staging post-flatten: arrays explotados, dedup, syntactic clean |
| `silver` | dbt | Modelos limpios: cast de tipos, normalización semántica, flattening de objetos anidados, constraints |

Contrato resultante:
- Gold lee solo de `silver.*`, nunca de `silver_raw.*`.
- dbt silver lee de `silver_raw.*` y de `bronze.*` (para objetos no-array como `credit_info`).
- Spark nunca escribe en `silver.*`.

**Beneficio operativo:** si dbt silver explota, `silver_raw` queda intacto y se puede re-correr dbt sin re-correr Spark. Aísla fallos por capa. Variable `SPARK_TARGET_SCHEMA=silver_raw` en `.env` parametriza el destino de Spark.

---

### 7. `silver.agg_customer_activity` como soporte estructural, no analítico

Esta tabla pre-agrega conteos puros (`accounts_count`, `transactions_count`, `loans_count`, `total_products`) por customer. Día 8 confirmó que su lugar en Silver es correcto: **solo contiene conteos estructurales, no métricas de negocio interpretadas**. Análogo a `dim_date` — facilita downstream sin imponer interpretaciones. Para Q24 (avg products per segment) evita 3 LEFT JOIN + COUNT DISTINCT en cada mart consumidor.

**Criterio general:** una agregación en Silver es legítima si solo cuenta/agrupa atributos estructurales del dataset (cardinalidad de relaciones). Una agregación que aplica reglas de negocio (revenue, delinquency rate, segment definitions) pertenece a Gold.

---

### 8. Refactor Silver: `stg_credit_info` y `stg_digital_engagement` → `dim_*`

**Contexto:** diseñando `mart_customer_360` se descubrió que `silver` tenía dos modelos con prefijo `stg_*` (legacy de un rename incompleto en Día 7). El resto de Silver ya seguía la convención `dim_*`/`fct_*`/`agg_*`. Esa asimetría se cerró:

- **`stg_credit_info` → `dim_credit_info`**: rename de archivo, drop de view huérfana en Postgres, actualización del YAML, `materialized='table'`. 9/9 tests PASS.
- **`stg_digital_engagement` → `dim_digital_engagement`**: idem. 7/7 tests PASS post-refactor.

**Estado final de Silver:** todas las tablas con prefijo `dim_*`/`fct_*`/`agg_*`. Ninguna `stg_*` en el schema `silver` (las `stg_*` viven solo en `silver_raw`, output de Spark).

**Lección operativa:** después de `git mv` de un modelo dbt, siempre seguir con `DROP` del objeto viejo en la DB + `dbt run --full-refresh` + actualizar `schema.yml`. Si no, queda inconsistencia silenciosa entre código y warehouse.

---

### 9. Hallazgo DQ: variantes boolean en digital_engagement

Al materializar `dim_digital_engagement` como table (antes era view), Postgres explotó con `invalid input syntax for type boolean: "si"`. Las 4 columnas booleanas (`mobile_app_registered`, `web_banking_registered`, `push_notifications`, `paperless_statements`) contienen variantes no estándar:

| Variante | Lenguaje/encoding | Filas afectadas |
|---|---|---|
| `true`/`false` | Estándar | ~8,500 |
| `yes`/`no` | Inglés casing | ~120 |
| `si` | Español sin tilde | ~86 |
| `0`/`1` | Numeric | ~118 |

**Solución:** macro `safe_cast_boolean()` análoga a `safe_cast_numeric()` del Día 6. Acepta `true/t/yes/y/sí/si/1` → `TRUE`, `false/f/no/n/0` → `FALSE`, todo lo demás → NULL.

**Lección arquitectónica:** **convertir views Silver a tables expone bugs latentes de cast.** View = cast lazy en read-time = bugs ocultos hasta que alguien consulta la fila ofensiva. Table = cast eager en write-time = bugs explícitos al `dbt run`. Materializar como table es mejor para DQ, no solo para performance.

**Análisis de impacto cruzado:** `bankruptcy_flag` en `credit_info` es la única otra columna boolean en Silver. Verificada limpia (solo `t`/`f`). No requiere fix.

**Principio derivado:** todo cast no-trivial desde JSON (numeric, boolean, date) debe usar macro defensiva con NULL fallback, no cast nativo `::tipo`. Macros disponibles: `safe_cast_numeric`, `safe_cast_boolean`, `parse_date_multi_format`. Cast nativo solo cuando los datos están demostrablemente limpios y el modelo es view (read-time, fail-fast aceptable).

---

### 10. Hallazgo DQ: 183 cuentas activas con balance NULL (~3.1%)

Descubierto durante validación cruzada de `mart_account_mix`. Distribución:
- Uniforme entre los 4 account_types (savings 29, checking 27, investment 25, credit_card 23 en USD)
- Aparece en las 8 currencies del dataset
- Sin patrón de concentración → ruido de generación de dataset sintético

**Tratamiento en Gold:** las 183 cuentas SE INCLUYEN en `accounts_count` (Q22 es sobre popularidad, no sobre balance) pero NO contribuyen a `total_balance`/`avg_balance` (SQL `SUM`/`AVG` ignoran NULL por definición). Columna nueva `accounts_with_balance` expone la discrepancia, haciendo la tasa de missing data queryable desde BI.

**Lección de modelado:** cuando se agrega una columna a un CTE intermedio en dbt, debe propagarse explícitamente en todos los CTEs downstream que hacen `SELECT enumerado`. Es la causa típica del bug "la columna existe en el archivo pero no en la tabla". `SELECT *` entre CTEs intermedios reduce este riesgo.

---

### 11. Hallazgo: bug en `get_risk_bucket` — `BETWEEN` con decimales

Descubierto al construir `mart_customer_360`. La macro original usaba `BETWEEN 0 AND 30`, `BETWEEN 31 AND 60`, etc. — patrón correcto para enteros pero **fatal con decimales**. Como `risk_score` es `numeric`, valores como 30.05, 30.08, 60.01, 85.01 caían en huecos entre buckets y se clasificaban como `'unknown'`. 147 customers (~3%) afectados.

**Solución:** reemplazar `BETWEEN` con comparadores explícitos.

```sql
-- Antes (buggy con decimales):
when {{ x }} between 0 and 30 then 'low'
when {{ x }} between 31 and 60 then 'medium'

-- Después (correcto):
when {{ x }} >= 0  and {{ x }} <= 30 then 'low'
when {{ x }} >  30 and {{ x }} <= 60 then 'medium'
```

**Convención adoptada:** bordes superiores inclusivos (`<=`), bordes inferiores exclusivos (`>`), excepto el primer bucket que usa `>=` para incluir 0.

**Análisis cruzado de las otras macros de bucketing:**
- `get_credit_score_bucket`: input `integer` (FICO scores enteros), `BETWEEN` funciona. ✅
- `get_days_past_due_bucket`: input `integer`, `BETWEEN` funciona. ✅
- `get_utilization_bucket`: input `numeric`, pero usa `<`/`>` sin BETWEEN — sin huecos. ✅

**Lección general:** macros de bucketing con `BETWEEN` solo son seguras para inputs enteros. Para decimales: usar comparadores explícitos.

---

### 12. Multi-currency en `mart_account_mix`: no convertir, granular por currency

**Decisión original (Día 8):** el dataset no incluía tabla de tasas FX. Sumar
`balance` entre USD y ARS sería matemáticamente incorrecto. Decisión: marts
que agregan balance llevan `currency` en el grano y evitan conversión
inventada.

Resultado en `mart_account_mix`: grano = `country × account_type × currency`.
Q22 (popularidad por type) sale agregando por type. Q2 (balances por country)
sale agregando por country mostrando currencies por país. El dashboard puede
filtrar por moneda o mostrar lado a lado.

**Actualización Día 9:** se agregó `seeds/fx_rates.csv` con conversiones USD
para las 8 monedas LATAM + EUR. `mart_revenue_by_segment_usd` consume este
seed. `mart_account_mix` **mantiene grano por currency** (decisión preservada)
porque balances típicamente se reportan en moneda nativa para audit, mientras
que revenue cross-country sí requiere unificación a USD. Power BI puede
aplicar conversión via join con `fx_rates` cuando el caso lo demande.

---

### 13. `mart_customer_360`: diseño completo

**Grano:** 1 fila por customer.
**Responde:** Q1 (revenue por segment), Q9 (risk buckets), Q10 (count by country/city), Q11 (age by segment), Q13 (status breakdown), Q14 (KYC distribution), Q24 (products per segment).

**Sources (todos LEFT JOIN desde `dim_customer` para no perder customers):**
- `silver.dim_customer` — identity, demographics, segment, age_bucket, tenure_bucket
- `silver.dim_credit_info` — credit_score, utilization, late_payments, bankruptcy_flag
- `silver.dim_digital_engagement` — mobile_app, web_banking
- `silver.agg_customer_activity` — accounts_count, loans_count, transactions_count, total_products (Q24)
- `silver.int_customer_monthly_revenue` — revenue centralizado (fees + interest, native + USD).
  Ver sección "Refactor: int_customer_monthly_revenue" (Día 10) para la definición completa.

**Nota Día 10:** las columnas de revenue del intermediate (`monthly_fee_revenue_native`,
`monthly_interest_revenue_native`, `total_monthly_revenue_native`) se renombran
dentro del mart al convention pre-refactor (`monthly_fee_revenue`, `monthly_interest_income`,
`total_revenue_monthly`) en un CTE `revenue` para preservar el contrato downstream
con Power BI sin tocar dashboards.

**Columnas finales (30):** identity (1) + demographics (5) + relationship (5) + risk (2) + credit profile (8) + digital (2) + products (4) + revenue (3).

**Decisiones críticas:**
- **LEFT JOIN siempre desde `dim_customer`:** no perdemos customers por falta de credit_info/digital/loans/transactions. Q10/Q13/Q14 mantienen universo completo.
- **`COALESCE(..., 0)` en counts y revenue:** customer sin transactions tiene `total_fees_paid = 0`, no NULL. Simplifica agregaciones en PowerBI.
- **NO usar `COALESCE` en credit_score/utilization_pct:** NULL ahí significa "dato no validable" (diferente de cero). PowerBI los filtra naturalmente.
- **`WHERE risk_score IS NOT NULL` como safeguard defensivo:** dataset actual no tiene NULLs, pero protege futuros loads.

---

### 14. Tests dbt para Gold: 65 tests creados, todos PASS

`dbt/models/gold/_gold__mart.yml` cubre los 3 marts del día con:
- `unique` + `not_null` en PKs.
- `relationships` desde `mart_customer_360.customer_id` → `dim_customer.customer_id`.
- `accepted_values` en todas las categóricas: country, account_type, currency, customer_segment, kyc_status, status, age_bucket, tenure_bucket, risk_bucket, credit_score_bucket, utilization_bucket.
- `dbt_utils.expression_is_true` para invariantes numéricos (no-negativos, rangos válidos).

**`accepted_values` de `risk_bucket`** intencionalmente NO incluye `'unknown'` — si aparece en algún run futuro, indica regresión en la macro `get_risk_bucket` (ver hallazgo #11).

**Lección sobre `dbt_utils.expression_is_true` a nivel de columna:** la macro auto-prefija el nombre de la columna antes de la expression. Patrones tipo `"col_x is null or col_x between 0 and 100"` generan SQL inválido (`where not(col_x col_x is null or...)`). **Solución:** mover esos tests a nivel de modelo (`tests:` hermano de `columns:`), donde la expression no se auto-prefija.

---

### 15. Gotcha operativa: `$env:VAR` vacías en sesiones nuevas de PowerShell

Las variables del `.env` solo se cargan en `docker compose`, NO en la sesión de PowerShell. Comandos como `docker exec qversity_postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "..."` fallan con `role "-d" does not exist` porque `$env:POSTGRES_USER` se expande a string vacío y `psql` reinterpreta los flags.

**Solución portátil:** leer las envs desde adentro del container con comillas simples por fuera:

```powershell
docker exec qversity_postgres bash -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB -c "..."'
```

Las comillas simples evitan que PowerShell expanda `$POSTGRES_USER` antes de mandar el comando al container; bash adentro del container sí tiene las envs cargadas.

---

## Definiciones de negocio para los marts Gold

Estas decisiones son upstream de todos los marts Gold construidos en el
Día 9. Cada una es un trade-off deliberado entre simplicidad, defensibilidad
profesional, y expresividad del dashboard. El rationale se preserva acá
para que la elección se pueda re-evaluar si el contexto de negocio cambia.

### 1. Revenue

`revenue = fee_income + interest_income_accrued`

- `fee_income = SUM(transactions.amount)` donde `type = 'fee' AND status = 'completed'`.
  Solo las fees `completed` cuentan; failed/reversed/pending se excluyen.
- `interest_income_accrued (mensual) = SUM(loans.outstanding_balance * interest_rate / 12)`
  donde `loans.status IN ('current', 'delinquent')`. Los loans `paid_off`
  y `default` no devengan interés (el primero está saldado, el segundo
  pasaría a non-accrual en contabilidad bancaria real).

**Trade-off documentado:** las interchange fees (comisiones de comercio
sobre transacciones de tarjeta) se excluyen porque el dataset no tiene un
campo para ellas. Esto sería un componente real de revenue en un banco
de producción.

### 2. Delinquency

Dos flags no-exclusivos, ambos derivados de `days_past_due` (no de `status`):

- `is_delinquent = days_past_due >= 30` — métrica operacional/de cobranzas
- `is_default = days_past_due >= 90 OR status = 'default'` — regulatorio
  (estándar Basel/IFRS9)

**El DPD tiene precedencia sobre el campo `status`** en caso de conflicto
(ej. status='current' pero DPD=45). El modelo emite un conteo de tales
conflictos como soft warning, sin fallar el build.

### 3. Buckets de días en mora (DPD)

Seis aging buckets alineados con la convención bancaria estándar. Los
prefijos numéricos aseguran ordenamiento alfabético correcto en visuales
de Power BI.

| Label del bucket         | Rango DPD  |
|--------------------------|------------|
| `00 - Current`           | 0          |
| `01 - Early (1-29)`      | 1-29       |
| `02 - 30-59 DPD`         | 30-59      |
| `03 - 60-89 DPD`         | 60-89      |
| `04 - 90-179 DPD`        | 90-179     |
| `05 - 180+ DPD`          | 180+       |

Los cortes en 30/60/90/180 coinciden con IFRS9 y los thresholds comunes
de charge-off.

### 4. Buckets de utilización de crédito

Seis buckets. NULLs aislados, over-100% explícitamente separados (ya que
el EDA confirmó valores reales > 100% en la data source — representan
estrés financiero, no corrupción).

| Label del bucket            | Rango de utilización |
|-----------------------------|----------------------|
| `01 - Healthy (<30%)`       | < 30                 |
| `02 - Moderate (30-60%)`    | 30-60                |
| `03 - High (60-90%)`        | 60-90                |
| `04 - Maxed (90-100%)`      | 90-100               |
| `05 - Over-limit (>100%)`   | > 100                |
| `99 - Unknown`              | NULL                 |

### 5. Buckets de credit score

Convención FICO (estándar de industria). Siete valores en total: Silver
retiene los valores fuera de rango como están (ya casteados a int), así
que el mart Gold los aísla en un bucket dedicado `00 - Invalid` en lugar
de dejarlos contaminar `01 - Poor` o `05 - Excellent`.

| Label del bucket              | Rango de score                    |
|-------------------------------|-----------------------------------|
| `00 - Invalid (out of range)` | NOT BETWEEN 300 AND 850           |
| `01 - Poor (300-579)`         | 300-579                           |
| `02 - Fair (580-669)`         | 580-669                           |
| `03 - Good (670-739)`         | 670-739                           |
| `04 - Very Good (740-799)`    | 740-799                           |
| `05 - Excellent (800-850)`    | 800-850                           |
| `99 - Unknown`                | NULL                              |

### 6. Buckets de edad

Rangos neutrales basados en décadas. La consigna del proyecto usa la frase
neutral "age group" (Q21), y los labels generacionales (Gen Z, Millennial...)
tienen cortes disputados según la fuente (Pew vs Strauss-Howe, etc.), así
que se prefieren bins neutrales.

| Label del bucket | Rango de edad |
|-----------------|---------------|
| `01 - 18-24`    | 18-24         |
| `02 - 25-34`    | 25-34         |
| `03 - 35-44`    | 35-44         |
| `04 - 45-54`    | 45-54         |
| `05 - 55-64`    | 55-64         |
| `06 - 65+`      | 65+           |
| `99 - Unknown`  | NULL          |

`age` ya está derivada en Silver desde `date_of_birth`.

### 7. Estrategia de currency

Híbrida: **15 marts en moneda nativa, 1 mart en USD.**

Los marts que reportan cifras monetarias (account_mix, loan_composition,
loan_dpd, tx_by_channel, tx_by_category, tx_by_dow, international_transfers,
customer_360 en su sección de revenue native) operan en la **moneda original**
del registro, con `currency` incluido en el grano cuando aplica. Esto evita
sumar incompatibilidades (USD + ARS sin tasa).

Solo `mart_revenue_by_segment_usd` aplica conversión a USD: revenue-per-segment
cruza países y demanda una unidad común para comparabilidad. Customer-grain
revenue se expone tanto en native (consumido por `mart_customer_360`) como
en USD (consumido por `mart_revenue_by_segment_usd`) desde el intermediate
`int_customer_monthly_revenue`.

**Infraestructura (compartida):**
- `seeds/fx_rates.csv` — currency → `rate_to_usd`, as_of_date
- `seeds/country_currency.csv` — código ISO de país → código de moneda local
- Macro `{{ to_usd(amount, currency) }}` — wraps el lookup de FX

**Tasas FX (snapshot mid-market, mayo 2026):**

| Currency | rate_to_usd |
|----------|-------------|
| USD      | 1.000000    |
| ARS      | 0.000717    |
| UYU      | 0.024850    |
| COP      | 0.000264    |
| MXN      | 0.058000    |
| CLP      | 0.001127    |
| PEN      | 0.270000    |
| BRL      | 0.200000    |

Fuentes: Xe.com, exchange-rates.org, tradingeconomics.com (mayo 2026). En
producción esto se reemplazaría por un feed FX diario.

**Insight del EDA (documentado, no bloqueante):** el dataset muestra ~50%
de actividad denominada en USD a lo largo de la región LATAM, reflejando
patrones reales de dolarización (notablemente UY y AR). Esto es un hallazgo
de negocio, no un issue de calidad de datos.

### 8. Transferencia internacional

`is_international = (type = 'transfer'
                     AND tx_currency != account_currency
                     AND tx_currency != customer_country_currency)`

La más estricta de tres definiciones consideradas: una transferencia es
internacional solo si su currency difiere de **ambos** el currency de la
cuenta originaria **y** el currency del país hogar del customer. Esto
minimiza falsos positivos (ej. un uruguayo con cuenta USD enviando USD a
otra cuenta USD se clasifica correctamente como doméstica).

El mapping país→currency se materializa como seed
(`seeds/country_currency.csv`) para mantener la lógica fuera del SQL del
modelo y consistente con el patrón del seed FX.

**Limitación documentada:** sin data de la cuenta destino, esto sigue
siendo un proxy. Una transferencia cross-border USD-a-USD que no requiere
FX se clasificaría como doméstica, lo cual es aceptable para propósitos
de atribución de revenue pero no para reportes AML.

---

### Bug encontrado: mismatch de escala temporal en revenue

Después de arreglar la escala de interest_rate (100x), los números de
revenue seguían siendo 1-2 órdenes de magnitud demasiado altos:
~$19k/customer/mes en USD, donde los benchmarks bancarios sugieren
~$50-500/customer/mes.

**Root cause:** `total_revenue_monthly` sumaba `total_fees_paid` (cumulativo
lifetime) con `monthly_interest_income` (proyección de un solo mes),
mezclando escalas temporales. El componente de fees dominaba por un
orden de magnitud proporcional a `tenure_months`.

**Fix:** normalizar las fees a promedio mensual dividiendo por tenure_months.
El renombrado `monthly_fee_revenue = total_fees_paid_lifetime / tenure_months`
provee consistencia temporal. El cumulativo lifetime se preserva como
`total_fees_paid_lifetime` para auditabilidad.

**Lección:** al sumar métricas, verificar que todos los componentes
comparten el mismo grain temporal. Agregar tests de invariantes sobre
magnitud (ej. revenue por customer en un rango razonable) para catchear
esta clase de bug.

**Edge case manejado:** ~0,9% de customers (45 de 5.000) tienen
`tenure_months = 0` (registrados en el load más reciente). Para estos,
`monthly_fee_revenue = NULLIF / 0` daría NULL y los excluiría de los
promedios por segment. Usamos `COALESCE(..., 0)` para atribuir cero
revenue mensual a estos customers — conceptualmente correcto (no tienen
tiempo acumulado de fees todavía) y preserva el conteo de población en
agregados.

### Expansión del scope de data: EUR agregado al seed FX

Al validar mart_tx_by_channel/category/dow contra fx_rates vía test de
relationships, dbt flaggeó 924 transactions denominadas en EUR (~1,8% del
volumen total). El scope original del seed era LATAM-only basado en la
consigna del proyecto, pero el dataset legítimamente incluye transactions
en EUR (probablemente clientes expat o internacionales).

**Fix:** EUR agregado a fx_rates.csv en 1.16 USD (mid-market, mayo 2026,
fuente Xe.com). Test `accepted_values` del seed actualizado en consecuencia.

**Lección:** el test `relationships` sobre currency fue efectivo — surfaceó
un gap real en el scope de data antes de llegar a los marts downstream.


### Hallazgo: data sintética muestra distribución anormal de DPD

`mart_loan_dpd` revela que el dataset source tiene un perfil de DPD inusual:
- Exactamente 50,5% de loans en DPD=0 ('Current')
- El ~49,5% restante se distribuye entre buckets de delinquency con un
  leve skew hacia duraciones más cortas (DPD promedio del subset
  delinquent ≈ 134 días, no exactamente uniforme).

En banca LATAM real, el patrón esperado es:
- 70-85% Current
- Decaimiento exponencial entre los buckets DPD (la mayoría de los
  pagadores tardíos se curan rápido)
- <2% en 180+ DPD (threshold de charge-off en la mayoría de jurisdicciones)

**Hipótesis:** el generador de data asigna DPD=0 a la mitad de la población
(aproximando loans performing) pero samplea el resto desde una distribución
biased-uniform en lugar de modelar dinámicas reales de delinquency. Esto
produce un portfolio que sería insolvente en realidad (~16-18% en 180+
DPD entre tipos de loan).

**Decisión:** mantener el bucketing alineado con la convención IFRS9/Basel
(decisions.md §3). Documentar la divergencia como un insight legible para
negocio en Power BI (Página 3: Risk & Credit). Las métricas se computan
correctamente; la distribución poco realista es una propiedad de la data
source sintética.

Patrón consistente con el hallazgo de flatness en revenue-by-segment
(Mart 1): el generador no correlaciona dimensiones financieras (DPD, monto
de loan, revenue) con segmentos de customer o tipos de loan de formas
realistas.

### Hallazgo: risk_score no correlaciona con delinquency observada

El output de `mart_risk_buckets` revela una observación crítica:

| risk_bucket | customer_count | delinquency_rate |
|-------------|----------------|------------------|
| low         | 1.495          | 65,3%            |
| medium      | 1.539          | 63,2%            |
| high        | 1.218          | 62,8%            |
| critical    |   748          | 60,4%            |

La relación es no-monotónica y contraintuitiva: customers clasificados
como "low risk" muestran una tasa de delinquency MÁS ALTA que los
clasificados como "critical". En un modelo de riesgo calibrado, el
gradiente debería ser el opuesto y abarcar decenas de puntos porcentuales
(ej. low: 2-5%, critical: 70%+).

**Interpretación:** el dataset sintético asigna `risk_score` independientemente
del comportamiento financiero subyacente del customer. Este es un hallazgo
más fuerte que la flatness previamente documentada en revenue y la
correlación utilization-delinquency: refuta directamente la validez
predictiva del campo `risk_score`.

Combinado con los otros hallazgos del Día 9 (revenue plano por segment,
correlación plana utilization-delinquency, distribución uniforme de DPD,
segmentación de riesgo casi plana), el patrón consistente es: el generador
de data source samplea las dimensiones relacionadas a riesgo financiero
independientemente en lugar de modelar sus correlaciones naturales. Esta
es una limitación conocida de generadores sintéticos que no implementan
distribuciones conjuntas.

**Decisión:** reportar los hallazgos honestamente en la página de Risk &
Credit de Power BI con narrativa explícita. Las métricas se computan
correctamente; la data exhibe patrones de independencia que no ocurrirían
en banca de producción.

Este es un insight del Día 9 que vale destacar en el README del proyecto
ya que demuestra el valor de la validación: un analista menos cuidadoso
habría entregado un dashboard de "risk_score" sin notar que no predice
nada.


### Hallazgo: las métricas de digital engagement no muestran gradiente demográfico

`mart_digital_adoption_by_segment` y `mart_channel_preference_by_age`
revelan que las métricas de digital engagement en el dataset son
independientes de la demografía del customer:

**Q20 — Adopción mobile por segment:**
- retail: 47,6%
- premium: 49,2%
- private_banking: 49,8%
- sme: 49,9%

Spread: 2,3 puntos porcentuales.

**Q21 — Preferencia de canal por edad:**
- Para cada age bucket (18-25, 26-35, 36-50, 51-65, 65+), los cinco
  canales (mobile/web/atm/branch/phone) capturan cada uno ~20% de los
  customers.
- Máximo spread dentro de cualquier age bucket es ~7 puntos porcentuales.

En banca del mundo real, ambas métricas muestran gradientes fuertes:
- Customers premium/private_banking (típicamente más viejos, mayor net
  worth) muestran MENOR adopción mobile que retail.
- Customers de 18-25 típicamente muestran 50%+ preferencia mobile;
  customers 65+ muestran 50%+ preferencia branch/phone. El dataset
  muestra ~20%/~20% para todos los grupos de edad en todos los canales.

La data sintética asigna los atributos de digital engagement uniformemente,
independiente de segment o edad. Combinado con los otros hallazgos del
Día 9 (revenue plano por segment, utilization-delinquency plana, risk_score
no predictivo de delinquency), esta es la quinta observación consistente
de que el generador samplea las dimensiones demográficas y conductuales
INDEPENDIENTEMENTE en lugar de modelar sus correlaciones naturales.

Este patrón es una limitación conocida de generadores sintéticos simples
que no implementan distribuciones conjuntas.

**Decisión:** los marts se computan correctamente. Los hallazgos se
reportan honestamente en el dashboard de Power BI con narrativa explícita,
lo cual demuestra rigor analítico (las métricas se producen, validan, y
contextualizan, en lugar de presentarse acríticamente).


### Hallazgo: las transferencias internacionales SÍ muestran patrones estructurados

Contrario a los otros hallazgos del Día 9 (revenue, DPD, utilization-delinquency,
risk_score, preferencia de canal — todos uniformemente distribuidos), el
mart de transferencia internacional revela estructura genuina en la data:

**Distribución bimodal de corredores de transferencia:**
- Corredores grandes (~900 tx cada uno): transferencias domésticas en
  currency local o en USD donde el currency de la cuenta coincide.
  Ejemplos: AR-ARS (956 tx, 0% intl), CO-USD (940 tx, 1,3% intl).
- Corredores chicos (~30 tx cada uno): transferencias exóticas raras,
  100% intl. Ejemplos: PE-EUR, MX-UYU, CL-MXN.

**Share internacional por país (rolled up):**
- PE: 9,2%, MX: 9,0%, AR: 8,3%, CO: 8,2%, CL: 7,9%, BR: 7,8%, UY: 7,8%
- Relativamente consistente entre países LATAM (spread 7,8-9,2%).

**Asimetría de valor por país:**
- UY muestra el VALOR de transferencia internacional más alto por país
  ($131.717 USD con solo 9 transacciones; ~$14.600 ticket promedio).
- Otros países LATAM: $30k-$70k totales, $4-5k ticket promedio.

**Interpretación:**
- El generador de data implementó algo de intención geográfica para
  transferencias (la mayoría son domésticas en currency local).
- El corredor de Uruguay de alto-valor/bajo-conteo es consistente con su
  rol real como hub financiero regional.
- La definición estricta de internacional (decisions.md Día 9 §8 — requiere
  que tx_currency difiera de AMBOS account_currency Y
  customer_country_currency) es crítica: una definición más permisiva
  habría clasificado todas las transactions USD desde países no-USD como
  internacionales, ocultando la señal real.

Este es el primer mart del Día 9 donde la data sintética muestra
estructura realista. Es reportable como hallazgo positivo en el dashboard.

## Intermediate models en silver (estado pre-Día 10)

Antes del Día 10 ya existían dos intermediate models en
`dbt/models/intermediate/`:

- `int_loan_portfolio_metrics` — consumed by `mart_loan_composition`,
  `mart_loan_dpd`. Centraliza derivaciones a nivel loan (`is_delinquent`,
  `is_default`, `monthly_interest_accrued`, dpd_bucket).
- `int_customer_risk_profile` — consumed by `mart_risk_buckets`,
  `mart_utilization_vs_delinquency`, `mart_delinquency_by_segment`.
  Centraliza derivaciones de risk a nivel customer (risk_bucket
  computado, observed_delinquency_flag, utilization_bucket lookup).

Día 10 agregó el tercero (`int_customer_monthly_revenue`) siguiendo el
mismo patrón — extraer la duplicación entre marts que comparten un grano
y una lógica de derivación. Ver sección "Refactor: int_customer_monthly_revenue"
para los detalles.

**Criterio general:** un `int_*` model se justifica cuando la misma lógica
no-trivial aparece en 2+ marts. Si solo lo usa un mart, vive como CTE
dentro del mart. Si lo usan 2+, sube a intermediate.

---

## Refactor: int_customer_monthly_revenue 

El bloque de la tarde del Día 10 extrajo el cómputo de revenue a nivel
customer grain hacia un modelo intermediate (`int_customer_monthly_revenue`)
consumido por ambos `mart_customer_360` y `mart_revenue_by_segment_usd`.
Antes del refactor, la misma lógica vivía (duplicada) en ambos marts con
dos diferencias cosméticas:

1. `mart_revenue_by_segment_usd` no redondeaba los valores por-customer; el
   redondeo solo sucedía en el rollup a nivel segment.
2. `mart_customer_360` redondeaba por-customer (es un mart de display) y
   usaba un path de JOIN más largo para atribución de fees
   (`fct_transactions → dim_account → customer_id`) donde el otro mart
   usaba `fct_transactions.customer_id` directamente.

Ambas diferencias se verificó que producen **valores de revenue a nivel
customer idénticos** en el dataset actual (ver `scripts/diagnose_fee_attribution.sql`
y `scripts/diagnose_revenue_mismatch.sql`). El refactor preserva semánticas
exactas: 0 / 5000 customers divergen en fee, interest, o total revenue
entre el SQL pre-refactor y el intermediate.

### Política de precisión adoptada

Cuando un modelo intermediate sirve a múltiples consumidores con patrones
de agregación diferentes, la precisión debe setearse por-columna, no
globalmente:

- **Columnas en moneda nativa** se redondean a 2 decimales a nivel
  customer grain. Son consumidas por `mart_customer_360` (display, sin
  agregación adicional adentro del mart).
- **Columnas en USD NO se redondean** a nivel customer grain. Son
  consumidas por `mart_revenue_by_segment_usd` vía `AVG(...)` sobre ~1.200
  customers por segment. Redondear antes del AVG introduciría pequeños
  deltas por segment (centavos propagándose a dólares a lo largo de
  cientos de customers).

**Lección:** redondear en el grain equivocado es un cambio semántico
silencioso — ningún test falla, pero los snapshots agregados drift-ean.
El bug solo surfacea al reconciliar numéricamente.

### Qué se centralizó

| Lógica | Ubicación pre-refactor | Ubicación post-refactor |
|--------|------------------------|-------------------------|
| Fee revenue por customer (native + USD) | Duplicado en ambos marts | `int_customer_monthly_revenue` |
| Interest revenue por customer (native + USD) | Duplicado en ambos marts | `int_customer_monthly_revenue` |
| Normalización `fees / tenure_months` | Duplicado, con NULLIF + COALESCE | `int_customer_monthly_revenue` |
| Manejo de zero-tenure (COALESCE a 0) | Duplicado | `int_customer_monthly_revenue` |
| Filtro de status de loans activos (current, delinquent) | Duplicado | `int_customer_monthly_revenue` |

`mart_customer_360` y `mart_revenue_by_segment_usd` ahora solo hacen su
propia lógica de agregación sobre el intermediate.

---

## Coverage map: business_questions.md (Día 10)

El documento `docs/business_questions.md` mapea cada una de las 24 preguntas
de la consigna a su query SQL contra el Gold layer, con muestra de resultado
real y bloque de "Key Findings" al final que destaca cuatro insights del
dataset:

1. **`risk_score` es anti-predictivo de delinquency** (low: 65,3%, critical:
   60,4% — no-monotónico, invertido).
2. **Patrón estructural de dolarización USD** (~50% en todos los países LATAM).
3. **Flatness en dimensiones financieras** (revenue/delinquency/failed_rate/
   digital_adoption casi uniformes — propiedad del generador sintético, no
   bug del pipeline).
4. **Honestidad de DQ:** el pipeline expone `accounts_with_balance` (Q2),
   bucket `unknown` (Q6) y test `accepted_values` en Q13, en vez de
   esconder problemas.

**Cobertura final:** 24/24 (23 ✅ + 1 ⚠️ Q13 con divergencia spec-dataset
documentada — el dataset usa `active/frozen/closed` a nivel account
mientras la spec dice `active/inactive/suspended/closed`).

Backed por `scripts/validate_business_questions.sql` y
`scripts/validation_output.txt` (snapshot reproducible — ver lección sobre
staleness más abajo).

---

## Lección: snapshot staleness vs equivalencia SQL (Día 10)

Al validar el refactor, el snapshot de la mañana en
`scripts/validation_output.txt` (sme 2890.38, premium 2676.87, retail 2635.99,
private_banking 2482.45) no matcheaba con el output post-refactor (sme 2897.17,
premium 2666.19, retail 2583.02, private_banking 2534.46).

El instinto fue asumir que el refactor había cambiado semánticas. **No lo
hizo.** Dos queries diagnósticas descartaron divergencia semántica:

1. `diagnose_fee_attribution.sql`: 0 de 3.613 fees `completed` tienen
   `t.customer_id != a.customer_id` (descartando drift de atribución de fees).
2. `diagnose_revenue_mismatch.sql`: re-implementó la lógica pre-refactor del
   mart inline y comparó customer por customer contra el intermediate;
   0 de 5.000 customers divergen en fee, interest, o total.

El snapshot en `validation_output.txt` se había vuelto stale entre la
corrida de la mañana (cuando se capturó el snapshot) y la validación del
refactor de la tarde. Alguna dependencia upstream — una re-corrida de
Silver, un re-load de Bronze, o un cambio de seed — produjo data
subyacente nueva sin re-generar el snapshot.

**Mejoras procedurales adoptadas:**

- **El acid test para equivalencia de refactor es comparación a nivel SQL,
  no comparación de snapshots.** Re-implementar la lógica pre-refactor
  inline y comparar row-by-row contra el nuevo intermediate es dispositivo
  de una forma en que las comparaciones de snapshot no lo son.
- **Los snapshots de validación deben regenerarse cuando cambien las
  dependencias upstream**, o tratarse como referencias aproximadas en lugar
  de contratos exactos. Las tablas sample-result de `business_questions.md`
  para Q1 se refrescaron al final del Día 10; los outputs de Q5 y Q9 se
  verificaron sin cambios entre snapshots (no requirieron refresh).
- **El runbook de validación debería incluir un paso de snapshot-generation**
  inmediatamente antes de empezar cualquier trabajo de refactor, así el
  snapshot refleja el estado pre-refactor del repo real, no un estado de
  más temprano en el día.


---

## PowerBI integration and downstream DQ findings

### Setup

PowerBI Desktop connected to PostgreSQL on localhost:5432 via native 
connector (Import mode, not DirectQuery). Only `gold.mart_*` tables 
imported; bronze and silver excluded from the .pbix.

No model relationships configured between marts. Each mart is pre-aggregated
in dbt with its own grain and self-contains the dimensional context. This
reflects a "wide marts" pattern intentionally chosen over star schema,
defensible because the agregaciones live in dbt (auditable, tested) rather
than in DAX (opaque, untested).

### Bug found via PowerBI integration: monthly_fee_revenue blowup

When validating the four base DAX measures on Page 1 KPI cards, 
`Avg Revenue per Customer USD` returned $1.03M/month/customer, which 
is implausible by 4+ orders of magnitude.

Diagnosis via SQL on `gold.mart_customer_360`:
- 721 customers (14.4%) have `total_revenue_monthly > $1M`
- max = $207,379,782 (CUST-0003449, sme, tenure_months=1)
- The top 10 outliers all share `tenure_months ≤ 3` OR have 
  abnormally high interest income.

**Root cause #1 — Fee revenue division-by-near-zero:**
The original formula `monthly_fee_revenue = total_fees_paid_lifetime / tenure_months`
correctly normalized lifetime fees to monthly scale (Day 9 fix), but
broke for customers with tenure 1-3 months. A customer who paid $200M in
fees during month 1 would project $200M/month, treating lifetime as
run-rate. This is a flaw in the formula, not in source data.

**Fix:** clamp denominator with `GREATEST(tenure_months, 3)`. Trade-off:
underestimates revenue for genuinely young + active customers, but
stabilizes the metric for the 38+71+74 = 183 customers (3.7%) with 
tenure ≤ 2. Applied in `mart_customer_360.sql` and `mart_revenue_by_segment_usd.sql`.

**Root cause #2 — Source data: extreme outliers in outstanding_balance:**
Independent of fee logic, `monthly_interest_income` showed customers with
$20-37M/month interest accrual. Investigation:
- `interest_rate_decimal`: validated, range [0.0301, 0.35], median 19.4%. 
  Day 9 fix is correct.
- `outstanding_balance` in `silver.fct_loans`: max $1,841,582,518.
  1,227 loans > $1M, 452 loans > $100M, all in COP currency for
  retail/SME customers labeled as auto loans.
- Example: CUST-0001189 has a COP $1,285M auto loan. The math is correct
  ($1.285B × 0.3469 / 12 = $37.16M COP interest/month).

This is a property of the **synthetic dataset**, not a pipeline bug. The
generator created retail loans with corporate-finance-scale balances,
inconsistent with the segments. This joins the growing list of generator
artifacts already documented (DPD distribution flat at 50%, risk_score
not correlated with delinquency, customer_segment not correlated with
revenue, etc.).

**Action:** instrumented as a `warn`-level test:
```yaml
- name: outstanding_balance
  tests:
    - dbt_utils.expression_is_true:
        expression: "< 100000000 or outstanding_balance is null"
        config:
          severity: warn
```
`dbt test` now produces a visible warning on every run, making this
DQ issue self-documenting in the build output.

Additionally, `mart_customer_360.total_revenue_monthly` got a `warn`-level
range test (>= 0 and < 1M). With the Day 11 fix, this is expected to 
warn on ~30-50 customers (down from 721), all attributable to interest
on the corporate-scale loans above. Confirms the fee bug is fully closed
and the residual is source-data, not formula.

### Dashboard implication: median over average

Given the long-tail residual from interest outliers, the Executive 
Overview KPI changed from `AVG(total_revenue_monthly)` to `MEDIAN`. 
The mediana is the statistically correct location measure for any 
right-skewed financial distribution (always true for revenue per customer
in real banking) and is robust to the residual synthetic outliers.

New DAX: 
Median Revenue per Customer USD =
MEDIANX(mart_customer_360, mart_customer_360[total_revenue_monthly])

Documented on the dashboard via a tooltip on the KPI card.

### Currency strategy in dashboard

[completar con lo que decidas: USD-only / global slicer / multi-leyenda]

### Process lesson

The bug had been latent since Day 9. dbt tests didn't catch it because:
- `expression_is_true: ">= 0"` passes for $206M (it IS >= 0).
- No magnitude / range / outlier test existed on revenue metrics.

The bug surfaced only when a downstream BI consumer rendered the number
to a human-readable card. **Lesson:** numeric metrics need magnitude 
tests, not just sign tests, especially when they're derived from divisions
or accruals. The new `warn`-level test on `total_revenue_monthly` is the
generalized fix.

### Post-fix diagnosis of residual outliers

After applying the tenure-clamp fix, 722 customers (down from 721) still
showed `total_revenue_monthly > $1M`. The count being nearly identical
suggested the fix had a different effect than naively reducing outlier
count — it instead reduced the **magnitude** of individual blowups
(CUST-0003449 fee_revenue went from $206M to $68.9M) without removing
them from the >$1M bucket.

Decomposition of the 722 outliers:
- 369 (51%) driven by interest_income alone (avg tenure 36.7 months,
  not affected by the clamp). Root cause: ~693 source loans with 
  outstanding_balance > $100M.
- 265 (37%) driven by fee_revenue alone (avg tenure 27.3 months,
  outside the clamp window). Root cause: source customers with 
  total_fees_paid_lifetime in the corporate-finance scale 
  (e.g., CUST-0000347: $348M lifetime fees over 27 months tenure).
- 73 (10%) with both components in the millions.
- 15 (2%) below $1M individually but above when summed.

The synthetic dataset thus contains two parallel "scale escapes" 
(loans + fees), both inconsistent with the implied retail/SME segments
of the affected customers. Both are now documented as warn-level dbt
tests, making them visible on every build without blocking the pipeline.

Conclusion: the Day 11 formula fix is complete. Residual outliers are
**source data, not pipeline behavior**. The median (not mean) is therefore
the statistically correct KPI for the dashboard, and the warn tests
serve as ongoing evidence of the source-data limitations.