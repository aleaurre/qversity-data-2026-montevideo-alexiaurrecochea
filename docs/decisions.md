# 01_eda

## Estrategia de canonicalización categórica

El dataset crudo tiene mismas categorías escritas en inglés/español y con
distintos casings. Se aplica un mapeo a un conjunto canónico único en Silver.

### customer_segment (canónico)
- retail        ← retail, Retail, RETAIL, minorista
- premium       ← premium, Premium, PREMIUM
- sme           ← sme, Sme, SME, pyme, PYME
- private_banking ← private_banking, Private_Banking, PRIVATE_BANKING, banca_privada

### status (canónico)
- active     ← active, Active, ACTIVE, activo, Activo, ACTIVO
- inactive   ← inactive, Inactive, INACTIVE, inactivo, Inactivo
- suspended  ← suspended, Suspended, SUSPENDED, suspendido
- closed     ← closed, Closed, CLOSED, cerrado, Cerrado

### kyc_status (canónico)
- pending, verified, expired, rejected (lowercase)

### gender (canónico)
- F, M, Other (casing exacto)
- "null", "N/A", "", "NA", "None" → NULL real

## Estrategia de fechas

`pd.to_datetime` con `errors="coerce"` falló porque hay formatos mixtos
(`MM-DD-YYYY` y `YYYY-MM-DD`). En PySpark/Silver se aplica parsing en cascada:
intentar formato ISO → formato US → marcar como inválido.

## Parsing de fechas (revisión)

Hipótesis confirmada por EDA: las columnas `registration_date` y `date_of_birth`
contienen fechas en MÚLTIPLES FORMATOS coexistiendo en el mismo campo
(típicamente ISO `YYYY-MM-DD` mezclado con US `MM-DD-YYYY`).

Estrategia en Silver (PySpark):
1. Intentar parse con `to_date(col, "yyyy-MM-dd")`.
2. Si falla, intentar `to_date(col, "MM-dd-yyyy")`.
3. Si falla, intentar `to_date(col, "dd/MM/yyyy")` (si aparece en EDA).
4. Si todo falla → NULL + flag `date_parse_failed`.

NO confiar en parsing automático de pandas/Spark con formatos mixtos.

## Formatos de fecha detectados

Tras EDA exhaustivo, registration_date y date_of_birth tienen 4 formatos
coexistiendo en el mismo campo (NO son nulls reales: el dataset tiene casi
0% de fechas faltantes una vez parseado correctamente).

| Formato        | registration_date | date_of_birth |
|----------------|-------------------|---------------|
| YYYY-MM-DD     | ~82%              | ~82%          |
| MM/DD/YYYY*    | ~6%               | ~6%           |
| MM-DD-YYYY*    | ~6%               | ~6%           |
| YYYYMMDD       | ~6%               | ~6%           |

*El orden MM/DD vs DD/MM se determina por análisis de rango (TBD según celda).

### Estrategia en PySpark

Aplicar parsing en cascada con `coalesce`:

```sql
COALESCE(
  TO_DATE(col, 'yyyy-MM-dd'),
  TO_DATE(col, 'MM-dd-yyyy'),       -- o dd-MM-yyyy según diagnóstico
  TO_DATE(col, 'MM/dd/yyyy'),       -- o dd/MM/yyyy según diagnóstico
  TO_DATE(col, 'yyyyMMdd')
)
```

Si todos los intentos fallan: NULL + agregar columna `date_parse_failed = TRUE`
para auditoría.

## Formatos de fecha en el dataset crudo

Tras EDA exhaustivo, registration_date y date_of_birth tienen 4 formatos
coexistiendo en el mismo campo. Crucialmente, el SEPARADOR DESAMBIGUA
totalmente el orden día/mes:

| Formato        | %     | Discriminador      |
|----------------|-------|--------------------|
| YYYY-MM-DD     | ~82%  | Tiene 4 dígitos al inicio |
| MM-DD-YYYY     | ~6%   | Separador `-`       |
| DD/MM/YYYY     | ~6%   | Separador `/`       |
| YYYYMMDD       | ~6%   | 8 dígitos sin separador |

→ 0% de fechas ambiguas o irrecuperables.

### Estrategia de parsing en PySpark (Silver)

Usar coalesce con orden de intentos del más restrictivo al más laxo:

```pythonF.coalesce(
F.to_date(col, "yyyy-MM-dd"),
F.to_date(col, "yyyyMMdd"),
F.to_date(col, "MM-dd-yyyy"),    # solo matchea con guión
F.to_date(col, "dd/MM/yyyy"),    # solo matchea con barra
)

Si todos fallan → NULL + flag `date_parse_failed = TRUE` para auditoría.Cierre del bloque de campos planos del customerResumen de hallazgos consolidados para Silver:AspectoHallazgoAcción en SilverSchema top-levelUniforme (24 keys, todos los records)Confiable como base.customer_id100 duplicados (50 pares) por casing distintoCanonicalizar primero, luego dedup.customer_segment16 variantes (es+en, casings)Mapeo a 4 valores canónicos.status20 variantesMapeo a 4 valores canónicos.kyc_status8 variantes (solo casings)lower().gender8 valores incluyendo nulls codificadosMapeo a F/M/Other/NULL.date_of_birth4 formatos, 0% nulls realesParsing en cascada.registration_date4 formatos, 0% nulls realesParsing en cascada.relationship_manager9.9% nulls realesImputar "UNASSIGNED".address7.8% nulls realesConservar NULL.risk_scoreFloat 0-100, sin outliers visiblesConfiable.

## Tratamiento de nulls reales

| Campo                  | % nulls | Decisión |
|------------------------|---------|----------|
| date_of_birth          | TBD*    | Conservar; campos derivados (age) quedan NULL. |
| registration_date      | TBD*    | Conservar; tenure queda NULL para esos casos. |
| relationship_manager   | 9.9%    | Imputar con "UNASSIGNED" en dim_customer. |
| address                | 7.8%    | Conservar NULL. No bloquea análisis. |
| gender                 | 5.4%    | Imputar con "Unknown" en dim_customer. |

*TBD: pendiente de confirmar tras parsing correcto en Silver.

## Deduplicación de customers

`customer_id` tiene 100 duplicados (50 pares), surgidos de variaciones de
casing en campos categóricos y emails.

Estrategia:
1. Canonicalizar todos los campos categóricos primero (lowercase).
2. Entre duplicados, quedarse con la fila que tenga MENOR cantidad de nulls.
3. Si empatan en nulls, ROW_NUMBER() OVER (PARTITION BY customer_id 
   ORDER BY registration_date DESC NULLS LAST).

## Tipos numéricos sucios

EDA detectó que columnas que deberían ser numéricas (balance, amount, 
principal, etc.) contienen valores guardados como strings. Esto rompe 
comparaciones directas y agregaciones.

Estrategia en Silver (PySpark):
- Cast explícito a DoubleType / IntegerType según el campo.
- Strings no parseables → NULL + columna de auditoría 
  (`<col>_parse_failed` boolean).
- dbt test: assert que % de fallos < umbral (ej. 1%).


## Estrategia de deduplicación (validada por EDA)

### Hallazgo
EDA confirmó que los duplicados en arrays nested (accounts, transactions, loans)
son 100% derivados de duplicados en customers. No existe duplicación independiente
en arrays. Cada PK duplicada se origina porque su customer_id padre aparece >1 vez.

### Estrategia
Una sola operación de dedup en customers cascadea limpieza a todas las arrays:

1. **Canonicalizar PRIMERO** todos los campos categóricos en customers (lowercase,
   español→inglés, trim). Esto es necesario porque los duplicados difieren en
   casing/idioma/whitespace.

2. **Dedup en customers** con criterio:
```sql
   ROW_NUMBER() OVER (
     PARTITION BY customer_id 
     ORDER BY 
       (cantidad de campos no-null) DESC,
       registration_date DESC NULLS LAST
   ) = 1
```
   El primer criterio (más campos no-null) prioriza el record más completo.
   El segundo desempata por reciente.

3. **Cascada a arrays:** después de quedarte con el customer "ganador", todos los
   arrays asociados (accounts, transactions, loans) se filtran haciendo INNER JOIN
   contra los customer_ids deduplicados. Los IDs duplicados desaparecen
   automáticamente porque su "fila perdedora" del customer ya no existe.

### Implementación en PySpark (Silver)
- Etapa 1 en PySpark: aplicar canonicalización + ROW_NUMBER en bronze →
  silver.stg_customers (sin duplicados).
- Etapa 2 en PySpark: explotar arrays haciendo JOIN con stg_customers (filtra duplicados).
- silver.stg_accounts, stg_transactions, stg_loans quedan sin duplicados de PK por construcción.

### Verificación en dbt
Tests unique en customer_id, account_id, transaction_id, loan_id deben pasar
después de la dedup en cascada.


# Decisiones de modelado y negocio
> Documento vivo. Actualizado tras EDA exhaustivo del dataset crudo.

## 1. Calidad de datos — hallazgos clave del EDA

### 1.1 Volumen
- **5100 records** de customer (consigna decía ~5000 ✓)
- 17,870 accounts | 89,470 transactions | 7,821 loans
- Schema top-level uniforme (24 keys idénticas en todos los records)

### 1.2 Inconsistencias categóricas masivas
Las categorías vienen sucias en 3 formas combinables:
- **Casing inconsistente** (`Retail` / `RETAIL` / `retail`)
- **Mezcla inglés/español** (`pyme` ↔ `sme`, `cerrado` ↔ `closed`, `reembolso` ↔ `refund`)
- **Whitespace** delante de valores (` credit_card`, `  investment`)

### 1.3 Nulls codificados como string
Los campos contienen `"null"`, `"N/A"`, `"NA"`, `"None"`, `""` que no son detectados por
parsers automáticos. Función `normalize_nulls()` los convierte a NULL real.

### 1.4 Fechas en 4 formatos coexistentes
Ningún null real — solo problemas de parsing. **Separador desambigua orden día/mes:**

| Formato      | %    | Discriminador           |
|--------------|------|-------------------------|
| YYYY-MM-DD   | 82%  | 4 dígitos al inicio     |
| MM-DD-YYYY   | 6%   | Separador `-` (US)      |
| DD/MM/YYYY   | 6%   | Separador `/` (latino)  |
| YYYYMMDD     | 6%   | 8 dígitos sin separador |

Aplica a: `registration_date`, `date_of_birth`, `accounts.opened_date`,
`transactions.date`, `loans.start_date`, `loans.end_date`, `digital_engagement.last_login_date`.

### 1.5 Numéricos como string
Algunos campos numéricos (~3%) vienen como strings con 4 sub-formatos:
- Limpio: `"424926.82"` → cast directo
- Latino: `"191286,14"` → reemplazar `,` por `.`
- Símbolo: `"$1810568162.59"` → quitar `$`
- Sufijo de moneda: `"404393.03 USD"` → quitar sufijo regex

Afecta: `accounts.balance`, `transactions.amount`, `loans.principal`,
`loans.outstanding_balance`, `loans.monthly_payment`.

### 1.6 Outliers numéricos extremos
Campos con valores aparentemente generados con escala equivocada (max ~$2 mil millones):
balance, principal, outstanding_balance, monthly_payment, total_limit, total_used.
- **NO descartar** — pueden ser legítimos en private_banking.
- Marcar con flag `is_outlier_<col>` por percentil 99.
- En Gold: KPIs de tendencia usan **mediana**, no media.

### 1.7 credit_score corrupto
**524 records (10.3%) tienen credit_score fuera del rango válido [300-850]**, con
valores como `-99` y `999999`. Probable artefacto de generación.
- En Silver: valores fuera de rango → NULL + flag `credit_score_invalid`.
- En Gold: las queries de credit_score filtran por valor válido.
- **Documentado en README como limitación que afecta business question #6.**

### 1.8 Booleans con valores mixtos
`bankruptcy_flag`, `push_notifications`, `paperless_statements` mezclan:
- bool reales (`True`/`False`)
- strings inglés (`"true"`/`"false"`)
- letras Y/N
- español (`"si"`)

`mobile_app_registered` y `web_banking_registered` son bool puros (sin sucesión).

---

## 2. Estrategia de canonicalización categórica

### 2.1 customer_segment → 4 valores
| Canónico          | Variantes a mapear |
|-------------------|--------------------|
| `retail`          | retail, Retail, RETAIL, minorista |
| `premium`         | premium, Premium, PREMIUM |
| `sme`             | sme, Sme, SME, pyme, PYME |
| `private_banking` | private_banking, Private_Banking, PRIVATE_BANKING, banca_privada |

### 2.2 customer.status → 4 valores
| Canónico    | Variantes |
|-------------|-----------|
| `active`    | active, Active, ACTIVE, activo, Activo, ACTIVO |
| `inactive`  | inactive, Inactive, INACTIVE, inactivo, Inactivo |
| `suspended` | suspended, Suspended, SUSPENDED, suspendido |
| `closed`    | closed, Closed, CLOSED, cerrado, Cerrado |

### 2.3 accounts.status → 3 valores
`active` | `frozen` | `closed` (incluye español: `congelado` → frozen, `cerrado` → closed).

### 2.4 transactions.type → 6 valores
`deposit` | `withdrawal` | `transfer` | `payment` | `refund` | `fee`
Variantes en español: `deposito`, `retiro`, `transferencia`, `pago`, `reembolso`, `comision`.

### 2.5 transactions.status → 4 valores
`pending` | `completed` | `failed` | `reversed` (solo casings, sin español).

### 2.6 loans.type, loans.status, kyc_status, gender
Solo casings → `lower()` + `trim()` resuelve todo. Para `gender`, mapear a `F`/`M`/`Other`/NULL.

### 2.7 Whitespace en account_type y transactions.category
Aplicar `trim()` antes de cualquier comparación.

### 2.8 Booleans
```sql
CASE
  WHEN LOWER(TRIM(col)) IN ('true','t','y','yes','si','1') THEN TRUE
  WHEN LOWER(TRIM(col)) IN ('false','f','n','no','0') THEN FALSE
  ELSE NULL
END
```

---

## 3. Estrategia de fechas (PySpark)

```python
F.coalesce(
    F.to_date(col, "yyyy-MM-dd"),
    F.to_date(col, "yyyyMMdd"),
    F.to_date(col, "MM-dd-yyyy"),    # solo matchea con guión
    F.to_date(col, "dd/MM/yyyy"),    # solo matchea con barra
)
```
Si todos fallan → NULL + flag `<col>_parse_failed`.

---

## 4. Estrategia de numéricos (PySpark)

```python
def parse_money(col):
    cleaned = F.regexp_replace(col.cast("string"), r"\s+(USD|EUR|ARS|BRL|CLP|COP|MXN|PEN|UYU)$", "")
    cleaned = F.regexp_replace(cleaned, r"[\$€£]", "")
    cleaned = F.when(
        cleaned.rlike(r"^\d{1,3}(\.\d{3})+,\d+$"),  # formato latino con miles
        F.regexp_replace(F.regexp_replace(cleaned, r"\.", ""), r",", ".")
    ).otherwise(
        F.regexp_replace(cleaned, r",", ".")
    )
    return cleaned.cast("double")
```

---

## 5. Deduplicación

### 5.1 Hallazgo (validado)
Los duplicados en arrays nested son **100% derivados** de duplicados en customers.
No existe duplicación independiente. Cada PK duplicada se origina porque su
`customer_id` padre aparece >1 vez (98 customers × 2 + 1 customer × 3 = 100 dups).

### 5.2 Estrategia (1 sola operación cascadea limpieza)
1. Canonicalizar PRIMERO todos los campos categóricos en customers.
2. Dedup en customers:
```sql
   ROW_NUMBER() OVER (
     PARTITION BY customer_id
     ORDER BY (count of non-null fields) DESC, registration_date DESC NULLS LAST
   ) = 1
```
3. Cascada a arrays: INNER JOIN con stg_customers deduplicado.

### 5.3 Verificación
dbt tests `unique` en customer_id, account_id, transaction_id, loan_id deben pasar
después de la dedup en cascada.

---

## 6. Tratamiento de nulls

| Campo | % nulls reales | Decisión |
|-------|----------------|----------|
| relationship_manager | 9.9% | Imputar `'UNASSIGNED'` en dim_customer |
| address | 7.8% | Conservar NULL |
| gender | 5.4% | Imputar `'Unknown'` |
| accounts.credit_limit | 74.6% | **Legítimo:** solo credit_card lo tiene |
| transactions.description | 10.2% | Aceptable |
| transactions.merchant | 8.3% | Aceptable |
| transactions.category | 4.9% | Conservar NULL |
| loans.collateral_type | 47.7% | **Legítimo:** unsecured loans → mapear a `'unsecured'` en Gold |
| digital_engagement.avg_monthly_logins | 7.4% | Validar hipótesis (¿coincide con no-registrados?) → imputar 0 si sí |

---

## 7. Definiciones de métricas de negocio

### Revenue
> Revenue = suma de `fees` + `interest_income` calculado proporcional.
> - Fees: `SUM(amount) WHERE type='fee'` por customer/segment/mes.
> - Interest income: `(loans.outstanding_balance * loans.interest_rate / 12)` por loan activo.

### Delinquency
> Loan está delinquent si `days_past_due > 30` OR `status IN ('delinquent', 'default')`.
> Delinquency rate = loans delinquent / loans totales (excluyendo paid_off).

### Buckets

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

### Tenure
> Tenure (años) = (today - registration_date) / 365.25

---

## 8. Casos especiales documentados

### 8.1 EUR en transactions
940 transacciones (1%) en EUR a pesar de ser un dataset LATAM. **Conservar** —
sirven para responder business question #19 (international transfers).

### 8.2 `phone` en preferred_channel
`digital_engagement.preferred_channel` tiene valores `mobile/atm/branch/phone/web`,
mientras que `transactions.channel` tiene `mobile/atm/branch/pos/web`. Universos
distintos: NO crear `dim_channel` única.

### 8.3 collateral_type = "None" (string)
~48% de loans no tienen colateral. El string `"None"` se mapea a NULL real en Silver
y se traduce como `'unsecured'` en Gold para queries de portfolio.

---

## 9. Versiones e infraestructura

### Imagen base de Airflow: 2.10.5
La consigna pide "Apache Airflow 2.7+". Elegimos 2.10.5 (última 2.x estable) por
seguridad — la imagen 2.7.3 acumula CVEs críticos por antigüedad.

### Usuario en runtime
Dockerfile usa `USER root` solo para apt-get y descarga de driver JDBC. El contenedor
en runtime corre como `airflow` (UID 50000), siguiendo recomendación oficial.

### PySpark en local mode
Spark corre en `local[*]` dentro del contenedor de Airflow (no cluster externo).
Suficiente para 5k records. Trade-off documentado: no escalaría a millones de filas.