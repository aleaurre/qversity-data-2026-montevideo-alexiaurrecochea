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