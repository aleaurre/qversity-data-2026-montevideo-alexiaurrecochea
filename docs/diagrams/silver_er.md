```mermaid
erDiagram
    %% =========================================================================
    %% Silver layer — entity relationships
    %% =========================================================================
    %% Grain: every "dim_*" has 1 row per business entity, "fct_*" has 1 row
    %% per event/transaction, "agg_*" has 1 row per rollup key, seeds are
    %% lookup tables.
    %%
    %% FK relationships are enforced via dbt `relationships` tests defined in
    %% _silver__dim.yml / _silver__fct.yml.

    dim_customer {
        string customer_id PK
        string first_name
        string last_name
        string email
        string country
        string nationality
        string city
        date date_of_birth
        int age
        string age_bucket
        date registration_date
        int tenure_months
        string tenure_bucket
        string customer_segment
        string kyc_status
        string status
        numeric risk_score
        string relationship_manager
        float lat
        float lon
        boolean is_geo_valid
    }

    dim_account {
        string account_id PK
        string customer_id FK
        string account_type
        string status
        string currency
        date opened_date
        int account_age_months
        numeric balance
        numeric credit_limit
        numeric interest_rate
        string branch_code
    }

    dim_loan {
        string loan_id PK
        string customer_id FK
        string loan_type
        string status
        string currency
        numeric principal
        numeric outstanding_balance
        numeric interest_rate
        numeric interest_rate_decimal
        numeric monthly_payment
        int term_months
        int days_past_due
        date start_date
        date end_date
        string collateral_type
        int loan_age_months
    }

    dim_credit_info {
        string customer_id PK_FK
        int credit_score
        boolean is_credit_score_valid
        numeric utilization_pct
        boolean is_utilization_pct_valid
        numeric total_limit
        numeric total_used
        int num_credit_accounts
        int oldest_account_age_months
        int late_payments_12m
        int inquiries_6m
        boolean bankruptcy_flag
        string currency
    }

    dim_digital_engagement {
        string customer_id PK_FK
        boolean mobile_app_registered
        boolean web_banking_registered
        date last_login_date
        int avg_monthly_logins
        string preferred_channel
        boolean push_notifications
        boolean paperless_statements
    }

    fct_transactions {
        string transaction_id PK
        string customer_id FK
        string account_id FK
        date transaction_date
        numeric amount
        string currency
        string transaction_type
        string category
        string channel
        string status
        boolean is_failed
        int day_of_week
        string merchant
    }

    fct_loans {
        string loan_id PK_FK
        string customer_id FK
        string status
        numeric outstanding_balance
        numeric interest_rate_decimal
        int days_past_due
    }

    agg_customer_activity {
        string customer_id PK_FK
        int accounts_count
        int transactions_count
        int loans_count
        int total_products
    }

    dim_geography {
        string country PK
        string country_name
        string region
        string currency_local
    }

    dim_date {
        date date_day PK
        int year
        int quarter
        int month
        int day_of_week_num
        boolean is_weekend
        int year_month_key
    }

    fx_rates {
        string currency PK
        numeric rate_to_usd
        date as_of_date
    }

    country_currency {
        string country PK
        string currency_local
    }

    %% =========================================================================
    %% Relationships
    %% =========================================================================

    %% Customer is the root entity. Every customer-grain table FKs back to it.
    dim_customer ||--o{ dim_account              : "owns"
    dim_customer ||--o{ dim_loan                 : "borrows"
    dim_customer ||--|| dim_credit_info          : "has"
    dim_customer ||--|| dim_digital_engagement   : "has"
    dim_customer ||--|| agg_customer_activity    : "summarized by"
    dim_customer ||--o{ fct_transactions         : "executes"
    dim_customer ||--o{ fct_loans                : "holds"

    %% Account is the second key entity. Transactions reference both customer
    %% and account; account ownership cascades from customer.
    dim_account  ||--o{ fct_transactions         : "settles"

    %% Loan: dim_loan and fct_loans share the same loan_id PK. fct_loans
    %% exists as a separate fact table for status-changing analytics (DPD,
    %% delinquency, default) — the dim provides static attributes.
    dim_loan     ||--|| fct_loans                : "tracked by"

    %% Geographic lookup. dim_customer.country FKs to dim_geography.
    dim_geography ||--o{ dim_customer            : "located in"

    %% Currency lookups (seeds).
    fx_rates     ||--o{ dim_account              : "denominated in"
    fx_rates     ||--o{ dim_loan                 : "denominated in"
    fx_rates     ||--o{ fct_transactions         : "denominated in"

    country_currency ||--o{ dim_customer         : "default currency"

    %% Date lookup. Time-grain dimensions reference dim_date implicitly
    %% (not enforced as FK in Postgres but used in Gold marts for joins).
    dim_date     ||--o{ fct_transactions         : "occurs on"
    dim_date     ||--o{ dim_account              : "opened on"
    dim_date     ||--o{ dim_loan                 : "started on"
    dim_date     ||--o{ dim_customer             : "registered on"
```
