from pandas.core.indexes.multi import sparsify_labels

from src.common.spark_utils import local_get_spark


if __name__ == "__main__":
    spark = local_get_spark()
    # Create a local database (schema)
    #spark.sql("SHOW TABLES IN raw").show()
    spark.sql("""
            CREATE DATABASE IF NOT EXISTS raw
            LOCATION 'file:///C:/fraud_detection_project/raw/'
        """)

    ddl = """
            CREATE TABLE IF NOT EXISTS raw.transactions (
                transaction_id     STRING,
                user_id            STRING,
                card_id            STRING,
                merchant_id        STRING,
                amount             STRING,
                `timestamp`        STRING,
                location           STRING,
                device_id          STRING,
                ip_address         STRING,
                channel            STRING,
                session_id         STRING,
                is_chargeback      BOOLEAN,
                date               DATE,
                kafka_topic        STRING,
                kafka_partition    INT,
                kafka_offset       BIGINT,
                ingested_ts        TIMESTAMP,
                schema_version     STRING,
                raw_payload        STRING,
                unparsed_fields    MAP<STRING, STRING>
            )
            USING DELTA
            PARTITIONED BY (date)
            LOCATION 'file:///C:/fraud_detection_project/raw/transactions/'
        """

    ddl_2 = """
            CREATE TABLE IF NOT EXISTS raw.login_events (
               -- Expected Fields (nullable for schema evolution safety)
              login_id     STRING,
              user_id            STRING ,
              `timestamp`        STRING ,
              device_id          STRING ,
              ip_address         STRING ,
              geo_location       STRING ,
              session_id         STRING ,
              date                DATE ,
            
              -- Kafka Metadata
              kafka_topic        STRING ,
              kafka_partition    INT ,
              kafka_offset       BIGINT ,
            
              -- Ingestion Metadata
              ingested_ts        TIMESTAMP ,
              schema_version     STRING ,
            
              -- Schema Evolution / Debugging
              raw_payload        STRING ,
              unparsed_fields    MAP<STRING, STRING> 
            )
            USING DELTA 
            PARTITIONED BY (date)
            LOCATION 'file:///C:/fraud_detection_project/raw/login_events/'
    """
    spark.sql(ddl)
    spark.sql(ddl_2)

    customer_ddl = """
        CREATE TABLE IF NOT EXISTS raw.customers ( 
          -- Business Entity fields 
          customer_id STRING,
          name        STRING COMMENT 'Customer name',
          email       STRING COMMENT 'Customer email address',
          phone       STRING COMMENT 'Customer phone number',
          dob         STRING COMMENT 'Customer date of birth',
          kyc_status  STRING COMMENT 'Customer KYC Status',
          registration_date STRING COMMENT 'Customer registration date',
        
          -- ✅ Ingestion Metadata
          source_db          STRING COMMENT 'Name of the source database (e.g. default)',
          source_schema       STRING COMMENT 'Name of the source schema (e.g. default)',
          source_table       STRING COMMENT 'Name of the source table (e.g. users)',
          schema_version     STRING COMMENT 'Schema version during ingestion',
          ingestion_ts       TIMESTAMP COMMENT 'When this record was ingested into the lake',
          date               DATE COMMENT 'Extracted Date for partitioning from ingested timestamp column',
          last_modified_at   TIMESTAMP COMMENT 'When this record was last modified in the source', 
        
          -- Fallback and Schema Evolution 
           raw_payload        STRING COMMENT 'Serialized JSON version of the full record from JDBC', 
           unparsed_fields    MAP<STRING, STRING> COMMENT 'Map of unparsed or unexpected fields for Alerting and Schema Evolution'
        
        )
        USING DELTA
        PARTITIONED BY (date)
        LOCATION 'file:///C:/fraud_detection_project/raw/customers/'
    """

    users_ddl = """
        CREATE TABLE IF NOT EXISTS raw.users ( 
          -- Business Entity fields 
          user_id STRING COMMENT 'Unique identifier for the user',
          customer_id STRING COMMENT 'Unique identifier for the customer',
          name        STRING COMMENT 'Customer name',
          email       STRING COMMENT 'Customer email address',
          phone       STRING COMMENT 'Customer phone number',
          dob         STRING COMMENT 'Customer date of birth',
          kyc_status  STRING COMMENT 'Customer KYC Status',
          registration_date STRING COMMENT 'Customer registration date',
        
          -- ✅ Ingestion Metadata
          source_db          STRING COMMENT 'Name of the source database (e.g. default)',
          source_schema       STRING COMMENT 'Name of the source schema (e.g. default)',
          source_table       STRING COMMENT 'Name of the source table (e.g. users)',
          schema_version     STRING COMMENT 'Schema version during ingestion',
          ingestion_ts       TIMESTAMP COMMENT 'When this record was ingested into the lake',
          date               DATE COMMENT 'Extracted Date for partitioning from ingested timestamp column',
          last_modified_at   TIMESTAMP COMMENT 'When this record was last modified in the source', 
        
          -- Fallback and Schema Evolution 
           raw_payload        STRING COMMENT 'Serialized JSON version of the full record from JDBC', 
           unparsed_fields    MAP<STRING, STRING> COMMENT 'Map of unparsed or unexpected fields for Alerting and Schema Evolution'
        
        )
        USING DELTA
        PARTITIONED BY (date)
        LOCATION 'file:///C:/fraud_detection_project/raw/users/'

    """

    credit_cards_ddl = """
        CREATE TABLE IF NOT EXISTS raw.credit_cards ( 
          -- Business Entity fields 
          card_id STRING COMMENT 'Unique identifier for the card',
          credit_card_number  STRING COMMENT 'Card number',
          customer_id STRING COMMENT 'Unique identifier for the customer',
          status STRING COMMENT 'Card status',
          issued_on  STRING COMMENT 'Card issue date',
          card_type STRING COMMENT 'Card type',
          limit     STRING COMMENT 'Card limit',
        
          -- ✅ Ingestion Metadata
          source_db          STRING COMMENT 'Name of the source database (e.g. default)',
          source_schema       STRING COMMENT 'Name of the source schema (e.g. default)',
          source_table       STRING COMMENT 'Name of the source table (e.g. users)',
          schema_version     STRING COMMENT 'Schema version during ingestion',
          ingestion_ts       TIMESTAMP COMMENT 'When this record was ingested into the lake',
          date               DATE COMMENT 'Extracted Date for partitioning from ingested timestamp column',
          last_modified_at   TIMESTAMP COMMENT 'When this record was last modified in the source', 
        
          -- Fallback and Schema Evolution 
           raw_payload        STRING COMMENT 'Serialized JSON version of the full record from JDBC', 
           unparsed_fields    MAP<STRING, STRING> COMMENT 'Map of unparsed or unexpected fields for Alerting and Schema Evolution'
        
        )
        USING DELTA
        PARTITIONED BY (date)
        LOCATION 'file:///C:/fraud_detection_project/raw/credit_cards/'
    """

    credit_card_map_ddl = """
        CREATE TABLE IF NOT EXISTS raw.credit_card_user_map ( 
          -- Business Entity fields 
          card_id STRING COMMENT 'Foreign key to the card table',
          user_id STRING COMMENT 'Foreign key to the user table',
          role STRING COMMENT 'Role of the user on the card',
          status STRING COMMENT 'Card status',
          linked_on  STRING COMMENT 'Card issue date',
        
          -- ✅ Ingestion Metadata
          source_db          STRING COMMENT 'Name of the source database (e.g. default)',
          source_schema       STRING COMMENT 'Name of the source schema (e.g. default)',
          source_table       STRING COMMENT 'Name of the source table (e.g. users)',
          schema_version     STRING COMMENT 'Schema version during ingestion',
          ingestion_ts       TIMESTAMP COMMENT 'When this record was ingested into the lake',
          date               DATE COMMENT 'Extracted Date for partitioning from ingested timestamp column',
          last_modified_at   TIMESTAMP COMMENT 'When this record was last modified in the source', 
        
          -- Fallback and Schema Evolution 
           raw_payload        STRING COMMENT 'Serialized JSON version of the full record from JDBC', 
           unparsed_fields    MAP<STRING, STRING> COMMENT 'Map of unparsed or unexpected fields for Alerting and Schema Evolution'
        
        )
        USING DELTA
        PARTITIONED BY (date)
        LOCATION 'file:///C:/fraud_detection_project/raw/credit_card_user_map/'
    """

    merchants_ddl = """
        CREATE TABLE IF NOT EXISTS raw.merchants ( 
          -- Business Entity fields 
          merchant_id STRING COMMENT 'Unique identifier for the merchant',
          name STRING COMMENT 'Merchant name',
          risk_score STRING COMMENT 'Merchant risk score',
        
          -- ✅ Ingestion Metadata
          source_db          STRING COMMENT 'Name of the source database (e.g. default)',
          source_schema       STRING COMMENT 'Name of the source schema (e.g. default)',
          source_table       STRING COMMENT 'Name of the source table (e.g. users)',
          schema_version     STRING COMMENT 'Schema version during ingestion',
          ingestion_ts       TIMESTAMP COMMENT 'When this record was ingested into the lake',
          date               DATE COMMENT 'Extracted Date for partitioning from ingested timestamp column',
          last_modified_at   TIMESTAMP COMMENT 'When this record was last modified in the source', 
        
          -- Fallback and Schema Evolution 
           raw_payload        STRING COMMENT 'Serialized JSON version of the full record from JDBC', 
           unparsed_fields    MAP<STRING, STRING> COMMENT 'Map of unparsed or unexpected fields for Alerting and Schema Evolution'
        
        )
        USING DELTA
        PARTITIONED BY (date)
        LOCATION 'file:///C:/fraud_detection_project/raw/merchants/'

    """
    devices_ddl = """
        CREATE TABLE IF NOT EXISTS raw.devices ( 
          -- Business Entity fields 
          device_id STRING COMMENT 'Unique identifier for the device',
          user_id  STRING COMMENT 'Foreign Key to the User table',
          first_seen_at STRING COMMENT 'First time the device was seen',
          device_type STRING COMMENT 'Device type',
          os STRING COMMENT 'Device OS',
          is_verified STRING COMMENT 'Device verification status',
          
          -- ✅ Ingestion Metadata
          source_db          STRING COMMENT 'Name of the source database (e.g. default)',
          source_schema       STRING COMMENT 'Name of the source schema (e.g. default)',
          source_table       STRING COMMENT 'Name of the source table (e.g. users)',
          schema_version     STRING COMMENT 'Schema version during ingestion',
          ingestion_ts       TIMESTAMP COMMENT 'When this record was ingested into the lake',
          date               DATE COMMENT 'Extracted Date for partitioning from ingested timestamp column',
          last_modified_at   TIMESTAMP COMMENT 'When this record was last modified in the source', 
        
          -- Fallback and Schema Evolution 
           raw_payload        STRING COMMENT 'Serialized JSON version of the full record from JDBC', 
           unparsed_fields    MAP<STRING, STRING> COMMENT 'Map of unparsed or unexpected fields for Alerting and Schema Evolution'
        
        )
        USING DELTA
        PARTITIONED BY (date)
        LOCATION 'file:///C:/fraud_detection_project/raw/devices/'
    """
    spark.sql(customer_ddl)
    spark.sql(users_ddl)
    spark.sql(credit_cards_ddl)
    spark.sql(credit_card_map_ddl)
    spark.sql(devices_ddl)
    spark.sql(merchants_ddl)

    spark.sql("""
                CREATE DATABASE IF NOT EXISTS silver
                LOCATION 'file:///C:/fraud_detection_project/silver/'
            """)
    silver_ddl_customers = """
        CREATE TABLE IF NOT EXISTS silver.customers (
          customer_sk BIGINT , -- Surrogate Key
        
          customer_id STRING,            -- Business key
          full_name STRING,              -- Normalized name
          email_masked STRING,           -- Masked for PII compliance
          phone_masked STRING,           -- Masked for PII compliance
          dob DATE,                      -- Date of birth
          age INT,                       -- Derived age
          kyc_status STRING,             -- Verification/KYC level
          registration_date DATE,        -- Date user registered
        
          -- SCD Type 2 Fields
          record_hash STRING,            -- For change detection
          effective_from TIMESTAMP,      -- SCD2: valid from
          effective_to TIMESTAMP,        -- SCD2: valid to
          current_flag BOOLEAN          -- SCD2: is current
          
        )
        USING DELTA
        COMMENT 'Silver dimension table for customers'
        LOCATION 'file:///C:/fraud_detection_project/silver/customers/';
    """

    silver_ddl_users = """
        CREATE TABLE IF NOT EXISTS silver.users (
          user_sk BIGINT , -- Surrogate Key
        
          user_id STRING NOT NULL,       -- Natural/business key
          customer_id STRING,            -- Associated customer
        
          full_name STRING,              -- Normalized name
          email_masked STRING,           -- Masked for PII compliance
          phone_masked STRING,           -- Masked for PII compliance
          dob DATE,                      -- Date of birth
          age INT,                       -- Derived age
          kyc_status STRING,             -- Verification/KYC level
          registration_date DATE,        -- Date user registered
        
          -- SCD Type 2 Fields
          record_hash STRING,            -- For change detection
          effective_from TIMESTAMP,      -- SCD2: valid from
          effective_to TIMESTAMP,        -- SCD2: valid to
          current_flag BOOLEAN          -- SCD2: is current
          
        )
        USING DELTA
        COMMENT 'Silver dimension table for users'
        LOCATION 'file:///C:/fraud_detection_project/silver/users/';

    """

    silver_ddl_credit_cards = """
        CREATE TABLE IF NOT EXISTS silver.credit_cards (
          card_sk BIGINT , -- Surrogate key
        
          card_id STRING NOT NULL,       -- Natural key from issuer system
          customer_id STRING,                -- Linked user (natural FK)
        
          card_last4 STRING,            -- Last 4 digits only (PII-masked)
          status STRING COMMENT 'Card status',
          issued_on DATE,
          limit NUMERIC,
        
          -- SCD Type 2 Fields
          record_hash STRING,
          effective_from TIMESTAMP,
          effective_to TIMESTAMP,
          current_flag BOOLEAN
        )
        USING DELTA
        COMMENT 'Silver dimension table for credit cards'
        LOCATION 'file:///C:/fraud_detection_project/silver/credit_cards/';
    """

    silver_ddl_credit_card_map = """
        CREATE TABLE IF NOT EXISTS silver.credit_card_user_map (
          card_sk BIGINT , -- Surrogate key
        
          card_id STRING NOT NULL,       -- Natural key from issuer system
          user_id STRING,                -- Linked user (natural FK)
        
          role STRING,            
          status STRING COMMENT 'Card status',
          linked_on DATE,
        
          -- SCD Type 2 Fields
          record_hash STRING,
          effective_from TIMESTAMP,
          effective_to TIMESTAMP,
          current_flag BOOLEAN
        )
        USING DELTA
        COMMENT 'Silver dimension table for mapping cards to users'
        LOCATION 'file:///C:/fraud_detection_project/silver/credit_card_user_map/';
    """
    drop_silver_devices = "DROP TABLE IF EXISTS silver.devices;"
    silver_ddl_devices = """
        CREATE TABLE IF NOT EXISTS silver.devices (
          device_sk BIGINT , -- Surrogate key
        
          device_id STRING NOT NULL,     -- Device identifier
          user_id STRING,                -- Associated user ID (natural key)
          first_seen_at TIMESTAMP,
          device_type STRING,            -- Mobile, Desktop, etc.
          os  STRING,                     -- OS version cleaned
          is_verified BOOLEAN,
        
          -- SCD Type 2 Fields
          record_hash STRING,
          effective_from TIMESTAMP,
          effective_to TIMESTAMP,
          current_flag BOOLEAN
        )
        USING DELTA
        COMMENT 'Silver dimension table for user devices'
        LOCATION 'file:///C:/fraud_detection_project/silver/devices/';
    """

    silver_ddl_merchants = """
        CREATE TABLE IF NOT EXISTS silver.merchants (
          merchant_sk BIGINT , -- Surrogate key
        
          merchant_id STRING NOT NULL,    -- Merchant business ID
          merchant_name STRING,           -- Cleaned name
          merchant_risk_tier STRING,      -- Optional risk rating (enriched)
        
          -- SCD Type 2 Fields
          record_hash STRING,
          effective_from TIMESTAMP,
          effective_to TIMESTAMP,
          current_flag BOOLEAN
        )
        USING DELTA
        COMMENT 'Silver dimension table for merchants'
        LOCATION 'file:///C:/fraud_detection_project/silver/merchants/'
    """

    spark.sql(silver_ddl_customers)
    spark.sql(silver_ddl_users)
    spark.sql(silver_ddl_credit_cards)
    spark.sql(silver_ddl_credit_card_map)
    #spark.sql(silver_ddl_devices)
    spark.sql(silver_ddl_merchants)

    drop_silver_logins = "DROP TABLE IF EXISTS silver.login_events;"
    silver_logins = """
        CREATE TABLE IF NOT EXISTS silver.login_events (
          login_id STRING,
          user_id STRING,
          device_id STRING,
          ip_address STRING,
          ip_country STRING,
          geo_location STRING,
          session_id STRING,
          timestamp TIMESTAMP,
          device_first_seen_at TIMESTAMP,
          device_is_verified BOOLEAN,
          user_name STRING,
          user_email_masked STRING,
          user_registration_date DATE
        )
        USING DELTA
        COMMENT 'Silver fact table for login_events'
        LOCATION 'file:///C:/fraud_detection_project/silver/login_events/';


    """

    drop_silver_transactions = "DROP TABLE IF EXISTS silver.transactions;"
    silver_transactions = """
        CREATE TABLE IF NOT EXISTS silver.transactions (
          transaction_id STRING,
          user_id STRING,
          card_id STRING,
          merchant_id STRING,
          amount DOUBLE,
          timestamp TIMESTAMP,
          location STRING,
          device_id STRING,
          ip_address STRING,
          ip_country STRING,
          channel STRING,
          session_id STRING,
          is_chargeback BOOLEAN,
          user_email_masked STRING,
          user_registration_date DATE,
          card_status STRING,
          card_limit NUMERIC,
          merchant_name STRING,
          merchant_risk_score INT,
        
          fraud_flag BOOLEAN,
          rule_triggered STRING
        )
        USING DELTA
        COMMENT 'Silver FACT table for TRANSACTIONS'
        LOCATION 'file:///C:/fraud_detection_project/silver/transactions';

    """
    drop_alert_transactions = "DROP TABLE IF EXISTS silver.alerts;"
    alert_transactions = """
        CREATE TABLE IF NOT EXISTS silver.alerts (
          transaction_id STRING,
          user_id STRING,
          card_id STRING,
          amount DOUBLE,
          timestamp TIMESTAMP,
          ip_country STRING,
          channel STRING,
          user_email_masked STRING,
          merchant_name STRING,
          device_id STRING,
          ip_address STRING,
          session_id STRING,

          fraud_flag BOOLEAN,
          rule_triggered STRING
        )
        USING DELTA
        COMMENT 'Silver ALERT table for FRAUDULENT TRANSACTIONS'
        LOCATION 'file:///C:/fraud_detection_project/silver/alerts';
    """

    #spark.sql(drop_silver_logins)
    #spark.sql(drop_silver_transactions)
    #spark.sql(drop_alert_transactions)
    #spark.sql(silver_logins)
    #spark.sql(silver_transactions)
    #spark.sql(alert_transactions)
    """spark.sql(drop_silver_devices)
    spark.sql(silver_ddl_devices)"""

    drop_database = ("""
                    DROP DATABASE IF EXISTS gold;
                """)
    spark.sql("""
                CREATE DATABASE IF NOT EXISTS gold
                LOCATION 'file:///C:/fraud_detection_project/gold/'
            """)

    drop_gold_user_fraud_summary = "DROP TABLE IF EXISTS gold.user_fraud_summary"
    drop_gold_fraud_transactions_summary = "DROP TABLE IF EXISTS gold.fraud_transactions_summary"
    drop_gold_fraud_transaction_summary = "DROP TABLE IF EXISTS gold.fraud_transaction_summary"

    drop_gold_transactions_enriched = "DROP TABLE IF EXISTS gold.transactions_enriched"
    gold_user_fraud_summary = """
        CREATE TABLE IF NOT EXISTS gold.user_fraud_summary (
            customer_id STRING,
            account_owner_name STRING, 
            user_id STRING,
            user_name STRING,
            age INT,
            registration_date DATE,
            card_id STRING,
            card_limit INT,
            
            total_transactions BIGINT,
            total_amount_spent DOUBLE,
            avg_transaction_amount DOUBLE,
            fraud_transaction_count BIGINT,
            last_transaction_ts TIMESTAMP
        )
        USING DELTA
        LOCATION 'file:///C:/fraud_detection_project/gold/user_fraud_summary';
    """

    gold_fraud_transactions_summary = """
        CREATE TABLE IF NOT EXISTS gold.fraud_transactions_summary (
            transaction_id STRING,
            user_id STRING,
            merchant_name STRING,
            card_id STRING,
            amount DOUBLE,
            timestamp TIMESTAMP,
            card_limit INT,
            is_chargeback BOOLEAN,
            country STRING,
            rule_triggered STRING
        )
        USING DELTA
        LOCATION 'file:///C:/fraud_detection_project/gold/fraud_transactions_summary';
    """

    gold_transaction_enriched = """
        CREATE TABLE IF NOT EXISTS gold.transactions_enriched (
            transaction_id STRING,
            user_id STRING,
            user_name STRING,
            account_created_at TIMESTAMP,
            card_id STRING,
            card_type STRING,
            device_id STRING,
            device_type STRING,
            location_id STRING,
            location_country STRING,
            amount DOUBLE,
            timestamp TIMESTAMP,
            fraud_score DOUBLE,
            fraud_type STRING,
            is_fraud BOOLEAN,
            created_at TIMESTAMP
        )
        USING DELTA
        LOCATION 'file:///C:/fraud_detection_project/gold/transactions_enriched';
    """

    spark.sql(drop_gold_user_fraud_summary)
    spark.sql(drop_gold_transactions_enriched)
    spark.sql(drop_gold_fraud_transactions_summary)
    spark.sql(drop_gold_fraud_transaction_summary)

    spark.sql("SHOW TABLES IN gold;").show()
    spark.sql(drop_database)
    spark.sql("""
                    CREATE DATABASE IF NOT EXISTS gold
                    LOCATION 'file:///C:/fraud_detection_project/gold/'
                """)
    spark.sql(gold_user_fraud_summary)
    spark.sql(gold_fraud_transactions_summary)
    spark.sql(gold_transaction_enriched)

    spark.sql("SHOW TABLES IN gold;").show()

    spark.sql("DESCRIBE TABLE silver.users").show()
    spark.sql("DESCRIBE TABLE silver.transactions").show()
    spark.sql("DESCRIBE TABLE gold.user_fraud_summary").show()
    spark.sql("DESCRIBE TABLE gold.fraud_transactions_summary").show()