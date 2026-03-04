## Cấu trúc thư mục
```
├── dag/
│   └── .py (Airflow)
├── dbt_ecommerce
│   ├── models
│   │   ├── bronze
│   │   ├── silver
│   │   └── gold
│   ├── seeds
│   ├── tests
│   └── profiles.yml
├── notebook
│   └── 01_ingest_bronze.ipynb
├── doc
│   └── SETUP.md
├── .gitignore
├── README.md
└── docker-compose.yaml
```
User need to create new file profiles.yml in folder dbt_ecommerce/ and fill in the information to connect to Databricks in Azure Databricks.

Format of profiles.yml:
```
brazilian_ecommerce:
  target: dev
  outputs:
    dev:
      type: databricks
      host: adb-7405619615010274.14.azuredatabricks.net
      http_path: /sql/1.0/warehouses/xxxxxxxxxxxxxxxx  # lấy từ SQL Warehouse
      token: dapixxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx       # Databricks access token
      catalog: hive_metastore
      schema: silver                                    # default schema dbt dùng
      threads: 4
```

## How to get http_path and token
### Token
```
Databricks
└── User Settings (icon góc trên phải)
    └── Developer
        └── Access Tokens
            └── Generate New Token
                ├── Comment: dbt-connection
                ├── Lifetime: 90 days
                └── Generate → Copy token ngay!
```

---

### Http_path
```
Databricks
└── SQL Warehouses
    └── Chọn warehouse
        └── Connection details
            └── HTTP Path  ← copy ở đây
```

---

## Important note
```
# .gitignore — thêm dòng này!
profiles.yml    ← không push lên GitHub!