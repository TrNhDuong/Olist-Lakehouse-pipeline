# Hướng dẫn Setup Databricks + ADLS2

## Tổng quan flow

```
Azure Key Vault (lưu secret)
        │
        ▼
Databricks Storage Credential (dùng Access Connector)
        │
        ▼
Databricks External Location (trỏ đến ADLS2 container)
        │
        ▼
Notebook đọc/ghi file từ ADLS2
```

---

## Bước 1 — Tạo Key Vault và lưu Account Key

### 1.1 Tạo Key Vault
```
Azure Portal → Create Resource → Key Vault
├── Name: duongbambo
├── Region: Southeast Asia
└── Create
```

### 1.2 Cấp quyền cho chính mình
```
Key Vault → Access Control (IAM)
└── Add role assignment
    ├── Role: Key Vault Secrets Officer
    └── Member: your-email@student.hcmus.edu.vn
```

### 1.3 Cấp quyền cho AzureDatabricks
```
Key Vault → Access Control (IAM)
└── Add role assignment
    ├── Role: Key Vault Secrets User
    └── Member: AzureDatabricks (Service Principal)
```

### 1.4 Tạo Secret chứa Account Key
```
Key Vault → Secrets → Generate/Import
├── Name: storage-key
└── Value: <Account Key từ Storage Account → Access keys → key1>
```

> ⚠️ Đợi 2-3 phút sau khi assign role mới có hiệu lực!

---

## Bước 2 — Tạo Databricks Secret Scope

Truy cập URL sau trên browser:
```
https://<your-workspace>.azuredatabricks.net#secrets/createScope
```

Điền thông tin:
```
Scope Name:  kv-scope
DNS Name:    https://duongbambo.vault.azure.net/
Resource ID: /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/duongbambo
```

> Lấy Resource ID: Key Vault → Overview → JSON View → copy "id" field

---

## Bước 3 — Cấp quyền ADLS2 cho Access Connector

```
Azure Portal
└── Storage Account: duongbambo
    └── Access Control (IAM)
        └── Add role assignment
            ├── Role: Storage Blob Data Contributor
            └── Member: Access Connector for Azure Databricks (Managed Identity)
```

---

## Bước 4 — Tạo Storage Credential trong Databricks

```
Databricks → Catalog → External Data → Storage Credentials
└── Create credential
    ├── Name: duongbambo-credential
    ├── Type: Azure Managed Identity
    └── Connector: Access Connector for Azure Databricks
```

---

## Bước 5 — Tạo External Location

```
Databricks → Catalog → External Data → External Locations
└── Create external location
    ├── Name: ecommerce-location
    ├── URL: abfss://ecommerce@duongbambo.dfs.core.windows.net/
    └── Credential: duongbambo-credential
```

Test connection → phải thấy Read/Write/Delete = Success ✅

---

## Bước 6 — Tạo Cluster (All-purpose, không dùng Serverless)

```
Databricks → Compute → Create compute
├── Name: lakehouse-cluster
├── Runtime: 13.3 LTS
├── Terminate after: 10 minutes
└── Create
```

> ⚠️ KHÔNG dùng Serverless — spark.conf.set không hoạt động trên Serverless!

---

## Bước 7 — Chạy Notebook

### 7.1 Attach notebook vào cluster
```
Notebook → Connect (góc trên phải) → chọn lakehouse-cluster
```

### 7.2 Cell 1 — Config kết nối ADLS2

```python
# Lấy key từ Databricks Secret Scope
storage_key = dbutils.secrets.get(
    scope="kv-scope",
    key="storage-key"
)

# Config kết nối
spark.conf.set(
    "fs.azure.account.key.duongbambo.dfs.core.windows.net",
    storage_key
)

# Test kết nối
dbutils.fs.ls("abfss://ecommerce@duongbambo.dfs.core.windows.net/")
```

### 7.3 Cell 2 — Đọc CSV từ Bronze

```python
container_name = "ecommerce"
account_name = "duongbambo"
base_path = f"abfss://{container_name}@{account_name}.dfs.core.windows.net"

# Đọc CSV
df_orders = spark.read.csv(
    f"{base_path}/bronze/csv/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

df_orders.show(5)
print(f"Total rows: {df_orders.count()}")
```

### 7.4 Cell 3 — Lưu sang Delta table (Bronze)

```python
# Lưu thành Delta table
df_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{base_path}/bronze/delta/raw_orders")

# Đăng ký thành table để dbt query được
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.raw_orders
    LOCATION '{base_path}/bronze/delta/raw_orders'
""")

print("✅ Bronze table created!")
```

### 7.5 Cell 4 — Load tất cả 9 bảng

```python
tables = {
    "raw_orders":        "olist_orders_dataset.csv",
    "raw_customers":     "olist_customers_dataset.csv",
    "raw_order_items":   "olist_order_items_dataset.csv",
    "raw_payments":      "olist_order_payments_dataset.csv",
    "raw_reviews":       "olist_order_reviews_dataset.csv",
    "raw_products":      "olist_products_dataset.csv",
    "raw_sellers":       "olist_sellers_dataset.csv",
    "raw_geolocation":   "olist_geolocation_dataset.csv",
    "raw_category":      "product_category_name_translation.csv",
}

for table_name, file_name in tables.items():
    print(f"Loading {file_name}...")
    
    df = spark.read.csv(
        f"{base_path}/bronze/csv/{file_name}",
        header=True,
        inferSchema=True
    )
    
    # Lưu Delta
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{base_path}/bronze/delta/{table_name}")
    
    # Đăng ký table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze.{table_name}
        LOCATION '{base_path}/bronze/delta/{table_name}'
    """)
    
    print(f"✅ {table_name}: {df.count()} rows loaded!")

print("\n🎉 All bronze tables loaded!")
```

---

## Checklist trước khi chạy

```
✅ Key Vault đã tạo và có secret
✅ AzureDatabricks có role Key Vault Secrets User
✅ Bản thân có role Key Vault Secrets Officer
✅ Access Connector có role Storage Blob Data Contributor trên ADLS2
✅ Storage Credential đã tạo trong Databricks
✅ External Location đã tạo, Test connection = Success
✅ Cluster All-purpose (không phải Serverless)
✅ Notebook attach vào cluster All-purpose
✅ 9 file CSV đã upload vào bronze/csv/ trên ADLS2
```

---

## Lỗi hay gặp

| Lỗi | Nguyên nhân | Fix |
|-----|-------------|-----|
| `CONFIG_NOT_AVAILABLE` | Đang dùng Serverless | Đổi sang All-purpose Cluster |
| `Secret does not exist` | Scope/key sai tên | List scope: `dbutils.secrets.listScopes()` |
| `PERMISSION_DENIED` | Role chưa propagate | Đợi 2-3 phút |
| `NO_PARENT_EXTERNAL_LOCATION` | External Location chưa tạo hoặc sai credential | Tạo lại với đúng Storage Credential |
| `Forbidden 403` | Chưa cấp quyền IAM | Add role assignment |

---

## Lệnh debug hay dùng

```python
# List tất cả secret scopes
dbutils.secrets.listScopes()

# List secrets trong scope
dbutils.secrets.list("kv-scope")

# Test đọc file
dbutils.fs.ls("abfss://ecommerce@duongbambo.dfs.core.windows.net/")

# Xem Delta table
spark.sql("SHOW TABLES IN bronze").show()
```