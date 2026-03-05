# 📊 Data Model Documentation

## Tổng quan Dataset

Bộ dữ liệu **Brazilian E-Commerce (Olist)** mô phỏng hoạt động thương mại điện tử tại Brazil, bao gồm thông tin chi tiết về đơn hàng, sản phẩm, khách hàng, người bán, thanh toán, đánh giá và tọa độ địa lý.

---

## Mô hình quan hệ (Relational Model — OLTP)

![Mô hình quan hệ dữ liệu gốc](../assets/MODEL.png)

Cơ sở dữ liệu gốc gồm **9 bảng** được tổ chức theo mô hình quan hệ:

| Bảng | Mô tả | Khóa chính |
|------|--------|------------|
| `order` | Trung tâm hệ thống — trạng thái và timeline đơn hàng | `order_id` |
| `order_item` | Chi tiết từng sản phẩm trong đơn (giá, phí vận chuyển) | `order_id` + `order_item_id` |
| `payment` | Giao dịch thanh toán (phương thức, giá trị, trả góp) | `order_id` + `payment_sequential` |
| `review` | Đánh giá từ khách hàng (điểm, bình luận) | `review_id` |
| `customer` | Thông tin người mua (thành phố, bang) | `customer_id` |
| `seller` | Thông tin người bán (thành phố, bang) | `seller_id` |
| `product` | Thông số sản phẩm (kích thước, cân nặng, danh mục) | `product_id` |
| `category` | Bảng dịch thuật tên danh mục (Bồ Đào Nha → Anh) | `product_category_name` |
| `geolocation` | Tọa độ GPS mapping theo mã bưu điện | `geolocation_zip_code_prefix` |

### Mối quan hệ chính

```
order ──┬── 1:N ──→ order_item ──┬── N:1 ──→ product ── N:1 ──→ category
        │                        └── N:1 ──→ seller
        ├── 1:N ──→ payment
        ├── 1:N ──→ review
        └── N:1 ──→ customer

customer ──→ geolocation  (qua zip_code_prefix)
seller   ──→ geolocation  (qua zip_code_prefix)
```

---

## Kiến trúc Lakehouse (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│                        ADLS Gen2                                │
│  abfss://ecommerce@duongbambo.dfs.core.windows.net/             │
│                                                                 │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐           │
│  │   bronze/    │   │   silver/   │   │    gold/    │           │
│  │  (Raw CSV    │──→│ (Cleaned    │──→│ (Star       │           │
│  │   → Delta)   │   │  Delta)     │   │  Schema)    │           │
│  └─────────────┘   └─────────────┘   └─────────────┘           │
│     Notebook           dbt run           dbt run                │
└─────────────────────────────────────────────────────────────────┘
```

| Layer | Mục đích | Công cụ | Materialization |
|-------|----------|---------|-----------------|
| **Bronze** | Dữ liệu thô từ CSV → Delta | Databricks Notebook | External Table |
| **Silver** | Làm sạch dữ liệu (lọc NULL, chuẩn hóa) | dbt | Managed Table |
| **Gold** | Mô hình phân tích Star Schema | dbt | Managed Table |

---

## Thiết kế Star Schema (Gold Layer)

### Bảng Sự kiện (Fact Tables)

Lưu trữ các giá trị **định lượng** (measures) và khóa ngoại liên kết tới Dimension.

| Fact Table | Grain (Mức chi tiết) | Metrics | Foreign Keys |
|------------|----------------------|---------|--------------|
| `fact_order_items` | 1 dòng = 1 mặt hàng trong đơn | `price`, `freight_value` | `order_id`, `product_id`, `seller_id`, `customer_id` |
| `fact_orders` | 1 dòng = 1 đơn hàng | Thời gian giao hàng (delivery lead time) | `order_id`, `customer_id`, `date_key` |
| `fact_payments` | 1 dòng = 1 giao dịch thanh toán | `payment_value`, `payment_installments` | `order_id` |
| `fact_reviews` | 1 dòng = 1 đánh giá | `review_score` | `order_id` |

### Bảng Thứ nguyên (Dimension Tables)

Cung cấp **ngữ cảnh** cho phân tích — trả lời câu hỏi Who, What, Where, When.

| Dimension | Mô tả | Nguồn dữ liệu |
|-----------|--------|----------------|
| `dim_customers` | Khách hàng (ID, thành phố, bang) | `silver.customers` |
| `dim_sellers` | Người bán (ID, thành phố, bang) | `silver.sellers` |
| `dim_products` | Sản phẩm + tên danh mục tiếng Anh (denormalized) | `silver.products` JOIN `silver.categories` |
| `dim_geolocation` | Tọa độ GPS theo mã bưu điện | `silver.geolocation` |
| `dim_date` | Bảng phát sinh: ngày, tháng, năm, quý, ngày trong tuần | Trích xuất từ `order_purchase_timestamp` |

### Sơ đồ Star Schema

```
                        ┌──────────────┐
                        │ dim_products │
                        └──────┬───────┘
                               │
┌──────────────┐    ┌──────────┴───────────┐    ┌──────────────┐
│dim_customers │────│   fact_order_items   │────│ dim_sellers   │
└──────────────┘    └──────────┬───────────┘    └──────────────┘
                               │
                        ┌──────┴───────┐
                        │   dim_date   │
                        └──────────────┘
```
