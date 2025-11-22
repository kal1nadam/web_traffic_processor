# **web_traffic_processor**  
### *E-shop Purchase Last-Click Non-Direct Attribution Engine*

A Python-based CLI application for analyzing e-shop web traffic and calculating **last-click non-direct attribution** using GTM-like event data stored in Parquet files.  
It processes raw events, computes last-click attribution, normalizes & deduplicates purchase orders, and stores everything in a local **DuckDB** database.

MIT Licensed.

---

## **📦 Features**

- Processes **Google GTM → BigQuery–style** event data  
- Local analytical storage using **DuckDB**  
- **Last-Click Non-Direct Attribution** logic fully in SQL  
- Typer CLI with multiple commands  
- Poetry for dependency and environment management  
- Fully **Docker Compose–ready**  
- Order & product **deduplication using normalized SHA256 hashing**  
- Clear modular architecture (adapters, domain, DTOs, mappers, services, utils)  

---

## **🗂 Project Structure**

```
web_traffic_processor/
│
├── data/
│   └── events/                   # Parquet sample data for seeding
│
├── src/
│   └── app/
│       ├── adapters/
│       │   ├── repositories/
│       │   │   └── db_repository.py
│       │   ├── duckdb_backend.py
│       │   └── sql/              # Attribution + order processing SQL files
│       │
│       ├── domain/
│       │   ├── services/
│       │   │   └── normalization.py
│       │   └── models.py
│       │
│       ├── dto/
│       │   └── order_dto.py
│       │
│       ├── mappers/
│       │   └── order_mapper.py
│       │
│       ├── services/
│       │   └── order_processing_service.py
│       │
│       ├── utils/
│       │   └── hash_utils.py
│       │
│       └── cli.py                # Typer CLI entry point
│
├── tests/                        # Unit test boilerplate
├── pyproject.toml                # Poetry dependencies
├── docker-compose.yml
├── docker/
│   └── Dockerfile
└── README.md
```

---

## **🧰 Technology Stack**

- **Python 3.12**
- **Poetry 1.8.3**
- **DuckDB**
- **Typer CLI**
- **Parquet**
- **Docker Compose**
- VS Code debugging support (`launch.json`)

---

## **📁 Sample Data (Input)**

`data/events/` contains anonymized Parquet files representing GTM-like BigQuery event exports.

### **Events table schema**

```sql
event_name VARCHAR,
event_timestamp BIGINT,
user_pseudo_id VARCHAR,
hostname VARCHAR,
event_params STRUCT(
    key VARCHAR,
    value STRUCT(
        string_value VARCHAR,
        int_value BIGINT,
        float_value DOUBLE,
        double_value DOUBLE
    )
)[],
items STRUCT(
    item_id VARCHAR,
    item_name VARCHAR,
    price DOUBLE,
    quantity BIGINT
)[]
```

---

## **🛢 Database Tables**

- `events`
- `purchase_last_click_attributions`
- `orders`
- `products`
- `order_to_products` (M:N)

---

# **🚀 CLI Commands**

All commands via Poetry:

```bash
poetry run python -m app.cli <command>
```

### **Commands**

| Command | Description |
|--------|-------------|
| `run` | Test command: prints greeting. |
| `init-db` | Recreates DB, seeds from Parquet. |
| `calc-attributions` | Computes last-click non-direct attribution. |
| `import-orders` | Imports & deduplicates orders/products. |
| `get-orders` | Reads stored orders. |

---

# **📄 Examples (Tabular Format)**

## **purchase_last_click_attributions**

| user_pseudo_id | event_timestamp | hostname | source | medium | campaign |
|----------------|-----------------|----------|--------|--------|----------|
| 953097468.1763317960 | 1763317960667863 | 99b6814ccfb074ad6acb28ae47e5db1a | google | cpc | PMAX christmas |

---

## **orders**

| id | event_timestamp | hostname | user_pseudo_id | currency | value | source | medium | campaign | hash |
|----|----------------|----------|----------------|----------|-------|--------|--------|----------|------|
| d94659fe-78b6-4c82-a2f6-ebbb1093db3b | 2025-11-17 00:58:38 | 99b6814ccfb074ad6acb28ae47e5db1a | 963890394.1763336481 | USD | 89.0 | google | organic | (organic) | 89a4ab08c2046ccd |

---

# **⚙️ Attribution Logic**

The **calc-attributions** command:

- Runs the SQL file `calc_attribution.sql`
- Selects **latest valid non-direct click before purchase**
- Stores results into `purchase_last_click_attributions`

---

# **🛒 Order Import Logic**

The **import-orders** command:

1. Executes `process_orders.sql`
2. Maps rows → DTO → domain model
3. Normalizes fields (domain responsibility)
4. Computes SHA256 hash (deduplication)
5. Deduplicates:
   - orders  
   - products (via M:N table)
6. Stores into:
   - `orders`
   - `products`
   - `order_to_products`

---

# **🐳 Docker & Docker Compose**

This project includes a ready-to-use `docker-compose.yml`.

### **Build**

```bash
docker compose build web_traffic_processor
```

- The `app/` and `data/` directories are **mounted as volumes**
- Enables live code editing and persistent local data

---

### **Run CLI commands**

```bash
docker compose run --rm web_traffic_processor <command>
```

Examples:

```bash
docker compose run --rm web_traffic_processor init-db
docker compose run --rm web_traffic_processor calc-attributions
docker compose run --rm web_traffic_processor import-orders
docker compose run --rm web_traffic_processor get-orders
docker compose run --rm web_traffic_processor run Adam
```

---

# **🧑‍💻 Development Setup**

Install dependencies:

```bash
poetry install
```

Run CLI:

```bash
poetry run python -m app.cli <command>
```

---

# **🧪 Tests**

```
tests/
```

Run:

```bash
pytest
```

---

# **📄 License**

This project is licensed under the **MIT License**.
