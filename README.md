NOTE: Branches funkyui and test are working as of now

# ⚡ Mini Query Optimizer — SQL Engine Simulator

A fully functional, interactive **SQL query optimization pipeline** built in pure Python. This project simulates the core query-planning subsystem of a relational database engine — from raw SQL text all the way to a physical execution plan — with a beautiful dark-themed Streamlit web interface.

---

## 🖼️ What It Does

```
SQL String
   │
   ▼
┌─────────────────────┐
│   SQL Parser        │  → Tokenizes & extracts tables, columns,
│   (sqlparse)        │    JOINs, WHERE conditions
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Logical Plan      │  → Unoptimized relational-algebra tree
│   Builder           │    Project → Select → Join → Scan
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Rule-Based        │  → Pass A — Predicate Pushdown: moves WHERE
│   Optimizer (RBO)   │    filter below JOINs to reduce row count early
│                     │  → Pass B — Projection Pushdown: inserts narrow
│                     │    ProjectNodes above each Scan to drop unused
│                     │    columns as early as possible (saves RAM)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Cost-Based        │  → Join Reordering: evaluates all table
│   Optimizer (CBO)   │    orderings using cardinality statistics,
│                     │    picks the cheapest execution order
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Physical Plan     │  → Optimal execution tree, displayed
│   + Visualizer      │    with ASCII box-drawing characters
└─────────────────────┘
```

---

## 📁 Project Structure

```
DBMS PROJECT/
├── app.py                  ← Streamlit interactive frontend (main entry point)
├── README.md               ← This file
└── engine/
    ├── __init__.py         ← Package marker
    ├── catalog.py          ← Mock database statistics (Catalog class)
    ├── nodes.py            ← Relational algebra tree nodes
    ├── parser.py           ← SQL → Logical Plan parser
    ├── rbo.py              ← Rule-Based Optimizer (Predicate + Projection Pushdown)
    ├── cbo.py              ← Cost-Based Optimizer (Join Reordering)
    └── visualizer.py       ← ASCII tree renderer (PlanVisualizer)
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.9 or higher
- pip

### Installation

```bash
# Clone or navigate to the project directory
cd "DBMS PROJECT"

# Install required libraries
pip install sqlparse streamlit

# Launch the web application
streamlit run app.py
```

The app will open automatically in your browser at **http://localhost:8501**.

---

## 📖 Component Deep-Dive

### `engine/catalog.py` — The Mock Catalog

Simulates a database system catalog (like PostgreSQL's `pg_statistic`).

| Table       | Row Count | Columns                         |
| ----------- | --------- | ------------------------------- |
| `users`     | 10,000    | `id`, `name`, `city_id`         |
| `cities`    | 100       | `id`, `city_name`, `country_id` |
| `countries` | 10        | `id`, `country_name`            |

**Key methods:**

```python
catalog = Catalog()
catalog.get_cardinality("users")   # → 10000
catalog.get_columns("cities")      # → ["id", "city_name", "country_id"]
catalog.get_all_stats()            # → {table: {row_count, columns}, ...}
```

---

### `engine/nodes.py` — Relational Algebra Nodes

Defines the building blocks of a query plan tree, each subclassing `PlanNode`:

| Class         | Operator       | Analogy                       |
| ------------- | -------------- | ----------------------------- |
| `ScanNode`    | SeqScan        | `FROM table` — reads all rows |
| `SelectNode`  | Filter (σ)     | `WHERE condition`             |
| `ProjectNode` | Projection (π) | `SELECT col1, col2`           |
| `JoinNode`    | Inner Join (⋈) | `JOIN table ON condition`     |

Each node has:

- `explain(depth)` — returns ASCII tree string
- `source_tables` — returns the set of base tables reachable from this node

---

### `engine/parser.py` — SQL Parser

Uses `sqlparse` to tokenize raw SQL, then extracts:

- **SELECT columns** → `ProjectNode`
- **FROM clause** → primary `ScanNode`
- **JOIN … ON clauses** → chain of `JoinNode`s
- **WHERE clause** → wrapping `SelectNode`

**Supported SQL shape:**

```sql
SELECT <col_list>
FROM <table>
[JOIN <table> ON <condition>]*
[WHERE <predicate>]
```

**Usage:**

```python
from engine.parser import QueryParser
parser = QueryParser()
tree   = parser.parse("SELECT users.name FROM users WHERE users.id > 100")
print(parser.explain_parse(sql))  # Debug extraction report
```

---

### `engine/rbo.py` — Rule-Based Optimizer

Applies **two algebraic rewrite rules** in sequence:

---

#### Rule 1 — Predicate Pushdown

> Move WHERE filter predicates as close to the base data (scan) as possible.

**Before (filter sits above all joins):**

```
Project [users.name, countries.country_name]
└── Filter [users.id > 500]           ← filter is HIGH in the tree
      └── InnerJoin [...]
            ├── InnerJoin [...]
            │    ├── SeqScan [users]
            │    └── SeqScan [cities]
            └── SeqScan [countries]
```

**After Predicate Pushdown (filter pushed to scan level):**

```
Project [users.name, countries.country_name]
└── InnerJoin [...]
      ├── InnerJoin [...]
      │    ├── Filter [users.id > 500]  ← pushed DOWN to scan level
      │    │    └── SeqScan [users]
      │    └── SeqScan [cities]
      └── SeqScan [countries]
```

**Algorithm:**

1. Walk the tree recursively top-down.
2. When a `SelectNode` is found above a `JoinNode`, extract the tables mentioned in the predicate.
3. If the predicate references only one side of the join → push it down to that side.
4. If the predicate spans both sides → leave it in place.

---

#### Rule 2 — Projection Pushdown

> Determine the minimum set of columns required by the query and insert narrow `ProjectNode`s directly above each `SeqScan` to eliminate unused columns as early as possible.

**Why this matters:** In a real database engine, each row passing through the operator pipeline carries all its columns in memory. If a table has 20 columns but the query only needs 2, the other 18 are wasted RAM through every join and filter. Projection Pushdown fixes this by **dropping unnecessary columns at the scan boundary** — before any data moves further up the tree.

**Savings in a real system:**

- 🧠 **RAM** — row buffers are narrower; more rows fit per memory page
- 🌐 **Network bandwidth** — in distributed systems (e.g., Spark, distributed SQL), narrower rows mean less data shipped between nodes
- 💾 **I/O** — columnar stores can skip entire column files entirely

**What the algorithm does:**

1. Collect all `table.column` references used anywhere in the tree (SELECT list, WHERE predicates, JOIN ON conditions) into a _required columns_ set.
2. Walk the tree; for every `ScanNode`, look up its catalog columns and compute which are in the required set.
3. Insert a `ProjectNode(columns=[needed_cols])` immediately above the `ScanNode` to drop the rest.

**After Projection Pushdown (narrow ProjectNodes inserted above scans):**

```
Project [users.name, countries.country_name]
└── InnerJoin [ON cities.country_id = countries.id]
      ├── InnerJoin [ON users.city_id = cities.id]
      │    ├── Filter [users.id > 500]
      │    │    └── Project [city_id, id, name]   ← only 3/3 needed cols kept
      │    │         └── SeqScan [users]
      │    └── Project [city_name, country_id, id] ← narrow cities scan
      │         └── SeqScan [cities]
      └── Project [country_name, id]             ← narrow countries scan
           └── SeqScan [countries]
```

In the example above, `users.id` (used in WHERE), `users.city_id` (used in JOIN), and `users.name` (used in SELECT) are all needed — so no columns are dropped from `users`. However, if your SELECT only referenced `users.name` and no JOIN needed `users.city_id`, then `city_id` and `id` would be eliminated.

---

### `engine/cbo.py` — Cost-Based Optimizer

Implements **Join Reordering** using a simple but effective cost model:

```
cost(A JOIN B) = cardinality(A) × cardinality(B)

Total cost of (A ⋈ B) ⋈ C:
  step1 = |A| * |B|
  step2 = step1 * |C|
  total = step1 + step2
```

For 3 tables (6 possible orderings), the CBO evaluates all permutations and chooses the minimum-cost ordering. This typically means joining the **smallest tables first**.

**Example (default query):**

| Ordering                                    | Cost                                    |
| ------------------------------------------- | --------------------------------------- |
| users(10,000) ⋈ cities(100) ⋈ countries(10) | 1,000,000 + 10,000,000 = **11,000,000** |
| countries(10) ⋈ cities(100) ⋈ users(10,000) | 1,000 + 10,000,000 = **10,001,000**     |
| **countries(10) ⋈ cities(100) → minimized** | **smallest first wins**                 |

---

### `engine/visualizer.py` — ASCII Tree Renderer

Converts any `PlanNode` tree to a clean, readable string:

```
└── 📋 Project [ users.name, countries.country_name ]
    └── 🔗 InnerJoin [ ON cities.country_id = countries.id ]
         ├── 🔗 InnerJoin [ ON users.city_id = cities.id ]
         │    ├── 🔍 Filter [ users.id > 500 ]
         │    │    └── 📂 SeqScan [ users ]
         │    └── 📂 SeqScan [ cities ]
         └── 📂 SeqScan [ countries ]
```

Box-drawing characters used: `├──`, `└──`, `│`

**Usage:**

```python
from engine.visualizer import PlanVisualizer
vis = PlanVisualizer()
print(vis.render(plan_root))
```

---

### `app.py` — Streamlit Frontend

The interactive web application features:

- **Dark premium UI** with glassmorphism-inspired cards and gradient header
- **Sidebar** showing live catalog statistics (tables, row counts, columns)
- **SQL text area** with the default 3-table JOIN query pre-loaded
- **4-tabbed output:**
  1. Parsed Logical Plan (with parser extraction details)
  2. After RBO — split into two sub-phases:
     - **2A — Predicate Pushdown**: shows filter movement rules applied
     - **2B — Projection Pushdown**: shows narrow projection nodes added above scans
  3. After CBO — shows cost report + side-by-side comparison
  4. Debug Info — raw Python reprs and JSON catalog dump
- **5 metric cards** showing tables joined, predicate rules, projection rules, min cost, optimal ordering
- **Black-background white-text tree output** (`<pre>` style) for maximum readability

---

## 🧪 Example Queries to Try

### Example 1 — Default (3 tables, with WHERE)

```sql
SELECT users.name, countries.country_name
FROM users
JOIN cities ON users.city_id = cities.id
JOIN countries ON cities.country_id = countries.id
WHERE users.id > 500
```

### Example 2 — Two table join

```sql
SELECT users.name, cities.city_name
FROM users
JOIN cities ON users.city_id = cities.id
WHERE users.id > 100
```

### Example 3 — Single table scan + filter

```sql
SELECT users.name
FROM users
WHERE users.id > 9000
```

### Example 4 — No WHERE clause

```sql
SELECT users.name, cities.city_name, countries.country_name
FROM users
JOIN cities ON users.city_id = cities.id
JOIN countries ON cities.country_id = countries.id
```

---

## 🏗️ Architecture Decisions

| Decision                         | Rationale                                                      |
| -------------------------------- | -------------------------------------------------------------- |
| No sqlite / SQLAlchemy           | Keeps the focus on the optimizer logic, not storage            |
| `dataclasses` for nodes          | Clean, self-documenting, minimal boilerplate                   |
| Regex-based SQL extraction       | Lightweight; avoids heavy AST library for simple SELECT subset |
| `itertools.permutations` for CBO | Exhaustive but correct for ≤3 tables (max 6 orderings)         |
| `copy.deepcopy` for each stage   | Ensures each pipeline stage gets a fresh tree to mutate safely |
| Stateless optimizers             | Re-create per query; no hidden state bugs between requests     |

---

## 📚 Key Concepts Illustrated

| Concept                   | Where Applied                           | Real DB Equivalent            |
| ------------------------- | --------------------------------------- | ----------------------------- |
| AST / Parse Tree          | `parser.py`                             | pg parser / bison grammar     |
| Logical Plan              | `nodes.py`                              | Volcano model / Cascades      |
| Predicate Pushdown (RBO)  | `rbo.py` — `_apply_predicate_pushdown`  | PostgreSQL `prepjointree.c`   |
| Projection Pushdown (RBO) | `rbo.py` — `_apply_projection_pushdown` | MySQL `Item_field` pruning    |
| Cardinality Statistics    | `catalog.py`                            | `pg_statistic` / `ANALYZE`    |
| Cost Model                | `cbo.py` — join cost formula            | Selinger optimizer            |
| Plan Enumeration          | `cbo.py` — permutations                 | DP table in Volcano/Columbia  |
| Explain Output            | `visualizer.py`                         | `EXPLAIN` / `EXPLAIN ANALYZE` |

---

## 📝 License

This project is for educational purposes — built as a DBMS course project to illustrate the internals of a query optimizer.
Generate your own ssh key + certificate using command `openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes`
