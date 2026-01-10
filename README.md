# PyORM

A modern, type-safe ORM for Python 3.13+, built on top of Pydantic.

> **Note**: This project is currently in a prototype stage.

## Features

-   **Type Safety**: Leverages Python 3.13 type hinting and Pydantic for robust data validation.
-   **Modern Syntax**: clean, pythonic API for database interactions.
-   **Automatic Schema**: Generates SQL tables directly from your Pydantic models.
-   **No boilerplate**: Minimal configuration required to get started.

## Installation

```bash
# Using uv (recommended)
uv add pyorm

# Using pip
pip install pyorm
```

## Quick Start

### 1. Define your Model

Models are just Pydantic models that inherit from `pyorm.Model`.

```python
from pyorm import Model
from typing import ClassVar

class User(Model):
    table_name: ClassVar[str] = "users"
    id: int | None = None  # Primary key (auto-incrementing)
    name: str
    email: str
    age: int = 18
```

### 2. Configure the Database

Initialize the connection with the SQLite backend.

```python
from pyorm import Database
from pyorm.backends.sqlite import SQLiteBackend

# Configure the database connection
db_backend = SQLiteBackend("my_app.db")
Database.configure_database(db_backend)
```

### 3. Create Tables

Automatically create the database table based on your model definition.

```python
User.create_model()
```

### 4. Usage

#### Create
```python
# Create a new user instance
user = User(name="Alice", email="alice@example.com")
user.save() # Persists to DB and populates user.id
print(f"User created with ID: {user.id}")
```

#### Read
```python
# specific user by ID (implied PK lookup)
user = User.get(id=1)

# Filter users
adults = User.filter(age=18)
for u in adults:
    print(u.name)
```

#### Update
```python
user = User.get(name="Alice")
user.email = "new_email@example.com"
user.save() # Detects changes and updates only modified fields
```

#### Delete
```python
user = User.get(id=1)
user.delete()
```
