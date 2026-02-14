# EBooklet Project Overview

`ebooklet` is a pure Python key-value file database that provides seamless synchronization with S3-compatible storage (AWS S3, Backblaze B2, etc.). It is built upon the [`booklet`](https://github.com/mullenkamp/booklet) package and extends it with remote synchronization capabilities.

## Core Concepts

- **Local-First with Remote Sync**: Designed to work primarily with local files for performance, with the ability to push/pull changes to/from S3.
- **Git-like Workflow**: Uses a "changes" model for managing interactions between local and remote data (e.g., `db.changes().push()`).
- **Safety**: Thread-safe for writes (using thread locks) and multiprocessing-safe (using file and S3 object locks).
- **Flexible Serialization**: Supports multiple serializers for values while requiring string keys.

## Main Technologies

- **Python**: 3.10+
- **Database Backend**: [`booklet`](https://github.com/mullenkamp/booklet)
- **S3 Interaction**: `s3func`
- **Build System**: `hatchling`
- **Quality Tools**: `ruff` (linting), `black` (formatting), `mypy` (type checking)
- **Testing**: `pytest`
- **Documentation**: `mkdocs` with `material` theme

## Project Structure

- `ebooklet/`: Main package directory.
    - `main.py`: Core logic for `EVariableLengthValue` (main DB class) and `RemoteConnGroup`.
    - `remote.py`: S3 connection and session management.
    - `utils.py`: Internal utility functions for syncing and file handling.
- `ebooklet/tests/`: Unit and integration tests.
- `docs/`: Project documentation.
- `pyproject.toml`: Project configuration and dependency management.

## Key Commands

The project uses `uv` for development tasks.

### Environment Management
- `uv env create`: Create the default development environment.
- `uv shell`: Enter the development shell.

### Testing
- `uv run test`: Run all tests.
- `uv run cov`: Run tests with coverage reporting.
- `uv run pytest`: Standard pytest command (if in the correct environment).

### Linting and Formatting
- `uv run lint:all`: Run both style and typing checks.
- `uv run lint:fmt`: Apply formatting (black) and lint fixes (ruff).
- `black .`: Run formatting.
- `ruff check .`: Run linting.

## Development Conventions

- **Formatting**: Strict adherence to `black` with a line length of 120.
- **Linting**: `ruff` is used with a comprehensive set of rules (see `pyproject.toml`).
- **Typing**: `mypy` is used for static type checking.
- **Syncing Model**: Always use `db.sync()` or a context manager to ensure data is flushed to disk before closing.
- **Remote Consistency**: EBooklet uses UUIDs to ensure that a local file matches its remote counterpart.

## Usage Example

```python
import ebooklet

remote_conn = ebooklet.S3Connection(
    access_key_id='...',
    access_key='...',
    db_key='data.blt',
    bucket='my-bucket',
    endpoint_url='https://...'
)

with ebooklet.open(remote_conn, 'local_data.blt', flag='c') as db:
    db['key'] = 'value'
    changes = db.changes()
    changes.push()
```
