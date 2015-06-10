brobbot-sqlite-brain
===================

A SQLite-backed brain for [brobbot](https://npmjs.org/package/brobbot).

## Usage

In your [brobbot-instance](https://github.com/b3nj4m/brobbot-instance):

```bash
npm install --save brobbot-sqlite-brain
./index.sh -b sqlite
```

## Configuration

### Database name

Set `BROBBOT_SQLITE_DB_NAME` to change the default db name (`'brobbot'`).

```bash
BROBBOT_SQLITE_DB_NAME=robotz ./index.sh -b sqlite
```

### Table name

Set `BROBBOT_SQLITE_TABLE_NAME` to change the default table name (`'brobbot'`).

```bash
BROBBOT_SQLITE_TABLE_NAME=robotz ./index.sh -b sqlite
```

### Data key prefix

Set `BROBBOT_SQLITE_DATA_PREFIX` to change the default key prefix (`'data:'`).

```bash
BROBBOT_SQLITE_DATA_PREFIX=brobbot-data: ./index.sh -b sqlite
```
