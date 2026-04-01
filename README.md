# Step-2_data-server-build

## CSV 匯入 MariaDB（自動建表）

此專案提供 `src/import_csv_to_mariadb.py`，可將 `data/` 底下的 CSV **遞迴掃描**後匯入 MariaDB。

### 1) 安裝

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r env/requirements.txt
```

### 2) 設定 MariaDB 連線（使用 `env/.env`）

在 `env/` 目錄建立 `.env`（可由範本複製）：

```bash
MARIADB_HOST=YOUR_HOST
MARIADB_PORT=3306
MARIADB_USER=YOUR_USER
MARIADB_PASSWORD=YOUR_PASSWORD
MARIADB_DATABASE=YOUR_DB
```

可直接由範本複製：`cp env/.env.example env/.env`
程式會自動讀取 `env/.env`，且無論你在哪個目錄執行，都會以專案根目錄為基準讀取 `data/`。

### 3) 執行匯入

先試跑 1 個檔：

```bash
# 在 env/.env 設定 IMPORT_LIMIT_FILES=1 後再執行
python3 src/import_csv_to_mariadb.py
```

正式全量匯入：

```bash
python3 src/import_csv_to_mariadb.py
```

### 4) 自動建表規則（重點）

- **資料表名稱**：由相對路徑 + 檔名組合並清理成 MariaDB 合法識別字；過長會截斷並加上短 hash
- **欄位名稱**：
  - `wide` 模式：用 CSV 表頭，若欄名重複會自動加 `_2`, `_3`…
  - `long` 模式：固定欄位 `source_file, row_idx, col_idx, col_name, value_float, value_str`
- **模式選擇**（預設 `IMPORT_MODE=auto`）：
  - 若偵測到 **表頭欄名重複** 或 **欄位數過多**（預設 >900）會自動改用 `long`

### 可選 `env/.env` 參數（非必要）

- `IMPORT_DATA_ROOT=data`
- `IMPORT_MODE=auto|wide|long`
- `IMPORT_IF_EXISTS=skip|drop`
- `IMPORT_WIDE_MAX_COLUMNS=900`
- `IMPORT_BATCH_SIZE=2000`
- `IMPORT_LIMIT_FILES=0`（0 代表不限制）