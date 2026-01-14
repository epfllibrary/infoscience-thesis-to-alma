# create_records.py — English Documentation

Python script that automatically creates **bibliographic records**, **holdings**, and **items** in Alma from **MARCXML** notices exported from [Infoscience (EPFL)](https://infoscience.epfl.ch).

---

**Author:** Sylvain Vuilleumier  
Documentary engineering specialist – EPFL  
sylvain.vuilleumier@epfl.ch  
**License: Apache 2.0**

---

## 🧭 Key Features

- Automatic harvesting of EPFL theses from Infoscience (Discover/Export API)
- Full pagination using `spc.page`
- Custom MARC mapping (Infoscience → EPFL MARC21)
- Optional XSD validation for MARC21, bib, holding, item
- SRU lookup in swisscovery
- Automated Alma creation:
  - Bibliographic record (IzBib)
  - Holdings per location
  - Items with configurable policies
- Generates a clear **CSV report**
- **Dry-run mode** to test without modifying Alma
- Full configuration via `create_records.ini`

---

## 📌 Workflow Summary

1. Retrieves the latest call number via **Alma Analytics**.  
2. Harvests notices from **Infoscience**, using **automatic pagination** (`spc.page`).  
3. Converts each source notice into the **final EPFL MARC21 record** (custom mapping).  
4. Queries **swisscovery** via **SRU** to check if a record already exists.  
5. If not found:
   - Generates an Alma `<bib>` structure  
   - Optionally validates XML using **XSD schemas**  
   - Creates the **bibliographic record** in Alma  
   - Creates the associated **holdings** and **items**  
6. Produces a structured **CSV report**.

---

## ⚙️ Requirements

- Python **3.10+**
- Libraries:
  - `requests`
  - `python-dotenv`
  - `pymarc`
  - `lxml`
  - `beautifulsoup4`
  - `almapiwrapper`

Install dependencies:

```bash
pip install requests python-dotenv pymarc lxml beautifulsoup4 almapiwrapper
```

⚠️ For almapiwrapper, please check the installation guide : https://almapi-wrapper.readthedocs.io/en/latest/getstarted.html

Set an environment variable named `alma_api_keys` and point it to your alma_api_key.json file.


---

## 🔐 Alma configuration (`.env` file)

This API is needed to get the callnumber from analytics.

Sensitive settings must be stored in `.env`:

```dotenv
ALMA_API_URL=https://api-eu.hosted.exlibrisgroup.com/almaws/v1/
ALMA_API_ANALYTICS_PATH=/analytics/reports?path=/shared/EPFL/Some/Report
ALMA_API_KEY=XXXXXXXXXXXXXXX
```

The script also writes:

```
last_call_number.txt
```

⚠️ This file is **for information only** — it is *never* used as a fallback.

The call number is retrieved from Alma Analytics at startup.
If the retrieval fails, the script automatically retries up to **3 times**
before stopping execution.

---

## 🧩 Main configuration (`config_sandbox.ini`)

All non-sensitive constants are configured here:

```ini
[general]
env = S
institution_code = HPH
check_xsd = true
report_prefix = report_
skip_sru_check = false

[infoscience]
spc_rpp = 100
of_format = xm
since_strategy = previous_month

[xsd]
marc21 = xsd/MARC21slim.xsd
bib = xsd/rest_bib.xsd
holding = xsd/rest_holding.xsd
item = xsd/rest_item.xsd

[holding]
library_code = hph_bjnbecip
locations = E02XA,E02SP
call_number_prefix = ZTK

[item]
po_line =
department_code = AcqDepthph_bjnbecip
material_type_code = THESIS
```

Command‑line arguments *override* INI settings.

---

## 🌐 Infoscience Harvesting (Pagination)

The script fetches MARCXML records using:

```
https://infoscience.epfl.ch/server/api/discover/export
  ?configuration=researchoutputs
  &spc.page=1
  &spc.rpp=100
  &f.types=thesis-coar-types:c_db06,authority
  &query=dc.publisher:EPFL dc.date.created:[YYYY-MM-01 TO *]
  &spc.sf=dc.date.accessioned
  &spc.sd=DESC
  &of=xm
```

- `spc.page` → page index (1, 2, 3, …)  
- `spc.rpp` → results per page  
- `of=xm` → MARCXML compatible with `pymarc`

`iter_infoscience_records()` keeps fetching pages until an empty page is found.

---

## 📂 Optional XSD Validation

Place XSD schemas under `xsd/`:

- `MARC21slim.xsd`
- `rest_bib.xsd`
- `rest_holding.xsd`
- `rest_item.xsd`

Disable XSD validation:

```bash
python create_records.py --no-xsd-check
```

---

## 🚀 Running the Script

General syntax:

```bash
python create_records.py [options]
```

### Main Options

| Option | Description |
|--------|-------------|
| `--dry-run` | Runs the pipeline **without creating anything** in Alma |
| `--use-static-url` | Fetch a single fixed Infoscience URL (debug mode) |
| `--spc-page` | Starting page for pagination (default 1) |
| `--spc-rpp` | Results per page (default 100) |
| `--env` | Alma environment (`S` sandbox / `P` production) |
| `--institution-code` | Alma IZ code (e.g., `HPH`, `EPF`) |
| `--max-records` | Limit total processed records |
| `--config-file` | Load configuration from an INI file |

---

## 🧪 Usage Examples

### 1. Dry‑run (no Alma writes)

```bash
python create_records.py --dry-run --max-records 5
```

### 2. Debugging a specific record

```bash
python create_records.py --dry-run --use-static-url
```

### 3. Full run with config file + pagination

```bash
python create_records.py   --config-file create_records.ini   --env S   --institution-code HPH   --max-records 10
```

### 4. Run without XSD validation

```bash
python create_records.py --no-xsd-check --max-records 3
```

---

## 📄 CSV Report

Output file:

```
report_YYYY-MM-DD.csv
```

Contains fields such as:

- Index  
- Infoscience ID  
- Title / Author  
- Call number  
- SRU match  
- MMS ID  
- Bib status  
- Holdings & items per location
- Warning (if any)
- Errors (if any)

---

### Warnings column

The CSV report includes a `warnings` column intended for **functional warnings**
that do not stop the processing but require attention.

Typical examples:

- Record already exists in swisscovery (SRU):
  - bibliographic creation is skipped
  - no call number is assigned
  - `call_number = N/A`
  - a warning explains the reason

Warnings are meant to provide **human-readable explanations**
for non-blocking situations.

### Call number assignment rules

The call number is assigned **only when a bibliographic record is actually created in Alma**.

If a record is found in swisscovery via SRU:

- creation is skipped
- the call number is not consumed
- the CSV report contains:

call_number = N/A
bib_status = SKIPPED_SRU_EXISTS
warnings = Record already exists in SRU: call number not assigned

## 🪵 Logging

Two log files are generated:

- `create_records.log` — normal operations (INFO+)  
- `erreurs.log` — detailed errors (ERROR+, with traceback)

Wrapper noise like `"no holding found"` is automatically filtered.

---

## 🧱 Code Structure

- Configuration loader (INI + `.env`)  
- Logger and error handler  
- MARC transformation layer  
- HTML normalization layer for Infoscience metadata (BeautifulSoup)
- XSD validation engine  
- Infoscience harvesting with pagination  
- SRU lookup  
- Alma integration (Bib / Holding / Item)  
- CSV report generator  
- CLI interface (argparse)  
- Main pipeline  

---


## ⚠️ Known limitations and special handling

### Infoscience HTML contamination in titles

Infoscience sometimes returns MARC titles containing HTML markup
(`<i>`, `<sub>`, `<sup>`, `<span>`, entities like `&amp;`, etc.).
These elements:

- pollute Alma bibliographic records
- break SRU lookups
- degrade CSV exports

For this reason, the script applies systematic HTML normalization
(using BeautifulSoup) on:

- MARC 245$a
- MARC 245$b
- responsibility statements (245$c)

Example:

```
Before:
<i>Data-driven</i> Control &amp; Optimization

After:
Data-driven Control & Optimization
```

This normalization is applied **before** MARC generation, SRU lookup
and CSV reporting.

### Alma Work Order API limitation

The script supports sending Work Order information when creating items:

```ini
[item]
work_order_type = AcqWorkOrder
department_code = AcqDepthph_bjnbecip
```

These are transmitted to Alma using:

```xml
<process_type>WORK_ORDER_DEPARTMENT</process_type>
<work_order_type>...</work_order_type>
<work_order_at>...</work_order_at>
```

However, as of 2025, Alma’s REST API ignores these values:

- items are created successfully
- but no Work Order is visible in the Alma UI

This is an API limitation, not a bug in the script.
The fields are kept for future compatibility.


## ✨ Future Ideas

- Add `--since-date` for custom Infoscience time windows  
- Add CLI for changing `of_format` (`xm`, `xmJ`, …)  

## 📜 License

Distributed under the **Apache License 2.0**.