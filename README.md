
# 🌌 Global SQM Data Repository

This repository hosts tools, workflows, and datasets for aggregating **Sky Quality Meter (SQM)** and **visual limiting magnitude** observations into a single, publicly accessible source.
Our goal: a **global, filterable repository** of sky quality data from multiple sources, freely accessible for public use, and API-accessible for organizations that can support the infrastructure.

---

## 📦 Contents

* `scripts/fetch_globe_at_night_2024.py`
  Python importer for **Globe at Night 2024** data.
  Downloads, cleans, and upserts data into our Supabase database.

* `.github/workflows/gan_2024_import.yml`
  GitHub Actions workflow that runs the importer on a schedule (and manually on demand).

---

## 📥 Data Sources

Current:

* **Globe at Night (2024)** – Monthly imports via CSV from [globeatnight.org](https://globeatnight.org).

Planned:

* **Globe at Night (2023)** – Historic import.
* **Unihedron SQM Live** – Direct device feeds where available.
* **Regional datasets** – e.g., Texas Night Sky Network, Oregon dark sky monitoring.
* **Community uploads** – Manual or API-based submissions from individuals and institutions.

---

## 🗄 Database Model

**Table:** `sqm_readings`

| Column                       | Type                 | Description                                             |
| ---------------------------- | -------------------- | ------------------------------------------------------- |
| `timestamp_utc`              | `timestamptz`        | Observation date/time (UTC).                            |
| `latitude`                   | `numeric`            | Latitude in decimal degrees.                            |
| `longitude`                  | `numeric`            | Longitude in decimal degrees.                           |
| `sky_brightness_mag_arcsec2` | `numeric` (nullable) | SQM reading.                                            |
| `limiting_magnitude`         | `numeric` (nullable) | Visual limiting magnitude.                              |
| `device_type`                | `text`               | `"SQM-LE"`, `"SQM-LU"`, `"visual"`, etc.                |
| `notes`                      | `text`               | Observer/location notes.                                |
| `source_type`                | `text`               | `"imported"`, `"manual"`, `"api"`.                      |
| `upload_method`              | `text`               | `"script"`, `"form"`, `"api"`.                          |
| `source_tag`                 | `text`               | Short identifier for source (e.g., `"globe_at_night"`). |

**Unique constraint:**

```
(timestamp_utc, latitude, longitude, device_type, source_tag)
```

**Check constraint:**
At least one of `sky_brightness_mag_arcsec2` or `limiting_magnitude` must be non-null.

---

## 🔄 Import Process

The importer:

1. Downloads the source CSV.
2. Normalizes column names and formats.
3. Parses timestamp, latitude, longitude, and readings.
4. Filters out invalid or out-of-range values.
5. Adds metadata (`source_tag`, `upload_method`, `source_type`).
6. Splits into 500-row chunks.
7. Upserts to Supabase via the REST API.

---

## ⚙️ Running Locally

### Requirements

```bash
pip install pandas requests python-dateutil
```

### Environment Variables

```
SUPABASE_URL=<your-supabase-project-url>
SUPABASE_SERVICE_ROLE=<your-service-role-key>
```

### Run

```bash
python scripts/fetch_globe_at_night_2024.py
```

---

## 🚀 GitHub Actions

Workflow file: `.github/workflows/gan_2024_import.yml`

Triggers:

* **Monthly cron** – keeps data up to date.
* **Manual dispatch** – run anytime from the GitHub Actions tab.

Secrets required in the repo:

* `SUPABASE_URL`
* `SUPABASE_SERVICE_ROLE`

---

## 🌍 Future Vision

This repository will grow into a **multi-source global SQM and limiting magnitude database**:

* Public, filterable maps and downloads.
* Historical + live feeds.
* API with tiered access for heavy usage.
* Contributor network for data validation and coverage expansion.

---

## 📄 License

Data licensing will follow the terms of each source dataset. Our aggregated repository will be released under [ODbL](https://opendatacommons.org/licenses/odbl/) to encourage open use with attribution.

---


Do you want me to prep that lighter version too?
