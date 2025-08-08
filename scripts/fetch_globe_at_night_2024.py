name: GaN 2024 Import (monthly upsert)

on:
  schedule:
    - cron: "0 5 2 * *"   # 05:00 UTC on the 2nd of each month
  workflow_dispatch:      # lets you run it manually from the Actions tab

jobs:
  import-gan-2024:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install requests pandas python-dateutil
      - name: Import GaN 2024
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE: ${{ secrets.SUPABASE_SERVICE_ROLE }}
        run: |
          python scripts/fetch_globe_at_night_2024.py
