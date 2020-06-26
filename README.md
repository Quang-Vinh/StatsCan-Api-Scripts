# Stats Can API Scripts
Some scripts to update tables using the StatsCan API https://www.statcan.gc.ca/eng/developers/wds/user-guide

# Scripts
- Download updated tables for current day. Can specify bucket to upload to MinIO bucket on Advanced Analytics Workspace as well.
```
python download_tables.py
python download_tables.py --minio_bucket=quangvinh-do
```
