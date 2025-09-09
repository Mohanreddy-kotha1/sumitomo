# Packaging for AWS Glue

Options to make your code & deps available to Glue:

1) **Zip the repo** and point Glue job Script to the zip's main script.
   - Upload zip to S3 (e.g., `s3://<bucket>/code/lakehouse.zip`)
   - In Glue job: Script path = s3 zip path + script within zip
   - Extra Python files: You can attach wheels/zip with dependencies as `--extra-py-files`.

2) **Build a wheel** (recommended for shared framework code).
   - Convert `framework/` into an installable package (pyproject.toml)
   - `pip wheel . -w dist/` then upload wheel to S3
   - In Glue, add to `--additional-python-modules` or `--extra-py-files`

3) **Use Glue Python Shell** for Excel→CSV (if needed)
   - Create a Python Shell job that reads from `paths.raw_excel` and writes to `paths.bronze_csv`
   - Use the same environment YAMLs to keep consistency

> Ensure Delta & Hadoop AWS JARs are configured. Glue 5.0 has Spark 3.4.x and Python 3.10.
