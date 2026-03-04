"""
BigQuery Client — เชื่อมต่อ Google BigQuery ผ่าน Service Account
"""
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "jbp-qa-qc"
DATASET_ID = "qc_qa_reporting"
QA_TABLE   = f"{PROJECT_ID}.{DATASET_ID}.qa_complaints"
QC_TABLE   = f"{PROJECT_ID}.{DATASET_ID}.qc_inspections"
REF_TABLE  = f"{PROJECT_ID}.{DATASET_ID}.product_ref"


@st.cache_resource
def get_client() -> bigquery.Client:
    """สร้าง BigQuery client จาก secrets.toml"""
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def run_query(sql: str) -> list:
    """รัน SQL query และคืนผลลัพธ์เป็น list of dicts"""
    client = get_client()
    rows = client.query(sql).result()
    return [dict(row) for row in rows]


def get_existing_unique_ids(unique_ids: list[str]) -> set[str]:
    """ตรวจสอบว่า unique_id ไหนมีอยู่แล้วใน qa_complaints"""
    if not unique_ids:
        return set()
    ids_str = ", ".join(f"'{uid}'" for uid in unique_ids)
    sql = f"""
        SELECT unique_id
        FROM `{QA_TABLE}`
        WHERE unique_id IN ({ids_str})
    """
    result = run_query(sql)
    return {row["unique_id"] for row in result}


def delete_qa_records(unique_ids: list[str]) -> None:
    """ลบ QA records ที่ต้องการ override (ก่อน insert ใหม่)"""
    if not unique_ids:
        return
    client = get_client()
    ids_str = ", ".join(f"'{uid}'" for uid in unique_ids)
    sql = f"DELETE FROM `{QA_TABLE}` WHERE unique_id IN ({ids_str})"
    client.query(sql).result()


def insert_rows(table: str, rows: list[dict]) -> list:
    """Insert rows เข้า BigQuery โดยใช้ Load Job (ไม่ใช่ Streaming)
    เพื่อหลีกเลี่ยงปัญหา streaming buffer ที่ทำให้ DELETE ไม่ได้ทันที"""
    if not rows:
        return []
    client = get_client()
    df = pd.DataFrame(rows)

    # แปลง date string → datetime.date สำหรับ BQ DATE columns
    for col in ["lot_date", "inspection_date", "complaint_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # แปลง upload_timestamp string → pandas Timestamp (UTC)
    if "upload_timestamp" in df.columns:
        df["upload_timestamp"] = pd.to_datetime(
            df["upload_timestamp"], errors="coerce", utc=True
        )

    table_ref = client.get_table(table)

    # เพิ่ม columns ที่อยู่ใน BQ schema แต่ไม่มีใน DataFrame (ป้องกัน ValueError)
    for field in table_ref.schema:
        if field.name not in df.columns:
            df[field.name] = None

    # แปลง column types ให้ตรงกับ BQ schema ก่อนส่ง pyarrow
    # (ป้องกัน float64 → STRING error เช่น color_no = 0.0 แทน "0000")
    for field in table_ref.schema:
        col = field.name
        if col not in df.columns:
            continue
        if field.field_type == "STRING":
            df[col] = df[col].where(df[col].notna(), other=None)
            df[col] = df[col].apply(lambda x: str(x) if x is not None else None)
        elif field.field_type in ("INTEGER", "INT64"):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif field.field_type in ("FLOAT", "FLOAT64", "NUMERIC"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif field.field_type in ("BOOLEAN", "BOOL"):
            df[col] = df[col].where(df[col].notna(), other=None)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=table_ref.schema,
    )
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()
    return []


def get_product_ref_map() -> dict[str, dict]:
    """ดึง product_ref ทั้งหมด return เป็น dict keyed by product_code
    เช่น {"01-01010": {"product_name": "...", "product_type": "...", ...}}
    """
    sql = f"""
        SELECT product_code, product_name, product_type, brand,
               product_subtype, product_series, product_grade, product_line
        FROM `{REF_TABLE}`
    """
    result = run_query(sql)
    ref_map = {}
    for row in result:
        code = row.get("product_code")
        if code:
            ref_map[code] = {
                "product_name": row.get("product_name"),
                "product_type": row.get("product_type"),
                "brand": row.get("brand"),
                "product_subtype": row.get("product_subtype"),
                "product_series": row.get("product_series"),
                "product_grade": row.get("product_grade"),
                "product_line": row.get("product_line"),
            }
    return ref_map


def overwrite_product_ref(rows: list[dict]) -> None:
    """Overwrite ตาราง product_ref ทั้งหมด (TRUNCATE + INSERT)"""
    if not rows:
        return
    client = get_client()
    df = pd.DataFrame(rows)

    # แปลง updated_at string → pandas Timestamp (UTC)
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(
            df["updated_at"], errors="coerce", utc=True
        )

    table_ref = client.get_table(REF_TABLE)

    # แปลง types ตาม schema
    for field in table_ref.schema:
        col = field.name
        if col not in df.columns:
            continue
        if field.field_type == "STRING":
            df[col] = df[col].where(df[col].notna(), other=None)
            df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=table_ref.schema,
    )
    job = client.load_table_from_dataframe(df, REF_TABLE, job_config=job_config)
    job.result()


def delete_qc_records(unique_ids: list[str]) -> None:
    """ลบ QC records ที่ต้องการ override (ก่อน insert ใหม่)"""
    if not unique_ids:
        return
    client = get_client()
    ids_str = ", ".join(f"'{uid}'" for uid in unique_ids)
    sql = f"DELETE FROM `{QC_TABLE}` WHERE qc_unique_id IN ({ids_str})"
    client.query(sql).result()


def get_qc_existing_unique_ids(unique_ids: list[str]) -> set[str]:
    """ตรวจสอบ QC qc_unique_id ที่มีอยู่แล้ว (composite key: case_id+date+product+lot+qc#)"""
    if not unique_ids:
        return set()
    ids_str = ", ".join(f"'{uid}'" for uid in unique_ids)
    sql = f"""
        SELECT qc_unique_id
        FROM `{QC_TABLE}`
        WHERE qc_unique_id IN ({ids_str})
    """
    result = run_query(sql)
    return {row["qc_unique_id"] for row in result}


def get_table_row_counts() -> dict:
    """นับจำนวน rows ปัจจุบันในทุกตาราง"""
    client = get_client()
    counts = {}
    for name, table in [("qa", QA_TABLE), ("qc", QC_TABLE), ("ref", REF_TABLE)]:
        try:
            sql = f"SELECT COUNT(*) AS cnt FROM `{table}`"
            rows = client.query(sql).result()
            counts[name] = list(rows)[0]["cnt"]
        except Exception:
            counts[name] = 0
    return counts


def clear_all_qa_data() -> int:
    """ลบข้อมูลทั้งหมดใน qa_complaints — คืนจำนวน rows ที่ลบ"""
    client = get_client()
    sql = f"DELETE FROM `{QA_TABLE}` WHERE TRUE"
    result = client.query(sql).result()
    return result.num_dml_affected_rows


def clear_all_qc_data() -> int:
    """ลบข้อมูลทั้งหมดใน qc_inspections — คืนจำนวน rows ที่ลบ"""
    client = get_client()
    sql = f"DELETE FROM `{QC_TABLE}` WHERE TRUE"
    result = client.query(sql).result()
    return result.num_dml_affected_rows
