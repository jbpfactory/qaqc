"""
Ref Processor — อ่าน, normalize ข้อมูล Product Reference สำหรับ BigQuery
ใช้เป็นตาราง Master เชื่อมโยง product_code กับ QA/QC
"""
import pandas as pd
from datetime import datetime, timezone


# Mapping: ชื่อ column ใน Excel → ชื่อ field ใน BigQuery
REF_COLUMN_MAP = {
    "product_code":    "product_code",
    "product_name":    "product_name",
    "product_type":    "product_type",
    "brand":           "brand",
    "product_subtype": "product_subtype",
    "product_series":  "product_series",
    "product_grade":   "product_grade",
    "product_line":    "product_line",
}

REQUIRED_COLUMNS = ["product_code"]

# ฟิลด์ที่ใช้ enrich QA/QC
PRODUCT_FIELDS = [
    "product_name", "product_type", "brand",
    "product_subtype", "product_series", "product_grade", "product_line",
]


def validate_ref_file(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """ตรวจสอบว่าไฟล์ Ref มี columns ที่จำเป็น"""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    return len(missing) == 0, missing


def normalize_ref(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize ข้อมูล Ref:
    - เลือกเฉพาะ columns ที่มีใน mapping
    - ตัด whitespace
    - เพิ่ม updated_at
    """
    cols_to_use = [c for c in REF_COLUMN_MAP.keys() if c in df.columns]
    df = df[cols_to_use].copy()

    # ตัด whitespace ทุก string column
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": None, "None": None, "": None})

    # ลบ rows ที่ไม่มี product_code
    df = df[df["product_code"].notna() & (df["product_code"] != "")]

    # เพิ่ม updated_at
    df["updated_at"] = datetime.now(timezone.utc).isoformat()

    return df


def df_to_bq_rows(df: pd.DataFrame) -> list[dict]:
    """แปลง DataFrame เป็น list of dicts สำหรับ BQ insert"""
    rows = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            if pd.isna(val) if not isinstance(val, str) else False:
                record[col] = None
            elif val == "nan" or val == "None":
                record[col] = None
            else:
                record[col] = val
        rows.append(record)
    return rows


def get_summary_stats(df: pd.DataFrame) -> dict:
    """สรุปสถิติไฟล์ Ref"""
    return {
        "total_products": len(df),
        "unique_codes": df["product_code"].nunique() if "product_code" in df.columns else 0,
        "unique_brands": df["brand"].nunique() if "brand" in df.columns else 0,
        "unique_types": df["product_type"].nunique() if "product_type" in df.columns else 0,
    }
