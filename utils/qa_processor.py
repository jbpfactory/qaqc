"""
QA Processor — อ่าน, normalize และ upsert ข้อมูล QA เข้า BigQuery
"""
import pandas as pd
from datetime import datetime, timezone


# Mapping: ชื่อ column ใน Excel → ชื่อ field ใน BigQuery
# หมายเหตุ: product_name, product_type, brand, product_subtype, product_series,
# product_grade, product_line ถูกลบออกจาก mapping แล้ว
# → ดึงจากตาราง product_ref แทน (ผ่าน enrich_with_product_ref)
QA_COLUMN_MAP = {
    "DATE":             "complaint_date",
    "CASE ID":          "case_id",
    "COUNTRY":          "country",
    "REGION":           "region",
    "PROVINCE":         "province",
    "SHOP TYPES":       "shop_type",
    "STORE NAME":       "store_name",
    "PRODUCT CODE":     "product_code",
    # Product fields — อ่านจากไฟล์ QA เป็น fallback
    # ถ้าเจอ product_code ใน Ref จะถูก override ด้วยข้อมูล Ref อัตโนมัติ
    "PRODUCT NAME":     "product_name",
    "PRODUCT TYPE":     "product_type",
    "BRAND":            "brand",
    "PRODUCT SUBTYPE":  "product_subtype",
    "PRODUCT SERIES":   "product_series",
    "PRODUCT GRADE":    "product_grade",
    "PRODUCT LINE":     "product_line",
    "LOT":              "lot",
    "LOT Date":         "lot_date",
    "COLOR NO.(EX.0000)": "color_no",
    "PRIMER":           "primer",
    "PROBLEM":          "problem",
    "PROBLEM TYPE":     "problem_type",
    "INSPCTOR":         "inspector",
    "ACTION DETAILS":   "action_details",
    "CORRECTIVE ACTION":"corrective_action",
    "PERSON IN CHARGE": "person_in_charge",
}

REQUIRED_COLUMNS = [
    "DATE", "CASE ID", "COUNTRY", "REGION", "PROVINCE",
    "SHOP TYPES", "STORE NAME", "PRODUCT CODE",
    "LOT", "LOT Date", "COLOR NO.(EX.0000)", "PRIMER", "PROBLEM",
    "INSPCTOR", "ACTION DETAILS", "CORRECTIVE ACTION", "PERSON IN CHARGE",
]

# ฟิลด์ที่ดึงจาก product_ref
PRODUCT_FIELDS = [
    "product_name", "product_type", "brand",
    "product_subtype", "product_series", "product_grade", "product_line",
]


def validate_qa_file(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """
    ตรวจสอบว่าไฟล์ QA มี columns ครบถ้วน
    Returns: (is_valid, missing_columns)
    """
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    return len(missing) == 0, missing


def normalize_qa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize ข้อมูล QA:
    - Rename columns ให้ตรงกับ BQ schema
    - แปลง data types
    - สร้าง unique_id
    - ตัด whitespace
    """
    # เลือกเฉพาะ columns ที่มีใน mapping (ทั้ง required และ optional เช่น PROBLEM TYPE)
    cols_to_use = [c for c in QA_COLUMN_MAP.keys() if c in df.columns]
    df = df[cols_to_use].copy()

    # Rename columns
    df = df.rename(columns={k: v for k, v in QA_COLUMN_MAP.items() if k in df.columns})

    # ตัด whitespace ทุก string column
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": None, "None": None, "": None})

    # แปลง complaint_date เป็น date string (YYYY-MM-DD)
    if "complaint_date" in df.columns:
        df["complaint_date"] = pd.to_datetime(df["complaint_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df["complaint_date"] = df["complaint_date"].where(df["complaint_date"].notna(), None)

    # แปลง lot_date เป็น date string (YYYY-MM-DD)
    if "lot_date" in df.columns:
        df["lot_date"] = pd.to_datetime(df["lot_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df["lot_date"] = df["lot_date"].where(df["lot_date"].notna(), None)

    # สร้าง unique_id = case_id + "_" + complaint_date + "_" + store_name
    df["unique_id"] = (
        df["case_id"].fillna("") + "_" +
        df["complaint_date"].fillna("") + "_" +
        df["store_name"].fillna("")
    )

    # เพิ่ม upload_timestamp
    df["upload_timestamp"] = datetime.now(timezone.utc).isoformat()

    # ลบ rows ที่ unique_id เป็นค่าไม่สมบูรณ์
    df = df[df["unique_id"].str.len() > 2]

    return df


def enrich_with_product_ref(df: pd.DataFrame, ref_map: dict[str, dict]) -> tuple[pd.DataFrame, list[str]]:
    """เพิ่มข้อมูลสินค้าจาก product_ref โดย lookup ด้วย product_code

    กลยุทธ์ Hybrid:
    - เจอใน Ref  → ใช้ข้อมูลจาก Ref (มาตรฐาน) override ข้อมูลจากไฟล์ QA
    - ไม่เจอใน Ref → ใช้ข้อมูลที่กรอกในไฟล์ QA เป็น fallback (ไม่ทำให้ NULL)

    Returns: (enriched_df, list ของ product_code ที่หาไม่เจอใน Ref)
    """
    missing_codes = []

    # เพิ่ม field ที่ยังไม่มีใน df (กรณีไฟล์ QA ไม่มี column นั้น)
    for field in PRODUCT_FIELDS:
        if field not in df.columns:
            df[field] = None

    for idx, row in df.iterrows():
        code = row.get("product_code")
        if code and code in ref_map:
            # เจอใน Ref → override ด้วยข้อมูล Ref (มาตรฐาน)
            for field in PRODUCT_FIELDS:
                df.at[idx, field] = ref_map[code].get(field)
        elif code:
            # ไม่เจอใน Ref → เก็บข้อมูลจากไฟล์ QA ไว้ (fallback)
            # ไม่ต้อง set เป็น None เพราะ normalize_qa อ่านมาแล้ว
            missing_codes.append(code)

    return df, list(set(missing_codes))


def df_to_bq_rows(df: pd.DataFrame) -> list[dict]:
    """แปลง DataFrame เป็น list of dicts สำหรับ BQ insert"""
    rows = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            # จัดการ NaN/None/pd.NA
            if pd.isna(val) if not isinstance(val, str) else False:
                record[col] = None
            elif val == "nan" or val == "None":
                record[col] = None
            else:
                # แปลง int64 เป็น Python int ปกติ
                if hasattr(val, "item"):
                    val = val.item()
                record[col] = val
        rows.append(record)
    return rows


def get_summary_stats(df: pd.DataFrame) -> dict:
    """สรุปสถิติไฟล์ QA ก่อน upload"""
    return {
        "total_rows": len(df),
        "missing_problem": int(df["problem"].isna().sum()) if "problem" in df.columns else 0,
        "missing_action": int(df["action_details"].isna().sum()) if "action_details" in df.columns else 0,
        "missing_person": int(df["person_in_charge"].isna().sum()) if "person_in_charge" in df.columns else 0,
        "unique_cases": df["case_id"].nunique() if "case_id" in df.columns else 0,
        "regions": df["region"].nunique() if "region" in df.columns else 0,
    }
