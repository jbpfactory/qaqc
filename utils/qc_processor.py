"""
QC Processor — อ่าน, normalize และ insert ข้อมูล QC เข้า BigQuery
"""
import pandas as pd
from datetime import datetime, timezone


# Mapping: ชื่อ column ใน Excel → ชื่อ field ใน BigQuery
# หมายเหตุ: product_type ถูกลบออกจาก mapping แล้ว
# → ดึงจากตาราง product_ref แทน (ผ่าน enrich_with_product_ref)
QC_COLUMN_MAP = {
    "CaseID":                           "case_id",
    "Date":                             "inspection_date",
    "Month":                            "month",
    "Base":                             "base",
    "Product Code":                     "product_code",
    "Lot":                              "lot",
    "QC #":                             "qc_number",
    "Status":                           "status",
    # T-Series
    "T1 ความละเอียด":                   "t1_clarity",
    "T2 เฉดสี":                         "t2_color",
    "T3 ความหนืด":                      "t3_viscosity",
    "T4 ความหนาแน่น":                   "t4_density",
    "T5 ค่ากำลังซ่อนแสง":              "t5_hide_power",
    "T6 ค่ากรด-ด่าง":                  "t6_ph",
    "T7 ค่าความเงา":                    "t7_gloss",
    "T8 ค่าความติดแน่น":               "t8_adhesion",
    "T9 คุณสมบัติการใช้งาน":           "t9_performance",
    "T10 ขอตัวอย่างใหม่":              "t10_request_sample",
    # D-Series (ชื่อตรงตาม Excel จริง)
    "D1 ค่าความละเอียด":               "d1_clarity",
    "D2.1 ค่าเฉดสีช่วงบน QC#1":       "d2_1_color_upper",
    "D2.2 ค่าเฉดสีช่วงล่าง QC#1":     "d2_2_color_lower",
    "D2.3 ค่าเฉดสีช่วงบนที่อนุมัติ":  "d2_3_color_upper_approved",
    "D2.4 ค่าเฉดสีช่วงล่างที่อนุมัติ": "d2_4_color_lower_approved",
    "D3 ค่าความหนืด":                  "d3_viscosity",
    "D 4 ค่าความหนาแน่น":              "d4_density",
    "D5 ค่ากำลังซ่อนแสง":             "d5_hide_power",
    "D6 ค่ากรด-ด่าง":                  "d6_ph",
    "D7.1 ความเงาบน":                  "d7_1_gloss_upper",
    "D7.2 ความเงาล่าง":                "d7_2_gloss_lower",
    "D8 ความติดแน่น":                  "d8_adhesion",
    "D9 คุณสมบัติการใช้งาน":           "d9_performance",
    # Chemical Adjustments
    "เคมีที่ใช้ในการปรับคุณภาพ 1":    "chemical_1",
    "ปริมาณ 1 (%)":                    "quantity_1_pct",
    "เคมีที่ใช้ในการปรับคุณภาพ 2":    "chemical_2",
    "ปริมาณ 2 (%)":                    "quantity_2_pct",
    "เคมีที่ใช้ในการปรับคุณภาพ 3":    "chemical_3",
    "ปริมาณ 3 (%)":                    "quantity_3_pct",
    "เคมีที่ใช้ในการปรับคุณภาพ 4":    "chemical_4",
    "ปริมาณ 4 (%)":                    "quantity_4_pct",
    # Additional
    "สูตรที่ใช้":                       "remarks",
    "เวลาที่ QCใช้":                    "time_used",
    "ข้อมูลเพิ่มเติม":                  "note",
}

REQUIRED_COLUMNS = [
    "CaseID", "Date", "Base",
    "Product Code", "Lot", "QC #", "Status"
]

# ฟิลด์ที่ดึงจาก product_ref
PRODUCT_FIELDS = [
    "product_name", "product_type", "brand",
    "product_subtype", "product_series", "product_grade", "product_line",
]

# T-Series columns ที่ต้องแปลงเป็น BOOL
T_SERIES_COLS = [
    "t1_clarity", "t2_color", "t3_viscosity", "t4_density",
    "t5_hide_power", "t6_ph", "t7_gloss", "t8_adhesion",
    "t9_performance", "t10_request_sample"
]

# D-Series columns ที่ต้องเป็น FLOAT
D_SERIES_COLS = [
    "d1_clarity",
    "d2_1_color_upper", "d2_2_color_lower",
    "d2_3_color_upper_approved", "d2_4_color_lower_approved",
    "d3_viscosity", "d4_density", "d5_hide_power", "d6_ph",
    "d7_1_gloss_upper", "d7_2_gloss_lower", "d8_adhesion", "d9_performance"
]

# Chemical quantity columns
QUANTITY_COLS = ["quantity_1_pct", "quantity_2_pct", "quantity_3_pct", "quantity_4_pct"]


def validate_qc_file(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """
    ตรวจสอบว่าไฟล์ QC มี columns หลักครบถ้วน
    Returns: (is_valid, missing_columns)
    """
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    return len(missing) == 0, missing


def normalize_pass_fail(val) -> bool | None:
    """แปลง 'ผ่าน' → True, 'ไม่ผ่าน' → False, อื่นๆ → None"""
    if pd.isna(val):
        return None
    val_str = str(val).strip()
    if val_str == "ผ่าน":
        return True
    elif val_str == "ไม่ผ่าน":
        return False
    return None


def normalize_qc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize ข้อมูล QC:
    - Rename columns ให้ตรงกับ BQ schema
    - แปลง T-series ผ่าน/ไม่ผ่าน → BOOL
    - แปลง D-series เป็น FLOAT
    - แปลง Month formula เป็น integer จริง
    - ตัด whitespace
    """
    # เลือกเฉพาะ columns ที่มีอยู่ใน mapping
    cols_to_use = [c for c in df.columns if c in QC_COLUMN_MAP]
    df = df[cols_to_use].copy()

    # Rename columns
    df = df.rename(columns={k: v for k, v in QC_COLUMN_MAP.items() if k in df.columns})

    # ตัด whitespace ทุก string column
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": None, "None": None, "": None})

    # แปลง inspection_date เป็น date string
    if "inspection_date" in df.columns:
        df["inspection_date"] = pd.to_datetime(
            df["inspection_date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        df["inspection_date"] = df["inspection_date"].where(
            df["inspection_date"].notna(), None
        )

    # แปลง month: อาจเป็น formula result หรือ integer ปกติ
    if "month" in df.columns:
        df["month"] = pd.to_numeric(df["month"], errors="coerce").astype("Int64")

    # แปลง qc_number เป็น int
    if "qc_number" in df.columns:
        df["qc_number"] = pd.to_numeric(df["qc_number"], errors="coerce").astype("Int64")

    # แปลง T-series → BOOL
    for col in T_SERIES_COLS:
        if col in df.columns:
            df[col] = df[col].apply(normalize_pass_fail)

    # แปลง D-series → FLOAT
    for col in D_SERIES_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].where(df[col].notna(), None)

    # แปลง quantity columns → FLOAT
    for col in QUANTITY_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].where(df[col].notna(), None)

    # สร้าง qc_unique_id = case_id + inspection_date + product_code + lot + qc_number
    id_cols = ["case_id", "inspection_date", "product_code", "lot", "qc_number"]
    if all(c in df.columns for c in id_cols):
        df["qc_unique_id"] = (
            df["case_id"].fillna("") + "_" +
            df["inspection_date"].fillna("") + "_" +
            df["product_code"].fillna("") + "_" +
            df["lot"].fillna("") + "_" +
            df["qc_number"].astype(str).str.replace("<NA>", "NA")
        )

    # เพิ่ม upload_timestamp
    df["upload_timestamp"] = datetime.now(timezone.utc).isoformat()

    # ลบ rows ที่ไม่มี case_id
    if "case_id" in df.columns:
        df = df[df["case_id"].notna() & (df["case_id"] != "")]

    return df


def enrich_with_product_ref(df: pd.DataFrame, ref_map: dict[str, dict]) -> tuple[pd.DataFrame, list[str]]:
    """เพิ่มข้อมูลสินค้าจาก product_ref โดย lookup ด้วย product_code
    Returns: (enriched_df, list ของ product_code ที่หาไม่เจอ)
    """
    missing_codes = []
    for field in PRODUCT_FIELDS:
        df[field] = None

    for idx, row in df.iterrows():
        code = row.get("product_code")
        if code and code in ref_map:
            for field in PRODUCT_FIELDS:
                df.at[idx, field] = ref_map[code].get(field)
        elif code:
            missing_codes.append(code)

    return df, list(set(missing_codes))


def df_to_bq_rows(df: pd.DataFrame) -> list[dict]:
    """แปลง DataFrame เป็น list of dicts สำหรับ BQ insert"""
    rows = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            try:
                if pd.isna(val):
                    record[col] = None
                    continue
            except (TypeError, ValueError):
                pass

            if val == "nan" or val == "None":
                record[col] = None
            elif hasattr(val, "item"):
                record[col] = val.item()
            else:
                record[col] = val
        rows.append(record)
    return rows


def get_summary_stats(df: pd.DataFrame) -> dict:
    """สรุปสถิติไฟล์ QC ก่อน upload"""
    failed_count = 0
    if "status" in df.columns:
        failed_count = int((df["status"] == "ไม่ผ่าน").sum())

    return {
        "total_rows": len(df),
        "unique_records": df["qc_unique_id"].nunique() if "qc_unique_id" in df.columns else len(df),
        "unique_lots": df["lot"].nunique() if "lot" in df.columns else 0,
        "failed_count": failed_count,
        "passed_count": len(df) - failed_count,
        "has_chemical_data": int(df["chemical_1"].notna().sum()) if "chemical_1" in df.columns else 0,
    }
