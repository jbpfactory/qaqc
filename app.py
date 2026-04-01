"""
QA/QC Data Upload System
Streamlit app สำหรับอัพโหลดข้อมูล QA และ QC เข้า BigQuery
"""
import streamlit as st
import pandas as pd
import io

from utils import qa_processor, qc_processor, ref_processor, bigquery_client as bq

# ============================================================
# Page Config
# ============================================================
st.set_page_config(
    page_title="JBP QA/QC Upload System",
    page_icon="🎨",
    layout="wide",
)

# ============================================================
# Header
# ============================================================
st.title("🎨 JBP QA/QC Data Upload System")
st.caption("ระบบอัพโหลดข้อมูล QA และ QC เข้า BigQuery สำหรับรายงาน Looker Studio")
st.divider()

# ============================================================
# Tabs
# ============================================================
tab_qa, tab_qc, tab_ref, tab_manage = st.tabs([
    "📋 อัพโหลดข้อมูล QA",
    "🔬 อัพโหลดข้อมูล QC",
    "📦 อัพโหลดข้อมูลสินค้า (Ref)",
    "🗑️ จัดการข้อมูล",
])


# ============================================================
# TAB 1: QA Upload
# ============================================================
with tab_qa:
    st.subheader("อัพโหลดข้อมูล QA (ข้อร้องเรียนจากลูกค้า)")
    st.info(
        "**ไฟล์ที่รองรับ:** Form QA.xlsx (Sheet: Input Data)  \n"
        "**Unique ID:** CASE ID + DATE + STORE NAME  \n"
        "**ข้อมูลสินค้า:** ดึงจากตาราง Product Ref อัตโนมัติ (ต้อง upload Ref ก่อน)"
    )

    qa_file = st.file_uploader(
        "เลือกไฟล์ Excel (.xlsx)", type=["xlsx"], key="qa_uploader"
    )

    if qa_file:
        # อ่านไฟล์
        with st.spinner("กำลังอ่านไฟล์..."):
            try:
                df_raw = pd.read_excel(qa_file, sheet_name="Input Data", header=0)
                # ลบ rows ที่ว่างทั้งหมด
                df_raw = df_raw.dropna(how="all")
            except Exception as e:
                st.error(f"❌ ไม่สามารถอ่านไฟล์ได้: {e}")
                st.stop()

        # Validate columns
        is_valid, missing_cols = qa_processor.validate_qa_file(df_raw)

        if not is_valid:
            st.error(f"❌ ไฟล์ไม่ถูกต้อง — columns ที่หายไป: {missing_cols}")
            st.stop()

        st.success(f"✅ อ่านไฟล์สำเร็จ — พบ {len(df_raw):,} rows")

        # Normalize
        with st.spinner("กำลัง normalize ข้อมูล..."):
            df_normalized = qa_processor.normalize_qa(df_raw)

        # Enrich ข้อมูลสินค้าจาก product_ref
        with st.spinner("กำลังดึงข้อมูลสินค้าจาก Product Ref..."):
            try:
                ref_map = bq.get_product_ref_map()
                df_normalized, missing_codes = qa_processor.enrich_with_product_ref(
                    df_normalized, ref_map
                )
                if missing_codes:
                    st.info(
                        f"ℹ️ พบ {len(missing_codes)} product_code ที่ไม่มีใน Ref — "
                        f"ใช้ข้อมูลสินค้าจากไฟล์ QA แทน (Fallback)  \n"
                        f"**รหัสที่หาไม่เจอใน Ref:** {', '.join(missing_codes[:10])}"
                        + (f" ...และอีก {len(missing_codes) - 10} รายการ" if len(missing_codes) > 10 else "")
                    )
                elif len(ref_map) == 0:
                    st.warning("⚠️ ยังไม่มีข้อมูลใน Product Ref — ระบบจะใช้ข้อมูลสินค้าจากไฟล์ QA แทน")
            except Exception as e:
                st.warning(f"⚠️ ไม่สามารถดึงข้อมูล Product Ref ได้: {e}")

        # Summary Stats
        stats = qa_processor.get_summary_stats(df_normalized)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("จำนวน Records", f"{stats['total_rows']:,}")
        col2.metric("Case ID ไม่ซ้ำ", f"{stats['unique_cases']:,}")
        col3.metric("ยังไม่มี Problem", f"{stats['missing_problem']:,}")
        col4.metric("ยังไม่มีผู้รับผิดชอบ", f"{stats['missing_person']:,}")

        # Preview
        with st.expander("🔍 ดูตัวอย่างข้อมูล (10 rows แรก)", expanded=False):
            st.dataframe(df_normalized.head(10), width='stretch')

        st.divider()

        # เลือกโหมดการ Upload
        upload_mode_qa = st.radio(
            "เลือกโหมดการ Upload:",
            options=["upsert", "insert_only", "reset_all"],
            format_func=lambda x: {
                "upsert":       "🔄 Upsert — ทับข้อมูลเก่าถ้าซ้ำ (แนะนำ)",
                "insert_only":  "➕ เพิ่มใหม่เท่านั้น — ถ้าซ้ำ จะข้ามและแสดงรายการที่ข้าม",
                "reset_all":    "⚠️ รีเซ็ตทั้งหมด — ลบข้อมูลเดิมทั้งหมดก่อน แล้ว upload ใหม่",
            }[x],
            key="qa_upload_mode",
        )

        if upload_mode_qa == "reset_all":
            st.warning("⚠️ โหมดนี้จะ **ลบข้อมูล QA ทั้งหมด** ออกก่อน แล้วใส่ข้อมูลจากไฟล์นี้แทน — ไม่สามารถกู้คืนได้")

        # Upload Button
        if st.button("⬆️ Upload ข้อมูล QA เข้า BigQuery", type="primary", key="qa_upload_btn"):

            if upload_mode_qa == "reset_all":
                with st.spinner("กำลังลบข้อมูลเดิมทั้งหมด..."):
                    bq.clear_all_qa_data()
                all_rows = qa_processor.df_to_bq_rows(df_normalized)
                with st.spinner(f"กำลัง insert {len(all_rows):,} records เข้า BigQuery..."):
                    errors = bq.insert_rows(bq.QA_TABLE, all_rows)
                if errors:
                    st.error(f"❌ เกิด error ระหว่าง insert: {errors[:3]}")
                else:
                    st.success(
                        f"✅ Upload สำเร็จ! (โหมด: รีเซ็ตทั้งหมด)  \n"
                        f"**เพิ่มใหม่:** {len(all_rows):,} records"
                    )
                    st.balloons()

            elif upload_mode_qa == "upsert":
                # ใช้ case_id ในการตรวจสอบ เพราะถ้าแก้ไขวันที่/ร้านค้า unique_id จะเปลี่ยน
                # แต่ case_id ยังคงเดิม ทำให้ลบ record เก่าได้ถูกต้อง
                with st.spinner("กำลังตรวจสอบข้อมูลซ้ำ..."):
                    case_ids = df_normalized["case_id"].tolist()
                    existing_case_ids = bq.get_existing_case_ids(case_ids)
                records_to_update = df_normalized[df_normalized["case_id"].isin(existing_case_ids)]
                records_to_insert = df_normalized[~df_normalized["case_id"].isin(existing_case_ids)]
                update_count = len(records_to_update)
                insert_count = len(records_to_insert)
                if update_count > 0:
                    with st.spinner(f"กำลังอัพเดต {update_count:,} records ที่ซ้ำ..."):
                        bq.delete_qa_records_by_case_ids(records_to_update["case_id"].tolist())
                all_rows = qa_processor.df_to_bq_rows(df_normalized)
                with st.spinner(f"กำลัง insert {len(all_rows):,} records เข้า BigQuery..."):
                    errors = bq.insert_rows(bq.QA_TABLE, all_rows)
                if errors:
                    st.error(f"❌ เกิด error ระหว่าง insert: {errors[:3]}")
                else:
                    st.success(
                        f"✅ Upload สำเร็จ! (โหมด: Upsert)  \n"
                        f"**เพิ่มใหม่:** {insert_count:,} records  \n"
                        f"**อัพเดต:** {update_count:,} records"
                    )
                    st.balloons()

            elif upload_mode_qa == "insert_only":
                with st.spinner("กำลังตรวจสอบข้อมูลซ้ำ..."):
                    unique_ids = df_normalized["unique_id"].tolist()
                    existing_ids = bq.get_existing_unique_ids(unique_ids)
                records_skipped = df_normalized[df_normalized["unique_id"].isin(existing_ids)]
                records_to_insert = df_normalized[~df_normalized["unique_id"].isin(existing_ids)]
                skip_count = len(records_skipped)
                insert_count = len(records_to_insert)
                if insert_count == 0:
                    st.warning(
                        f"⚠️ ไม่มีข้อมูลใหม่ที่ต้อง insert — "
                        f"ทุก record ซ้ำกับข้อมูลในระบบ ({skip_count:,} records ถูกข้าม)"
                    )
                else:
                    new_rows = qa_processor.df_to_bq_rows(records_to_insert)
                    with st.spinner(f"กำลัง insert {insert_count:,} records ใหม่เข้า BigQuery..."):
                        errors = bq.insert_rows(bq.QA_TABLE, new_rows)
                    if errors:
                        st.error(f"❌ เกิด error ระหว่าง insert: {errors[:3]}")
                    else:
                        st.success(
                            f"✅ Upload สำเร็จ! (โหมด: เพิ่มใหม่เท่านั้น)  \n"
                            f"**เพิ่มใหม่:** {insert_count:,} records  \n"
                            f"**ข้ามเพราะซ้ำ:** {skip_count:,} records"
                        )
                        st.balloons()
                if skip_count > 0:
                    with st.expander(f"📋 รายการที่ถูกข้าม ({skip_count:,} records)", expanded=(insert_count == 0)):
                        show_cols = [c for c in ["unique_id", "case_id", "complaint_date", "store_name"] if c in records_skipped.columns]
                        st.dataframe(records_skipped[show_cols].reset_index(drop=True), width='stretch')


# ============================================================
# TAB 2: QC Upload
# ============================================================
with tab_qc:
    st.subheader("อัพโหลดข้อมูล QC (ตรวจสอบคุณภาพ Auto Tint)")
    st.info(
        "**ไฟล์ที่รองรับ:** Form QC.xlsx (Sheet: Input Data)  \n"
        "**ข้อมูลที่ตรวจ:** เฉพาะสินค้า Auto Tint เท่านั้น  \n"
        "**ข้อมูลสินค้า:** ดึงจากตาราง Product Ref อัตโนมัติ (ต้อง upload Ref ก่อน)"
    )

    qc_file = st.file_uploader(
        "เลือกไฟล์ Excel (.xlsx)", type=["xlsx"], key="qc_uploader"
    )

    if qc_file:
        # อ่านไฟล์
        with st.spinner("กำลังอ่านไฟล์..."):
            try:
                df_raw = pd.read_excel(qc_file, sheet_name="Input Data", header=0)
                df_raw = df_raw.dropna(how="all")
            except Exception as e:
                st.error(f"❌ ไม่สามารถอ่านไฟล์ได้: {e}")
                st.stop()

        # Validate columns
        is_valid, missing_cols = qc_processor.validate_qc_file(df_raw)

        if not is_valid:
            st.error(f"❌ ไฟล์ไม่ถูกต้อง — columns ที่หายไป: {missing_cols}")
            st.stop()

        st.success(f"✅ อ่านไฟล์สำเร็จ — พบ {len(df_raw):,} rows")

        # Normalize
        with st.spinner("กำลัง normalize ข้อมูล..."):
            df_normalized = qc_processor.normalize_qc(df_raw)

        # Enrich ข้อมูลสินค้าจาก product_ref
        with st.spinner("กำลังดึงข้อมูลสินค้าจาก Product Ref..."):
            try:
                ref_map = bq.get_product_ref_map()
                df_normalized, missing_codes = qc_processor.enrich_with_product_ref(
                    df_normalized, ref_map
                )
                if missing_codes:
                    st.warning(
                        f"⚠️ พบ {len(missing_codes)} product_code ที่ไม่มีใน Ref — "
                        f"ข้อมูลสินค้าจะเป็นค่าว่าง (QC ไม่มี fallback จากไฟล์)  \n"
                        f"**รหัสที่หาไม่เจอใน Ref:** {', '.join(missing_codes[:10])}"
                        + (f" ...และอีก {len(missing_codes) - 10} รายการ" if len(missing_codes) > 10 else "")
                    )
                elif len(ref_map) == 0:
                    st.warning("⚠️ ยังไม่มีข้อมูลใน Product Ref — กรุณา upload ไฟล์ Ref ก่อนที่ Tab 📦")
            except Exception as e:
                st.warning(f"⚠️ ไม่สามารถดึงข้อมูล Product Ref ได้: {e}")

        # Summary Stats
        stats = qc_processor.get_summary_stats(df_normalized)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("จำนวน Records", f"{stats['total_rows']:,}")
        col2.metric("Records ที่ไม่ซ้ำ", f"{stats['unique_records']:,}")
        col3.metric("ผ่าน", f"{stats['passed_count']:,}")
        col4.metric("ไม่ผ่าน", f"{stats['failed_count']:,}")

        # Preview
        with st.expander("🔍 ดูตัวอย่างข้อมูล (10 rows แรก)", expanded=False):
            # แสดงเฉพาะ columns หลัก
            preview_cols = [c for c in [
                "case_id", "inspection_date", "product_type", "product_name",
                "product_code", "lot", "qc_number", "status",
                "t1_clarity", "t2_color", "t3_viscosity", "t4_density"
            ] if c in df_normalized.columns]
            st.dataframe(df_normalized[preview_cols].head(10), width='stretch')

        st.divider()

        # เลือกโหมดการ Upload
        upload_mode_qc = st.radio(
            "เลือกโหมดการ Upload:",
            options=["upsert", "insert_only", "reset_all"],
            format_func=lambda x: {
                "upsert":       "🔄 Upsert — ทับข้อมูลเก่าถ้าซ้ำ (แนะนำ)",
                "insert_only":  "➕ เพิ่มใหม่เท่านั้น — ถ้าซ้ำ จะข้ามและแสดงรายการที่ข้าม",
                "reset_all":    "⚠️ รีเซ็ตทั้งหมด — ลบข้อมูลเดิมทั้งหมดก่อน แล้ว upload ใหม่",
            }[x],
            key="qc_upload_mode",
        )

        if upload_mode_qc == "reset_all":
            st.warning("⚠️ โหมดนี้จะ **ลบข้อมูล QC ทั้งหมด** ออกก่อน แล้วใส่ข้อมูลจากไฟล์นี้แทน — ไม่สามารถกู้คืนได้")

        # Upload Button
        if st.button("⬆️ Upload ข้อมูล QC เข้า BigQuery", type="primary", key="qc_upload_btn"):

            if upload_mode_qc == "reset_all":
                with st.spinner("กำลังลบข้อมูลเดิมทั้งหมด..."):
                    bq.clear_all_qc_data()
                all_rows = qc_processor.df_to_bq_rows(df_normalized)
                with st.spinner(f"กำลัง insert {len(all_rows):,} records เข้า BigQuery..."):
                    errors = bq.insert_rows(bq.QC_TABLE, all_rows)
                if errors:
                    st.error(f"❌ เกิด error ระหว่าง insert: {errors[:3]}")
                else:
                    st.success(
                        f"✅ Upload สำเร็จ! (โหมด: รีเซ็ตทั้งหมด)  \n"
                        f"**เพิ่มใหม่:** {len(all_rows):,} records"
                    )
                    st.balloons()

            elif upload_mode_qc == "upsert":
                with st.spinner("กำลังตรวจสอบข้อมูลซ้ำ..."):
                    all_unique_ids = df_normalized["qc_unique_id"].tolist()
                    existing_ids = bq.get_qc_existing_unique_ids(all_unique_ids)
                records_to_update = df_normalized[df_normalized["qc_unique_id"].isin(existing_ids)]
                records_to_insert = df_normalized[~df_normalized["qc_unique_id"].isin(existing_ids)]
                update_count = len(records_to_update)
                insert_count = len(records_to_insert)
                if update_count > 0:
                    with st.spinner(f"กำลังอัพเดต {update_count:,} records ที่ซ้ำ..."):
                        bq.delete_qc_records(records_to_update["qc_unique_id"].tolist())
                all_rows = qc_processor.df_to_bq_rows(df_normalized)
                with st.spinner(f"กำลัง insert {len(all_rows):,} records เข้า BigQuery..."):
                    errors = bq.insert_rows(bq.QC_TABLE, all_rows)
                if errors:
                    st.error(f"❌ เกิด error ระหว่าง insert: {errors[:3]}")
                else:
                    st.success(
                        f"✅ Upload สำเร็จ! (โหมด: Upsert)  \n"
                        f"**เพิ่มใหม่:** {insert_count:,} records  \n"
                        f"**อัพเดต:** {update_count:,} records"
                    )
                    st.balloons()

            elif upload_mode_qc == "insert_only":
                with st.spinner("กำลังตรวจสอบข้อมูลซ้ำ..."):
                    all_unique_ids = df_normalized["qc_unique_id"].tolist()
                    existing_ids = bq.get_qc_existing_unique_ids(all_unique_ids)
                records_skipped = df_normalized[df_normalized["qc_unique_id"].isin(existing_ids)]
                records_to_insert = df_normalized[~df_normalized["qc_unique_id"].isin(existing_ids)]
                skip_count = len(records_skipped)
                insert_count = len(records_to_insert)
                if insert_count == 0:
                    st.warning(
                        f"⚠️ ไม่มีข้อมูลใหม่ที่ต้อง insert — "
                        f"ทุก record ซ้ำกับข้อมูลในระบบ ({skip_count:,} records ถูกข้าม)"
                    )
                else:
                    new_rows = qc_processor.df_to_bq_rows(records_to_insert)
                    with st.spinner(f"กำลัง insert {insert_count:,} records ใหม่เข้า BigQuery..."):
                        errors = bq.insert_rows(bq.QC_TABLE, new_rows)
                    if errors:
                        st.error(f"❌ เกิด error ระหว่าง insert: {errors[:3]}")
                    else:
                        st.success(
                            f"✅ Upload สำเร็จ! (โหมด: เพิ่มใหม่เท่านั้น)  \n"
                            f"**เพิ่มใหม่:** {insert_count:,} records  \n"
                            f"**ข้ามเพราะซ้ำ:** {skip_count:,} records"
                        )
                        st.balloons()
                if skip_count > 0:
                    with st.expander(f"📋 รายการที่ถูกข้าม ({skip_count:,} records)", expanded=(insert_count == 0)):
                        show_cols = [c for c in ["qc_unique_id", "case_id", "inspection_date", "product_code", "lot", "qc_number"] if c in records_skipped.columns]
                        st.dataframe(records_skipped[show_cols].reset_index(drop=True), width='stretch')


# ============================================================
# TAB 3: Product Ref Upload
# ============================================================
with tab_ref:
    st.subheader("อัพโหลดข้อมูลสินค้า (Product Reference)")
    st.info(
        "**ไฟล์ที่รองรับ:** Ref Product code.xlsx (Sheet: product_code)  \n"
        "**วิธีการ upload:** Overwrite ทั้งหมด — ข้อมูลเก่าจะถูกลบ แล้วใส่ข้อมูลใหม่แทนที่  \n"
        "**สำคัญ:** ต้อง upload ไฟล์นี้ก่อน upload QA/QC เพื่อให้ข้อมูลสินค้าถูกเชื่อมโยงได้"
    )

    ref_file = st.file_uploader(
        "เลือกไฟล์ Excel (.xlsx)", type=["xlsx"], key="ref_uploader"
    )

    if ref_file:
        # อ่านไฟล์
        with st.spinner("กำลังอ่านไฟล์..."):
            try:
                df_raw = pd.read_excel(ref_file, sheet_name="product_code", header=0)
                df_raw = df_raw.dropna(how="all")
            except Exception as e:
                st.error(f"❌ ไม่สามารถอ่านไฟล์ได้: {e}")
                st.stop()

        # Validate columns
        is_valid, missing_cols = ref_processor.validate_ref_file(df_raw)

        if not is_valid:
            st.error(f"❌ ไฟล์ไม่ถูกต้อง — columns ที่หายไป: {missing_cols}")
            st.stop()

        st.success(f"✅ อ่านไฟล์สำเร็จ — พบ {len(df_raw):,} rows")

        # Normalize
        with st.spinner("กำลัง normalize ข้อมูล..."):
            df_normalized = ref_processor.normalize_ref(df_raw)

        # Summary Stats
        stats = ref_processor.get_summary_stats(df_normalized)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("จำนวนสินค้า", f"{stats['total_products']:,}")
        col2.metric("รหัสไม่ซ้ำ", f"{stats['unique_codes']:,}")
        col3.metric("จำนวน Brand", f"{stats['unique_brands']:,}")
        col4.metric("จำนวนประเภท", f"{stats['unique_types']:,}")

        # Preview
        with st.expander("🔍 ดูตัวอย่างข้อมูล (10 rows แรก)", expanded=False):
            st.dataframe(df_normalized.head(10), width='stretch')

        st.divider()

        # Upload Button
        st.warning("⚠️ การ upload จะ **ลบข้อมูลเก่าทั้งหมด** แล้วใส่ข้อมูลใหม่แทน")
        if st.button("⬆️ Upload ข้อมูลสินค้าเข้า BigQuery (Overwrite)", type="primary", key="ref_upload_btn"):
            rows = ref_processor.df_to_bq_rows(df_normalized)

            with st.spinner(f"กำลัง overwrite {len(rows):,} รายการสินค้าเข้า BigQuery..."):
                try:
                    bq.overwrite_product_ref(rows)
                    st.success(
                        f"✅ Upload สำเร็จ!  \n"
                        f"**จำนวนสินค้าทั้งหมด:** {len(rows):,} รายการ  \n"
                        f"**โหมด:** Overwrite (ลบเก่า → ใส่ใหม่ทั้งหมด)"
                    )
                    st.balloons()
                except Exception as e:
                    st.error(f"❌ เกิด error ระหว่าง upload: {e}")


# ============================================================
# TAB 4: จัดการข้อมูล (ล้างข้อมูลเก่า)
# ============================================================
with tab_manage:
    st.subheader("จัดการข้อมูลใน BigQuery")

    # แสดงจำนวน rows ปัจจุบัน
    st.write("**จำนวนข้อมูลปัจจุบันในระบบ:**")
    with st.spinner("กำลังตรวจสอบข้อมูล..."):
        try:
            counts = bq.get_table_row_counts()
            col1, col2, col3 = st.columns(3)
            col1.metric("📋 QA Complaints", f"{counts.get('qa', 0):,} rows")
            col2.metric("🔬 QC Inspections", f"{counts.get('qc', 0):,} rows")
            col3.metric("📦 Product Ref", f"{counts.get('ref', 0):,} rows")
        except Exception as e:
            st.warning(f"ไม่สามารถตรวจสอบจำนวนข้อมูลได้: {e}")

    st.divider()

    # ล้างข้อมูล QA
    st.write("### ล้างข้อมูล QA (qa_complaints)")
    st.error("⚠️ การลบข้อมูลไม่สามารถกู้คืนได้ — กรุณาแน่ใจก่อนดำเนินการ")
    confirm_qa = st.checkbox(
        "ฉันเข้าใจและยืนยันว่าต้องการลบข้อมูล QA ทั้งหมด",
        key="confirm_clear_qa"
    )
    if st.button(
        "🗑️ ลบข้อมูล QA ทั้งหมด",
        type="primary",
        disabled=not confirm_qa,
        key="btn_clear_qa"
    ):
        with st.spinner("กำลังลบข้อมูล QA..."):
            try:
                deleted = bq.clear_all_qa_data()
                st.success(f"✅ ลบข้อมูล QA สำเร็จ — {deleted:,} rows ถูกลบ")
                st.info("ตอนนี้สามารถ upload ข้อมูล QA ใหม่ได้ที่ Tab 📋")
            except Exception as e:
                st.error(f"❌ เกิดข้อผิดพลาด: {e}")

    st.divider()

    # ล้างข้อมูล QC
    st.write("### ล้างข้อมูล QC (qc_inspections)")
    st.error("⚠️ การลบข้อมูลไม่สามารถกู้คืนได้ — กรุณาแน่ใจก่อนดำเนินการ")
    confirm_qc = st.checkbox(
        "ฉันเข้าใจและยืนยันว่าต้องการลบข้อมูล QC ทั้งหมด",
        key="confirm_clear_qc"
    )
    if st.button(
        "🗑️ ลบข้อมูล QC ทั้งหมด",
        type="primary",
        disabled=not confirm_qc,
        key="btn_clear_qc"
    ):
        with st.spinner("กำลังลบข้อมูล QC..."):
            try:
                deleted = bq.clear_all_qc_data()
                st.success(f"✅ ลบข้อมูล QC สำเร็จ — {deleted:,} rows ถูกลบ")
                st.info("ตอนนี้สามารถ upload ข้อมูล QC ใหม่ได้ที่ Tab 🔬")
            except Exception as e:
                st.error(f"❌ เกิดข้อผิดพลาด: {e}")


# ============================================================
# Footer
# ============================================================
st.divider()
st.caption(
    "JBP QA/QC Upload System | ข้อมูลถูกเก็บใน BigQuery Dataset: `jbp-qa-qc.qc_qa_reporting`"
)
