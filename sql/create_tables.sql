-- ============================================================
-- BigQuery DDL: สร้าง Tables สำหรับ QA/QC Reporting System
-- Project: jbp-qa-qc
-- Dataset: qc_qa_reporting
-- ============================================================
-- วิธีใช้: Copy แต่ละ block ไปรันใน BigQuery Console
-- ============================================================


-- ============================================================
-- TABLE 1: qa_complaints
-- ข้อมูลข้อร้องเรียนจากลูกค้า
-- ============================================================
CREATE TABLE IF NOT EXISTS `jbp-qa-qc.qc_qa_reporting.qa_complaints` (
  unique_id         STRING    NOT NULL OPTIONS(description="CASE_ID_DATE_STORE_NAME — ใช้เป็น Upsert Key"),
  case_id           STRING    OPTIONS(description="รหัสเคส เช่น QA0001"),
  complaint_date    DATE      OPTIONS(description="วันที่ร้องเรียน — ใช้กับ Looker Studio Date Range Control"),
  country           STRING,
  region            STRING    OPTIONS(description="ภูมิภาค เช่น Northern Region"),
  province          STRING,
  shop_type         STRING    OPTIONS(description="ประเภทร้านค้า เช่น Modern Trade"),
  store_name        STRING,
  product_code      STRING    OPTIONS(description="รหัสสินค้า — Key เชื่อมกับ QC"),
  product_name      STRING,
  product_type      STRING,
  brand             STRING,
  product_subtype   STRING,
  product_series    STRING,
  product_grade     STRING,
  product_line      STRING    OPTIONS(description="สีระบบน้ำ / สีระบบน้ำมัน"),
  lot               STRING    OPTIONS(description="หมายเลข Lot — Key เชื่อมกับ QC"),
  lot_date          DATE,
  color_no          STRING,
  primer            STRING,
  problem           STRING    OPTIONS(description="ปัญหาที่ร้องเรียน อาจว่างรอสรุปผล"),
  problem_type      STRING    OPTIONS(description="หมวดหมู่ของปัญหา เช่น คุณภาพ / การบริการ"),
  inspector         STRING,
  action_details    STRING    OPTIONS(description="รายละเอียดการดำเนินการ อาจว่างรอสรุปผล"),
  corrective_action STRING    OPTIONS(description="การแก้ไข อาจว่างรอสรุปผล"),
  person_in_charge  STRING    OPTIONS(description="ผู้รับผิดชอบ อาจว่างรอสรุปผล"),
  upload_timestamp  TIMESTAMP OPTIONS(description="เวลาที่ upload เข้าระบบ")
)
OPTIONS(
  description="ข้อมูลข้อร้องเรียนจากลูกค้า — Upsert by unique_id"
);


-- ============================================================
-- TABLE 2: qc_inspections
-- ข้อมูลตรวจสอบคุณภาพสินค้า Auto Tint
-- ============================================================
CREATE TABLE IF NOT EXISTS `jbp-qa-qc.qc_qa_reporting.qc_inspections` (
  -- Unique Key (composite: case_id + inspection_date + product_code + lot + qc_number)
  qc_unique_id         STRING    OPTIONS(description="Composite key สำหรับตรวจสอบซ้ำ"),

  -- Basic Info
  case_id              STRING    NOT NULL OPTIONS(description="รหัสเคส QC เช่น QC0001"),
  inspection_date      DATE      OPTIONS(description="วันที่ตรวจสอบ"),
  month                INT64     OPTIONS(description="เดือน 1-12"),
  product_type         STRING    OPTIONS(description="ประเภทผลิตภัณฑ์ — จาก product_ref"),
  product_name         STRING    OPTIONS(description="ชื่อสินค้า — จาก product_ref"),
  brand                STRING    OPTIONS(description="แบรนด์ — จาก product_ref"),
  product_subtype      STRING    OPTIONS(description="ประเภทย่อย — จาก product_ref"),
  product_series       STRING    OPTIONS(description="ซีรีส์ — จาก product_ref"),
  product_grade        STRING    OPTIONS(description="เกรด — จาก product_ref"),
  product_line         STRING    OPTIONS(description="สายผลิตภัณฑ์ — จาก product_ref"),
  base                 STRING    OPTIONS(description="Base สี เช่น A, B, C"),
  product_code         STRING    OPTIONS(description="รหัสสินค้า — Key เชื่อมกับ QA และ product_ref"),
  lot                  STRING    OPTIONS(description="หมายเลข Lot — Key เชื่อมกับ QA"),
  qc_number            INT64     OPTIONS(description="ครั้งที่ตรวจ: 1=แรก, 2=ส่งกลับปรับแล้วตรวจใหม่"),
  status               STRING    OPTIONS(description="สถานะ: ผ่าน / ไม่ผ่าน"),

  -- T-Series: เกณฑ์ทดสอบ (TRUE=ผ่าน, FALSE=ไม่ผ่าน)
  t1_clarity           BOOL      OPTIONS(description="T1 ความละเอียด"),
  t2_color             BOOL      OPTIONS(description="T2 เฉดสี"),
  t3_viscosity         BOOL      OPTIONS(description="T3 ความหนืด"),
  t4_density           BOOL      OPTIONS(description="T4 ความหนาแน่น"),
  t5_hide_power        BOOL      OPTIONS(description="T5 ค่ากำลังซ่อนแสง"),
  t6_ph                BOOL      OPTIONS(description="T6 ค่ากรด-ด่าง pH"),
  t7_gloss             BOOL      OPTIONS(description="T7 ค่าความเงา"),
  t8_adhesion          BOOL      OPTIONS(description="T8 ค่าความติดแน่น"),
  t9_performance       BOOL      OPTIONS(description="T9 คุณสมบัติการใช้งาน"),
  t10_request_sample   BOOL      OPTIONS(description="T10 ขอตัวอย่างใหม่ — ไม่มีค่า D คู่"),

  -- D-Series: ค่าจริงที่วัดได้
  d1_clarity           FLOAT64   OPTIONS(description="D1 ค่าจริงความละเอียด"),
  d2_1_color_upper          FLOAT64   OPTIONS(description="D2.1 ค่าเฉดสีช่วงบน QC#1 — ค่าที่วัดได้"),
  d2_2_color_lower          FLOAT64   OPTIONS(description="D2.2 ค่าเฉดสีช่วงล่าง QC#1 — ค่าที่วัดได้"),
  d2_3_color_upper_approved FLOAT64   OPTIONS(description="D2.3 ค่าเฉดสีช่วงบนที่อนุมัติ — ค่ามาตรฐาน"),
  d2_4_color_lower_approved FLOAT64   OPTIONS(description="D2.4 ค่าเฉดสีช่วงล่างที่อนุมัติ — ค่ามาตรฐาน"),
  d3_viscosity              FLOAT64   OPTIONS(description="D3 ค่าจริงความหนืด"),
  d4_density           FLOAT64   OPTIONS(description="D4 ค่าจริงความหนาแน่น"),
  d5_hide_power        FLOAT64   OPTIONS(description="D5 ค่าจริงกำลังซ่อนแสง"),
  d6_ph                FLOAT64   OPTIONS(description="D6 ค่าจริง pH"),
  d7_1_gloss_upper     FLOAT64   OPTIONS(description="D7.1 ความเงาบน"),
  d7_2_gloss_lower     FLOAT64   OPTIONS(description="D7.2 ความเงาล่าง"),
  d8_adhesion          FLOAT64   OPTIONS(description="D8 ค่าจริงความติดแน่น"),
  d9_performance       FLOAT64   OPTIONS(description="D9 ค่าจริงคุณสมบัติการใช้งาน"),

  -- Chemical Adjustments: เคมีที่ใช้ปรับคุณภาพ
  chemical_1           STRING    OPTIONS(description="เคมีปรับคุณภาพ ตัวที่ 1"),
  quantity_1_pct       FLOAT64   OPTIONS(description="ปริมาณเคมี 1 (%)"),
  chemical_2           STRING    OPTIONS(description="เคมีปรับคุณภาพ ตัวที่ 2"),
  quantity_2_pct       FLOAT64   OPTIONS(description="ปริมาณเคมี 2 (%)"),
  chemical_3           STRING    OPTIONS(description="เคมีปรับคุณภาพ ตัวที่ 3"),
  quantity_3_pct       FLOAT64   OPTIONS(description="ปริมาณเคมี 3 (%)"),
  chemical_4           STRING    OPTIONS(description="เคมีปรับคุณภาพ ตัวที่ 4"),
  quantity_4_pct       FLOAT64   OPTIONS(description="ปริมาณเคมี 4 (%)"),

  -- Additional
  time_used            STRING    OPTIONS(description="เวลาที่ใช้ในการปรับ"),
  note                 STRING    OPTIONS(description="บันทึกเพิ่มเติมของ Lab เช่น สภาพแวดล้อม, ผู้ตรวจ"),
  remarks              STRING    OPTIONS(description="หมายเหตุ: Master=สูตรสำเร็จ, RD1=สูตรทดลอง"),
  upload_timestamp     TIMESTAMP OPTIONS(description="เวลาที่ upload เข้าระบบ")
)
OPTIONS(
  description="ข้อมูลตรวจสอบคุณภาพ Auto Tint — 1 Lot อาจมีหลาย record ตาม qc_number"
);


-- ============================================================
-- TABLE 3: product_ref
-- ตาราง Master สินค้า — ใช้ร่วมกันระหว่าง QA และ QC
-- Upload แบบ Overwrite ทั้งหมดทุกครั้ง
-- ============================================================
CREATE TABLE IF NOT EXISTS `jbp-qa-qc.qc_qa_reporting.product_ref` (
  product_code    STRING    NOT NULL OPTIONS(description="รหัสสินค้า — Primary Key เชื่อมกับ QA/QC"),
  product_name    STRING    OPTIONS(description="ชื่อสินค้า"),
  product_type    STRING    OPTIONS(description="ประเภทผลิตภัณฑ์"),
  brand           STRING    OPTIONS(description="แบรนด์"),
  product_subtype STRING    OPTIONS(description="ประเภทย่อย"),
  product_series  STRING    OPTIONS(description="ซีรีส์"),
  product_grade   STRING    OPTIONS(description="เกรด"),
  product_line    STRING    OPTIONS(description="สายผลิตภัณฑ์"),
  updated_at      TIMESTAMP OPTIONS(description="เวลาที่ upload/อัพเดตล่าสุด")
)
OPTIONS(
  description="ตาราง Master สินค้า — ใช้ lookup product_code → product details สำหรับ QA/QC"
);
