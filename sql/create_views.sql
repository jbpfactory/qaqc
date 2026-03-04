-- ============================================================
-- BigQuery Views สำหรับ QA/QC Reporting
-- Project: jbp-qa-qc | Dataset: qc_qa_reporting
-- ============================================================
-- วิธีใช้: Copy แต่ละ block ไปรันใน BigQuery Console ตามลำดับ
-- ============================================================


-- ============================================================
-- VIEW 1: v_qc_chemical_summary
-- สำหรับ QC4: สรุปเคมีที่ใช้ปรับคุณภาพ (SUM สะสมต่อ Lot)
-- ตัวอย่าง: ครั้งที่1 น้ำ 2% + ครั้งที่2 น้ำ 1% → น้ำ = 3%
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qc_chemical_summary` AS
WITH qc1_color AS (
  -- ดึงค่าเฉดสีจากการตรวจรอบแรก (QC#1) เท่านั้น
  SELECT
    product_code,
    lot,
    ROUND(d2_1_color_upper, 2) AS color_upper_qc1,
    ROUND(d2_2_color_lower, 2) AS color_lower_qc1
  FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
  WHERE qc_number = 1
    AND d2_1_color_upper IS NOT NULL
),
chemicals AS (
  -- เคมีตัวที่ 1
  SELECT inspection_date, product_type, product_name, brand, product_subtype,
         product_series, product_grade, product_line, product_code, lot, base, remarks, note,
         chemical_1 AS chemical_name, quantity_1_pct AS quantity_pct
  FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
  WHERE chemical_1 IS NOT NULL AND chemical_1 != ''

  UNION ALL

  -- เคมีตัวที่ 2
  SELECT inspection_date, product_type, product_name, brand, product_subtype,
         product_series, product_grade, product_line, product_code, lot, base, remarks, note,
         chemical_2, quantity_2_pct
  FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
  WHERE chemical_2 IS NOT NULL AND chemical_2 != ''

  UNION ALL

  -- เคมีตัวที่ 3
  SELECT inspection_date, product_type, product_name, brand, product_subtype,
         product_series, product_grade, product_line, product_code, lot, base, remarks, note,
         chemical_3, quantity_3_pct
  FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
  WHERE chemical_3 IS NOT NULL AND chemical_3 != ''

  UNION ALL

  -- เคมีตัวที่ 4
  SELECT inspection_date, product_type, product_name, brand, product_subtype,
         product_series, product_grade, product_line, product_code, lot, base, remarks, note,
         chemical_4, quantity_4_pct
  FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
  WHERE chemical_4 IS NOT NULL AND chemical_4 != ''
)
SELECT
  -- ใช้วันแรกที่ตรวจเป็นเดือนอ้างอิง สำหรับ Date Range Control
  DATE_TRUNC(MIN(c.inspection_date), MONTH) AS inspection_month,
  c.product_type,
  c.product_name,
  c.brand,
  c.product_subtype,
  c.product_series,
  c.product_grade,
  c.product_line,
  c.product_code,
  c.lot,
  c.base,
  MAX(qc1.color_upper_qc1) AS color_upper_qc1,
  MAX(qc1.color_lower_qc1) AS color_lower_qc1,
  c.chemical_name,
  SUM(c.quantity_pct)      AS total_quantity_pct,
  MAX(c.remarks)           AS remarks,
  MAX(c.note)              AS note
FROM chemicals c
LEFT JOIN qc1_color qc1
  ON  c.product_code = qc1.product_code
  AND c.lot          = qc1.lot
GROUP BY c.product_type, c.product_name, c.brand, c.product_subtype,
         c.product_series, c.product_grade, c.product_line, c.product_code, c.lot, c.base, c.chemical_name
ORDER BY c.product_code, c.lot, c.chemical_name;


-- ============================================================
-- VIEW 2: v_qc_overview
-- สำหรับ QC3: ภาพรวมนับตาม unique Product Code+Lot (ไม่นับครั้ง)
-- ใช้ผลของ QC# ล่าสุดเป็นตัวแทนของแต่ละรุ่น
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qc_overview` AS
WITH latest_qc AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY product_code, lot
      ORDER BY qc_number DESC
    ) AS rn
  FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
)
SELECT
  DATE_TRUNC(inspection_date, MONTH) AS inspection_month,
  product_type,
  product_name,
  brand,
  product_subtype,
  product_series,
  product_grade,
  product_line,
  product_code,
  -- นับจำนวนรุ่นที่ผ่าน / ไม่ผ่าน แต่ละหัวข้อ
  COUNTIF(t1_clarity = TRUE)        AS t1_pass,
  COUNTIF(t1_clarity = FALSE)       AS t1_fail,
  COUNTIF(t2_color = TRUE)          AS t2_pass,
  COUNTIF(t2_color = FALSE)         AS t2_fail,
  COUNTIF(t3_viscosity = TRUE)      AS t3_pass,
  COUNTIF(t3_viscosity = FALSE)     AS t3_fail,
  COUNTIF(t4_density = TRUE)        AS t4_pass,
  COUNTIF(t4_density = FALSE)       AS t4_fail,
  COUNTIF(t5_hide_power = TRUE)     AS t5_pass,
  COUNTIF(t5_hide_power = FALSE)    AS t5_fail,
  COUNTIF(t6_ph = TRUE)             AS t6_pass,
  COUNTIF(t6_ph = FALSE)            AS t6_fail,
  COUNTIF(t7_gloss = TRUE)          AS t7_pass,
  COUNTIF(t7_gloss = FALSE)         AS t7_fail,
  COUNTIF(t8_adhesion = TRUE)       AS t8_pass,
  COUNTIF(t8_adhesion = FALSE)      AS t8_fail,
  COUNTIF(t9_performance = TRUE)    AS t9_pass,
  COUNTIF(t9_performance = FALSE)   AS t9_fail,
  COUNT(*)                          AS total_products
FROM latest_qc
WHERE rn = 1
GROUP BY inspection_month, product_type, product_name, brand, product_subtype,
         product_series, product_grade, product_line, product_code
ORDER BY inspection_month, product_code;


-- ============================================================
-- VIEW 3: v_qa_qc_linked
-- สำหรับ Cross-Analysis: QA complaint เชื่อมกับผล QC
-- Join ด้วย product_code + lot
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qa_qc_linked` AS
SELECT
  -- QA fields
  qa.case_id           AS qa_case_id,
  qa.complaint_date,
  qa.region,
  qa.province,
  qa.store_name,
  qa.shop_type,
  qa.product_code,
  qa.lot,
  qa.product_name,
  qa.product_type      AS qa_product_type,
  qa.brand,
  qa.product_subtype,
  qa.product_series,
  qa.product_grade,
  qa.product_line,
  qa.problem,
  qa.problem_type,
  qa.action_details,
  qa.corrective_action,
  qa.person_in_charge,

  -- QC fields
  qc.case_id           AS qc_case_id,
  qc.qc_number,
  qc.status            AS qc_status,
  qc.inspection_date,
  qc.t1_clarity,
  qc.t2_color,
  qc.t3_viscosity,
  qc.t4_density,
  qc.t5_hide_power,
  qc.t6_ph,
  qc.t7_gloss,
  qc.t8_adhesion,
  qc.t9_performance,
  -- D2: ค่าเฉดสีที่วัดได้ vs ค่าที่อนุมัติ (ใช้เปรียบเทียบใน Looker Studio)
  qc.d2_1_color_upper,
  qc.d2_2_color_lower,
  qc.d2_3_color_upper_approved,
  qc.d2_4_color_lower_approved,
  qc.remarks           AS qc_remarks,
  qc.note              AS qc_note,

  -- Risk Flag: คำนวณอัตโนมัติ
  CASE
    WHEN qc.case_id IS NULL
      THEN 'No QC Data'
    WHEN qc.status = 'ไม่ผ่าน'
      THEN '🔴 High Risk'
    WHEN qc.status = 'ผ่าน'
      THEN '🟡 Investigate'
    ELSE 'Unknown'
  END AS risk_flag

FROM `jbp-qa-qc.qc_qa_reporting.qa_complaints` qa
LEFT JOIN `jbp-qa-qc.qc_qa_reporting.qc_inspections` qc
  ON  qa.product_code = qc.product_code
  AND qa.lot          = qc.lot
WHERE qa.product_code IS NOT NULL
  AND qa.lot          IS NOT NULL
ORDER BY qa.complaint_date DESC;


-- ============================================================
-- VIEW 4: v_qa_base
-- Row-level QA data — Primary Data Source สำหรับ Looker Studio
-- รองรับทุกกราฟ: ScoreCard, Pie, Bar, Line, Heatmap, Table Drill-down
-- แก้ปัญหา: problem_type เป็น NULL → ใช้ problem_group แทน
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qa_base` AS
SELECT
  -- วันที่
  complaint_date,
  DATE_TRUNC(complaint_date, MONTH)  AS complaint_month,

  -- ข้อมูลเคส
  case_id,
  country,
  region,
  province,
  shop_type,
  store_name,

  -- ข้อมูลสินค้า
  product_code,
  lot,
  lot_date,
  color_no,
  primer,
  product_name,
  product_type,
  brand,
  product_subtype,
  product_series,
  product_grade,
  product_line,

  -- ปัญหา
  problem_type,
  problem,
  -- ป้องกัน problem_type เป็น NULL → fallback ไปที่ problem
  -- ใช้ problem_group แทน problem_type ในทุกกราฟที่แสดง "กลุ่มปัญหา"
  COALESCE(
    NULLIF(TRIM(CAST(problem_type AS STRING)), ''),
    NULLIF(TRIM(CAST(problem AS STRING)), ''),
    'ไม่ระบุ'
  ) AS problem_group,

  -- การดำเนินการ
  inspector,
  action_details,
  corrective_action,
  person_in_charge,

  -- Flags (1/0) สำหรับ AVG ใน Looker Studio → แสดงเป็น % Completion
  CASE WHEN action_details   IS NOT NULL AND TRIM(CAST(action_details   AS STRING)) != '' THEN 1 ELSE 0 END AS has_action,
  CASE WHEN corrective_action IS NOT NULL AND TRIM(CAST(corrective_action AS STRING)) != '' THEN 1 ELSE 0 END AS has_corrective,
  CASE WHEN person_in_charge  IS NOT NULL AND TRIM(CAST(person_in_charge  AS STRING)) != '' THEN 1 ELSE 0 END AS has_pic,

  upload_timestamp
FROM `jbp-qa-qc.qc_qa_reporting.qa_complaints`
ORDER BY complaint_date DESC;


-- ============================================================
-- VIEW 5: v_qc_first_pass_yield (สำหรับ FPY KPI)
-- Row-level: แต่ละแถว = 1 Lot ที่ตรวจรอบแรก (QC#1)
-- ใช้ is_passed (1/0) ให้ Looker Studio คำนวณ FPY เอง
-- วิธีใช้ใน LS: AVG(is_passed) → format เป็น Percent
-- สามารถ filter ตาม product_series, brand ฯลฯ ได้โดย FPY ยังถูกต้อง
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qc_first_pass_yield` AS
SELECT
  DATE_TRUNC(inspection_date, MONTH) AS inspection_month,
  inspection_date,
  case_id,
  product_type,
  product_name,
  brand,
  product_subtype,
  product_series,
  product_grade,
  product_line,
  product_code,
  lot,
  status,
  -- 1/0 → ใช้ SUM หรือ AVG ใน Looker Studio ได้เลย
  CASE WHEN status = 'ผ่าน' THEN 1 ELSE 0 END    AS is_passed,
  CASE WHEN status = 'ไม่ผ่าน' THEN 1 ELSE 0 END AS is_failed,
  note
FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
WHERE qc_number = 1
ORDER BY inspection_date DESC;


-- ============================================================
-- VIEW 6: v_qc_failure_by_criteria (นับซ้ำ — ทุก QC# ทุกครั้ง)
-- UNPIVOT T1-T9 เป็นแถว เพื่อดูว่าเกณฑ์ไหนไม่ผ่านบ่อยที่สุด
-- แยกตาม product_series, product_type, เดือน
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qc_failure_by_criteria` AS
SELECT
  DATE_TRUNC(inspection_date, MONTH) AS inspection_month,
  product_type,
  product_name,
  brand,
  product_subtype,
  product_series,
  product_grade,
  product_line,
  product_code,
  base,
  criteria_name,
  COUNTIF(test_result = FALSE) AS fail_count,
  COUNTIF(test_result = TRUE)  AS pass_count,
  COUNT(*)                     AS total_tested,
  note
FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
UNPIVOT (
  test_result FOR criteria_name IN (
    t1_clarity      AS 'T1 ความละเอียด',
    t2_color        AS 'T2 เฉดสี',
    t3_viscosity    AS 'T3 ความหนืด',
    t4_density      AS 'T4 ความหนาแน่น',
    t5_hide_power   AS 'T5 กำลังซ่อนแสง',
    t6_ph           AS 'T6 กรด-ด่าง',
    t7_gloss        AS 'T7 ความเงา',
    t8_adhesion     AS 'T8 ความติดแน่น',
    t9_performance  AS 'T9 คุณสมบัติการใช้งาน'
  )
)
GROUP BY inspection_month, product_type, product_name, brand, product_subtype,
         product_series, product_grade, product_line, product_code, base, criteria_name, note
ORDER BY fail_count DESC;


-- ============================================================
-- VIEW 7: v_qc_failure_by_criteria_unique (ไม่นับซ้ำ)
-- นับ 1 ครั้งต่อ Lot: ถ้า Lot เคยไม่ผ่านเกณฑ์ใดในรอบใดก็ตาม = 1
-- ตัวอย่าง: Lot A ไม่ผ่าน T2 ทั้งรอบ 1 และ 2 → นับ T2 fail = 1 (ไม่ใช่ 2)
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qc_failure_by_criteria_unique` AS
WITH unpivoted AS (
  -- UNPIVOT T1-T9 เป็นแถว (ทุก QC# ทุกรอบ)
  SELECT
    inspection_date,
    product_type,
    product_name,
    brand,
    product_subtype,
    product_series,
    product_grade,
    product_line,
    product_code,
    lot,
    base,
    criteria_name,
    test_result,
    note
  FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
  UNPIVOT (
    test_result FOR criteria_name IN (
      t1_clarity      AS 'T1 ความละเอียด',
      t2_color        AS 'T2 เฉดสี',
      t3_viscosity    AS 'T3 ความหนืด',
      t4_density      AS 'T4 ความหนาแน่น',
      t5_hide_power   AS 'T5 กำลังซ่อนแสง',
      t6_ph           AS 'T6 กรด-ด่าง',
      t7_gloss        AS 'T7 ความเงา',
      t8_adhesion     AS 'T8 ความติดแน่น',
      t9_performance  AS 'T9 คุณสมบัติการใช้งาน'
    )
  )
),
-- สรุประดับ Lot: ถ้าเคยไม่ผ่านเกณฑ์ใดใน QC# ใดก็ตาม = lot_ever_failed
-- ไม่แยกเดือน → 1 Lot = 1 แถวต่อ criteria เสมอ
lot_summary AS (
  SELECT
    -- ใช้วันแรกที่ตรวจเป็นเดือนอ้างอิง
    DATE_TRUNC(MIN(inspection_date), MONTH) AS inspection_month,
    product_type,
    product_name,
    brand,
    product_subtype,
    product_series,
    product_grade,
    product_line,
    product_code,
    lot,
    base,
    criteria_name,
    MAX(CASE WHEN test_result = FALSE THEN 1 ELSE 0 END) AS lot_ever_failed,
    MAX(note) AS note
  FROM unpivoted
  GROUP BY product_type, product_name, brand, product_subtype,
           product_series, product_grade, product_line, product_code, lot, base, criteria_name
)
-- นับจำนวน Lot ที่เคยไม่ผ่านแต่ละเกณฑ์
SELECT
  inspection_month,
  product_type,
  product_name,
  brand,
  product_subtype,
  product_series,
  product_grade,
  product_line,
  product_code,
  base,
  criteria_name,
  SUM(lot_ever_failed)                          AS fail_count,
  SUM(CASE WHEN lot_ever_failed = 0 THEN 1 ELSE 0 END) AS pass_count,
  COUNT(*)                                      AS total_lots,
  MAX(note)                                     AS note
FROM lot_summary
GROUP BY inspection_month, product_type, product_name, brand, product_subtype,
         product_series, product_grade, product_line, product_code, base, criteria_name
ORDER BY fail_count DESC;


-- ============================================================
-- VIEW 8: v_qc_max_rounds
-- Lot ไหนต้องตรวจซ้ำมากที่สุด (Max QC#)
-- ใช้ดู Lot ที่มีปัญหาคุณภาพสูง ต้องแก้ไขหลายรอบ
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qc_max_rounds` AS
SELECT
  -- ใช้วันแรกที่ตรวจเป็นเดือนอ้างอิง สำหรับ Date Range Control
  DATE_TRUNC(MIN(inspection_date), MONTH) AS inspection_month,
  case_id,
  product_type,
  product_name,
  brand,
  product_subtype,
  product_series,
  product_grade,
  product_line,
  product_code,
  lot,
  base,
  MAX(qc_number)                     AS max_qc_rounds,
  -- ดึง status จากรอบสุดท้าย
  ARRAY_AGG(status ORDER BY qc_number DESC LIMIT 1)[OFFSET(0)] AS final_status,
  -- ดึง note จากรอบสุดท้าย
  ARRAY_AGG(note ORDER BY qc_number DESC LIMIT 1)[OFFSET(0)]   AS latest_note,
  MIN(inspection_date)               AS first_inspection_date,
  MAX(inspection_date)               AS last_inspection_date
FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
GROUP BY case_id, product_type, product_name, brand, product_subtype,
         product_series, product_grade, product_line, product_code, lot, base
HAVING MAX(qc_number) > 1
ORDER BY max_qc_rounds DESC;


-- ============================================================
-- VIEW 9: v_qc_monthly_failure_summary
-- Row-level: แต่ละแถว = 1 QC record พร้อม is_latest (QC# ล่าสุดของ Lot)
-- ใช้ใน Looker Studio:
--   นับซ้ำ:    COUNT(*) WHERE is_failed=1
--   ไม่นับซ้ำ: COUNT(*) WHERE is_failed=1 AND is_latest=1
-- สามารถ filter ตาม product_series, brand ฯลฯ ได้โดยค่าถูกต้อง
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qc_monthly_failure_summary` AS
WITH base AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY product_code, lot
      ORDER BY qc_number DESC
    ) AS rn
  FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
)
SELECT
  DATE_TRUNC(inspection_date, MONTH) AS inspection_month,
  inspection_date,
  case_id,
  product_type,
  product_name,
  brand,
  product_subtype,
  product_series,
  product_grade,
  product_line,
  product_code,
  lot,
  qc_number,
  status,
  -- 1 = ไม่ผ่าน, 0 = ผ่าน → ใช้ SUM ใน LS ได้เลย
  CASE WHEN status = 'ไม่ผ่าน' THEN 1 ELSE 0 END AS is_failed,
  -- 1 = QC# ล่าสุดของ Lot นี้ → ใช้ filter "ไม่นับซ้ำ" ได้
  CASE WHEN rn = 1 THEN 1 ELSE 0 END              AS is_latest,
  note
FROM base
ORDER BY inspection_date DESC;


-- ============================================================
-- VIEW 10: v_qc_failure_with_country
-- เชื่อม QC กับ QA เพื่อดูว่า Lot ที่ไม่ผ่าน QC ถูกร้องเรียนจากประเทศไหน
-- ใช้สำหรับวิเคราะห์ปัญหาคุณภาพแยกตามประเทศ
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qc_failure_with_country` AS
SELECT
  DATE_TRUNC(qc.inspection_date, MONTH) AS inspection_month,
  qa.country,
  qa.region,
  qc.product_type,
  qc.product_name,
  qc.brand,
  qc.product_subtype,
  qc.product_series,
  qc.product_grade,
  qc.product_line,
  qc.product_code,
  qc.lot,
  qc.status                             AS qc_status,
  qc.qc_number,
  qc.note,
  qa.problem                             AS qa_problem,
  qa.problem_type                        AS qa_problem_type
FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections` qc
INNER JOIN `jbp-qa-qc.qc_qa_reporting.qa_complaints` qa
  ON qc.product_code = qa.product_code
  AND qc.lot = qa.lot
WHERE qc.status = 'ไม่ผ่าน'
ORDER BY inspection_month DESC, qa.country;


-- ============================================================
-- VIEW 11: v_qc_repeat_offenders
-- สินค้า (Product Code) ที่ไม่ผ่าน QC บ่อยที่สุด
-- สรุปจำนวน Lot ที่มีปัญหา + จำนวนครั้งที่ต้องแก้ไข
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qc_repeat_offenders` AS
SELECT
  product_code,
  product_name,
  product_type,
  brand,
  product_subtype,
  product_series,
  product_grade,
  product_line,
  COUNT(DISTINCT lot)                              AS total_lots,
  COUNTIF(status = 'ไม่ผ่าน')                      AS total_fail_inspections,
  COUNT(DISTINCT CASE WHEN status = 'ไม่ผ่าน' THEN lot END) AS lots_with_failures,
  MAX(qc_number)                                   AS max_qc_rounds_ever,
  ROUND(
    COUNTIF(status = 'ไม่ผ่าน') * 1.0 / NULLIF(COUNT(*), 0), 3
  ) AS overall_fail_rate
FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
GROUP BY product_code, product_name, product_type, brand, product_subtype,
         product_series, product_grade, product_line
HAVING COUNTIF(status = 'ไม่ผ่าน') > 0
ORDER BY total_fail_inspections DESC;


-- ============================================================
-- VIEW 12: v_qa_problem_summary
-- Pre-aggregated ปัญหา QA — สำหรับกราฟ Stacked Bar, Treemap, Pivot
-- Dimension: เดือน × กลุ่มปัญหา × ปัญหาย่อย × ภาค × ร้านค้า × สินค้า
-- ใช้ problem_group แทน problem_type เพื่อแก้ปัญหา NULL
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qa_problem_summary` AS
SELECT
  DATE_TRUNC(complaint_date, MONTH)  AS complaint_month,
  country,
  region,
  province,
  shop_type,
  product_type,
  brand,
  product_line,
  product_series,
  product_subtype,
  product_grade,
  problem_type,
  problem,
  -- กลุ่มปัญหา: fallback จาก problem_type → problem → 'ไม่ระบุ'
  COALESCE(
    NULLIF(TRIM(CAST(problem_type AS STRING)), ''),
    NULLIF(TRIM(CAST(problem     AS STRING)), ''),
    'ไม่ระบุ'
  ) AS problem_group,
  COUNT(*) AS complaint_count
FROM `jbp-qa-qc.qc_qa_reporting.qa_complaints`
GROUP BY
  DATE_TRUNC(complaint_date, MONTH),
  country, region, province, shop_type,
  product_type, brand, product_line, product_series, product_subtype, product_grade,
  problem_type, problem,
  COALESCE(
    NULLIF(TRIM(CAST(problem_type AS STRING)), ''),
    NULLIF(TRIM(CAST(problem     AS STRING)), ''),
    'ไม่ระบุ'
  )
ORDER BY complaint_month DESC, complaint_count DESC;


-- ============================================================
-- VIEW 13: v_qc_color_with_chemicals
-- ตาราง D2 (ค่าเฉดสีช่วงบน/ล่าง QC#1) + เคมีที่ใช้ปรับ
-- แต่ละแถว = 1 เคมีต่อ 1 Lot (เหมือนตารางในรายงาน)
-- ใช้ดูว่า Lot ไหนปรับสีด้วยเคมีอะไร ปริมาณเท่าไหร่
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qc_color_with_chemicals` AS
WITH qc1_color AS (
  -- ดึงค่าเฉดสีจากการตรวจรอบแรก (QC#1) เท่านั้น
  SELECT
    product_code,
    lot,
    ROUND(d2_1_color_upper, 2) AS color_upper_qc1,
    ROUND(d2_2_color_lower, 2) AS color_lower_qc1
  FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
  WHERE qc_number = 1
    AND d2_1_color_upper IS NOT NULL
)
SELECT
  chem.inspection_month,
  chem.product_type,
  chem.product_name,
  chem.brand,
  chem.product_subtype,
  chem.product_series,
  chem.product_grade,
  chem.product_line,
  chem.product_code,
  chem.lot,
  qc1.color_upper_qc1,
  qc1.color_lower_qc1,
  chem.chemical_name,
  chem.total_quantity_pct,
  chem.remarks,
  chem.note
FROM `jbp-qa-qc.qc_qa_reporting.v_qc_chemical_summary` chem
LEFT JOIN qc1_color qc1
  ON  chem.product_code = qc1.product_code
  AND chem.lot          = qc1.lot
ORDER BY chem.inspection_month DESC, chem.product_code, chem.lot, chem.chemical_name;


-- ============================================================
-- VIEW 14: v_qc_lot_criteria_pivot
-- ตาราง T1-T10 fail count per Lot (Pivot แนวนอน)
-- แต่ละแถว = 1 Lot, แต่ละคอลัมน์ = จำนวนครั้งที่ไม่ผ่านเกณฑ์นั้น
-- ค่า 0 = ผ่าน, ค่า >= 1 = ไม่ผ่านกี่รอบ
-- ใช้ใน LS: Table chart โดยตั้ง Metric = SUM, conditional format สีแดงเมื่อ > 0
-- ============================================================
CREATE OR REPLACE VIEW `jbp-qa-qc.qc_qa_reporting.v_qc_lot_criteria_pivot` AS
SELECT
  DATE_TRUNC(MIN(inspection_date), MONTH) AS inspection_month,
  MIN(inspection_date)                    AS first_inspection_date,
  product_type,
  product_name,
  brand,
  product_subtype,
  product_series,
  product_grade,
  product_line,
  product_code,
  lot,
  base,
  COUNTIF(t1_clarity    = FALSE) AS t1_fail,
  COUNTIF(t2_color      = FALSE) AS t2_fail,
  COUNTIF(t3_viscosity  = FALSE) AS t3_fail,
  COUNTIF(t4_density    = FALSE) AS t4_fail,
  COUNTIF(t5_hide_power = FALSE) AS t5_fail,
  COUNTIF(t6_ph         = FALSE) AS t6_fail,
  COUNTIF(t7_gloss      = FALSE) AS t7_fail,
  COUNTIF(t8_adhesion   = FALSE) AS t8_fail,
  COUNTIF(t9_performance       = FALSE) AS t9_fail,
  COUNTIF(t10_request_sample   = FALSE) AS t10_fail,
  MAX(qc_number)                AS max_qc_rounds,
  ARRAY_AGG(status ORDER BY qc_number DESC LIMIT 1)[OFFSET(0)] AS final_status
FROM `jbp-qa-qc.qc_qa_reporting.qc_inspections`
GROUP BY product_type, product_name, brand, product_subtype,
         product_series, product_grade, product_line, product_code, lot, base
ORDER BY inspection_month DESC, product_code, lot;
