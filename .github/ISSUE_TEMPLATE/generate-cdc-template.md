---
name: Generate CDC Template
about: Create template cdc
title: "[CDC_TEMPLATE_NAME_FILE]"
labels: ''
assignees: ''

---

{
"schema": "INDOM_ZARA",
"table": "ESTADO_SUBPEDIDO",
"connections_file": "connections.yml",
"origin_connection": "indom_zara_ir",
"sf_origin_prefix": "IEECDB2V1",
"sf_bbdd": "SECURE_RESOURCES",
"sf_bbdd_target": "DIGITAL_DATA",
"sf_cdc_table": "NOT_YET",
"sf_cdc_il_table": "IL_IEECDB2V1_INDOM_ZARA_ESTADO_SUBPEDIDO_385224543",
"sf_cdc_schema": "LANDING",
"sf_snap_schema": "FLATTENED",
"sf_datasource_schema": "RAW",
"sf_landing_schema": "LANDING",
"sf_flat_schema": "FLATTENED",
"sf_warehouse": "DIGITAL_DATA_PIPELINE_RT_WH_MED",
"sf_table": "ESTADO_SUBPEDIDO",
"maintenance_user_list": ["MAINTENANCE_USER1", "MAINTENANCE_USER2"],
"fields_to_hash": [],
"fields_truncated": {}
}
