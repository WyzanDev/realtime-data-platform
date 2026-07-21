"""
Đẩy metadata quản trị dữ liệu (governance) lên DataHub cho ba pipeline.

Script tạo trong DataHub:

  1. Dataset cho các bảng ở mọi tầng (nguồn → Bronze → Silver/Gold → Feature).
  2. DataFlow + DataJob cho ba pipeline DP1/DP2/DP3, kèm **lineage**: mỗi
     pipeline nối tới bảng đầu vào và đầu ra của nó. Đây là phần "Lineage
     between the pipeline and tables".
  3. **Assertion** (kết quả kiểm tra dữ liệu) cho các bảng quan trọng — phần
     "Data validation".
  4. **Data Contract** cho một bảng đại diện của mỗi DP — phần "Data contract".

Chạy trong container `datahub-actions` (đã có sẵn thư viện datahub), nối tới
GMS nội bộ `http://datahub-gms:8080`.
"""

from __future__ import annotations

import time

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_dataset_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    DataContractPropertiesClass,
    DataContractStateClass,
    DataContractStatusClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataQualityContractClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    DatasetPropertiesClass,
)

GMS = "http://datahub-gms:8080"
ENV = "PROD"


def ds(platform: str, name: str) -> str:
    """Tạo URN dataset cho một bảng.

    Args:
        platform: nền tảng lưu trữ (s3, postgres, kafka, hive).
        name: tên bảng đủ tầng.

    Returns:
        Chuỗi URN của dataset.
    """
    return make_dataset_urn(platform, name, ENV)


# --- Khai báo bảng theo tầng ---
SRC_PG = [ds("postgres", f"source_system.{t}") for t in ("customer", "restaurant", "driver")]
SRC_S3 = [ds("s3", f"raw/{t}") for t in ("order", "order_item", "menu_item", "review")]
SRC_KAFKA = [ds("kafka", "gps-topic")]

BRONZE = [
    ds("s3", f"bronze.{t}")
    for t in (
        "raw_orders", "raw_order_items", "raw_menu_items", "raw_reviews",
        "raw_customers", "raw_restaurants", "raw_drivers", "raw_delivery_events",
    )
]
SILVER = [ds("hive", f"silver.{t}") for t in ("stg_orders", "stg_delivery_events")]
GOLD = [
    ds("hive", f"gold.{t}")
    for t in (
        "dim_customer", "dim_restaurant", "dim_driver", "dim_menu_item",
        "fact_orders", "fact_delivery_events", "fact_order_items",
    )
]
FEATURE = [
    ds("hive", f"gold.{t}")
    for t in (
        "feat_customer_order_freq_7d",
        "feat_restaurant_avg_prep_time_30d",
        "feat_driver_acceptance_rate_7d",
    )
]


def emit_datasets(emitter: DatahubRestEmitter) -> None:
    """Tạo dataset tối thiểu (tên + mô tả) cho mọi bảng.

    Args:
        emitter: bộ phát REST tới GMS.
    """
    labels = {
        **{u: "Nguồn ngoài (phòng ban khác)" for u in SRC_PG + SRC_S3 + SRC_KAFKA},
        **{u: "Tầng Bronze — dữ liệu thô" for u in BRONZE},
        **{u: "Tầng Silver — đã khử trùng" for u in SILVER},
        **{u: "Tầng Gold — chiều/sự kiện" for u in GOLD},
        **{u: "Tầng Feature — chuẩn Feast" for u in FEATURE},
    }
    for urn, desc in labels.items():
        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DatasetPropertiesClass(description=desc),
            )
        )
    print(f"  Đã tạo {len(labels)} dataset", flush=True)


def emit_pipeline(
    emitter: DatahubRestEmitter,
    flow_id: str,
    job_id: str,
    description: str,
    inputs: list[str],
    outputs: list[str],
) -> None:
    """Tạo một pipeline (DataFlow + DataJob) kèm lineage vào/ra.

    Args:
        emitter: bộ phát REST.
        flow_id: mã DAG trên Airflow.
        job_id: mã task đại diện trong DAG.
        description: mô tả pipeline.
        inputs: danh sách URN dataset đầu vào.
        outputs: danh sách URN dataset đầu ra.
    """
    flow_urn = make_data_flow_urn("airflow", flow_id, ENV)
    job_urn = make_data_job_urn("airflow", flow_id, job_id, ENV)

    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(name=flow_id, description=description),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(name=job_id, type="COMMAND", description=description),
        )
    )
    # Lineage: pipeline nối tới bảng vào/ra.
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=inputs, outputDatasets=outputs, inputDatajobs=[]
            ),
        )
    )
    print(f"  Pipeline {flow_id}: {len(inputs)} vào → {len(outputs)} ra", flush=True)


def emit_assertion(
    emitter: DatahubRestEmitter,
    assertion_id: str,
    dataset_urn: str,
    field: str,
    operator: str,
    description: str,
) -> None:
    """Tạo một assertion (luật kiểm tra) và một kết quả chạy THÀNH CÔNG.

    Args:
        emitter: bộ phát REST.
        assertion_id: mã định danh assertion.
        dataset_urn: dataset được kiểm tra.
        field: cột được kiểm tra (rỗng nếu ở mức bảng).
        operator: phép kiểm (ví dụ NOT_NULL).
        description: mô tả luật.
    """
    assertion_urn = f"urn:li:assertion:{assertion_id}"
    scope = (
        DatasetAssertionScopeClass.DATASET_COLUMN
        if field
        else DatasetAssertionScopeClass.DATASET_ROWS
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=AssertionInfoClass(
                type=AssertionTypeClass.DATASET,
                description=description,
                datasetAssertion=DatasetAssertionInfoClass(
                    dataset=dataset_urn,
                    scope=scope,
                    operator=operator,
                    fields=[
                        make_schema_field_urn(dataset_urn, field)
                    ] if field else None,
                ),
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=AssertionRunEventClass(
                timestampMillis=int(time.time() * 1000),
                runId=f"run-{assertion_id}",
                assertionUrn=assertion_urn,
                asserteeUrn=dataset_urn,
                status=AssertionRunStatusClass.COMPLETE,
                result=AssertionResultClass(type=AssertionResultTypeClass.SUCCESS),
            ),
        )
    )


def make_schema_field_urn(dataset_urn: str, field: str) -> str:
    """Tạo URN cho một cột trong dataset.

    Args:
        dataset_urn: URN dataset.
        field: tên cột.

    Returns:
        URN schemaField.
    """
    return f"urn:li:schemaField:({dataset_urn},{field})"


def emit_contract(
    emitter: DatahubRestEmitter,
    contract_id: str,
    dataset_urn: str,
    assertion_ids: list[str],
) -> None:
    """Tạo một data contract cho một dataset, tham chiếu các assertion của nó.

    Data contract gói các luật chất lượng (assertion) thành cam kết chính thức
    về dữ liệu mà pipeline phải bảo đảm.

    Args:
        emitter: bộ phát REST.
        contract_id: mã định danh contract.
        dataset_urn: dataset mà contract áp lên.
        assertion_ids: danh sách mã assertion thuộc contract.
    """
    contract_urn = f"urn:li:dataContract:{contract_id}"
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=contract_urn,
            aspect=DataContractPropertiesClass(
                entity=dataset_urn,
                dataQuality=[
                    DataQualityContractClass(assertion=f"urn:li:assertion:{a}")
                    for a in assertion_ids
                ],
            ),
        )
    )
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=contract_urn,
            aspect=DataContractStatusClass(state=DataContractStateClass.ACTIVE),
        )
    )


def main() -> None:
    """Đẩy toàn bộ metadata governance lên DataHub."""
    emitter = DatahubRestEmitter(gms_server=GMS)

    print(">>> Tạo dataset...", flush=True)
    emit_datasets(emitter)

    print(">>> Tạo pipeline + lineage...", flush=True)
    emit_pipeline(
        emitter, "dp1_ingest_bronze", "ingest_and_validate",
        "Nạp dữ liệu thô vào Bronze (Ingest + Validate)",
        SRC_PG + SRC_S3 + SRC_KAFKA, BRONZE,
    )
    emit_pipeline(
        emitter, "dp2_transform_gold", "build_gold",
        "Bronze → Silver/Gold (SCD2 star schema)",
        BRONZE, SILVER + GOLD,
    )
    emit_pipeline(
        emitter, "dp3_compute_features", "build_features",
        "Gold → bảng feature (chuẩn Feast)",
        GOLD, FEATURE,
    )

    print(">>> Tạo assertion (data validation)...", flush=True)
    emit_assertion(emitter, "dp1-raw-orders-orderid", ds("s3", "bronze.raw_orders"),
                   "order_id", AssertionStdOperatorClass.NOT_NULL,
                   "DP1: raw_orders.order_id không rỗng")
    emit_assertion(emitter, "dp2-dim-customer-sk", ds("hive", "gold.dim_customer"),
                   "customer_sk", AssertionStdOperatorClass.NOT_NULL,
                   "DP2: dim_customer.customer_sk (khoá) không rỗng")
    emit_assertion(emitter, "dp2-dim-customer-iscurrent", ds("hive", "gold.dim_customer"),
                   "is_current", AssertionStdOperatorClass.NOT_NULL,
                   "DP2: dim_customer.is_current (SCD2) không rỗng")
    emit_assertion(emitter, "dp3-feat-customer-eventts",
                   ds("hive", "gold.feat_customer_order_freq_7d"),
                   "event_timestamp", AssertionStdOperatorClass.NOT_NULL,
                   "DP3: feat_customer_order_freq_7d.event_timestamp không rỗng")
    emit_assertion(emitter, "dp3-feat-customer-created",
                   ds("hive", "gold.feat_customer_order_freq_7d"),
                   "created", AssertionStdOperatorClass.NOT_NULL,
                   "DP3: feat_customer_order_freq_7d.created không rỗng")
    print("  Đã tạo 5 assertion + kết quả SUCCESS", flush=True)

    print(">>> Tạo data contract...", flush=True)
    emit_contract(emitter, "dp1-bronze-orders", ds("s3", "bronze.raw_orders"),
                  ["dp1-raw-orders-orderid"])
    emit_contract(emitter, "dp2-gold-customer", ds("hive", "gold.dim_customer"),
                  ["dp2-dim-customer-sk", "dp2-dim-customer-iscurrent"])
    emit_contract(emitter, "dp3-feat-customer", ds("hive", "gold.feat_customer_order_freq_7d"),
                  ["dp3-feat-customer-eventts", "dp3-feat-customer-created"])
    print("  Đã tạo 3 data contract (DP1/DP2/DP3)", flush=True)

    print(">>> Xong. Mở DataHub UI localhost:9002 để xem lineage/validation/contract.", flush=True)


if __name__ == "__main__":
    main()
