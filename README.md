# Tower Claims Fabric Real-Time Intelligence Demo

This repository contains a synthetic Microsoft Fabric Real-Time Intelligence demo for a Tower Insurance NZ claims operations scenario. It is modeled on the artifact structure of the banking loan fraud sample in `ecotte/fabric-rti-demos`, but implements an original insurance claims experience and operational performance demo.

> **Synthetic demo only:** all claim, policy, customer, supplier, assessor, location, cost, SLA, and weather values are generated for demonstration. This project is not an official Tower Insurance system and must not be used with real customer data without appropriate governance, privacy, and security review.

## Business scenario

A severe weather event affects Auckland and creates a spike in insurance claims across House, Contents, Motor, and Travel products. Tower operations teams need to understand claim volume, backlog, SLA risk, supplier workload, digital lodgement adoption, estimated exposure, and customer experience indicators in near real time.

The demo storyline is:

1. A synthetic Auckland rain and wind event begins.
2. A Fabric notebook generates claims and streams JSON events into an Eventstream custom endpoint.
3. Eventstream routes messages into an Eventhouse/KQL database table named `bronze_claims`.
4. KQL queries power a Real-Time Dashboard called **Tower Claims Command Centre**.
5. The presenter explains how Fabric provides ingestion, storage, analysis, and visualisation, and how Fabric IQ could add business meaning through insurance entities and relationships.

## Repository structure

```text
tower-claims/tower-claims-experience/
├── TowerClaims.Eventstream/
│   ├── .platform
│   ├── eventstream.json
│   └── eventstreamProperties.json
├── TowerClaims.KQLDashboard/
│   ├── .platform
│   └── RealTimeDashboard.json
├── TowerClaims.Notebook/
│   ├── .platform
│   └── notebook-content.py
└── sample-data/
    └── claims-schema.json
```

## Artifacts

### `TowerClaims.Eventstream`

The Eventstream artifact models this topology:

```text
CustomEndpoint-Source → TowerClaimsStream-stream → Eventhouse destination → bronze_claims
```

The Eventhouse destination uses `DirectIngestion` and the destination table `bronze_claims`.

Environment-specific values are intentionally placeholders in `eventstream.json`:

| Placeholder | Replace with |
| --- | --- |
| `workspaceId: 00000000-0000-0000-0000-000000000000` | Fabric workspace ID |
| `itemId: 00000000-0000-0000-0000-000000000000` | Eventhouse item ID |
| `connectionName: REPLACE_WITH_TOWER_CLAIMS_EVENTHOUSE_CONNECTION` | Eventhouse destination connection name created in Fabric |
| `mappingRuleName: REPLACE_WITH_BRONZE_CLAIMS_MAPPING_RULE` | Ingestion mapping rule for `bronze_claims` |

### `TowerClaims.Notebook`

The notebook is a Fabric PySpark notebook source file that:

- Installs/imports `azure-eventhub` and `semantic-link-labs` if they are not already available in the session.
- Discovers the `TowerClaimsStream` custom endpoint connection with `sempy_labs.eventstream` APIs.
- Sends JSON events through the Event Hubs-compatible custom endpoint.
- Generates deterministic/randomized synthetic claims with configurable `max_events`, `sleep_seconds`, `batch_size`, and `random_seed` variables.
- Ends with a pandas DataFrame snapshot of recently generated events.

Key generated dimensions include:

- Products: `House`, `Contents`, `Motor`, `Travel`
- Channels: `My Tower`, `Phone`, `Broker`
- Statuses: `Lodged`, `Validated`, `Assessment`, `Repair`, `Settled`, `Rejected`
- Regions/suburbs: Auckland-focused NZ locations, plus Northland, Waikato, and Wellington
- Operational fields: supplier, assessor, SLA, cost, contact count, satisfaction, weather intensity
- Derived flags: `is_weather_event`, `sla_at_risk`, `manual_intervention_required`, `digital_lodgement`

### `TowerClaims.KQLDashboard`

The dashboard artifact follows the banking sample's Real-Time Dashboard JSON style with:

- `schema_version: 67`
- a Fabric `kusto-trident` data source
- database name `TowerClaims`
- placeholder workspace/database IDs
- one duration parameter named **Time range** that supplies `_startTime` and `_endTime`
- KQL queries over table `bronze_claims`

Dashboard tiles include:

- Total Claims
- Open Claims
- SLA-at-Risk Claims
- Average Days Open / Settlement
- Digital Lodgement Rate
- Claims over time
- Claims by region
- Claims by product
- Claims by status
- Supplier workload and SLA risk
- Average estimated cost by product

> Fabric dashboard JSON bindings can be tenant-specific. The UUIDs in this repository are valid-looking placeholders for a portable sample; after import, verify or rebind the dashboard data source to your workspace and Eventhouse/KQL database.

### `sample-data/claims-schema.json`

Documents the synthetic event payload schema, allowed values, field types, and descriptions. Use it as a reference when creating the KQL table or explaining the payload to business stakeholders.

## Prerequisites

You need:

- A Microsoft Fabric tenant and workspace with Real-Time Intelligence enabled.
- Permission to create or import Fabric items from Git integration.
- A Fabric Eventhouse/KQL database. The recommended database display name is `TowerClaims`.
- A KQL table named `bronze_claims`.
- A Fabric Eventstream named `TowerClaimsStream` with a custom endpoint source.
- A Fabric notebook runtime that can install/use:
  - `azure-eventhub==5.11.5` (pinned to the Event Hubs client version used by the reference notebook pattern)
  - `semantic-link-labs` (left unpinned so the notebook can use the Fabric-compatible version available in the workspace; pin a tenant-validated version if your environment requires fully reproducible installs)
  - `pandas`
- Workspace permissions that allow the notebook to inspect Eventstream topology and source connection details.

No credentials, secrets, access keys, or real connection strings are stored in this repository.

## Suggested KQL table schema

Create the target table in the `TowerClaims` KQL database before streaming, or allow your Fabric setup process to create it with equivalent columns.

```kusto
.create table bronze_claims (
    claim_id:string,
    policy_id:string,
    customer_id:string,
    event_id:string,
    event_sequence:int,
    product:string,
    claim_type:string,
    status:string,
    lodgement_channel:string,
    region:string,
    suburb:string,
    country:string,
    supplier:string,
    assessor:string,
    weather_event_id:string,
    weather_event_name:string,
    weather_severity:string,
    rainfall_mm_24h:real,
    wind_gust_kmh:real,
    estimated_cost:real,
    actual_cost:real,
    excess_amount:real,
    reserve_amount:real,
    sla_days:int,
    days_open:int,
    days_to_settle:int,
    contact_count:int,
    customer_satisfaction:real,
    is_weather_event:bool,
    sla_at_risk:bool,
    manual_intervention_required:bool,
    digital_lodgement:bool,
    lodged_at:datetime,
    updated_at:datetime,
    timestamp:datetime,
    source_system:string,
    demo_notice:string
)
```

If you use an Eventstream destination mapping, create or update a JSON ingestion mapping that aligns to the fields in `sample-data/claims-schema.json`, then replace `REPLACE_WITH_BRONZE_CLAIMS_MAPPING_RULE` in the Eventstream artifact.

## Import or recreate the Fabric artifacts

Recommended flow:

1. Create or select a Fabric workspace.
2. Create an Eventhouse and a KQL database named `TowerClaims`.
3. Create the `bronze_claims` table using the schema above.
4. Import this repository or recreate the three Fabric artifacts from the files under `tower-claims/tower-claims-experience/`.
5. In `TowerClaims.Eventstream/eventstream.json`, replace the workspace ID, Eventhouse item ID, connection name, and mapping rule placeholder values.
6. In `TowerClaims.KQLDashboard/RealTimeDashboard.json`, replace:
   - `dataSources[].workspace` with the Fabric workspace ID.
   - `dataSources[].database` with the Eventhouse/KQL database item ID used by your tenant.
   - Verify the data source name `TowerClaims` points to the correct database after import.
7. Open the Eventstream in Fabric and confirm the CustomEndpoint source and Eventhouse destination are connected.
8. Open the notebook and confirm `eventstream = "TowerClaimsStream"` and `eventstream_source_name = "CustomEndpoint-Source"` match the imported Eventstream item.
9. After import, verify the notebook `known_event_streams` metadata in `TowerClaims.Notebook/notebook-content.py`. Fabric may rewrite this automatically during Git sync; if it does not, update or clear the placeholder `artifact_id` and `stream_id` values so the notebook binds to the imported Eventstream item in your workspace.

## Running the notebook

The notebook defaults to a finite run:

```python
max_events = 250
sleep_seconds = 1.0
batch_size = 10
random_seed = 42
```

For a quick validation run, reduce the event count:

```python
max_events = 25
sleep_seconds = 0.25
batch_size = 5
```

For continuous demo mode, set:

```python
max_events = None
```

Stop or interrupt the notebook cell to end continuous streaming. The generator keeps a rolling sample of the latest events so the final cell can display a DataFrame snapshot without unbounded memory growth.

## Validation queries

After streaming starts, use these KQL queries in the `TowerClaims` KQL database.

Count events:

```kusto
bronze_claims
| count
```

Check latest events:

```kusto
bronze_claims
| top 20 by timestamp desc
```

Confirm the weather spike:

```kusto
bronze_claims
| summarize Claims=count(), WeatherClaims=countif(is_weather_event == true) by bin(timestamp, 5m)
| order by timestamp asc
```

Find operational risk:

```kusto
bronze_claims
| where status !in ("Settled", "Rejected")
| summarize Claims=count(), SLARisk=countif(sla_at_risk == true), Manual=countif(manual_intervention_required == true) by product, region
| order by SLARisk desc
```

Supplier workload:

```kusto
bronze_claims
| where isnotempty(supplier)
| summarize Claims=count(), AvgDaysOpen=round(avg(todouble(days_open)), 1), SLARisk=countif(sla_at_risk == true) by supplier
| order by Claims desc
```

Digital lodgement rate:

```kusto
bronze_claims
| summarize DigitalRate=round(100.0 * countif(digital_lodgement == true) / count(), 1)
```

## Suggested presentation flow

1. **Business context:** Tower needs to respond quickly to an Auckland severe-weather event while maintaining customer experience.
2. **Fabric ingestion:** Show the Eventstream custom endpoint and Eventhouse destination.
3. **Synthetic stream:** Run the notebook and explain finite versus continuous generation.
4. **Real-time operations:** Open the dashboard and highlight claim volume, SLA risk, digital lodgement, regional impact, and supplier workload.
5. **KQL analysis:** Drill into high-risk open claims and supplier bottlenecks with validation queries.
6. **Fabric platform value:** Explain that Fabric brings streaming ingestion, OneLake/Eventhouse storage, KQL analysis, notebooks, dashboards, governance, and collaboration together.
7. **Fabric IQ value:** Explain how business semantics can sit above raw telemetry so people and agents understand Claim, Policy, Customer, Supplier, and WeatherEvent relationships.

## Fabric IQ ontology suggestion

This repository does not include a Fabric IQ export format because such tenant-specific artifacts are not represented by the banking sample and should not be fabricated. For a Tower presentation, describe an ontology with entities such as:

- `Customer`
- `Policy`
- `Claim`
- `ClaimStatus`
- `Product`
- `Supplier`
- `Assessor`
- `WeatherEvent`
- `Region`
- `CustomerInteraction`
- `Repair`
- `Payment`

Suggested relationships:

```text
Customer owns Policy
Policy generates Claim
Claim has ClaimStatus
Claim relates to Product
Claim occurs in Region
Claim caused_by WeatherEvent
Claim assigned_to Assessor
Claim referred_to Supplier
Claim may_require Repair
Claim may_result_in Payment
Claim has CustomerInteraction
```

Suggested business questions for a Fabric IQ / data-agent demonstration:

- Which open weather claims are likely to breach SLA today?
- Which suppliers are overloaded after the Auckland storm?
- Are My Tower lodged claims settling faster than phone or broker claims?
- Which regions have the highest estimated exposure?
- Which high-cost claims require manual intervention?
- What claims operations actions should a manager prioritise this morning?

## Safety and data notes

- All data is synthetic and generated in the notebook.
- Synthetic customer IDs use the `SYN-CUST-` prefix.
- No real Tower data, personal information, credentials, access keys, or connection strings are included.
- Placeholder IDs use values such as `00000000-0000-0000-0000-000000000000` and must be replaced after import or binding in Fabric.
