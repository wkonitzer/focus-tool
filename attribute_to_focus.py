#!/usr/bin/env python3
"""
Attribute API -> FOCUS v1.3 Billing Format Converter
=====================================================

Fetches customer cost attribution data from the Attribute API and transforms
it into the FinOps Open Cost and Usage Specification (FOCUS) v1.3 format.

- FOCUS spec: https://focus.finops.org/focus-specification/v1-3/
- Attribute API: see the Attribute API documentation for endpoint details.

Usage
-----
    export ATTRIBUTE_API_TOKEN=<your-jwt>
    python attribute_to_focus.py --date 2024-12-04 --granularity daily --out focus.csv
    python attribute_to_focus.py --date 2024-11-01 --granularity monthly --format jsonl --out focus.jsonl

Mapping notes & caveats
-----------------------
The Attribute `CustomerEntryObject` carries a single numeric metric
(`amortizedCost`) per resource attribution. FOCUS v1.3 mandates four cost
metrics: BilledCost, EffectiveCost, ContractedCost, and ListCost. Because
Attribute does not distinguish list vs. contracted vs. billed vs. effective,
this script populates all four columns with `amortizedCost`. Downstream
consumers that need true billed or list costs should join this output with
the original cloud provider billing export.

Customer identifiers have no native column in FOCUS, so they are emitted as
custom columns (`x_CustomerName`, `x_CustomerRuleIdentifier`,
`x_OrganizationId`) per FOCUS section 2.8 (Custom Columns).

Dependencies
------------
    pip install requests
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Iterator, Optional

import requests


DEFAULT_BASE_URL = "https://api.app.attrb.io"
DEFAULT_CURRENCY = "USD"

JOB_COMPLETED = "completed"
JOB_FAILED = "failed"
JOB_IN_PROGRESS = {"pending", "running"}


# ---------------------------------------------------------------------------
# FOCUS v1.3 schema
# ---------------------------------------------------------------------------

# Output column order. We emit every mandatory column from the v1.3 Cost and
# Usage dataset plus commonly-used conditional columns we can populate from
# Attribute data. Extend if you have richer source data.
FOCUS_COLUMNS: list[str] = [
    # --- Mandatory ---
    "BilledCost",
    "BillingAccountId",
    "BillingAccountName",
    "BillingCurrency",
    "BillingPeriodEnd",
    "BillingPeriodStart",
    "ChargeCategory",
    "ChargeClass",
    "ChargeDescription",
    "ChargePeriodEnd",
    "ChargePeriodStart",
    "ContractedCost",
    "EffectiveCost",
    "HostProviderName",
    "InvoiceIssuerName",
    "ListCost",
    "PricingQuantity",
    "PricingUnit",
    "ProviderName",   # Deprecated in v1.2, still Mandatory per v1.3 spec §3.1.47
    "PublisherName",  # Deprecated in v1.2, still Mandatory per v1.3 spec §3.1.48
    "ServiceCategory",
    "ServiceName",
    "ServiceProviderName",
    # --- Conditional / recommended ---
    "RegionId",
    "RegionName",
    "ResourceId",
    "ResourceName",
    "ResourceType",
    "SubAccountId",
    "SubAccountName",
    "SubAccountType",
    # --- Custom (FOCUS §2.8) ---
    "x_CustomerName",
    "x_CustomerRuleIdentifier",
    "x_OrganizationId",
]


# Minimal mapping from AWS resource types to FOCUS-defined (ServiceCategory,
# ServiceName) tuples. FOCUS v1.3 defines a fixed set of ServiceCategory
# values (see spec §3.1.55). Extend this table to cover the resource types
# produced by your Attribute integration.
RESOURCE_TYPE_MAP: dict[str, tuple[str, str]] = {
    # resource_type  ->  (ServiceCategory, ServiceName)
    "EC2":             ("Compute", "Amazon Elastic Compute Cloud"),
    "EKS":             ("Compute", "Amazon Elastic Kubernetes Service"),
    "ECS":             ("Compute", "Amazon Elastic Container Service"),
    "Lambda":          ("Compute", "AWS Lambda"),
    "Fargate":         ("Compute", "AWS Fargate"),
    "S3":              ("Storage", "Amazon Simple Storage Service"),
    "EBS":             ("Storage", "Amazon Elastic Block Store"),
    "EFS":             ("Storage", "Amazon Elastic File System"),
    "RDS":             ("Databases", "Amazon Relational Database Service"),
    "DynamoDB":        ("Databases", "Amazon DynamoDB"),
    "ElastiCache":     ("Databases", "Amazon ElastiCache"),
    "Redshift":        ("Analytics", "Amazon Redshift"),
    "Athena":          ("Analytics", "Amazon Athena"),
    "Glue":            ("Analytics", "AWS Glue"),
    "CloudFront":      ("Networking", "Amazon CloudFront"),
    "VPC":             ("Networking", "Amazon Virtual Private Cloud"),
    "Route53":         ("Networking", "Amazon Route 53"),
    "ELB":             ("Networking", "Elastic Load Balancing"),
    "NAT":             ("Networking", "AWS NAT Gateway"),
    "APIGateway":      ("Networking", "Amazon API Gateway"),
    "SNS":             ("Integration", "Amazon Simple Notification Service"),
    "SQS":             ("Integration", "Amazon Simple Queue Service"),
    "KMS":             ("Security", "AWS Key Management Service"),
    "SecretsManager":  ("Security", "AWS Secrets Manager"),
    "CloudWatch":      ("Management and Governance", "Amazon CloudWatch"),
    "CloudTrail":      ("Management and Governance", "AWS CloudTrail"),
}
DEFAULT_CATEGORY = "Other"


def classify_service(cloud_provider: str, resource_type: Optional[str]) -> tuple[str, str]:
    """Return a (ServiceCategory, ServiceName) pair for a given resource type."""
    if not resource_type:
        return DEFAULT_CATEGORY, cloud_provider or "Unknown"
    mapped = RESOURCE_TYPE_MAP.get(resource_type)
    if mapped:
        return mapped
    # Unknown type: preserve the raw resource_type as the service name so the
    # output is still informative, and flag it as Other.
    return DEFAULT_CATEGORY, resource_type


def period_bounds(date_str: str, granularity: str) -> tuple[str, str]:
    """Return (start, end) ISO-8601 UTC strings. End is *exclusive* per FOCUS."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if granularity == "daily":
        start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    elif granularity == "monthly":
        start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1, day=1)
        else:
            end = start.replace(month=start.month + 1, day=1)
    else:
        raise ValueError(f"Unsupported granularity: {granularity!r}")
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return start.strftime(fmt), end.strftime(fmt)


# ---------------------------------------------------------------------------
# Attribute API client
# ---------------------------------------------------------------------------

class AttributeAPIError(RuntimeError):
    """Raised for non-2xx responses and job failures."""


class AttributeClient:
    """Minimal JWT-authenticated client for the Attribute API."""

    def __init__(
        self,
        token: str,
        base_url: str = DEFAULT_BASE_URL,
        poll_interval_s: float = 3.0,
        poll_timeout_s: float = 600.0,
        request_timeout_s: float = 60.0,
    ) -> None:
        if not token:
            raise ValueError("token is required")
        self.base_url = base_url.rstrip("/")
        self.poll_interval_s = poll_interval_s
        self.poll_timeout_s = poll_timeout_s
        self.request_timeout_s = request_timeout_s
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })

    def _get(self, path: str, **params: Any) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        # Strip parameters whose value is None so we don't send "None" strings.
        clean = {k: v for k, v in params.items() if v is not None}
        resp = self.session.get(url, params=clean, timeout=self.request_timeout_s)

        if resp.status_code == 401:
            raise AttributeAPIError("Unauthorized (HTTP 401) — check your JWT token.")
        if resp.status_code == 429:
            raise AttributeAPIError("Rate limited (HTTP 429) — back off and retry.")
        if resp.status_code == 404:
            raise AttributeAPIError(f"Not found (HTTP 404) at {url}: {resp.text[:300]}")
        if resp.status_code >= 400:
            raise AttributeAPIError(
                f"HTTP {resp.status_code} from {url}: {resp.text[:500]}"
            )
        try:
            return resp.json()
        except ValueError as e:
            raise AttributeAPIError(f"Invalid JSON from {url}: {e}") from e

    # --- Job lifecycle ------------------------------------------------------

    def start_customer_job(
        self,
        granularity: str,
        date: str,
        account: Optional[str] = None,
        allcustomers: bool = False,
    ) -> str:
        """POST-equivalent: initiate a job and return its id.

        The Attribute API uses GET to kick off a job; we follow that here.
        """
        data = self._get(
            f"/api/v1/customer/{granularity}/{date}",
            account=account,
            allcustomers=("true" if allcustomers else None),
        )
        job_id = data.get("id")
        if not job_id:
            raise AttributeAPIError(f"No job id in response: {data!r}")
        return job_id

    def wait_for_job(self, job_id: str) -> None:
        """Poll until the job completes, fails, or times out."""
        deadline = time.monotonic() + self.poll_timeout_s
        while True:
            data = self._get(f"/api/v1/job/{job_id}/status")
            status = data.get("status")
            if status == JOB_COMPLETED:
                return
            if status == JOB_FAILED:
                raise AttributeAPIError(
                    f"Job {job_id} failed: {data.get('reason') or 'unknown reason'}"
                )
            if status not in JOB_IN_PROGRESS:
                raise AttributeAPIError(f"Unknown job status {status!r}: {data!r}")
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"Job {job_id} did not complete within {self.poll_timeout_s:.0f}s"
                )
            time.sleep(self.poll_interval_s)

    def fetch_results(self, job_id: str, page_size: int = 500) -> Iterator[dict[str, Any]]:
        """Yield CustomerEntryObject dicts, handling token-based pagination."""
        token: Optional[int] = None
        while True:
            data = self._get(
                f"/api/v1/job/{job_id}/fetch",
                maxResults=page_size,
                token=token,
            )
            for entry in data.get("results") or []:
                yield entry
            nxt = data.get("nextToken")
            if not nxt:
                return
            token = nxt


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------

def _to_float(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def entries_to_focus_rows(
    entries: Iterable[dict[str, Any]],
    *,
    billing_period_start: str,
    billing_period_end: str,
    currency: str,
) -> Iterator[dict[str, Any]]:
    """Flatten CustomerEntryObject records into one FOCUS row per attribution."""
    for entry in entries:
        customer_name = entry.get("customerName") or ""
        organization_id = entry.get("organizationId") or ""

        for item in entry.get("data") or []:
            resource = item.get("resource") or {}
            cost = item.get("cost") or {}

            amortized = _to_float(cost.get("amortizedCost"))

            cloud_provider = resource.get("cloudProvider") or ""
            account_id = resource.get("accountId") or ""
            resource_name = resource.get("resourceName") or ""
            resource_type = resource.get("resourceType") or ""
            resource_region = resource.get("resourceRegion") or ""
            rule_id = resource.get("customerRuleIdentifier") or ""

            service_category, service_name = classify_service(cloud_provider, resource_type)

            yield {
                # Mandatory
                "BilledCost":         amortized,
                "BillingAccountId":   account_id,
                "BillingAccountName": "",   # Not provided by Attribute
                "BillingCurrency":    currency,
                "BillingPeriodEnd":   billing_period_end,
                "BillingPeriodStart": billing_period_start,
                "ChargeCategory":     "Usage",
                "ChargeClass":        "",   # null: not a correction
                "ChargeDescription":  f"{service_name} usage attributed to customer '{customer_name}'",
                "ChargePeriodEnd":    billing_period_end,
                "ChargePeriodStart":  billing_period_start,
                "ContractedCost":     amortized,  # Attribute does not distinguish
                "EffectiveCost":      amortized,
                "HostProviderName":   cloud_provider,
                "InvoiceIssuerName":  cloud_provider,
                "ListCost":           amortized,  # Attribute does not distinguish
                "PricingQuantity":    "",   # Not provided
                "PricingUnit":        "",   # Not provided
                "ProviderName":       cloud_provider,   # Deprecated, still mandatory
                "PublisherName":      cloud_provider,   # Deprecated, still mandatory
                "ServiceCategory":    service_category,
                "ServiceName":        service_name,
                "ServiceProviderName": cloud_provider,
                # Conditional / recommended
                "RegionId":           resource_region,
                "RegionName":         resource_region,
                "ResourceId":         resource_name,
                "ResourceName":       resource_name,
                "ResourceType":       resource_type,
                "SubAccountId":       account_id,
                "SubAccountName":     "",
                "SubAccountType":     "",
                # Custom (FOCUS §2.8)
                "x_CustomerName":            customer_name,
                "x_CustomerRuleIdentifier":  rule_id,
                "x_OrganizationId":          organization_id,
            }


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_csv(rows: Iterable[dict[str, Any]], out_path: str) -> int:
    count = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FOCUS_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def write_jsonl(rows: Iterable[dict[str, Any]], out_path: str) -> int:
    count = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
            count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Convert Attribute API customer-attribution data into FOCUS v1.3 billing format.",
    )
    p.add_argument("--date", required=True,
                   help="Date in YYYY-MM-DD. For --granularity monthly, the day part is ignored.")
    p.add_argument("--granularity", choices=("daily", "monthly"), default="daily")
    p.add_argument("--account", default=None,
                   help="Restrict to a specific Attribute account id (optional).")
    p.add_argument("--allcustomers", action="store_true",
                   help="Do not aggregate minor customers (default: aggregate).")
    p.add_argument("--base-url",
                   default=os.environ.get("ATTRIBUTE_API_BASE_URL", DEFAULT_BASE_URL),
                   help=f"Attribute API base URL (default: {DEFAULT_BASE_URL}).")
    p.add_argument("--token",
                   default=os.environ.get("ATTRIBUTE_API_TOKEN"),
                   help="JWT token. Defaults to ATTRIBUTE_API_TOKEN env var.")
    p.add_argument("--currency", default=DEFAULT_CURRENCY,
                   help=f"Billing currency ISO-4217 code (default: {DEFAULT_CURRENCY}).")
    p.add_argument("--out", default="focus.csv",
                   help="Output file path (default: focus.csv).")
    p.add_argument("--format", choices=("csv", "jsonl"), default="csv")
    p.add_argument("--page-size", type=int, default=500,
                   help="Page size for /fetch pagination (default: 500).")
    p.add_argument("--poll-interval", type=float, default=3.0,
                   help="Seconds between job status polls (default: 3).")
    p.add_argument("--poll-timeout", type=float, default=600.0,
                   help="Max seconds to wait for job completion (default: 600).")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    if not args.token:
        print("ERROR: provide --token or set ATTRIBUTE_API_TOKEN.", file=sys.stderr)
        return 2

    start, end = period_bounds(args.date, args.granularity)
    print(f"Billing period: {start} -> {end}", file=sys.stderr)

    client = AttributeClient(
        token=args.token,
        base_url=args.base_url,
        poll_interval_s=args.poll_interval,
        poll_timeout_s=args.poll_timeout,
    )

    try:
        print("Starting customer-data job...", file=sys.stderr)
        job_id = client.start_customer_job(
            granularity=args.granularity,
            date=args.date,
            account=args.account,
            allcustomers=args.allcustomers,
        )
        print(f"Job id: {job_id}. Polling...", file=sys.stderr)
        client.wait_for_job(job_id)

        print("Job completed. Streaming results and converting to FOCUS...", file=sys.stderr)
        entries = client.fetch_results(job_id, page_size=args.page_size)
        rows = entries_to_focus_rows(
            entries,
            billing_period_start=start,
            billing_period_end=end,
            currency=args.currency,
        )

        if args.format == "csv":
            n = write_csv(rows, args.out)
        else:
            n = write_jsonl(rows, args.out)

    except AttributeAPIError as e:
        print(f"API error: {e}", file=sys.stderr)
        return 1
    except TimeoutError as e:
        print(f"Timeout: {e}", file=sys.stderr)
        return 1

    print(f"Wrote {n} FOCUS rows to {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
