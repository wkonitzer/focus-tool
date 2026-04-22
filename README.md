# Attribute Focus Billing Tool

Fetches customer cost attribution data from the Attribute API and transforms
it into the FinOps Open Cost and Usage Specification (FOCUS) v1.3 format.

- FOCUS spec: https://focus.finops.org/focus-specification/v1-3/
- Attribute API: https://attrb.io/documents/attribute-configurations/attribute-api/

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Mapping Decisions](#mapping)
- [Contributing](#contributing)

## Installation

Make sure you have python3 installed as a pre-requisite.

Clone the repository:
```shell
git clone https://github.com/yourusername/focus-tool.git
cd rss-server
```

Set up a virtual environment and activate it:

```shell
python -m venv venv
# For Windows
.\venv\Scripts\activate
# For Unix or MacOS
source venv/bin/activate
```

Install the required Python packages:
```shell
pip install -r requirements.txt
```

## Usage

Start the RSS server:

```shell
export ATTRIBUTE_API_TOKEN=<your-jwt>
python3 attribute_to_focus.py --date 2026-04-15 --granularity daily --out focus.csv
ython3 attribute_to_focus.py --date 2026-04-01 --granularity monthly --format jsonl --out focus.jsonl
```

## Mapping
- `amortizedCost` → `EffectiveCost` is the clean mapping. FOCUS also requires `BilledCost`, `ContractedCost`, and `ListCost`, which the Attribute API doesn't expose separately — the script writes the same value to all four. 
- `cloudProvider` is written to `ServiceProviderName`, `HostProviderName`, and `InvoiceIssuerName` — all three are mandatory in FOCUS, and for direct-from-AWS attribution they're the same entity.
- `ChargeCategory` is hard-coded to `"Usage"`` since Attribute's customer data represents consumption attribution; if you start pulling in purchases/credits elsewhere, you'd map those separately.
- `ServiceCategory/ServiceName` need FOCUS-defined values (not raw "EKS"), so there's a lookup table — extend `RESOURCE_TYPE_MAP` as you encounter resource types not yet listed.
- `customerName`, `customerRuleIdentifier`, and `organizationId` go into `x_`-prefixed custom columns per FOCUS §2.8.

One caveat to keep in mind: the AWS 48-hour data-availability delay mentioned in the Attribute docs means the script will emit empty results for dates less than two days old, which isn't an error — just a reminder.

## Contributing

Contributions are welcome! If you'd like to contribute to this project, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and test them.
4. Commit your changes with clear and concise messages.
5. Push your changes to your fork.
6. Create a pull request to the main repository.