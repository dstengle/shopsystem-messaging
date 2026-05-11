# shopsystem-messaging

Messaging bounded context of the shopsystem. Will own the Pydantic schemas
for the inter-shop message catalog and the `shop-msg` CLI.

Empty pending extraction per ADR-001. Until extraction runs, the canonical
content lives in the source repo at
[`docs/shop-system/`](https://github.com/dstengle/ddd-product-system/tree/main/docs/shop-system)
(framework documentation) and
[`prototypes/message-catalog-v1/`](https://github.com/dstengle/ddd-product-system/tree/main/prototypes/message-catalog-v1)
(code that will be extracted from `catalog/` and `shop-msg-bc/`).

Framework split rationale and sequencing:
[ADR-001](https://github.com/dstengle/ddd-product-system/blob/main/docs/shop-system/adr-001-framework-packaging.md).

Depends on [shopsystem-scenarios](https://github.com/dstengle/shopsystem-scenarios)
for the canonicalization rule used by `ScenarioPayload.hash`.
