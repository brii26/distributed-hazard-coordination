# Problem 3: schema policy

What the Aggregator accepts from a source and what it parks. The check is `checkContract` in `services/aggregator/mapper.go`, called at the top of the PVMBG mapping.

## Contract

PVMBG has six contract fields. All are required, not null, and of this type:

| Field | Type |
|---|---|
| `report_id`, `volcano_id`, `alert_level`, `reported_at` | string |
| `eruption_count_24h`, `ash_column_height_m` | number |

Any other field is allowed and goes to `attributes`.

## PVMBG decisions

| Change seen | Example | Decision | Reason text |
|---|---|---|---|
| Legacy schema | the six fields | accept | - |
| Extended schema | `confidence_level: 0.75` added | accept, field in `attributes` | - |
| Optional field absent | no `confidence_level` | accept | - |
| New unknown field | `sensor_count: 7` | accept, in `attributes` | - |
| Field order changed | any order | accept | - |
| Value not in reference table | `volcano_id: "NOWHERE"` | accept, canonical field null, raw value in `attributes` | - |
| Contract field renamed | `eruption_count_24h` becomes `eruption_count` | park | `schema_incompatible: field 'eruption_count_24h' missing` |
| Contract field retyped | `ash_column_height_m: "1200.5"` | park | `schema_incompatible: field 'ash_column_height_m' changed from number to string` |
| Contract field missing | no `volcano_id` | park | `schema_incompatible: field 'volcano_id' missing` |
| Contract field `null` | `alert_level: null` | park (same as missing) | `... field 'alert_level' missing` |
| Several at once | all of the above | park | all parts in field-name order, joined by `"; "` |
| Unusable value | `reported_at: "yesterday"` | park (existing check) | existing text |
| Malformed bytes | truncated JSON, too deep | park before the mapper | `malformed_json`, `too_deep`, ... |

A rename looks like "field missing" because the mapper cannot tell a rename from a removal, and it does not guess.

## Reason format

`schema_incompatible: ` followed by one part per broken field, sorted by field name and joined by `; `. A part is `field 'x' missing` or `field 'x' changed from <contract type> to <seen type>`. Seen types are `string`, `number`, `bool`, `list`, `object`.

## Errors for callers

- `*IncompatibleSchema{Reason}` is returned for a contract break. A mapper error is never retryable: the same input fails the same way until the mapping changes.
- `mapAll` wraps each failure in `*recordError{index}`, the position in the batch, so the caller can keep the exact record. Use `errors.As` to reach both.

## BMKG

BMKG is unchanged and stays tolerant: a missing, renamed or retyped field becomes null with the raw value in `attributes`. This is deliberate and is the policy for BMKG.
