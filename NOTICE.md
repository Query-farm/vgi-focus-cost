# Third-party notices

## FOCUS specification (CC BY 4.0)

`src/focus_worker/data/focus_spec.json` (exposed as `focus.main.focus_columns`) is compiled by
`scripts/compile_focus_spec.py` from the column definitions of the FinOps Open Cost and Usage
Specification (FOCUS), versions 1.0, 1.1, 1.2, 1.3 and 1.4:
https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec (tags v1.0 to v1.4, retrieved 2026-11-15).

Copyright (c) Joint Development Foundation Projects, LLC, FinOps Open Cost and Usage Specification (FOCUS)
Series and its contributors. Licensed under the Creative Commons Attribution 4.0 International license
(CC BY 4.0): https://creativecommons.org/licenses/by/4.0/legalcode

Changes made: only each column's ID, display name, one-sentence description, constraint values (column
type, feature level, nullability, data type, value format), allowed values and introduced version are
extracted and restructured as JSON. The license was verified from `license.md` at tag v1.2 on 2026-11-15.

## FOCUS sample data (CC BY 4.0, tests only)

`test/fixtures/focus_sample/focus_sample_1_0.csv` is a 300-row selection of the FinOps Foundation's
FOCUS-Sample-Data (https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS-Sample-Data), CC BY 4.0. It is
used by the test suite only and is not part of the distributed package. See the NOTICE.md next to it.

FOCUS is a trademark of the FinOps Foundation. This project is not affiliated with or endorsed by it.
