# Ingestion Workflow

This repository specifies the ingestion pipeline
for adding new studies to neurostore.

It is modular in design, the main file being
orchastrator.py which calls up services to:
1. find articles
2. download articles
3. extract tables from articles
4. create analyses from tables
5. upload studies and analyses to neurostore
6. syncronize neurostore base-study ids with ns-pond

## Design Principles
- re-using existing code from dependencies where possible
- being DRY and modular
- using batching parallel processing for cpu bound tasks
- using batching when calling external APIs whenever possible (only have function signatures for batch calls, not single calls)
