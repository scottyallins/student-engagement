# Deploying the Pipeline (Declarative Automation Bundle)

The nightly job is defined as code in `databricks.yml`: 29 tasks, explicit dependency DAG, 8:00 AM EST schedule, serverless compute. The orchestration is version-controlled together with the notebooks it runs.

## One-time local setup

1. Install the Databricks CLI (v0.218 or newer): `brew install databricks` (macOS) or `pip install databricks`
2. Authenticate (opens a browser prompt):
   `databricks auth login https://dbc-0b4f64f4-b3df.cloud.databricks.com`
3. Clone this repo locally and `cd` into it. Always deploy from a fresh `git pull`.

## Deploy

```
databricks bundle validate --target prod
databricks bundle deploy --target prod
```

Deploying syncs the notebooks to
`/Workspace/Users/scottsbv@gmail.com/.bundle/student_engagement_pipeline/prod/files/...`
and creates or updates the job to run those deployed copies.

## One-time migration from the UI-managed job

A bundle cannot adopt an existing UI job, so the job name must be free before the first deploy:

1. `databricks bundle validate --target prod` — confirm the config is valid before touching anything
2. `databricks jobs delete 299306717398586` — deletes the old UI job (its run history is deleted with it)
3. `databricks bundle deploy --target prod`
4. Trigger a run from the new job's page to validate end-to-end. The 8:00 AM schedule resumes automatically.

Until you run step 2, the existing UI job remains authoritative.

## How changes ship after migration (workflow change)

* Editing notebooks in the workspace Git folder alone no longer changes the running job.
* To ship any change: commit + push, `git pull` locally, then `databricks bundle deploy --target prod`.
* Orchestration changes (tasks, schedule, dependencies) are edits to `databricks.yml` + the same redeploy.
* Rollback is `git revert` + redeploy.

## Notes

* `mode: production` with an explicit `run_as` is set per DAB best practice. The next hardening step at team scale is a service principal for `run_as` and CI-driven deploys (GitHub Actions has first-class bundle support).
* `databricks bundle validate` runs before every deploy — treat it as the lint step.