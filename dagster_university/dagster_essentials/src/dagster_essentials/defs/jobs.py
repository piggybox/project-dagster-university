# src/dagster_essentials/defs/jobs.py
import dagster as dg
from pydantic.deprecated.tools import T

trips_by_week = dg.AssetSelection.assets("trips_by_week")

trip_update_job = dg.define_asset_job(
    name="trip_update_job", selection=dg.AssetSelection.all() - trips_by_week
)

trips_by_week_job = dg.define_asset_job(
    name="trips_by_week_job", selection=trips_by_week
)
