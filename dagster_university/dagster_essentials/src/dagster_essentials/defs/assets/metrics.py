import os

import dagster as dg
import duckdb
import geopandas as gpd
import matplotlib.pyplot as plt
from dagster._utils.backoff import backoff

from dagster_essentials.defs.assets import constants
from dagster_duckdb import DuckDBResource
from dagster_essentials.defs.partitions import weekly_partition


# src/dagster_essentials/defs/assets/metrics.py
@dg.asset(deps=["taxi_trips", "taxi_zones"])
def manhattan_stats(database: DuckDBResource) -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, "w") as output_file:
        output_file.write(trips_by_zone.to_json())


# src/dagster_essentials/defs/assets/metrics.py
@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(
        column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black"
    )
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # Adjust longitude range
    ax.set_ylim([40.70, 40.82])  # Adjust latitude range

    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)


@dg.asset(deps=["taxi_trips"], partitions_def=weekly_partition)
def trips_by_week(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    period_to_fetch = context.partition_key

    query = f"""
        select
            date_trunc('week', pickup_datetime) as period,
            count(1) as num_trips,
            sum(passenger_count) as passenger_count,
            sum(total_amount) as total_amount,
            sum(trip_distance) as trip_distance
        from trips
        where pickup_datetime >= '{period_to_fetch}'
            and pickup_datetime < '{period_to_fetch}'::date + interval '1 week'
        group by period
    """

    with database.get_connection() as conn:
        trips_by_vendor = conn.execute(query).fetch_df()

    with open(constants.TRIPS_BY_WEEK_FILE_PATH, "w") as output_file:
        output_file.write(trips_by_vendor.to_csv(index=False))
