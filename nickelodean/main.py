from datetime import date as Date
import polars as pl

nick_processed_schema: dict[str, pl.DataType] = {
    "H2": pl.Utf8,
    "H3": pl.Utf8,
    "H4": pl.Utf8,
    "H5": pl.Utf8,
    "SubIndex": pl.UInt32,
    "Title": pl.Utf8,
    "PermiereDate": pl.Date,
    "FinaleDate": pl.Date,
    "NumberSeasons": pl.UInt16,
}

nick_df: pl.DataFrame = pl.scan_parquet(source="./data/nick.parquet").collect()

# which Nicktoons had the longest run
animated_length_df: pl.DataFrame = (
    nick_df.lazy()
    .filter(pl.col("H4") == 'Animated ("Nicktoons")')
    .with_columns(pl.col("FinaleDate").fill_null(Date(2024, 5, 19)))
    .with_columns(
        # active or not
        pl.when(pl.col("H2") == "Current programming")
        .then(pl.lit(True, pl.Boolean))
        .when(pl.col("H2") == "Former programming")
        .then(pl.lit(False, pl.Boolean))
        .otherwise(pl.lit(None, pl.Boolean))
        .alias("Active"),
        # length
        (pl.col("FinaleDate") - pl.col("PremiereDate"))
        .dt.total_days()
        .alias("DeltaDays"),
    )
    .sort("DeltaDays", descending=True)
    .select("Title", "Active", "PremiereDate", "FinaleDate", "DeltaDays")
    .collect()
)
animated_length_df.write_csv(file="./data/animated_length.csv")
