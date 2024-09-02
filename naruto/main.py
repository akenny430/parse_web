import plotnine as pn
import polars as pl

seasons_count_df: pl.DataFrame = pl.DataFrame(
    data=[
        (1, 32),
        (2, 21),
        (3, 18),
        (4, 17),
        (5, 24),
        (6, 31),
        (7, 8),
        (8, 24),
        (9, 21),
        (10, 25),
        (11, 21),
        (12, 33),
        (13, 20),
        (14, 25),
        (15, 28),
        (16, 13),
        (17, 11),
        (18, 21),
        (19, 20),
        (20, 45),
        (21, 21),
        (22, 21),
    ],
    schema={
        "SeasonID": pl.UInt8,
        "EpisodeCount": pl.UInt32,
    },
    orient="row",
)

apple_df: pl.DataFrame = pl.DataFrame(
    data=[
        (1, True),
        (2, True),
        (3, False),
        (4, False),
        (5, False),
        (6, True),
        (7, True),
        (8, True),
        (9, False),
        (10, True),
        (11, True),
        (12, True),
        (13, False),
        (14, True),
        (15, True),
        (16, True),
        (17, True),
        (18, True),
        (19, False),
        (20, True),
        (21, True),
        (22, False),
    ],
    schema={
        "SeasonID": pl.UInt8,
        "AppleTV": pl.Boolean,
    },
    orient="row",
)


# https://naruto-official.com/en/anime/naruto2
arc_official_df: pl.DataFrame = (
    pl.DataFrame(
        data=[
            (221, 252, "Kazekage Rescue", False),
            (253, 273, "Long-Awaited Reunion", False),
            (274, 291, "Guardian Shinobi Twelve", True),
            (292, 308, "Immortal Devastators: Hidan and Kakuzu", False),
            (309, 332, "Three-Tails' Appearance", True),
            (333, 363, "Master's Prophecy and Vengence", False),
            (364, 371, "Six-Tails Unleashed", True),
            (372, 395, "Two Saviors", False),
            (396, 416, "Past Arc: The Locus of the Leaf", True),
            (417, 441, "The Five Kage Assemble", False),
            (442, 462, "Paradise Life on a Boat", True),
            (463, 495, "Nine-Tails Taiming and Karmic Encounters", False),
            (496, 509, "The Seven Ninja Swordmen", False),
            (510, 515, "Power", True),
            (516, 540, "The Great Ninja War: ssailants from Afar", False),
            (541, 568, "The Great Ninja War: Sasuke and Itachi", False),
            (569, 581, "Kakashi: Shadow of the Anbu Black Ops", True),
            (582, 592, "The Great Ninja War: Team 7 Returns", False),
            (593, 613, "The Great Ninja War: Obito Uchiha", False),
            (614, 633, "In Naruto's Footsteps: The Friends' Path", True),
            (634, 651, "Infinite Tsukuyomi: The Invocation", False),
            (652, 670, "Jiraiya Shinobi Handbook: The Tale of Naruto the Hero", True),
            (671, 678, "Itachi's Story: Daylight / Midnight", True),
            (679, 689, "The Origins of Ninshu: The Two Souls, Indra and Ashura", True),
            (690, 699, "Naruto and Sasuke", False),
            (700, 703, "Nostalgic Days", True),
            (704, 708, "Sasuke Shinden: Book of Sunrise", True),
            (709, 713, "Shikamaru Hiden: A Cloud Drifting in Silent Darkness", True),
            (714, 720, "The Perfect Day for a Wedding", True),
        ],
        schema={
            "EpisodeStart": pl.UInt32,
            "EpisodeEnd": pl.UInt32,
            "ArcName": pl.Utf8,
            "AnimeOriginal": pl.Boolean,
        },
        orient="row",
    )
    # subtract 220 from original Naruto
    .with_columns(
        (pl.col("EpisodeStart") - 220),
        (pl.col("EpisodeEnd") - 220),
    )
    .with_row_index(
        name="ArcID",
        offset=1,
    )
)

episodes_df: pl.DataFrame = (
    seasons_count_df.select(pl.all().repeat_by(by=pl.col("EpisodeCount")).explode())
    .with_row_index(
        name="EpisodeID",
        offset=1,
    )
    .select("EpisodeID", "SeasonID")
    .join(
        other=arc_official_df.select(pl.all().exclude("EpisodeEnd")),
        left_on="EpisodeID",
        right_on="EpisodeStart",
        how="left",
    )
    .with_columns(
        pl.col("ArcID").fill_null(strategy="forward"),
        pl.col("ArcName").fill_null(strategy="forward"),
        pl.col("AnimeOriginal").fill_null(strategy="forward"),
    )
    # .with_columns(
    #     (pl.col("ArcID").cast(pl.Utf8).str.zfill(2) + " - " + pl.col("ArcName")).alias(
    #         "ArcName"
    #     ),
    # )
    .join(
        other=apple_df,
        on="SeasonID",
        how="left",
    )
)

seasons_arcs_df: pl.DataFrame = episodes_df.group_by(
    "ArcID",
    maintain_order=True,
).agg(
    pl.first("ArcName"),
    pl.count("EpisodeID").alias("EpisodeCount"),
    pl.first("SeasonID"),
    pl.first("AnimeOriginal"),
    pl.first("AppleTV"),
)
print(seasons_arcs_df.filter(~pl.col("AnimeOriginal")))
core_seasons = (
    seasons_arcs_df.filter(
        ~pl.col("AnimeOriginal"),
        pl.col("AppleTV"),
    )
    .get_column("SeasonID")
    .unique()
    .sort()
)
print(core_seasons)
print(
    seasons_arcs_df.filter(
        ~pl.col("AnimeOriginal"),
        ~pl.col("AppleTV"),
    )
)

# episode_plot: pn.ggplot = (
#     pn.ggplot(
#         data=episodes_df,
#     )
#     + pn.geom_point(
#         mapping=pn.aes(
#             x="EpisodeID",
#             y="SeasonID",
#             color="ArcName",
#             shape="AnimeOriginal",
#             alpha="AppleTV",
#         ),
#         show_legend=False,
#     )
#     + pn.scale_alpha_manual(
#         values={
#             True: 1.00,
#             False: 0.10,
#         },
#     )
#     # + pn.scale_shape_manual(
#     #     values={
#     #         True: 19,  # default
#     #         False: 15,
#     #     },
#     # )
#     + pn.theme_bw()
# )
# episode_plot.save(
#     filename="./naruto_arc_breakdown.pdf",
#     width=10,
#     height=5,
#     verbose=False,
# )
