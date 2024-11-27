import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# Database connection string
db_uri = "postgresql://postgres:1234@localhost:5433/pipeline_database"  # Replace credentials

# Create SQLAlchemy engine
engine = create_engine(db_uri)

try:
    # Queries to fetch data from tables
    queries = {
        "average_counts_per_day": "SELECT * FROM average_counts_per_day;",
        "status_counts": "SELECT * FROM status_counts;",
        "user_type_metrics": "SELECT * FROM user_type_metrics;",
        "user_type_per_day_metrics": "SELECT * FROM user_type_per_day_metrics;"  # New table
    }

    # Fetch data into DataFrames
    dataframes = {name: pd.read_sql(query, engine) for name, query in queries.items()}

    # Visualization for the new CSV (user_type_per_day_metrics)
    df_user_type_per_day = dataframes["user_type_per_day_metrics"]
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df_user_type_per_day["day_of_week"] = pd.Categorical(df_user_type_per_day["day_of_week"], categories=days_order, ordered=True)
    df_user_type_per_day.sort_values("day_of_week", inplace=True)

    # Rename metric_value to active_user_count_daywise for clarity
    df_user_type_per_day.rename(columns={"metric_value": "active_user_count_daywise"}, inplace=True)



    # Plot 1: Grouped Bar Chart with Custom Colors
    plt.figure(figsize=(16, 10))
    sns.barplot(
        x="day_of_week",
        y="active_user_count_daywise",
        hue="user_type",
        data=df_user_type_per_day,
        palette=["#ef7c8e", "#fae8e0", "#b6e2d3", "#d8a7b1"]
    )

    plt.title("📊 Grouped Bar Chart: Active User Count Per Day", fontsize=18, fontweight='bold', pad=15)
    plt.xlabel("Day of the Week", fontsize=14)
    plt.ylabel("Active User Count (Daywise)", fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(title="User Type", fontsize=12)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.show()

    # Pivot the data for stacked bar chart
    pivot_df = df_user_type_per_day.pivot(index="day_of_week", columns="user_type", values="active_user_count_daywise")

    # Calculate proportions for 100% stacked bar chart
    proportions_df = pivot_df.div(pivot_df.sum(axis=1), axis=0)

    # Plot: Horizontal 100% Stacked Bar Chart
    proportions_df.plot(kind="barh", stacked=True, figsize=(16, 10), color=["#ef7c8e", "#fae8e0", "#b6e2d3", "#d8a7b1"])
    plt.title("📊 100% Stacked Bar Chart: Proportion of Active Users Per Day", fontsize=18, fontweight='bold', pad=15)
    plt.xlabel("Proportion of Active Users", fontsize=14)
    plt.ylabel("Day of the Week", fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(title="User Type", fontsize=12, bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(axis="x", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.show()


    # Plot: Stacked Bar Chart
    pivot_df.plot(kind="bar", stacked=True, figsize=(16, 10), color=["#ef7c8e", "#fae8e0", "#b6e2d3", "#d8a7b1"])
    plt.title("📊 Stacked Bar Chart: Active User Count Per Day", fontsize=18, fontweight='bold', pad=15)
    plt.xlabel("Day of the Week", fontsize=14)
    plt.ylabel("Active User Count (Daywise)", fontsize=14)
    plt.xticks(rotation=0, fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(title="User Type", fontsize=12)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    # plt.show()


    # Plot: Bubble Chart
    plt.figure(figsize=(16, 10))
    sns.scatterplot(
        data=df_user_type_per_day,
        x="day_of_week",
        y="user_type",
        size="active_user_count_daywise",
        sizes=(100, 1000),
        hue="user_type",
        palette=["#ef7c8e", "#fae8e0", "#b6e2d3", "#d8a7b1"],
        legend=False
    )
    for i, row in df_user_type_per_day.iterrows():
        plt.text(
            row["day_of_week"],
            row["user_type"],
            f'{row["active_user_count_daywise"]:.0f}',
            fontsize=10,
            color="black",
            ha="center",
            va="center"
        )
    plt.title("🔵 Bubble Chart: Active User Count Per Day", fontsize=18, fontweight='bold', pad=15)
    plt.xlabel("Day of the Week", fontsize=14)
    plt.ylabel("User Type", fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    # plt.show()

    # Plot 2: Line Chart with Updated Data
    plt.figure(figsize=(16, 10))
    sns.lineplot(
        data=df_user_type_per_day,
        x="day_of_week",
        y="active_user_count_daywise",
        hue="user_type",
        style="user_type",
        markers=True,
        palette=["#ef7c8e", "#fae8e0", "#b6e2d3", "#d8a7b1"]  # Same custom colors
    )
    for user_type in df_user_type_per_day["user_type"].unique():
        user_data = df_user_type_per_day[df_user_type_per_day["user_type"] == user_type]
        for i, row in user_data.iterrows():
            plt.text(
                row["day_of_week"],
                row["active_user_count_daywise"] + 0.5,
                f'{row["active_user_count_daywise"]:.1f}',
                fontsize=10,
                color="black",
                ha="center"
            )
    plt.title("📈 Line Chart: Active User Count Per Day with Custom Colors", fontsize=18, fontweight='bold', pad=15)
    plt.xlabel("Day of the Week", fontsize=14)
    plt.ylabel("Active User Count (Daywise)", fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.grid(axis="y", linestyle="--", alpha=0.6)
    plt.tight_layout()
    # plt.show()

except Exception as e:
    print(f"An error occurred: {e}")


