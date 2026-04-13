import mysql.connector
import pandas as pd
import os

# ── Connect to MySQL ──────────────────────────────────────────────────────────
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Lerial@3672",   # ⚠️ Replace with your MySQL root password
        database="socialmediadb"
    )
    print("✅ Connected to MySQL successfully!")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit()

# ── Export each table to CSV ───────────────────────────────────────────────────
tables = ["users", "posts", "engagement"]

for table in tables:
    print(f"\n⏳ Exporting {table}...")

    # Read the entire table into a Pandas DataFrame
    df = pd.read_sql(f"SELECT * FROM {table}", conn)

    # Save to CSV in the data/ folder
    output_path = os.path.join("data", f"{table}.csv")
    df.to_csv(output_path, index=False)

    print(f"✅ {table}.csv saved! ({len(df)} rows, {len(df.columns)} columns)")

conn.close()
print("\n✅ All tables exported to the data/ folder!")

# =============================================================================
# STEP 4 — LOAD CSV FILES INTO PANDAS
# =============================================================================

print("\n" + "="*60)
print("STEP 4: LOADING CSV FILES INTO PANDAS")
print("="*60)

# ── Load each CSV into a DataFrame ────────────────────────────────────────────
users      = pd.read_csv("data/users.csv")
posts      = pd.read_csv("data/posts.csv", parse_dates=["posted_at"])
engagement = pd.read_csv("data/engagement.csv", parse_dates=["recorded_at"])

print("\n✅ All 3 CSV files loaded successfully!")

# ── Check Shape (rows and columns) ────────────────────────────────────────────
print("\n" + "-"*40)
print("📐 SHAPE (rows, columns)")
print("-"*40)
print(f"   users:      {users.shape}")
print(f"   posts:      {posts.shape}")
print(f"   engagement: {engagement.shape}")

# ── Check Data Types ──────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("🔎 DATA TYPES — USERS")
print("-"*40)
print(users.dtypes)

print("\n" + "-"*40)
print("🔎 DATA TYPES — POSTS")
print("-"*40)
print(posts.dtypes)

print("\n" + "-"*40)
print("🔎 DATA TYPES — ENGAGEMENT")
print("-"*40)
print(engagement.dtypes)

# ── Check Null Values ─────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("⚠️  NULL VALUES — USERS")
print("-"*40)
print(users.isnull().sum())

print("\n" + "-"*40)
print("⚠️  NULL VALUES — POSTS")
print("-"*40)
print(posts.isnull().sum())

print("\n" + "-"*40)
print("⚠️  NULL VALUES — ENGAGEMENT")
print("-"*40)
print(engagement.isnull().sum())

# ── Preview First 5 Rows of Each Table ───────────────────────────────────────
print("\n" + "-"*40)
print("👀 PREVIEW — USERS (first 5 rows)")
print("-"*40)
print(users.head())

print("\n" + "-"*40)
print("👀 PREVIEW — POSTS (first 5 rows)")
print("-"*40)
print(posts.head())

print("\n" + "-"*40)
print("👀 PREVIEW — ENGAGEMENT (first 5 rows)")
print("-"*40)
print(engagement.head())

# =============================================================================
# STEP 5 — CLEAN THE DATA
# =============================================================================

print("\n" + "="*60)
print("STEP 5: CLEANING THE DATA")
print("="*60)

# ── Record original row counts before cleaning ────────────────────────────────
users_before      = len(users)
posts_before      = len(posts)
engagement_before = len(engagement)

# ─────────────────────────────────────────────────────────────────────────────
# FIX 1 — Handle NULL usernames
# We will fill NULL usernames with "unknown_user"
# Reason: dropping them would lose valid platform/country/follower data
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("🔧 FIX 1: Handling NULL usernames")
print("-"*40)

nulls_before = users["username"].isnull().sum()
users["username"] = users["username"].fillna("unknown_user")
nulls_after = users["username"].isnull().sum()

print(f"   NULL usernames before: {nulls_before}")
print(f"   NULL usernames after:  {nulls_after}")
print("   ✅ NULL usernames filled with 'unknown_user'")

# ─────────────────────────────────────────────────────────────────────────────
# FIX 2 — Remove duplicate posts
# Keep the first occurrence, drop the rest
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("🔧 FIX 2: Removing duplicate posts")
print("-"*40)

dupes_before = posts.duplicated(subset=["user_id", "platform", "posted_at", "caption"]).sum()
posts = posts.drop_duplicates(subset=["user_id", "platform", "posted_at", "caption"], keep="first")
dupes_after = posts.duplicated(subset=["user_id", "platform", "posted_at", "caption"]).sum()

print(f"   Duplicate rows before: {dupes_before}")
print(f"   Duplicate rows after:  {dupes_after}")
print(f"   Rows remaining in posts: {len(posts)}")
print("   ✅ Duplicates removed")

# ─────────────────────────────────────────────────────────────────────────────
# FIX 3 — Remove negative likes
# Any likes value below 0 is invalid
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("🔧 FIX 3: Removing negative likes")
print("-"*40)

neg_before = (engagement["likes"] < 0).sum()
engagement = engagement[engagement["likes"] >= 0]
neg_after  = (engagement["likes"] < 0).sum()

print(f"   Negative likes before: {neg_before}")
print(f"   Negative likes after:  {neg_after}")
print(f"   Rows remaining in engagement: {len(engagement)}")
print("   ✅ Negative likes removed")

# ─────────────────────────────────────────────────────────────────────────────
# FIX 4 — Remove future dates
# Posts with posted_at in the future are invalid
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("🔧 FIX 4: Removing future post dates")
print("-"*40)

now = pd.Timestamp.now()
future_before = (posts["posted_at"] > now).sum()
posts = posts[posts["posted_at"] <= now]
future_after = (posts["posted_at"] > now).sum()

print(f"   Future dated posts before: {future_before}")
print(f"   Future dated posts after:  {future_after}")
print(f"   Rows remaining in posts: {len(posts)}")
print("   ✅ Future dates removed")

# ─────────────────────────────────────────────────────────────────────────────
# FIX 5 — Standardise platform names
# Convert all variations to Title Case e.g. Insta → Instagram is not possible
# but tiktok → Tiktok, YOUTUBE → Youtube gets standardised at minimum
# We also map known wrong values to correct ones
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("🔧 FIX 5: Standardising platform names")
print("-"*40)

print("   Platform values before cleaning:")
print(f"   {posts['platform'].value_counts().to_dict()}")

# Map known wrong values to correct ones
platform_mapping = {
    "Insta"   : "Instagram",
    "insta"   : "Instagram",
    "tiktok"  : "TikTok",
    "TIKTOK"  : "TikTok",
    "youtube" : "YouTube",
    "YOUTUBE" : "YouTube",
    "Youtube" : "YouTube",
}
posts["platform"] = posts["platform"].replace(platform_mapping)

# Apply title case as a final safety net
posts["platform"] = posts["platform"].str.strip()

print("   Platform values after cleaning:")
print(f"   {posts['platform'].value_counts().to_dict()}")
print("   ✅ Platform names standardised")

# ── Summary of Cleaning ───────────────────────────────────────────────────────
print("\n" + "="*60)
print("🧹 CLEANING SUMMARY")
print("="*60)
print(f"   users:      {users_before} → {len(users)} rows")
print(f"   posts:      {posts_before} → {len(posts)} rows")
print(f"   engagement: {engagement_before} → {len(engagement)} rows")

# ── Export cleaned data to new CSV files ─────────────────────────────────────
print("\n⏳ Saving cleaned CSV files...")
users.to_csv("data/users_cleaned.csv", index=False)
posts.to_csv("data/posts_cleaned.csv", index=False)
engagement.to_csv("data/engagement_cleaned.csv", index=False)
print("✅ Cleaned files saved:")
print("   data/users_cleaned.csv")
print("   data/posts_cleaned.csv")
print("   data/engagement_cleaned.csv")

# =============================================================================
# STEP 6 — TRANSFORM & ANALYSE
# =============================================================================

print("\n" + "="*60)
print("STEP 6: TRANSFORM & ANALYSE")
print("="*60)

# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — Join all 3 tables into one combined DataFrame
# We join posts + engagement first, then add users
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("🔗 TASK 1: Joining all 3 tables")
print("-"*40)

df = posts.merge(engagement, on="post_id", how="inner") \
          .merge(users, on="user_id", how="inner")

print(f"   posts rows:      {len(posts)}")
print(f"   engagement rows: {len(engagement)}")
print(f"   users rows:      {len(users)}")
print(f"   Combined df:     {len(df)} rows, {len(df.columns)} columns")
print("   ✅ Tables joined successfully!")

# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — Add engagement_rate column
# Formula: (likes + comments + shares) / views
# We use replace(0, pd.NA) to avoid divide-by-zero errors
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("📐 TASK 2: Calculating engagement_rate")
print("-"*40)

df["engagement_rate"] = (
    (df["likes"] + df["comments"] + df["shares"])
    / df["views"].replace(0, pd.NA)
)

print(f"   Min engagement rate:  {df['engagement_rate'].min():.4f}")
print(f"   Max engagement rate:  {df['engagement_rate'].max():.4f}")
print(f"   Mean engagement rate: {df['engagement_rate'].mean():.4f}")
print("   ✅ engagement_rate column added!")

# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 — Total posts per platform
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("📊 TASK 3: Total posts per platform")
print("-"*40)

posts_per_platform = df.groupby("platform_x")["post_id"].count().reset_index()
posts_per_platform.columns = ["platform", "total_posts"]
posts_per_platform = posts_per_platform.sort_values("total_posts", ascending=False)

print(posts_per_platform.to_string(index=False))
print("   ✅ Posts per platform calculated!")

# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 — Top 5 hashtags by total engagement
# Each post has multiple hashtags so we need to split and explode them
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("🏷️  TASK 4: Top 5 hashtags by total engagement")
print("-"*40)

# Calculate total engagement per post
df["total_engagement"] = df["likes"] + df["comments"] + df["shares"]

# Split hashtags into individual tags and explode into separate rows
hashtag_df = df[["hashtags", "total_engagement"]].copy()
hashtag_df["hashtags"] = hashtag_df["hashtags"].str.split()
hashtag_df = hashtag_df.explode("hashtags")

# Group by hashtag and sum total engagement
top_hashtags = hashtag_df.groupby("hashtags")["total_engagement"] \
                          .sum() \
                          .reset_index() \
                          .sort_values("total_engagement", ascending=False) \
                          .head(5)

top_hashtags.columns = ["hashtag", "total_engagement"]
print(top_hashtags.to_string(index=False))
print("   ✅ Top 5 hashtags calculated!")

# ─────────────────────────────────────────────────────────────────────────────
# TASK 5 — Average engagement rate by post type
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "-"*40)
print("🎬 TASK 5: Avg engagement rate by post type")
print("-"*40)

avg_by_post_type = df.groupby("post_type")["engagement_rate"] \
                     .mean() \
                     .reset_index() \
                     .sort_values("engagement_rate", ascending=False)

avg_by_post_type.columns = ["post_type", "avg_engagement_rate"]
avg_by_post_type["avg_engagement_rate"] = avg_by_post_type["avg_engagement_rate"].round(4)

print(avg_by_post_type.to_string(index=False))
print("   ✅ Avg engagement rate by post type calculated!")

# ── Save the final combined DataFrame to CSV ──────────────────────────────────
print("\n⏳ Saving final combined dataset...")
df.to_csv("data/final_combined.csv", index=False)
print("✅ data/final_combined.csv saved!")
print(f"   Shape: {df.shape}")

# =============================================================================
# STEP 7 — VISUALISATIONS
# =============================================================================
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

print("\n" + "="*60)
print("STEP 7: CREATING VISUALISATIONS")
print("="*60)

# ── Style settings ────────────────────────────────────────────────────────────
plt.rcParams["figure.facecolor"] = "#f9f9f9"
plt.rcParams["axes.facecolor"]   = "#ffffff"
plt.rcParams["axes.spines.top"]  = False
plt.rcParams["axes.spines.right"]= False
plt.rcParams["font.family"]      = "sans-serif"

COLORS = ["#4C72B0", "#DD8452", "#55A868"]

# ─────────────────────────────────────────────────────────────────────────────
# CHART 1 — Bar chart: Total posts per platform
# ─────────────────────────────────────────────────────────────────────────────
print("\n⏳ Creating Chart 1: Posts per platform...")

fig, ax = plt.subplots(figsize=(8, 5))

ax.bar(
    posts_per_platform["platform"],
    posts_per_platform["total_posts"],
    color=COLORS,
    edgecolor="white",
    width=0.5
)

# Add value labels on top of each bar
for i, val in enumerate(posts_per_platform["total_posts"]):
    ax.text(i, val + 1, str(val), ha="center", va="bottom", fontsize=11, fontweight="bold")

ax.set_title("Total Posts per Platform", fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Platform", fontsize=11)
ax.set_ylabel("Number of Posts", fontsize=11)
ax.set_ylim(0, posts_per_platform["total_posts"].max() + 15)

plt.tight_layout()
plt.savefig("charts/chart1_posts_per_platform.png", dpi=150, bbox_inches="tight")
plt.close()
print("✅ charts/chart1_posts_per_platform.png saved!")

# ─────────────────────────────────────────────────────────────────────────────
# CHART 2 — Line chart: Total likes over time (by week)
# ─────────────────────────────────────────────────────────────────────────────
print("\n⏳ Creating Chart 2: Likes over time...")

# Group likes by week
df["week"] = pd.to_datetime(df["posted_at"]).dt.to_period("W").dt.start_time
likes_over_time = df.groupby("week")["likes"].sum().reset_index()
likes_over_time.columns = ["week", "total_likes"]

fig, ax = plt.subplots(figsize=(12, 5))

ax.plot(
    likes_over_time["week"],
    likes_over_time["total_likes"],
    color="#4C72B0",
    linewidth=2,
    marker="o",
    markersize=4
)

# Shade area under the line
ax.fill_between(
    likes_over_time["week"],
    likes_over_time["total_likes"],
    alpha=0.15,
    color="#4C72B0"
)

ax.set_title("Total Likes Over Time (by Week)", fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Week", fontsize=11)
ax.set_ylabel("Total Likes", fontsize=11)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
plt.xticks(rotation=45, ha="right")

plt.tight_layout()
plt.savefig("charts/chart2_likes_over_time.png", dpi=150, bbox_inches="tight")
plt.close()
print("✅ charts/chart2_likes_over_time.png saved!")

# ─────────────────────────────────────────────────────────────────────────────
# CHART 3 — Horizontal bar chart: Top 5 hashtags by total engagement
# ─────────────────────────────────────────────────────────────────────────────
print("\n⏳ Creating Chart 3: Top 5 hashtags...")

# Sort ascending so biggest bar is at the top
top_hashtags_sorted = top_hashtags.sort_values("total_engagement", ascending=True)

fig, ax = plt.subplots(figsize=(9, 5))

bars = ax.barh(
    top_hashtags_sorted["hashtag"],
    top_hashtags_sorted["total_engagement"],
    color="#55A868",
    edgecolor="white",
    height=0.5
)

# Add value labels at the end of each bar
for bar in bars:
    width = bar.get_width()
    ax.text(
        width + 50000,
        bar.get_y() + bar.get_height() / 2,
        f"{int(width):,}",
        va="center",
        fontsize=10,
        fontweight="bold"
    )

ax.set_title("Top 5 Hashtags by Total Engagement", fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Total Engagement", fontsize=11)
ax.set_ylabel("Hashtag", fontsize=11)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
ax.set_xlim(0, top_hashtags_sorted["total_engagement"].max() + 800000)

plt.tight_layout()
plt.savefig("charts/chart3_top_hashtags.png", dpi=150, bbox_inches="tight")
plt.close()
print("✅ charts/chart3_top_hashtags.png saved!")

# ── Final summary ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("🎉 ALL STEPS COMPLETE!")
print("="*60)
print("""
Your pipeline is fully built! Here's what was produced:

📁 data/
   ├── users.csv
   ├── posts.csv
   ├── engagement.csv
   ├── users_cleaned.csv
   ├── posts_cleaned.csv
   ├── engagement_cleaned.csv
   └── final_combined.csv

📊 charts/
   ├── chart1_posts_per_platform.png
   ├── chart2_likes_over_time.png
   └── chart3_top_hashtags.png
""")