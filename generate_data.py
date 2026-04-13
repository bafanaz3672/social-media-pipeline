import mysql.connector
import random
from faker import Faker
from datetime import datetime, timedelta

# ── Setup ─────────────────────────────────────────────────────────────────────
fake = Faker()
random.seed(42)

# ── Connect to MySQL ──────────────────────────────────────────────────────────
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Lerial@3672",   # ⚠️ Replace with your MySQL root password
        database="socialmediadb"
    )
    cursor = conn.cursor()
    print("✅ Connected to MySQL successfully!")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit()

# ── Clear existing data (safe to re-run) ──────────────────────────────────────
cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
cursor.execute("TRUNCATE TABLE engagement")
cursor.execute("TRUNCATE TABLE posts")
cursor.execute("TRUNCATE TABLE users")
cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
conn.commit()
print("✅ Tables cleared and reset!")

# ── Reference Data ────────────────────────────────────────────────────────────
platforms       = ["Instagram", "TikTok", "YouTube"]
post_types      = ["image", "video", "reel", "story"]
countries       = ["South Africa", "Nigeria", "Kenya", "Ghana", "Egypt",
                   "USA", "UK", "Canada", "India", "Brazil"]
hashtag_options = [
    "#fitness", "#travel", "#food", "#fashion", "#tech",
    "#health", "#lifestyle", "#nature", "#music", "#sports",
    "#motivation", "#beauty", "#art", "#photography", "#business"
]

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — INSERT USERS (220 rows)
# ✴️ Problem 1: ~10% of usernames will be NULL
# ─────────────────────────────────────────────────────────────────────────────
print("\n⏳ Inserting users...")

for i in range(220):
    # Inject NULL username for every 10th user (~10%)
    if i % 10 == 0:
        username = None                           # ✴️ intentional NULL
    else:
        username = fake.user_name()

    platform       = random.choice(platforms)
    country        = random.choice(countries)
    follower_count = random.randint(100, 1_000_000)
    joined_date    = str(fake.date_between(start_date="-5y", end_date="today"))

    cursor.execute("""
        INSERT INTO users (username, platform, country, follower_count, joined_date)
        VALUES (%s, %s, %s, %s, %s)
    """, (username, platform, country, follower_count, joined_date))

conn.commit()
print("✅ 220 users inserted!")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — INSERT POSTS (220 rows)
# ✴️ Problem 2: 5 duplicate posts inserted at the end
# ✴️ Problem 3: Future dates for every 40th post
# ✴️ Problem 4: Inconsistent platform names
# ─────────────────────────────────────────────────────────────────────────────
print("\n⏳ Inserting posts...")

for i in range(220):
    user_id   = random.randint(1, 220)
    post_type = random.choice(post_types)

    # Inject wrong platform name for every 15th post
    if i % 15 == 0:
        platform = random.choice(["Insta", "tiktok", "YOUTUBE"])  # ✴️ bad casing
    else:
        platform = random.choice(platforms)

    # Inject future date for every 40th post
    if i % 40 == 0:
        posted_at = str(datetime.now() + timedelta(days=random.randint(1, 30)))  # ✴️ future
    else:
        posted_at = str(fake.date_time_between(start_date="-2y", end_date="now"))

    caption  = fake.sentence(nb_words=10)
    hashtags = " ".join(random.sample(hashtag_options, 3))

    cursor.execute("""
        INSERT INTO posts (user_id, platform, post_type, posted_at, caption, hashtags)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (user_id, platform, post_type, posted_at, caption, hashtags))

conn.commit()

# Inject duplicate posts
print("⏳ Injecting duplicate posts...")
cursor.execute("SELECT user_id, platform, post_type, posted_at, caption, hashtags FROM posts LIMIT 5")
duplicates = cursor.fetchall()
for row in duplicates:
    cursor.execute("""
        INSERT INTO posts (user_id, platform, post_type, posted_at, caption, hashtags)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, row)

conn.commit()
print("✅ 225 posts inserted (including 5 duplicates)!")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — INSERT ENGAGEMENT (one record per post)
# ✴️ Problem 5: Negative likes for every 30th record
# ─────────────────────────────────────────────────────────────────────────────
print("\n⏳ Inserting engagement records...")

cursor.execute("SELECT post_id FROM posts")
post_ids = [row[0] for row in cursor.fetchall()]

for i, post_id in enumerate(post_ids):
    views    = random.randint(500, 500_000)
    comments = random.randint(0, 5000)
    shares   = random.randint(0, 3000)

    # Inject negative likes for every 30th record
    if i % 30 == 0:
        likes = random.randint(-100, -1)          # ✴️ invalid negative likes
    else:
        likes = random.randint(0, views)

    recorded_at = str(fake.date_time_between(start_date="-1y", end_date="now"))

    cursor.execute("""
        INSERT INTO engagement (post_id, likes, comments, shares, views, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (post_id, likes, comments, shares, views, recorded_at))

conn.commit()
print("✅ Engagement records inserted!")

# ── Final Count Check ─────────────────────────────────────────────────────────
print("\n📊 Final row counts:")
for table in ["users", "posts", "engagement"]:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"   {table}: {count} rows")

cursor.close()
conn.close()
print("\n✅ All done! Data generation complete.")

