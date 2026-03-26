"""Generates synthetic post-export JSON files and a notes text file.

The original data used on Databricks is not published.
These files have the same shape: two arrays of post structs with different fields,
overlapping short codes, a few null short codes, and a notes file that covers only some posts.
"""
import json, random, string, datetime, os

random.seed(7)
os.makedirs("data", exist_ok=True)

def code():
    return "".join(random.choice(string.ascii_letters + string.digits + "_-") for _ in range(11))

def ts(i):
    start = datetime.datetime(2025, 6, 1, 12, 0, 0)
    d = start + datetime.timedelta(days=i * 4, minutes=random.randint(0, 600))
    return d.strftime("%Y-%m-%dT%H:%M:%S.000Z")

CAPTIONS = ["Morning walk", "New desk setup", "Behind the scenes", "Trip notes", "Weekend project", "Q&A", "Quick tip", "Gear review"]
TYPES = ["Video", "Image", "Sidecar"]

codes = [code() for _ in range(60)]
feed_codes = codes[:40]
scraped_codes = codes[25:60]            # 15 short codes appear in both arrays

def feed_post(i, c):
    return {"shortCode": c, "type": random.choice(TYPES), "likesCount": random.randint(50, 5000),
            "commentsCount": random.randint(0, 200), "videoViewCount": random.randint(0, 40000),
            "caption": random.choice(CAPTIONS), "timestamp": ts(i), "displayUrl": f"https://example.com/img/{c}.jpg",
            "ownerId": "1000001"}

def scraped_post(i, c):
    return {"shortCode": c, "type": random.choice(TYPES), "likesCount": random.randint(50, 5000),
            "commentsCount": random.randint(0, 200), "videoViewCount": random.randint(0, 40000),
            "caption": random.choice(CAPTIONS), "timestamp": ts(i), "audioUrl": f"https://example.com/a/{c}.m4a",
            "musicInfo": {"title": "demo track", "artist": "demo artist"}}

feed = [feed_post(i, c) for i, c in enumerate(feed_codes)]
scraped = [scraped_post(i + 25, c) for i, c in enumerate(scraped_codes)]
for k in range(3):                      # posts without a short code
    p = scraped_post(70 + k, None); p["shortCode"] = None; scraped.append(p)

# split into 3 files, like several exports of the same account
for n in range(3):
    doc = {"username": "demo_creator",
           "feedPosts": feed[n::3], "scrapedPosts": scraped[n::3], "taggedPosts": []}
    with open(f"data/posts_export_{n+1}.json", "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)

# notes file: covers about 60% of the posts, plus one "Unknown" block
VIBES = ["Documentary", "Casual vlog", "Tutorial", "Interview", "Travel"]
OBJECTS = ["person, phone, desk", "car, road", "laptop, coffee cup", "dog, park", "guitar, microphone"]
covered = random.sample(codes, 36)
lines = []
for c in covered:
    lines.append(f"\U0001F4C2 POST PATH: /videos/demo_creator/{c}.mp4")
    lines.append(f"\U0001F539 Shortcode: {c}")
    lines.append(f"Vibe: {random.choice(VIBES)}")
    lines.append(f"Objects: {random.choice(OBJECTS)} | Speech: yes")
    lines.append("")
lines += ["\U0001F4C2 POST PATH: /videos/demo_creator/unknown.mp4", "\U0001F539 Shortcode: Unknown", "Vibe: n/a", "Objects: n/a | Speech: no", ""]
with open("data/video_analysis_notes.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print("posts:", len(feed), "feed +", len(scraped), "scraped; notes blocks:", len(covered) + 1)
