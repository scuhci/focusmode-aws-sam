import os
import json
import boto3 # type: ignore
import hashlib
import time
import requests # type: ignore
import pandas as pd # type: ignore


from boto3.dynamodb.conditions import Key # type: ignore
from datetime import datetime, timedelta
from decimal import Decimal
from googleapiclient.discovery import build # type: ignore


# constants:
USER_TABLE_NAME = "focusmode-FocusModeUserTable-6JC0TNI2RB93"               #os.environ.get("UserTableName", None)
DATA_TABLE_NAME = "focusmode-FocusModeDataCollectionTable-1KRCB5ZWJ6ONL"    #os.environ.get("DataTableName", None)
ADMIN_TABLE_NAME = "focusmode-FocusModeAdminTable-1L8IZJNFRPT8F"            #os.environ.get("AdminTableName", None)
USER_PREFERENCE_DATA_TABLE_NAME = "focusmode-FocusModeUserPreferenceDataTable-1GDK11Q0RIAAO" #os.environ.get("UserPreferenceDataTableName") 
DAILY_SURVEY_DATA_TABLE_NAME = "focusmode-FocusModeDailySurveyResponseTable-NESGY2X0XTDN" #os.environ.get("DailySurveyDataTableName")
POST_STAGE_SURVEY_DATA_TABLE_NAME = "focusmode-FocusModePostStageSurveyResponseTable-1I0Z60LUCHJ13" #os.environ.get("PostStageSurveyDataTableName")
POST_STUDY_SURVEY_DATA_TABLE_NAME = "focusmode-FocusModePostStudySurveyResponseTable-DV2UIJHGAI1U"
VIDEO_RECORD_LOG_TABLE_NAME = "focusmode-FocusModeVideoRecordLogTable-YQFLJM3NFHAV" #os.environ.get("VideoRecordLogTableName")

CORS_HEADERS = {
    "Access-Control-Allow-Headers" : "Content-Type",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
}

YOUTUBE_API_KEY = os.environ["YouTubeApiKey"]

dynamodb = boto3.resource("dynamodb")
admin_table = dynamodb.Table(ADMIN_TABLE_NAME)
user_table = dynamodb.Table(USER_TABLE_NAME)
user_pref_data_table = dynamodb.Table(USER_PREFERENCE_DATA_TABLE_NAME)
video_record_log_table = dynamodb.Table(VIDEO_RECORD_LOG_TABLE_NAME)




# FEW_SHOT_EXAMPLES = """
# #### Rule 1 (Any previous focusMode is True)


# Title: "Astronomy 101: Life Cycle of Stars"
# Description: "This video explores how stars form, evolve, and end, featuring illustrations and expert narration."
# Current category: "Education"
# User-selected focus categories: ["Education", "Science and Technology", "Documentary"]
# History: [
#  {"categoryId": "Education", "focusMode": true},
#  {"categoryId": "Music", "focusMode": false},
#  {"categoryId": "Gaming", "focusMode": false}
# ]
# Subscribed: false
# Intent source: "/home"
# Rules:
# 1: true
# 2: false
# 3: true
# 4: true
# 5: false
# 6: false
# 7: false
# 8: false
# 9: true ("explores", "stars", "evolve")
# 10: true
# → **True**
# Reason: Prior focus session and matching focus category.
# explanation_summary: "Confidence: 90% | Key Evidence: Prior focus and matching category."
# confidence: "90%"

# ---
# ### Rule 2 (Previous category == current AND focus was True) — Example 1


# ### Rule 2 (Previous category == current AND focus was True)


# Title: "Tesla Model S Deep Dive"
# Description: "We examine the design, features, and performance of the 2024 Model S."
# Current category: "Autos and Vehicles"
# User-selected focus categories: ["Science and Technology", "Autos and Vehicles"]
# History: [
#  {"categoryId": "Autos and Vehicles", "focusMode": true},
#  {"categoryId": "Comedy", "focusMode": false},
#  {"categoryId": "Gaming", "focusMode": false}
# ]
# Subscribed: false
# Intent source: "/watch"
# Rules:
# 1: true
# 2: true
# 3: false
# 4: true
# 5: true
# 6: false
# 7: false
# 8: true
# 9: true ("design", "performance")
# 10: true
# → **True**
# Reason: Prior focus in same category.
# explanation_summary: "Confidence: 85% | Key Evidence: Same category with prior focus."
# confidence: "85%"
# ---

# ### Rule 2 — Example 2

# Title: "Acoustic Guitar Fingerpicking Lesson"
# Description: "Learn basic fingerpicking techniques with this step-by-step tutorial for beginners."
# Current categoryId: 27
# User-selected focus categories: ["27"]
# History: [
#  {"categoryId": "27", "focusMode": true},
#  {"categoryId": "10", "focusMode": false},
#  {"categoryId": "24", "focusMode": false}
# ]
# Subscribed: true
# Intent source: "/channel"
# Rules:
# 1: true
# 2: true
# 3: true
# 4: true
# 5: true
# 6: true
# 7: true
# 8: true
# 9: true ("lesson", "tutorial", "techniques")
# 10: true
# → **True**
# Reason: Past session had focus in same category, user is subscribed, and content is highly instructional.

# ---
# ### Rule 3 (Current category in focus-supporting list)


# Title: "Machine Learning Basics"
# Description: "A crash course in supervised vs. unsupervised learning with examples."
# Current category: "Education"
# User-selected focus categories: ["Education", "Science and Technology", "Howto and Style"]
# History: [
#  {"categoryId": "Comedy", "focusMode": false},
#  {"categoryId": "Gaming", "focusMode": false},
#  {"categoryId": "Education", "focusMode": false}
# ]
# Subscribed: false
# Intent source: "/search"
# Rules:
# 1: false
# 2: false
# 3: true
# 4: true
# 5: false
# 6: false
# 7: false
# 8: true
# 9: true ("crash course", "learning")
# 10: true
# → **True**
# Reason: Education category and learning keywords.
# explanation_summary: "Confidence: 80% | Key Evidence: Learning topic and search intent."
# confidence: "100%"
# ---

# ### Rule 3 — Example 2


# Title: "Science of Sleep: How the Brain Rests"
# Description: "Explore the biology behind sleep cycles, REM, and memory consolidation."
# Current categoryId: 27
# User-selected focus categories: ["27"]
# History: [
#  {"categoryId": "17", "focusMode": false},
#  {"categoryId": "10", "focusMode": false},
#  {"categoryId": "24", "focusMode": false}
# ]
# Subscribed: true
# Intent source: "/channelPage"
# Rules:
# 1: false
# 2: false
# 3: true
# 4: false
# 5: false
# 6: false
# 7: true
# 8: true
# 9: true ("biology", "sleep cycles", "REM")
# 10: true
# → **True**
# Reason: Clear science/education topic, user subscribed, high relevance.


# ---
# ### Rule 4 (Any previous category matches current) — Example 1


# ### Rule 4 (Any previous category matches current)


# Title: "Guitar Chord Progressions Explained"
# Description: "Understand how to build beautiful chord progressions for songwriting."
# Current category: "Music"
# User-selected focus categories: ["Music", "Education"]
# History: [
#  {"categoryId": "Comedy", "focusMode": false},
#  {"categoryId": "Music", "focusMode": false},
#  {"categoryId": "Gaming", "focusMode": false}
# ]
# Subscribed: false
# Intent source: "/home"
# Rules:
# 1: false
# 2: false
# 3: true
# 4: true
# 5: false
# 6: false
# 7: false
# 8: false
# 9: true ("chord", "songwriting", "explained")
# 10: true
# → **True**
# Reason: Category match and learning keywords.
# explanation_summary: "Confidence: 75% | Key Evidence: Category match and learning keywords."
# confidence: "75%"



# ---
# ### Rule 4 — Example 2


# Title: "Dog Training Tips: Stop Barking"
# Description: "Practical tips for training your dog to reduce excessive barking at home."
# Current categoryId: 15
# User-selected focus categories: ["10"]
# History: [
#  {"categoryId": "15", "focusMode": false},
#  {"categoryId": "15", "focusMode": false},
#  {"categoryId": "2", "focusMode": false}
# ]
# Subscribed: false
# Intent source: "/watch"
# Rules:
# 1: false
# 2: false
# 3: false
# 4: true
# 5: false
# 6: false
# 7: false
# 8: true
# 9: true ("training", "tips", "dog")
# 10: true
# → **True**
# Reason: Category repeated and clear how-to framing triggers learning intent.


# ---
# ### Rule 5 (Focus=True AND category matched current)


# Title: "Python Loops Explained"
# Description: "Learn how for-loops and while-loops work in Python with examples."
# Current category: "Education"
# User-selected focus categories: ["Education", "Science and Technology"]
# History: [
#  {"categoryId": "Music", "focusMode": false},
#  {"categoryId": "Education", "focusMode": true},
#  {"categoryId": "Comedy", "focusMode": false}
# ]
# Subscribed: false
# Intent source: "/search"
# Rules:
# 1: true
# 2: true
# 3: true
# 4: true
# 5: true
# 6: false
# 7: false
# 8: true
# 9: true ("learn", "examples", "loops")
# 10: true
# → **True**
# Reason: Prior focus and learning content.
# explanation_summary: "Confidence: 90% | Key Evidence: Prior focus and matching category."
# confidence: "90%"
# ---
# ### Rule 6 (Current category appears ≥2×) — Example 1


# ### Rule 6 (Current category repeated ≥2×)


# Title: "Beginner Workout Plan"
# Description: "Daily fitness routines for those new to the gym."
# Current category: "Howto and Style"
# User-selected focus categories: ["Howto and Style", "Education"]
# History: [
#  {"categoryId": "Howto and Style", "focusMode": false},
#  {"categoryId": "Howto and Style", "focusMode": false},
#  {"categoryId": "Gaming", "focusMode": false}
# ]
# Subscribed: false
# Intent source: "/home"
# Rules:
# 1: false
# 2: false
# 3: true
# 4: true
# 5: false
# 6: true
# 7: false
# 8: false
# 9: true ("fitness", "routines")
# 10: true
# → **True**
# Reason: Repeated category and instructional keywords.
# explanation_summary: "Confidence: 75% | Key Evidence: Repeated category and learning topic."
# confidence: "75%"
# ---
# ### Rule 6 — Example 2


# Title: "Beginner Workout Plan"
# Description: "Daily fitness routines for those new to the gym and home workouts."
# Current categoryId: 17
# User-selected focus categories: ["17"]
# History: [
#  {"categoryId": "17", "focusMode": false},
#  {"categoryId": "17", "focusMode": false},
#  {"categoryId": "17", "focusMode": false}
# ]
# Subscribed: false
# Intent source: "/home"
# Rules:
# 1: false
# 2: false
# 3: true
# 4: true
# 5: false
# 6: true
# 7: false
# 8: false
# 9: true ("fitness", "routines", "workouts")
# 10: true
# → **True**
# Reason: User repeatedly explores this category; clear learning intent.


# ### Rule 7 (User is Subscribed)


# ### Rule 7 (User is Subscribed)


# Title: "How to Start a YouTube Channel"
# Description: "A full beginner's guide on equipment and branding."
# Current category: "Education"
# User-selected focus categories: ["Education", "Howto and Style"]
# History: [
#  {"categoryId": "Education", "focusMode": false},
#  {"categoryId": "Gaming", "focusMode": false},
#  {"categoryId": "Comedy", "focusMode": false}
# ]
# Subscribed: true
# Intent source: "/search"
# Rules:
# 1: false
# 2: false
# 3: true
# 4: true
# 5: false
# 6: false
# 7: true
# 8: true
# 9: true ("guide", "branding")
# 10: true
# → **True**
# Reason: Subscribed and searching for educational content.
# explanation_summary: "Confidence: 85% | Key Evidence: Subscribed and learning topic."
# confidence: "85%"

# ---
# ### Rule 8 (Intent source contains Search/Channel/ChannelPage)

# Title: "Learn HTML in 20 Minutes"
# Description: "Get started with web development by learning basic HTML tags and structure."
# Current category: "Education"
# User-selected focus categories: ["Education", "Howto and Style"]
# History: [
#   {"categoryId": "Entertainment", "focusMode": false},
#   {"categoryId": "Education", "focusMode": false},
#   {"categoryId": "Science and Technology", "focusMode": false}
# ]
# Subscribed: false
# Intent source: "/channel"
# Rules:
# 1: false
# 2: false
# 3: true
# 4: true
# 5: false
# 6: false
# 7: false
# 8: true
# 9: true ("learn", "web development", "tags")
# 10: true
# → **True**
# Reason: Channel-based intent, educational keywords, and matching category support focus mode.
# explanation_summary: "Confidence: 85% | Key Evidence: Channel-based learning content and category match."
# confidence: "85%"

# ---
# ### Rule 9 (Title/description contains focus keywords)

# Title: "Lecture: World War II Causes & Consequences"
# Description: "University history lecture covering major geopolitical causes and outcomes of WWII."
# Current category: "Education"
# User-selected focus categories: ["Education", "Documentary"]
# History: [
#   {"categoryId": "Music", "focusMode": false},
#   {"categoryId": "Entertainment", "focusMode": false},
#   {"categoryId": "Education", "focusMode": false}
# ]
# Subscribed: false
# Intent source: "/home"
# Rules:
# 1: false
# 2: false
# 3: true
# 4: true
# 5: false
# 6: false
# 7: false
# 8: false
# 9: true ("lecture", "history", "causes", "outcomes")
# 10: true
# → **True**
# Reason: Strong educational framing, keyword-rich description, and matching focus categories.
# explanation_summary: "Confidence: 80% | Key Evidence: Educational lecture content and keywords."
# confidence: "80%"
# ---

# ### Rule 10 (Description >50 words AND has focus keywords)

# Title: "Complete JavaScript Course for Beginners"
# Description: "This full-length course will teach you everything from variables and loops to DOM manipulation, events, and ES6. Perfect for anyone getting started with JavaScript."
# Current category: "Education"
# User-selected focus categories: ["Education", "Howto and Style"]
# History: [
#   {"categoryId": "Education", "focusMode": false},
#   {"categoryId": "Science and Technology", "focusMode": false},
#   {"categoryId": "Entertainment", "focusMode": false}
# ]
# Subscribed: true
# Intent source: "/search"
# Rules:
# 1: false
# 2: false
# 3: true
# 4: true
# 5: false
# 6: false
# 7: true
# 8: true
# 9: true ("course", "teach", "JavaScript")
# 10: true
# → **True**
# Reason: Long, detailed description, clear learning content, subscription, and search intent all strongly support focus mode.
# explanation_summary: "Confidence: 90% | Key Evidence: Detailed course content with learning keywords."
# confidence: "90%"
# ---
# ### False Example 1 – No Signals

# Title: "Daily Vlog: Grocery Shopping and Cooking"
# Description: "Spend the day with me as I run errands and cook dinner."
# Current category: "People and Blogs"
# User-selected focus categories: ["Education", "Science and Technology"]
# History: [
#   {"categoryId": "People and Blogs", "focusMode": false},
#   {"categoryId": "People and Blogs", "focusMode": false},
#   {"categoryId": "People and Blogs", "focusMode": false}
# ]
# Subscribed: false
# Intent source: "/home"
# Rules:
# 1: false
# 2: false
# 3: false
# 4: true
# 5: false
# 6: true
# 7: false
# 8: false
# 9: false
# 10: false
# → **False**
# Reason: No learning signals or focus intent.
# explanation_summary: "Confidence: 10% | Key Evidence: No focus or learning indicators."
# confidence: "50%"

# ### Now classify this new session:
# """

FEW_SHOT_EXAMPLES = """
### Example 1 – Education Focus with Search Intent

Title: "Complete JavaScript Course for Beginners"
Description: "Learn everything from variables to DOM manipulation in this full-length course."
Current category: "Education"
User-selected focus categories: ["Education", "Science and Technology"]
History: [
  {"categoryId": "Education", "focusMode": true},
  {"categoryId": "Gaming", "focusMode": false},
  {"categoryId": "Music", "focusMode": false}
]
Subscribed: true
Intent source: "/search"
Rules:
1: true
2: true
3: true
4: true
5: true
6: false
7: true
8: true
9: true ("course", "learn", "JavaScript")
10: true
→ **True**
Reason: Prior Education focus, subscription, strong keywords.
explanation_summary: "Confidence: 95% | Key Evidence: Prior focus and subscription in Education."
confidence: "95%"

---

### Example 2 – Sports with Repeated Category but No Prior Focus

Title: "Top 50 NBA Dunks"
Description: "Relive the best slam dunks in NBA history."
Current category: "Sports"
User-selected focus categories: ["Sports", "Entertainment"]
History: [
  {"categoryId": "Sports", "focusMode": false},
  {"categoryId": "Sports", "focusMode": false},
  {"categoryId": "Entertainment", "focusMode": false}
]
Subscribed: false
Intent source: "/home"
Rules:
1: false
2: false
3: true
4: true
5: false
6: true
7: false
8: false
9: true ("dunks", "NBA")
10: true
→ **True**
Reason: Repeated category and user focus preference.
explanation_summary: "Confidence: 75% | Key Evidence: Repeated Sports category."
confidence: "75%"

---

### Example 3 – Documentary with Channel Intent

Title: "Secrets of the Ocean"
Description: "Explore marine life in this documentary series."
Current category: "Documentary"
User-selected focus categories: ["Documentary", "Education"]
History: [
  {"categoryId": "Documentary", "focusMode": false},
  {"categoryId": "Travel and Events", "focusMode": false},
  {"categoryId": "Documentary", "focusMode": false}
]
Subscribed: true
Intent source: "/channel"
Rules:
1: false
2: false
3: true
4: true
5: false
6: true
7: true
8: true
9: true ("documentary", "marine life")
10: true
→ **True**
Reason: Repeated Documentary category, subscription, and keywords.
explanation_summary: "Confidence: 85% | Key Evidence: Repeated Documentary and subscription."
confidence: "85%"

---

### Example 4 – Music with Prior Focus

Title: "Live Concert – Symphony No.9"
Description: "Experience Beethoven's 9th Symphony performed live."
Current category: "Music"
User-selected focus categories: ["Music", "Entertainment"]
History: [
  {"categoryId": "Music", "focusMode": true},
  {"categoryId": "Entertainment", "focusMode": false},
  {"categoryId": "Music", "focusMode": false}
]
Subscribed: false
Intent source: "/watch"
Rules:
1: true
2: true
3: true
4: true
5: true
6: true
7: false
8: false
9: true ("concert", "symphony", "live")
10: true
→ **True**
Reason: Prior Music focus, repeated category.
explanation_summary: "Confidence: 90% | Key Evidence: Music focus history."
confidence: "90%"

---

### Example 5 – Howto & Style with Search Intent

Title: "How to Bake Sourdough Bread"
Description: "Step-by-step guide to baking perfect sourdough bread."
Current category: "Howto and Style"
User-selected focus categories: ["Howto and Style", "Education"]
History: [
  {"categoryId": "Howto and Style", "focusMode": false},
  {"categoryId": "Education", "focusMode": false},
  {"categoryId": "Howto and Style", "focusMode": false}
]
Subscribed: true
Intent source: "/search"
Rules:
1: false
2: false
3: true
4: true
5: false
6: true
7: true
8: true
9: true ("guide", "baking", "sourdough")
10: true
→ **True**
Reason: How-to keywords, search intent, subscription.
explanation_summary: "Confidence: 85% | Key Evidence: How-to keywords and search intent."
confidence: "85%"

---

### Example 6 – News & Politics Learning Content

Title: "Global Economic Outlook 2024"
Description: "An in-depth analysis of world markets and policy impacts."
Current category: "News and Politics"
User-selected focus categories: ["News and Politics", "Education"]
History: [
  {"categoryId": "News and Politics", "focusMode": false},
  {"categoryId": "Education", "focusMode": false},
  {"categoryId": "News and Politics", "focusMode": false}
]
Subscribed: false
Intent source: "/search"
Rules:
1: false
2: false
3: true
4: true
5: false
6: true
7: false
8: true
9: true ("analysis", "markets", "policy")
10: true
→ **True**
Reason: Learning keywords, search intent, repeated category.
explanation_summary: "Confidence: 80% | Key Evidence: Learning content and repeated category."
confidence: "80%"

---

### Example 7 – Sports with Prior Focus

Title: "Marathon Training Guide"
Description: "Learn the best techniques to prepare for your first marathon."
Current category: "Sports"
User-selected focus categories: ["Sports", "Health"]
History: [
  {"categoryId": "Sports", "focusMode": true},
  {"categoryId": "Health", "focusMode": false},
  {"categoryId": "Sports", "focusMode": false}
]
Subscribed: true
Intent source: "/search"
Rules:
1: true
2: true
3: true
4: true
5: true
6: true
7: true
8: true
9: true ("training", "marathon")
10: true
→ **True**
Reason: Prior focus, subscription, repeated Sports category.
explanation_summary: "Confidence: 95% | Key Evidence: Prior focus and subscription."
confidence: "95%"

---

### Example 8 – Documentary with No Prior Focus but Repeated Category

Title: "Wildlife in the Sahara"
Description: "A documentary exploring animals in the desert."
Current category: "Documentary"
User-selected focus categories: ["Documentary"]
History: [
  {"categoryId": "Documentary", "focusMode": false},
  {"categoryId": "Documentary", "focusMode": false},
  {"categoryId": "Travel and Events", "focusMode": false}
]
Subscribed: false
Intent source: "/channelPage"
Rules:
1: false
2: false
3: true
4: true
5: false
6: true
7: false
8: true
9: true ("documentary", "wildlife")
10: true
→ **True**
Reason: Repeated category and strong keywords.
explanation_summary: "Confidence: 80% | Key Evidence: Repeated category and keywords."
confidence: "80%"

---

### Example 9 – Gaming Entertainment No Focus

Title: "Fortnite Funny Moments"
Description: "Hilarious clips and fails from Fortnite matches."
Current category: "Gaming"
User-selected focus categories: ["Education"]
History: [
  {"categoryId": "Gaming", "focusMode": false},
  {"categoryId": "Entertainment", "focusMode": false},
  {"categoryId": "Gaming", "focusMode": false}
]
Subscribed: false
Intent source: "/home"
Rules:
1: false
2: false
3: false
4: true
5: false
6: true
7: false
8: false
9: false
10: false
→ **False**
Reason: Entertainment content with no focus signals.
explanation_summary: "Confidence: 20% | Key Evidence: No learning or focus signals."
confidence: "20%"

---

### Example 10 – Comedy with No Focus Signals

Title: "Best Stand-Up Comedy Clips"
Description: "Laugh along with the funniest stand-up routines."
Current category: "Comedy"
User-selected focus categories: ["Education", "Documentary"]
History: [
  {"categoryId": "Comedy", "focusMode": false},
  {"categoryId": "Entertainment", "focusMode": false},
  {"categoryId": "Comedy", "focusMode": false}
]
Subscribed: false
Intent source: "/home"
Rules:
1: false
2: false
3: false
4: true
5: false
6: true
7: false
8: false
9: false
10: false
→ **False**
Reason: Pure entertainment with no focus signals.
explanation_summary: "Confidence: 15% | Key Evidence: No focus signals."
confidence: "15%"

---

### Example 11 – Film Trailer with Subscribed but No Focus Signals

Title: "Official Trailer – New Sci-Fi Blockbuster"
Description: "Watch the thrilling new trailer for this summer's biggest sci-fi film."
Current category: "Film and Animation"
User-selected focus categories: ["Education"]
History: [
  {"categoryId": "Film and Animation", "focusMode": false},
  {"categoryId": "Entertainment", "focusMode": false},
  {"categoryId": "Film and Animation", "focusMode": false}
]
Subscribed: true
Intent source: "/home"
Rules:
1: false
2: false
3: false
4: true
5: false
6: true
7: true
8: false
9: false
10: false
→ **False**
Reason: Subscription alone insufficient; no learning or focus signals.
explanation_summary: "Confidence: 25% | Key Evidence: Subscribed but entertainment trailer."
confidence: "25%"

---

### Example 12 – People and Blogs Vlog

Title: "Daily Vlog: Grocery Shopping and Cooking"
Description: "Spend the day with me running errands and cooking."
Current category: "People and Blogs"
User-selected focus categories: ["Education"]
History: [
  {"categoryId": "People and Blogs", "focusMode": false},
  {"categoryId": "People and Blogs", "focusMode": false},
  {"categoryId": "People and Blogs", "focusMode": false}
]
Subscribed: false
Intent source: "/home"
Rules:
1: false
2: false
3: false
4: true
5: false
6: true
7: false
8: false
9: false
10: false
→ **False**
Reason: No learning signals or focus indicators.
explanation_summary: "Confidence: 10% | Key Evidence: No focus signals."
confidence: "10%"
"""

CATEGORY_KEYWORDS = {
    "Film and Animation": [  # Film & Animation
        "movie", "film", "animation", "trailer", "cinematography",
        "short film", "anime", "cartoon", "storyboard", "CGI"
    ],
    "Autos & Vehicles": [  # Autos & Vehicles
        "car", "vehicle", "engine", "test drive", "racing",
        "motorcycle", "auto repair", "horsepower", "tuning", "drift"
    ],
    "Music": [  # Music
        "song", "music video", "album", "live performance", "cover",
        "remix", "concert", "lyrics", "instrumental", "playlist"
    ],
    "Pets & Animals": [  # Pets & Animals
        "cat", "dog", "wildlife", "zoo", "animal rescue",
        "pet care", "training", "exotic", "veterinary", "puppy"
    ],
    "Sports": [  # Sports
        "football", "basketball", "soccer", "highlights", "olympics",
        "workout", "training", "athlete", "fitness", "UFC", "tennis", "F1"
    ],
    "Short Movies": [  # Short Movies
        "short film", "indie film", "film festival", "microfilm",
        "vignette", "story"
    ],
    "Travel & Events": [  # Travel & Events
        "travel vlog", "destination", "tourism", "festival", "adventure",
        "backpacking", "road trip", "sightseeing", "cruise", "hotel"
    ],
    "Gaming": [  # Gaming
        "gameplay", "walkthrough", "let's play", "eSports", "speedrun",
        "VR", "console", "PC gaming", "Minecraft", "Fortnite"
    ],
    "Videoblogging": [  # Videoblogging
        "vlog", "daily vlog", "lifestyle vlog", "storytime", "behind the scenes",
        "channel update"
    ],
    "People & Blogs": [  # People & Blogs
        "personal vlog", "storytime", "advice", "lifestyle", "Q&A",
        "commentary", "haul", "opinion", "self-improvement", "routine"
    ],
    "Comedy": [  # Comedy
        "stand-up", "sketch", "parody", "meme", "improv", "sitcom", "slapstick", "spoof",
        "roast", "prank", "satire", "comedy special", "lol", "skit",
        "laugh", "comedic",
    ],
    "Entertainment": [  # Entertainment
        "celebrity news", "gossip", "pop culture", "movie review", "reality TV",
        "award show", "red carpet", "fan theory"
    ],
    "News & Politics": [  # News & Politics
        "breaking news", "election", "policy", "debate", "journalist",
        "world affairs", "crisis", "protest", "analysis", "government"
    ],
    "Howto & Style": [  # How-to & Style
        "tutorial", "DIY", "makeup", "fashion", "skincare",
        "hair", "life hack", "home improvement", "renovation"
    ],
    "Education": [  # Education
        "lecture", "lesson", "course", "study", "tutorial",
        "exam prep", "classroom", "teacher", "learning", "module"
    ],
    "Science and Technology": [  # Science & Technology
        "science", "tech review", "AI", "machine learning", "robotics",
        "gadgets", "experiment", "NASA", "innovation", "quantum"
    ],
    "Nonprofits & Activism": [  # Nonprofits & Activism
        "charity", "activism", "fundraiser", "social justice", "climate change",
        "volunteer", "awareness", "sustainability", "human rights"
    ],
    "Movies": [  # Movies
        "blockbuster", "cinema", "screening", "box office", "movie critique",
        "genre", "director", "actor", "film history"
    ],
    "Anime/Animation": [  # Anime/Animation
        "anime", "manga", "Studio Ghibli", "cosplay", "OVA",
        "AMV", "cartoon", "animated series"
    ],
    "Action/Adventure": [  # Action/Adventure
        "action movie", "stunts", "hero", "adventure", "chase",
        "battle", "quest", "thriller"
    ],
    "Classics": [  # Classics
        "classic film", "vintage", "retro", "black and white", "Golden Age",
        "film history", "old movie"
    ],
    "Documentary": [  # Documentary
        "documentary", "docu", "true story", "investigation", "biography",
        "nature doc", "historical doc"
    ],
    "Drama": [  # Drama
        "dramatic", "soap opera", "melodrama", "character study",
        "theatrical", "emotional"
    ],
    "Family": [  # Family
        "family film", "kids", "children", "parenting", "Disney",
        "animated", "family-friendly"
    ],
    "Foreign": [  # Foreign
        "foreign film", "international cinema", "subtitles", "world cinema",
        "international", "global film"
    ],
    "Horror": [  # Horror
        "horror movie", "scary", "ghost", "zombie", "paranormal",
        "slasher", "haunted"
    ],
    "Sci-Fi/Fantasy": [  # Sci-Fi/Fantasy
        "sci-fi", "fantasy", "space opera", "aliens", "magic",
        "dragons", "futuristic", "dystopia"
    ],
    "Thriller": [  # Thriller
        "thriller", "suspense", "mystery", "crime", "detective",
        "psychological", "plot twist"
    ],
    "Shorts": [  # Shorts
        "shorts", "#shorts", "clip", "microvideo", "vertical video"
    ],
    "Shows": [  # Shows
        "TV show", "series", "episode", "sitcom", "reality show",
        "season", "streaming"
    ],
    "Trailers": [  # Trailers
        "trailer", "teaser", "preview", "official trailer", "sneak peek"
    ],
}

def build_prompt(row):
    # [your existing extraction logic…]
    prev_focuses = [str(row.get(f"focusMode_{i+1}", "")).lower()=="true" for i in range(3)]
    prev_cats = [str(row.get(f"categoryId_{i+1}", "")) for i in range(3)]
    focus_cats = row["focus_categories"][0]
    title          = str(row.get("title","")).replace("\n"," ")
    desc           = str(row.get("description","")).replace("\n"," ")
    current_cat    = str(row.get("video_category",""))
    desc_wc        = len(desc.split())
    is_sub         = str(row.get("isSubscribed",False)).lower()=="true"
    intent_source  = str(row.get("curr_intent_source","")).lower()

    categories_list = [cat.strip() for cat in focus_cats.split(",")]
    user_kw_list = []
    for cat_str in categories_list:
        # print("cat_str : ")
        # print(cat_str)
    # pick top keywords per category
        kws = CATEGORY_KEYWORDS.get(cat_str, [])
      
        # print("kws : ")
        # print(kws)
        user_kw_list.extend(kws)

    # de-dup & lowercase
    user_kw_list = list({kw.lower() for kw in user_kw_list})
    # print("user_kw_list : ")
    # print(user_kw_list)
    # 5) Find which of those actually appear in title or description
    key_hits = [kw for kw in user_kw_list
                if kw in title.lower().split(" ") or kw in desc.lower().split(" ")]

    # key_hits       = [kw for kw in focus_keys if kw in title.lower() or kw in desc.lower()]
    cat_focus_map  = "\n".join(
        f"{i+1}. categoryId_{i+1}={prev_cats[i]} → focusMode_{i+1}={prev_focuses[i]}"
        for i in range(3)
    )

    # print("key_hits : ")
    # print(key_hits)
    # your original template, but **without** the few-shot block
    main_prompt = f"""
You are a YouTube Focus Mode decision assistant.

Your goal is to evaluate whether **Focus Mode** should be enabled for the current session. Focus Mode should be enabled if any **strong signals** suggest the user is watching with intentional focus.

Evaluate the following:

### Current Video Info:
- Title: "{title}"
- Description: "{desc[:140]}..."
- Current categoryId: {current_cat}

### User Focus Context:
- User-selected focus categories: {focus_cats or "None"}

### Past Sessions:
{cat_focus_map}

### Evaluation Rules
1. Any previous focusMode is True.
2. If any previous categoryId==current and that session had focusMode=True.**Strong Signal**
3. If the current category is in the user's selected focus categories, treat this as a strong signal. Even if no prior focus or other signals exist, this alone is enough to enable Focus Mode.
4. Any previous category matches current.**Strong Signal**
5. A previous focusMode=True AND categoryId of that focusMode matched current vidoes categoryId.
6. Current category appears ≥2 times.**Strong Signal**
7. User is subscribed → {is_sub}
8. Intent source contains Search/Channel → {intent_source}
9. Title/description contains focus keywords → {', '.join(key_hits) or "None"}
10. Description >50 words AND has focus keywords → {desc_wc>50 and len(key_hits)>0}


### Guidance for Predictions
- Use the examples as reference, not as strict rules.
- It is acceptable for similar cases to have different outcomes if context differs.
- Return a confidence score between 60–100%, representing how confident you are in the decision you’ve made (whether true or false).
	- If your evaluation strongly supports Focus Mode = true, assign high confidence (e.g. 90–100%).
	- If your evaluation strongly supports Focus Mode = false, assign high confidence (e.g. 85–95%).
	- If the signals are mixed or weak, reduce the score accordingly (e.g. 60–70%).
- Always base your prediction on the full evidence above, not just pattern matching.

Your goal is to evaluate whether **Focus Mode** should be enabled for the current session.
You MUST evaluate ALL 10 rules. 
IF ANY OF THE *Strong Signals* IS VERIFIED RETURN -> true.  Return JSON only:
```json
{{
  "category":"true" or "false",
  "rule":[…],
  "explanation":"A detailed explanation of your reasoning.",
  "explanation_summary": "A short summary in this format: 'Confidence: [number]% | Key Evidence: [short phrase supporting the confidence. Not more than 20 words. Do NOT repeat the explanation]'",
  "confidence":"0-100% score indicating how confident the model is in the decision (true or false). High score = strong belief in that decision."
}}
```"""
    return FEW_SHOT_EXAMPLES + main_prompt


def update_last_active_time(user_id : str):

    missing_id_message = check_id(user_id)
    if missing_id_message:
        return missing_id_message

    try:
        current_timestamp = get_current_datetime_str()
        response = user_table.update_item(
            Key={"User_Id": user_id},
            UpdateExpression="SET Last_Active_At_Time = :ts",
            ExpressionAttributeValues={":ts": current_timestamp},
            ReturnValues="UPDATED_NEW"
        )
        
        # Return only the updated timestamp
        return {"User_Id": user_id, "Last_Active_At_Time": response["Attributes"]["Last_Active_At_Time"]}
    except Exception as e:
        return {"error": f"Failed to update Last_Active_At_Time: {str(e)}"}


def get_next_stage(user_stage_orders, current_stage):
    for i, stage_number in enumerate(user_stage_orders):
        if stage_number == current_stage and i != len(user_stage_orders)-2:
            return user_stage_orders[i+1]
    return None

def decimal_to_int(obj):
    if isinstance(obj, Decimal):
        return int(obj)
    raise TypeError("Type not serializable")


def getStageResponseObject(response_object, user_id, is_stage_changed, is_study_completed):
    current_week = -1
    for i, stage_num in enumerate(response_object["Stage_Order_List"]):
        if stage_num == response_object["Current_Stage"]:
            current_week = i+1

    if current_week == -1:   
        return None     
    data = {
            "user_Id": user_id,
            "current_stage": response_object["Current_Stage"],
            "is_stage_changed": is_stage_changed,
            "is_study_completed": is_study_completed,
            "current_week": current_week
        }
    return data

def get_current_study_stage(stage_start_times: dict[str, str], last_active_str: str) -> int:
    last_active = get_datetime_obj(last_active_str)
    
    # Convert to list of tuples: (stage_number, datetime_object)
    stage_entries = [
        (stage, get_datetime_obj(start_time)) for stage, start_time in stage_start_times.items()
    ]

    # Sort by datetime so we can find the latest stage that started before last active
    stage_entries.sort(key=lambda x: x[1])

    current_stage = 0
    for stage, start_dt in stage_entries:
        if last_active >= start_dt:
            current_stage = int(stage)
        else:
            break

    return current_stage


def is_study_over(stage_start_times: dict[str, str], stage_sequence: list[int], last_active_str: str) -> bool:
    last_active = get_datetime_obj(last_active_str)
    
    final_stage = stage_sequence[-1]
    final_start = get_datetime_obj(stage_start_times[str(final_stage)])
    final_end = final_start + timedelta(days=7)

    return last_active >= final_end

def update_user_stage(user_id : str):
    missing_id_message = check_id(user_id)
    if missing_id_message:
        return missing_id_message
    
    try:
        # Fetch user details based on primary key
        response = user_table.get_item(Key={"User_Id": user_id})
        response = response["Item"]
       
        current_study_stage = response.get("Current_Stage")
        user_stage_order_list = response["Stage_Order_List"]
        stage_start_times = response["Stage_Start_Times"]
        last_active_timestamp = response["Last_Active_At_Time"]

        stage = get_current_study_stage(stage_start_times, last_active_timestamp)
        is_study_completed = is_study_over(stage_start_times, user_stage_order_list, last_active_timestamp)

        if not current_study_stage and stage == 0:
            first_stage = user_stage_order_list[0]
            response = user_table.update_item(
                Key={"User_Id": user_id},
                UpdateExpression="SET Current_Stage = :new_stage",
                ExpressionAttributeValues={":new_stage": first_stage},
                ReturnValues="ALL_NEW"
            )
            databaseAttributes = response["Attributes"]
            
            data = getStageResponseObject(databaseAttributes, user_id, True, False)
            if data is None:
                return  {
                    "statusCode": 500,
                    "headers": CORS_HEADERS,
                    "body": json.dumps({
                        "message": "Internal Error: currnet stage not found"
                    }),
                }
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "data": data,
                    "message": f"First stage for the user started successfully."
                },  default=decimal_to_int),
            }
        
        if is_study_completed:
            response = user_table.update_item(
                Key={"User_Id": user_id},
                UpdateExpression=(
                    f"SET Current_Stage = :new_stage,"
                    f"User_Completed_Stages = list_append(User_Completed_Stages, :last_stage)"
                ),
                ExpressionAttributeValues={
                    ":new_stage": stage,
                    ":last_stage": [current_study_stage]
                },
                ReturnValues="ALL_NEW"
            )
            databaseAttributes = response["Attributes"]
            data = getStageResponseObject(databaseAttributes, user_id, True, True)
            if data is None:
                return  {
                    "statusCode": 500,
                    "headers": CORS_HEADERS,
                    "body": json.dumps({
                        "message": "Internal Error: currnet stage not found"
                    }),
                }
            
            return {
                    "statusCode": 200,
                    "headers": CORS_HEADERS,
                    "body": json.dumps({
                        "data": data,
                        "message": f"Study for the user with id: {user_id} completed."
                    }, default=decimal_to_int),
                }

        if stage != current_study_stage:
            response = user_table.update_item(
                Key={"User_Id": user_id},
                UpdateExpression=(
                    f"SET Current_Stage = :new_stage,"
                    f"User_Completed_Stages = list_append(User_Completed_Stages, :last_stage)"
                ),
                ExpressionAttributeValues={
                    ":new_stage": stage,
                    ":last_stage": [current_study_stage]
                },
                ReturnValues="ALL_NEW"
            )
            databaseAttributes = response["Attributes"]
            data = getStageResponseObject(databaseAttributes, user_id, True, False)

            if data is None:
                return  {
                    "statusCode": 500,
                    "headers": CORS_HEADERS,
                    "body": json.dumps({
                        "message": "Internal Error: currnet stage not found"
                    }),
                }
            return {
                        "statusCode": 200,
                        "headers": CORS_HEADERS,
                        "body": json.dumps({
                            "data": data,
                            "message": f"started a new stage for the user as previous is completed",
                        }, default=decimal_to_int),
                    }
        else:
            # No need to change any stage information
            # return response with user_id:
            data = getStageResponseObject(response, user_id, False, False)
            if data is None:
                return  {
                    "statusCode": 500,
                    "headers": CORS_HEADERS,
                    "body": json.dumps({
                        "message": "Internal Error: currnet stage not found"
                    }),
                }
            
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "data": data,
                    "message": f"No stage update as user is still in the current stage time limit"
                }, default=decimal_to_int),
            }

        
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "data": {
                    "error": f"Failed to update user stage information for user {user_id}: {str(e)}",
                },
                "message": "ERROR: Failed to update user stage information"
            }),
        }

def check_query_parameters(event_query_string_parameters: list[str], required_parameters: list[str]):
    # missing all parameters
    if event_query_string_parameters == None:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": f"Missing the query parameter(s): {", ".join(required_parameters)}"
            }),
        }
    
    parameters_missing = set(required_parameters) - set(event_query_string_parameters)
    
    # missing some parameters
    if len(parameters_missing) != 0:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": f"Missing the query parameter(s): {", ".join(parameters_missing)}"
            }),
        }
    
    # no parameters missing!
    return None

def check_id(prolific_id: str) -> bool:
    # Retrieve the item from DynamoDB table
    response = user_table.get_item(Key={"User_Id": prolific_id})
    response = response.get('Item')
    
    if response:
        # If a record with this User_Id exists, return None (meaning OK)
        return None
    else:
        # If no record exists, return a 401 Unauthorized response
        return {
            "statusCode": 401,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": "Unauthorized"
            }),
        }


def update_user_with_focus_status(id, prolific_id, focus):
    response = user_pref_data_table.update_item(
        Key={
            "prolificId": prolific_id,
            "Id" : id},
        UpdateExpression="SET focus = :focus_status ",
        ExpressionAttributeValues={":focus_status": focus},
        ReturnValues="ALL_NEW"
    )

    databaseAttributes = response["Attributes"]
    return databaseAttributes


def fetch_and_insert_user_entry(prolificId, newEntry, entry_id):
    
    # Step 1: Query existing entries for the same prolificId and sessionId
    response = user_pref_data_table.query(
        KeyConditionExpression=Key('prolificId').eq(prolificId),
        FilterExpression='sessionId = :sid',
        ExpressionAttributeValues={
            ':sid': newEntry['sessionId']
        }
    )

    items = response.get('Items', [])

    # Step 2: Sort by timestamp descending
    items.sort(key=lambda x: datetime.fromisoformat(x['timestamp'].replace('Z', '+00:00')), reverse=True)

    # Step 3: Get latest three entries
    latest_three = items[:3]

    # Step 4: Flatten previous focus/category data into current entry
    for i in range(1, 4):
        entry = latest_three[i - 1] if i - 1 < len(latest_three) else {}

        newEntry[f'focusMode_{i}'] = entry.get('focus', None)
        newEntry[f'categoryId_{i}'] = (
            entry.get('youTubeApiData', {})
                 .get('snippet', {})
                 .get('categoryId', None)
        )

    # Step 5: Insert the new entry
    item_to_insert = {
        'prolificId': prolificId,
        'Id': entry_id,
        **newEntry
    }

    result = user_pref_data_table.put_item(Item=item_to_insert)
    return result, newEntry

# Function to retrive the youtube data for given youtube video_id
def fetch_youtube_data(video_id):
    if not video_id:
        return None

    url = (
        "https://www.googleapis.com/youtube/v3/videos"
        f"?part=snippet,statistics&id={video_id}&key={YOUTUBE_API_KEY}"
    )

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        return data["items"][0] if data.get("items") else None

    except requests.exceptions.RequestException as e:
        print(f"YouTube API request failed: {str(e)}")
        return None

# Utility to generate a unique verification code (e.g., SHA-256 of prolificId + timestamp)
def generate_verification_code(prolific_id: str) -> str:
    timestamp = str(int(time.time() * 1000)) 
    to_hash = f"{prolific_id}-{timestamp}"
    hash_value = hashlib.sha256(to_hash.encode()).hexdigest()
    return hash_value


def generate_weekly_stage_start_times(start_ts_str: str, stage_order_list: list[int]) -> dict[str, str]:
    start_dt = get_datetime_obj(start_ts_str)
    stage_map = {}
    for i, stage in enumerate(stage_order_list):
        stage_start = start_dt + timedelta(days=7 * i)
        stage_map[str(stage)] = format_datetime_str(stage_start)
    return stage_map

def format_datetime_str(datetime: datetime) -> str: 
    return datetime.isoformat(timespec='seconds')

def get_current_datetime_str() -> str:
    return format_datetime_str(datetime.now())

def get_datetime_obj(datetime_str: str) -> datetime:
    return datetime.fromisoformat(datetime_str)






def normalize_category_names(cat):
    return cat.replace('&', 'and').strip()

def get_unique_video_categories():
  """
  Retrieves a unique list of video categories from the YouTube Data API.

  Returns:
    A set of unique video category titles.
  """
  youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
  region_code = "US"
  category_request = youtube.videoCategories().list(part="snippet", regionCode=region_code)
  category_response = category_request.execute()

  unique_categories = set()
  category_id_to_name = {}
  for category in category_response['items']:
    formattedCategory = normalize_category_names(category['snippet']['title'])
    unique_categories.add(formattedCategory)
    category_id_to_name[category['id']] = formattedCategory

  return list(unique_categories), category_id_to_name


# JSON to pandas convertor:
def flatten_dict(d, parent_key="", sep="."):
    """
    Recursively flattens a nested dictionary using dot notation.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def parse_video_entry_to_df(json_obj: dict) -> pd.DataFrame:
    """
    Parses a single video entry JSON object into a flattened DataFrame row.
    Handles nested structure inside 'newPreferenceData' and attaches 'prolificId' at top level.
    """

    if not isinstance(json_obj, dict):
        raise ValueError("Expected a JSON object (Python dict)")

    prolific_id = json_obj.get("prolificId", None)
    preference_data = json_obj.get("newPreferenceData", {})

    # Add prolificId to the preference data before flattening
    preference_data["prolificId"] = prolific_id

    # Flatten the entire structure (youTubeApiData and others)
    flat_data = flatten_dict(preference_data)

    # Convert to DataFrame
    return pd.DataFrame([flat_data])

def get_time_of_day(hour):
    if 5 <= hour < 12:
        return 'morning'
    elif 12 <= hour < 17:
        return 'afternoon'
    elif 17 <= hour < 21:
        return 'evening'
    else:
        return 'night'


def expand_intent_node(df):
    """
    Parses and expands the 'intentNode' JSON string column into multiple flat columns.

    Args:
        df (pd.DataFrame): Input DataFrame with an 'intentNode' column.

    Returns:
        pd.DataFrame: DataFrame with expanded intent fields.
    """

    def parse_json(value):
        if pd.isna(value):
            return {}

        try:
            # Try parsing directly
            return json.loads(value)
        except json.JSONDecodeError:
            try:
                # Try parsing after unescaping (for cases like: "\"{\"key\":\"value\"}\"")
                return json.loads(json.loads(value))
            except Exception:
                return {}

    # Parse intentNode column into dictionaries
    intent_parsed = df['intentNode'].apply(parse_json)

    # Expand into new columns
    intent_df = intent_parsed.apply(pd.Series)

    # Add prefix to avoid column name clashes
    intent_df.columns = [f"{col}" for col in intent_df.columns]

    # Combine with original DataFrame
    df = pd.concat([df.drop(columns=['intentNode']), intent_df], axis=1)

    return df


def extract_features(df: pd.DataFrame, category_id_to_name: dict) -> pd.DataFrame:
    def extract_features(row):
        try:
            # Video category
            cat_id = str(row.get('youTubeApiData.snippet.categoryId', ''))
            cat_id_1 = str(row.get('categoryId_1', ''))
            cat_id_2 = str(row.get('categoryId_2', ''))
            cat_id_3 = str(row.get('categoryId_3', ''))
            row['video_category'] = category_id_to_name.get(cat_id, "Unknown")
            row['categoryId_1'] = category_id_to_name.get(cat_id_1, "Unknown")
            row['categoryId_2'] = category_id_to_name.get(cat_id_2, "Unknown")
            row['categoryId_3'] = category_id_to_name.get(cat_id_3, "Unknown")

        except Exception as e:
            row['error'] = str(e)
        return pd.Series(row)

    transformed_df = df.apply(extract_features, axis=1)
    return transformed_df


def drop_unwanted_columns(df):
    """
    Drops unwanted columns including specific fields and those starting with 'youTubeApiData.snippet.thumbnails'.

    Args:
        df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: Cleaned DataFrame with specified columns dropped.
    """
    # Columns to drop explicitly
    columns_to_drop = [
        'timestamp',
        'youTubeApiData.snippet.publishedAt',
        'youTubeApiData.id',
        'youTubeApiData.kind',
        'youTubeApiData.etag',
        'youTubeApiData.snippet.channelId',
        'youTubeApiData.snippet.localized.title',
        'youTubeApiData.snippet.localized.description',
        'youTubeApiData.statistics.viewCount',
        'youTubeApiData.statistics.favoriteCount',
        'youTubeApiData.statistics.commentCount'
    ]

    # Add all columns that start with 'youTubeApiData.snippet.thumbnails'
    thumbnail_cols = [col for col in df.columns if col.startswith('youTubeApiData.snippet.thumbnails')]

    # Combine and drop
    all_cols_to_drop = columns_to_drop + thumbnail_cols
    return df.drop(columns=[col for col in all_cols_to_drop if col in df.columns])


def rename_columns_to_last_segment(df):
    """
    Renames only the columns that contain a dot ('.') by extracting the last part after the dot.
    Other column names remain unchanged.

    E.g.:
    - 'youTubeApiData.snippet.categoryId' → 'categoryId'
    - 'prolificId' → 'prolificId' (unchanged)

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Renamed DataFrame.
    """
    new_columns = {
        col: col.split('.')[-1] if '.' in col else col
        for col in df.columns
    }
    return df.rename(columns=new_columns)

def update_intent_data(df):
    """
    For rows where:
    - curr_intent_source != '/SearchPage', set curr_intent_data to title
    - curr_intent_source == '/ChannelPage', set curr_intent_data to channelTitle
    """
    # If it's ChannelPage, override first
    mask_channel = df["curr_intent_source"] == "/ChannelPage"
    if "youTubeApiData.snippet.channelTitle" in df.columns:
        df.loc[mask_channel, "curr_intent_data"] = df.loc[mask_channel, "youTubeApiData.snippet.channelTitle"]

    # If not SearchPage and not ChannelPage (so, all others), set to title
    mask_other = (df["curr_intent_source"].notna()) & (~df["curr_intent_source"].isin(["/SearchPage", "/ChannelPage"]))
    if "youTubeApiData.snippet.title" in df.columns:
        df.loc[mask_other, "curr_intent_data"] = df.loc[mask_other, "youTubeApiData.snippet.title"]

    return df

def preprocess_video_json_entry(json_obj):
    """
    Full preprocessing pipeline to convert a single JSON video entry to a final processed DataFrame row.

    Args:
        json_obj (dict): Raw JSON object representing one video entry.
        category_id_to_name (dict): Mapping of YouTube categoryId to category name.

    Returns:
        pd.DataFrame: Processed single-row DataFrame.
    """
    _, category_id_to_name = get_unique_video_categories()

    # Step 1: Flatten JSON to DataFrame
    df = parse_video_entry_to_df(json_obj)

    # Step 2: Expand nested intentNode column
    df = expand_intent_node(df)

    df = update_intent_data(df)
    # Step 3: Extract time/contextual/video-based features
    df = extract_features(df, category_id_to_name)

    # Step 4: Drop columns not needed for training
    df = drop_unwanted_columns(df)

    # Step 5: Rename remaining columns (e.g., youTubeApiData.snippet.categoryId → categoryId)
    df = rename_columns_to_last_segment(df)

    return df.to_dict(orient="records")[0]