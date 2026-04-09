#!/usr/bin/env python
# coding: utf-8

# In[ ]:



import streamlit as st
import os
from dotenv import load_dotenv
import pytz
load_dotenv()
# In[1]:


try:
    APIFY_TOKEN = st.secrets["APIFY_TOKEN"]
except Exception:
    APIFY_TOKEN = os.getenv("APIFY_TOKEN")


# In[2]:


from apify_client import ApifyClient
import pandas as pd
from datetime import datetime, timedelta
import pytz

#APIFY_TOKEN = "YOUR_APIFY_TOKEN"
client = ApifyClient(APIFY_TOKEN)

companies = {
    "ENBD_C": "https://www.linkedin.com/company/emirates-nbd/posts/?feedView=all",
    "ENBD": 'https://www.linkedin.com/search/results/content/?keywords=%22emirates%20nbd%22%20NOT%20job%20NOT%20hiring%20NOT%20career%20NOT%20%22new%20role%22%20NOT%20%22joined%22&sortBy=%5B%22date_posted%22%5D&datePosted=%5B%22past-24h%22%5D',
    "ENBD1": "https://www.linkedin.com/search/results/content/?keywords=%22enbd%22%20NOT%20job%20NOT%20hiring%20NOT%20career%20NOT%20%22new%20role%22%20NOT%20%22joined%22&sortBy=%5B%22date_posted%22%5D&datePosted=%5B%22past-24h%22%5D",
    "EI_C": "https://www.linkedin.com/company/emirates-islamic-bank/posts/?feedView=all",
    "EI":'https://www.linkedin.com/search/results/content/?keywords=%22Emirates%20Islamic%22%20NOT%20job%20NOT%20hiring%20NOT%20%20career%20NOT%20%22new%20role%22%20NOT%20%22joined%22&origin=FACETED_SEARCH&sortBy=%5B%22date_posted%22%5D&datePosted=%5B%22past-24h%22%5D',

    "ADIB_C": "https://www.linkedin.com/company/abu-dhabi-islamic-bank/posts/?feedView=all",    
    "ADCB_C": "https://www.linkedin.com/company/adcbofficial/posts/?feedView=all",
    "FAB_C": "https://www.linkedin.com/company/first-abu-dhabi-bank/posts/?feedView=all",
    "CBD_C": "https://www.linkedin.com/company/commercial-bank-of-dubai/posts/?feedView=all",
    #"mashreq_C": "https://www.linkedin.com/company/mashreq-uae/posts/?feedView=all",

    "ADCB":'https://www.linkedin.com/search/results/content/?keywords=%22Abu%20Dhabi%20Commercial%20Bank%22%20NOT%20job%20NOT%20hiring%20NOT%20%20career%20NOT%20%22new%20role%22%20NOT%20%22joined%22&origin=FACETED_SEARCH&sortBy=%5B%22date_posted%22%5D&datePosted=%5B%22past-24h%22%5D',
    "ADCB1":'https://www.linkedin.com/search/results/content/?keywords=%22ADCB%22%20NOT%20job%20NOT%20hiring%20NOT%20%20career%20NOT%20%22new%20role%22%20NOT%20%22joined%22&origin=FACETED_SEARCH&sortBy=%5B%22date_posted%22%5D&datePosted=%5B%22past-24h%22%5D',
    "ADIB":'https://www.linkedin.com/search/results/content/?keywords=%22Abu%20Dhabi%20Islamic%20Bank%22%20NOT%20job%20NOT%20hiring%20NOT%20%20career%20NOT%20%22new%20role%22%20NOT%20%22joined%22&origin=FACETED_SEARCH&sortBy=%5B%22date_posted%22%5D&datePosted=%5B%22past-24h%22%5D',
    "CBD":'https://www.linkedin.com/search/results/content/?keywords=%22Commercial%20Bank%20of%20Dubai%22%20NOT%20job%20NOT%20hiring&origin=FACETED_SEARCH&sortBy=%5B%22date_posted%22%5D&datePosted=%5B%22past-24h%22%5D',
    "FAB":'https://www.linkedin.com/search/results/content/?keywords=%22first%20abu%20dhabi%20bank%22%20NOT%20job%20NOT%20hiring%20NOT%20career%20NOT%20%22new%20role%22%20NOT%20%22joined%22&origin=FACETED_SEARCH&sortBy=%5B%22date_posted%22%5D&datePosted=%5B%22past-24h%22%5D',
    #"Mashreq":'https://www.linkedin.com/search/results/content/?keywords=%22mashreq%22%20NOT%20job%20NOT%20hiring%20NOT%20career%20NOT%20%22new%20role%22%20NOT%20%22joined%22&origin=FACETED_SEARCH&sortBy=%5B%22date_posted%22%5D&datePosted=%5B%22past-month%22%5D',

}

dubai_tz = pytz.timezone("Asia/Dubai")
cutoff_time = datetime.now(dubai_tz) - timedelta(hours=24)

company_pages = ["ENBD_C", "EI_C", "ADIB_C", "ADCB_C", "FAB_C", "CBD_C", "mashreq_C"]

all_records = []

for name, url in companies.items():

    print(f"Running for {name}")

    # 🎯 dynamic maxPosts
    if name in company_pages:
        max_posts = 60
    else:
        max_posts = 200

    run_input = {
        "urls": [url],   # ✅ FIXED HERE
        "maxPosts": max_posts,
        "proxy": {"useApifyProxy": True},
    }

    run = client.actor("supreme_coder/linkedin-post").call(run_input=run_input)
    dataset_id = run["defaultDatasetId"]

    for item in client.dataset(dataset_id).iterate_items():

        created = item.get("postedAtISO")

        try:
            created_dt = pd.to_datetime(created, utc=True)
            created_dt = created_dt.tz_convert("Asia/Dubai").tz_localize(None)
        except:
            created_dt = None

        # filter only for company page
        if name in ["ENBD_C", "EI_C","ADIB_C","ADCB_C","FAB_C","CBD_C","mashreq_C"]:
            if created_dt is None or created_dt < cutoff_time.replace(tzinfo=None):
                continue

        item["source"] = name
        item["postedAt_dubai"] = created_dt

        all_records.append(item)

df = pd.DataFrame(all_records)

# remove duplicates
if "postUrl" in df.columns:
    df = df.drop_duplicates(subset="postUrl")

# save
df.to_excel("linkedin_fixed.xlsx", index=False)

print("✅ Fixed and done")


# In[ ]:





# In[3]:


raw_df1 = df.drop(columns=["author", "comments"])


# In[4]:


df_final = raw_df1.rename(columns={
    "authorName": "author",
    "postedAt_dubai": "post_date",
    "text": "text",
    "numLikes": "likes",
    "numComments": "comments",
    "numShares": "reposts",
    "type": "content_type",
    "authorFollowersCount": "followers",
    "url": "post_link",
    "urn": "urn",
    "authorType": "company_person_Type",
    "source": "source"
})[
    [
        "author",
        "post_date",
        "text",
        "likes",
        "comments",
        "reposts",
        "content_type",
        "followers",
        "post_link",
        "urn",
        "company_person_Type",
        "source"
    ]
]


# In[ ]:





# In[5]:


df=df_final
#df


# In[6]:


len(df_final)


# In[7]:


df=df_final


# In[8]:


# b) Table with the remaining DUPLICATE rows (excluding the kept one)
duplicates_df = df[df.duplicated(subset="urn", keep="first")].copy()

df = df.drop_duplicates(subset="urn", keep="first").copy()


# In[9]:


len(df)


# In[10]:


# Remove blank post
df=df[(df['text'].notna()) & (df['text'].str.strip() != '')]


# In[11]:


# Remove rows containing unwanted text
unwanted_phrases = [
    "check out this job", "we're hiring", "join us", "apply now", "hiring", "looking for a new job", "job by emirates nbd",
    "careers in","jobs in uae",'job openings','get a job','send cv','job opportunity','dear hiring team',"jobs opening","careers uae",
    "Job vacanc","human resources","job","my cv", "i am looking a","new position"
]


df = df[~df['text'].str.lower().str.contains('|'.join(phrase.lower() for phrase in unwanted_phrases))]


# In[ ]:





# In[12]:



# Filter df where 'author' does not contain 'Job'
df = df[~df['author'].str.contains('Jobs', case=False, na=False)]

df['post'] = (
    df['text']
    .str.replace('\n', ' ', regex=False)         # Replace line breaks with space
    .str.replace(r'\s+', ' ', regex=True)        # Remove multiple spaces
    .str.strip()                                 # Remove leading/trailing spaces
    .str.lower()                                 # Convert to lowercase
)


# In[13]:


# Function to remove emojis and symbols
def remove_emojis(text):
    emoji_pattern = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # Emoticons
        u"\U0001F300-\U0001F5FF"  # Symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # Transport & map symbols
        u"\U0001F700-\U0001F77F"  # Alchemical
        u"\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
        u"\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
        u"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        u"\U0001FA00-\U0001FA6F"  # Chess Symbols
        u"\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        u"\U00002700-\U000027BF"  # Dingbats
        u"\U000024C2-\U0001F251"  # Enclosed characters
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)


# In[14]:


import re


# In[15]:


df['post'] = df['post'].apply(remove_emojis)


# In[16]:


import re

def clean_post_series(s):
    # 1) normalize whitespace first
    s = (s.str.replace('\n', ' ', regex=False)
           .str.replace(r'\s+', ' ', regex=True)
           .str.strip())

    # 2) remove simple boilerplate phrases (case-insensitive, word-bounded)
    simple_phrases = [
        r'turn closed captions on',
        r'show captions',
        r'unmute',
        r'turn fullscreen(?: on)?',
        r'close modal window',
        r'(?:full)?screen media player modal window',
        r'media player modal window',
        r'play media is loading',
        r'playback speed',
        r'stream type'
    ]
    simple_pat = re.compile(r'(?i)\b(?:' + '|'.join(simple_phrases) + r')\b')
    s = s.str.replace(simple_pat, ' ', regex=True)

    # 3) SPECIAL: remove “*t&cs apply” ONLY if it appears at the end (with optional stars/spaces)
    tc_end_pat = re.compile(r'(?i)[\s*]*t\s*&\s*c\s*s\s*apply[\s*]*$')
    s = s.str.replace(tc_end_pat, ' ', regex=True)

    # 4) final tidy
    s = s.str.replace(r'\s+', ' ', regex=True).str.strip()
    return s

# usage:
# df['post1'] = clean_post_series(df['post'])


# In[17]:


# usage:
df['post'] = clean_post_series(df['post'])


# In[18]:


# Function to remove URLs
def remove_links(text):
    return re.sub(r'http\S+|www\.\S+', '', text).strip()


# In[19]:


df['post'] = df['post'].apply(remove_links)


# In[20]:


# Keep only the first part before newline
df["author_name"] = df["author"]


# In[21]:


#df.to_excel("ENBD.xlsx")

from openai import OpenAI
# In[22]:
try:
    key = st.secrets["OPENAI_API_KEY_linkedin"]
except Exception:
    key = os.getenv("OPENAI_API_KEY_linkedin")



# In[23]:


def analyze_sentiment(text):

    prompt = f"""
The post may contain Arabic or English or mixed language text.

TASK:
1) If the text is Arabic, translate it to English first (keep original meaning).
2) Then classify sentiment ONLY about these banks:
   First Abu Dhabi Bank, Emirates NBD, Mashreq, Emirates Islamic,
   Abu Dhabi Islamic Bank, Commercial Bank of Dubai,
   Abu Dhabi Commercial Bank.
3) Output exactly one of these words only:
   Positive, Negative, Neutral.

Rules:
- If negative sentiment toward secific bank → Negative
- If unclear or not about banks → Neutral

Your output must be only the final sentiment word.

Post:
{text}
""".strip()

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[{"role": "user", "content": prompt}]
    )
    
    return response.choices[0].message.content.strip()


# In[24]:


def _clean_text(t):
    if t is None: return ""
    t = str(t).strip()
    # Collapse whitespace
    t = re.sub(r"\s+", " ", t)
    # Trim super long
    return t[:6000]  # safety

def genai_highlights(text, max_bullets=3, timeout_s=20):
    """
    Returns a list of 2-3 short bullet highlights.
    Uses OpenAI if API key is present; otherwise falls back to a local method.
    """
    text = _clean_text(text)
    if not text:
        return []

    if _USE_GENAI and _client is not None:
        try:
            prompt = (
                "You are a concise analyst. Read the post text and extract the top 2–3 key highlights.\n"
                "- Each highlight must be a single, short bullet (max ~15 words)\n"
                "- Be factual and avoid marketing fluff\n"
                "- If numbers, product names, or dates are present, keep them\n"
                " convert text to English\n"
                f"\nPOST TEXT:\n{text}\n\nReturn bullets only."
            )
            resp = _client.chat.completions.create(
                model=_MODEL,
                temperature=0.2,
                messages=[{"role": "user", "content": prompt}],
                timeout=timeout_s,
            )
            out = resp.choices[0].message.content.strip()
            # Split into bullets robustly
            bullets = [b.strip("•- ").strip() for b in re.split(r"[\n\r]+", out) if b.strip()]
            bullets = [b for b in bullets if len(b) > 0][:max_bullets]
            return bullets
        except Exception:
            pass  # fall through to local

    # --- Local fallback: simple extractive highlights (no deps) ---
    # 1) sentence split
    sents = re.split(r"(?<=[\.\!\?])\s+", text)
    sents = [s.strip() for s in sents if 25 <= len(s.strip()) <= 200][:15]
    if not sents:
        return [text[:120] + ("…" if len(text) > 120 else "")]
    # 2) score by keyword frequency
    words = re.findall(r"[A-Za-z0-9%]+", text.lower())
    stop = set("""a an the and or of to for in on at by with as is are was were be been being this that these those it its from into over under about more most less least up down out not no yes your our their his her they we you i""".split())
    freq = {}
    for w in words:
        if w in stop or len(w) < 3: continue
        freq[w] = freq.get(w, 0) + 1
    def score(sent):
        tokens = re.findall(r"[A-Za-z0-9%]+", sent.lower())
        sc = sum(freq.get(w, 0) for w in tokens)
        # reward digits/percents slightly
        sc += 1.5 * sum(ch.isdigit() for ch in sent)
        return sc
    ranked = sorted(sents, key=score, reverse=True)[:max_bullets]
    return [re.sub(r"\s+", " ", s) for s in ranked]


# In[25]:


df['org_post']=df['post']


# In[26]:


# Apply sentiment analysis
df["Sentiment"] = df["org_post"].apply(analyze_sentiment)


# In[27]:


df["post"] = df["org_post"]


# In[28]:


df["row_number"] = range(1, len(df) + 1)


# In[29]:


df1=df


# In[30]:


import os


# In[31]:


_USE_GENAI = bool(os.getenv(key))
if _USE_GENAI:
    try:
        from openai import OpenAI
        _client = OpenAI()
        _MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # fast & cheap; change if you like
    except Exception:
        _USE_GENAI = False
        _client = None


# In[32]:



df1["post_highlights"] = df1["post"].apply(
    lambda txt: " • ".join(genai_highlights(txt, max_bullets=3))
)


# In[33]:


from datetime import datetime, timedelta


# In[34]:


# Format into DDMMMYYYY (e.g., 01OCT2025)
date_str = datetime.today().strftime("%d%b%Y").upper()

# Create filename
file_name = f"ENBD_daily_linkedIn_Data_{date_str}.xlsx"


# In[35]:


file_name


# In[36]:


#df1.to_excel(file_name,index=False)


# In[37]:


df_merged=df1


# In[38]:


df_merged["Sentiment"].value_counts()


# In[39]:


df_merged.loc[(df_merged["Sentiment"] == "Negative") & (df_merged["author_name"] == "Emirates NBD"), "Sentiment"] = "Positive"


# In[40]:


ath=df_merged[df_merged["author_name"].str.contains("Emirates NBD", case=False, na=False)]


# In[41]:


ath['author_name'].value_counts()


# In[42]:


#df_merged.to_excel("df_merge.xlsx")


# In[ ]:





# In[43]:


df_merged["author_type"] = df_merged["author_name"].apply(
    lambda x: "Bank" if x in ["Emirates NBD", "Emirates NBD Egypt","Emirates Islamic",
                             'ADIB - Abu Dhabi Islamic Bank','Abu Dhabi Commercial Bank','Commercial Bank of Dubai','Abu Dhabi Commercial Bank - Egypt','Mashreq Corporate & Investment Banking Group',
'Mashreq Corporate & Investment Banking Group','Mashreq NEO','Mashreq','First Abu Dhabi Bank (FAB)','First Abu Dhabi Bank'] else "Cust"
)


# In[44]:


df_merged['author_type'].value_counts()


# In[45]:


df_merged.head(1)


# In[ ]:





# In[46]:


# Convert to string format DDMMYYYY
df_merged["post_date_ddmmyyyy"] = pd.to_datetime(df_merged["post_date"]).dt.strftime("%d-%m-%Y")


# In[47]:


today = pd.Timestamp.now().normalize()
yesterday = today - pd.Timedelta(days=1)

df_merged = df_merged[
    (df_merged["post_date"] >= yesterday)
]


# In[48]:


# Get min and max dates
min_date = df_merged["post_date_ddmmyyyy"].min()
max_date = df_merged["post_date"].max()


# In[49]:


min_date


# In[50]:


max_date


# In[51]:


df_merged["day_name"] = pd.to_datetime(df_merged["post_date"]).dt.day_name()


# In[52]:


df_merged["day_name"].value_counts()


# In[ ]:





# In[53]:


mapping = {

"ADCB_C":'4. Abu Dhabi Commercial Bank (ADCB)',
"ADCB":'4. Abu Dhabi Commercial Bank (ADCB)',
"ADCB1":'4. Abu Dhabi Commercial Bank (ADCB)',
"ADCB2":'4. Abu Dhabi Commercial Bank (ADCB)',
"ADCB3":'4. Abu Dhabi Commercial Bank (ADCB)',
"ADCB4":'4. Abu Dhabi Commercial Bank (ADCB)',

"ADIB_C":"5. Abu Dhabi Islamic Bank (ADIB)",
"ADIB":"5. Abu Dhabi Islamic Bank (ADIB)",
"ADIB1":"5. Abu Dhabi Islamic Bank (ADIB)",
"ADIB2":"5. Abu Dhabi Islamic Bank (ADIB)",
"ADIB3":"5. Abu Dhabi Islamic Bank (ADIB)",
"ADIB4":"5. Abu Dhabi Islamic Bank (ADIB)",

"ENBD_C":"1. Emirates NBD Bank (ENBD)",
"ENBD":"1. Emirates NBD Bank (ENBD)",
"ENBD1":"1. Emirates NBD Bank (ENBD)",
"ENBD2":"1. Emirates NBD Bank (ENBD)",
"ENBD3":"1. Emirates NBD Bank (ENBD)",
"ENBD4":"1. Emirates NBD Bank (ENBD)",
"ENBD5":"1. Emirates NBD Bank (ENBD)",
"ENBD6":"1. Emirates NBD Bank (ENBD)",
    
"EI_C":'2. Emirates Islamic Bank (EIB)',
"EI":'2. Emirates Islamic Bank (EIB)',
"EI1":'2. Emirates Islamic Bank (EIB)',
"EI2":'2. Emirates Islamic Bank (EIB)',
"EI3":'2. Emirates Islamic Bank (EIB)',
"EI4":'2. Emirates Islamic Bank (EIB)',

"CBD_C":"6. Commercial Bank of Dubai (CBD)",
"CBD":"6. Commercial Bank of Dubai (CBD)",
"CBD1":"6. Commercial Bank of Dubai (CBD)",
"CBD2":"6. Commercial Bank of Dubai (CBD)",
"CBD3":"6. Commercial Bank of Dubai (CBD)",
"CBD4":"6. Commercial Bank of Dubai (CBD)",

"FAB_C":"3. First Abu Dhabi Bank (FAB)",
"FAB":"3. First Abu Dhabi Bank (FAB)",
"FAB1":"3. First Abu Dhabi Bank (FAB)",
"FAB2":"3. First Abu Dhabi Bank (FAB)",
"FAB3":"3. First Abu Dhabi Bank (FAB)",
"FAB4":"3. First Abu Dhabi Bank (FAB)",

"mashreq_C":'7. Mashreq Bank',
"Mashreq":'7. Mashreq Bank',
"Mashreq1":'7. Mashreq Bank',
"Mashreq2":'7. Mashreq Bank',
"Mashreq3":'7. Mashreq Bank',
"Mashreq4":'7. Mashreq Bank',
    
    #"HSBC": '8. HSBC Bank',
    #"CITI": '9. Citi Bank',
    #"Mashreq": '7. Mashreq Bank',
}



# In[54]:


#df_merged=df_yesterday


# In[ ]:





# In[55]:


df_merged.head()


# In[56]:


df_merged["Bank_Name"] = df_merged["source"].map(mapping)


# In[57]:


# remove leading 1., 2), 3-, 4: (any digits + optional punctuation) + spaces
df_merged["Name_of_bank"] = df_merged["Bank_Name"].str.replace(r"^\s*\d+[\.\)\-:]*\s*", "", regex=True)


# In[58]:


export=f"linkedin_fixed_{date_str}.xlsx"


# In[59]:


df_merged.to_excel(export)


# In[60]:


#df_merged.dtypes


# In[61]:


df_merged["followers"] = (
    df_merged["followers"]
    .astype(str)                # ensure string
    .str.replace(",", "")       # remove commas
    .str.replace(" ", "")       # remove spaces (if any)
)

df_merged["followers"] = pd.to_numeric(df_merged["followers"], errors="coerce")


# In[ ]:





# In[62]:


df1=df_merged


# In[63]:


df1.head()


# In[64]:


df1['Sentiment'].value_counts()


# In[65]:


df1['ddmmyyyy']=df1['post_date_ddmmyyyy']


# In[66]:


df1.head()


# In[67]:


summary = (
    df1.groupby(["Bank_Name","Name_of_bank"])
    .agg(
        totl_nbr_post=("Bank_Name", "size"),
        post_by_bank=("author_type", lambda x: (x == "Bank").sum()),
        post_by_Customers=("author_type", lambda x: (x == "Cust").sum()),

        total_likes_for_Bank_Posts=("likes", lambda x: x[df1.loc[x.index, "author_type"] == "Bank"].sum()),
        total_comments_for_Bank_Posts=("comments", lambda x: x[df1.loc[x.index, "author_type"] == "Bank"].sum()),
        total_reposts_for_Bank_Posts=("reposts", lambda x: x[df1.loc[x.index, "author_type"] == "Bank"].sum()),
        
        total_likes_for_Customer_Posts=("likes", lambda x: x[df1.loc[x.index, "author_type"] == "Cust"].sum()),
        total_comments_for_Customer_Posts=("comments", lambda x: x[df1.loc[x.index, "author_type"] == "Cust"].sum()),
        total_reposts_for_Customer_Posts=("reposts", lambda x: x[df1.loc[x.index, "author_type"] == "Cust"].sum()),
        
        Nbr_followers=("followers", 
                        lambda s: s.where(df1.loc[s.index, "author_type"] == "Bank").max()),
        min_post_date=("post_date", "min"),
        max_post_date=("post_date", "max"),

        neg_post_count=("Sentiment", lambda x: (x == "Negative").sum()),

        # ✅ NEW: count of Bank posts with likes > 100
         bank_posts_likes_gt_100=(
            "likes",
            lambda x: ((df1.loc[x.index, "author_type"] == "Bank") & (x > 100)).sum()
        )

    )
    .reset_index()
    .sort_values("Bank_Name")  # order by Bank_Name ascending
)


# In[68]:


summary


# In[69]:


post_from_dates = summary["min_post_date"].dropna().unique()
post_to_dates = summary["max_post_date"].dropna().unique()
print(f"\n LinkDin Post Analysis from {post_from_dates[0]} to {post_to_dates[0]}")


# In[70]:


df1["weekday_name"] = df1["post_date"].dt.day_name()


# In[71]:


bank_df=df1[df1['author_type']=='Bank']


# In[72]:


weekday_bank_counts = bank_df.groupby(
    ["weekday_name", "Bank_Name","Name_of_bank"]
).size().reset_index(name="post_count")


# In[73]:


######html
import datetime


# In[74]:


today_date = datetime.datetime.today().strftime("%d%b%Y").upper()


# In[75]:


base_name="linkedin_post_analysis"


# In[76]:


OUT_PATH = f"{base_name}.html"


# In[77]:


# ------------------------------
# Helpers
# ------------------------------
def fmt_date9(dt):
    if pd.isna(dt) or dt is None:
        return ""
    return pd.to_datetime(dt).strftime("%d%b%Y").upper()

def fmt_dt_full(dt):
    if pd.isna(dt) or dt is None:
        return ""
    return pd.to_datetime(dt).strftime("%d%b%Y %H:%M:%S").upper()

def esc(x):
    if x is None:
        return ""
    if isinstance(x, float) and np.isnan(x):
        return ""
    return str(x)


# In[78]:


df = df1.copy()


# In[79]:


df['Sentiment']=df['Sentiment'].str.lower()


# In[80]:


# ensure types
df["post_date"] = pd.to_datetime(df["post_date"], errors="coerce")
for c in ["likes", "comments", "reposts"]:
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)


# In[81]:


# split masks
is_bank_authored = df["author_type"].str.startswith("Bank", na=False)


# In[82]:


# Sort first by Bank_Name then likes descending
df = df.sort_values(['Bank_Name', 'likes'], ascending=[True, False])


# In[83]:


def rank_top3(d):
    # sort by engagement desc, then most recent date
    return d.sort_values(["likes", "post_date"], ascending=[False, False]).head(3)


# In[84]:


# per-bank top 3
top3_bank_authored = (
    df[is_bank_authored]
    .groupby("Bank_Name", group_keys=False)
    .apply(rank_top3)
)


# In[85]:



top3_customer_authored = (
    df[~is_bank_authored]
    .groupby("Bank_Name", group_keys=False)
    .apply(rank_top3)
)


# In[86]:


# (optional) merge both into one dict for rendering
per_bank = {}
for bank in sorted(df["Bank_Name"].dropna().astype(str).unique()):
    per_bank[bank] = {
        "bank": top3_bank_authored[top3_bank_authored["Bank_Name"] == bank],
        "customer": top3_customer_authored[top3_customer_authored["Bank_Name"] == bank],
    }


# In[87]:


neg_table=df1[(df1['Sentiment'] == 'Negative')]


# In[88]:


#neg_table.head()
len(neg_table)


# In[89]:


# ============================================
# LinkedIn Post Analysis - Attractive HTML
# Inputs (must exist in your session):
#   summary                         (bank-level aggregates)
#   neg_table                       (negative posts)
#   top3_bank_authorised OR top3_bank_authored
#   top3_customer_athorised OR top3_cust_authored
#
# Output: linkedin_post_analysis_final_abhi.html
# ============================================


from io import BytesIO
import base64
import matplotlib.pyplot as plt
import html


# In[90]:


# ---------- helpers ----------
def fmt_date9(dt):
    if pd.isna(dt) or dt is None: return ""
    return pd.to_datetime(dt).strftime("%d%b%Y").upper()

def fmt_dt_full(dt):
    if pd.isna(dt) or dt is None: return ""
    return pd.to_datetime(dt).strftime("%d%b%Y %H:%M:%S").upper()

def safe(x):
    if x is None or (isinstance(x, float) and np.isnan(x)): return ""
    return str(x)

def highlight_text(text):
    """Escape HTML then highlight hashtags, numbers, and some keywords."""
    if text is None: return ""
    s = html.escape(str(text))

    # Hashtags
    s = re.sub(r"(?<!&)#(\w+)", r"<mark>#\1</mark>", s)

    # Numbers / percents
    #s = re.sub(r"\b(\d[\d,\.]*%?)\b", r"<mark>\1</mark>", s)

    # Key terms (extend as needed)
    KEYWORDS = ["fraudulent",
        "offer", "update", "launch", "award", "promo", "fee", "fees",
        "complaint", "issue", "outage", "delay", "bug", "fix", "refund",
        "limited", "deadline", "today", "urgent","fraud"
    ]
    def kw_mark(m): return f"<mark>{m.group(0)}</mark>"
    for kw in KEYWORDS:
        s = re.sub(rf"\b{re.escape(kw)}\b", kw_mark, s, flags=re.IGNORECASE)

    return s


# In[91]:


def format_k_m(x):
    try:
        num = float(x)
    except (ValueError, TypeError):
        return x
    if num >= 1_000_000:
        return f"{num/1_000_000:,.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:,.0f}K"
    else:
        return f"{num:,.0f}"


# In[92]:


def render_badge(sent):
    s = (sent or "").strip().lower()
    if s == "positive": return '<span class="badge badge-pos">Positive</span>'
    if s == "negative": return '<span class="badge badge-neg">Negative</span>'
    return '<span class="badge badge-neu">Neutral</span>'


# In[93]:


# ---------- validations & inputs ----------
def must_have(df, name):
    if name not in globals():
        raise ValueError(f"Missing DataFrame: `{name}`")
    obj = globals()[name]
    if not isinstance(obj, pd.DataFrame) or obj.empty:
        raise ValueError(f"`{name}` must be a non-empty pandas DataFrame.")
    return obj


# In[94]:


summary_df = must_have(summary, "summary")
#neg_df     = must_have(neg_table, "neg_table")
bank3     = must_have(top3_bank_authored, "top3_bank_authored")
cust3     = must_have(top3_customer_authored, "top3_customer_authored")
neg_df=neg_table


# In[95]:



# ---------- Part 1 (summary-only) ----------
# Ensure dtypes
for c in ["Nbr_followers","totl_nbr_post","post_by_bank","post_by_Customers","comments","likes","reposts"]:
    if c in summary_df.columns:
        summary_df[c] = pd.to_numeric(summary_df[c], errors="coerce")
for c in ["min_post_date","max_post_date"]:
    if c in summary_df.columns:
        summary_df[c] = pd.to_datetime(summary_df[c], errors="coerce")

duration_text = f"{fmt_date9(summary_df['min_post_date'].min())} to {fmt_dt_full(summary_df['max_post_date'].max())}"

# Table (exact columns from summary)
part1_cols = {
    "Name_of_bank": "Bank Name",
    "Nbr_followers": "Number of Followers",
    #"totl_nbr_post": "Number of Posts Considered",
    "post_by_bank": "Posts By Bank",
    #"post_by_Customers": "Posts By Customer",
    "total_likes_for_Bank_Posts": "Total likes for Bank Posts",
    "total_comments_for_Bank_Posts": "Total comments for Bank Posts",
    "total_reposts_for_Bank_Posts": "Total reposts for Bank Posts",

    #"total_likes_for_Customer_Posts": "Total likes for Cust Posts",
    #"total_comments_for_Customer_Posts": "Total comments for Cust Posts",
    #"total_reposts_for_Customer_Posts": "Total reposts for Cust Posts",
    "bank_posts_likes_gt_100": "Bank posts with 100+ likes",
    #"neg_post_count": "Negative Post"

    
}



# In[96]:


missing = [k for k in part1_cols.keys() if k not in summary_df.columns]
if missing:
    raise ValueError(f"`summary` missing columns: {missing}")
summary_tbl = summary_df[list(part1_cols.keys())].rename(columns=part1_cols)


# In[97]:


summary_tbl


# In[98]:



cols_to_format = ["Number of Followers"]
for col in cols_to_format:
    summary_tbl[col] = summary_tbl[col].apply(format_k_m)


# In[99]:



# Table (exact columns from summary)
part2_cols = {
    "Name_of_bank": "Bank Name",
    "Nbr_followers": "Number of Followers",
    #"totl_nbr_post": "Number of Posts Considered",
    #"post_by_bank": "Posts By Bank",
    "post_by_Customers": "Customer's post Mentioning Bank",
    "neg_post_count": "Negative Post",
    # "total_likes_for_Bank_Posts": "Total likes for Bank Posts",
    # "total_comments_for_Bank_Posts": "Total comments for Bank Posts",
    # "total_reposts_for_Bank_Posts": "Total reposts for Bank Posts",

    "total_likes_for_Customer_Posts": "Total likes for Cust Posts",
    "total_comments_for_Customer_Posts": "Total comments for Cust Posts",
    "total_reposts_for_Customer_Posts": "Total reposts for Cust Posts",
    #"bank_posts_likes_gt_100": "Bank posts with 100+ likes",
    

    
}


# In[100]:


missing = [k for k in part1_cols.keys() if k not in summary_df.columns]
if missing:
    raise ValueError(f"`summary` missing columns: {missing}")
summary_tbl_2 = summary_df[list(part2_cols.keys())].rename(columns=part2_cols)


# In[101]:


summary_tbl_2


# In[102]:



cols_to_format = ["Number of Followers"]
for col in cols_to_format:
    summary_tbl_2[col] = summary_tbl_2[col].apply(format_k_m)


# In[103]:


summary_tbl_2


# In[ ]:





# In[104]:


# Sort weekdays in natural order (Mon–Sun)
weekday_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
weekday_bank_counts["weekday"] = pd.Categorical(weekday_bank_counts["weekday_name"], categories=weekday_order, ordered=True)

pivot_df = weekday_bank_counts.pivot(index="weekday", columns="Name_of_bank", values="post_count").fillna(0)


# In[105]:


# --- Matplotlib static bar chart -> base64 <img> for Outlook ---
import io, base64
import matplotlib.pyplot as plt

# Optional: ensure weekday order on x-axis if needed
if 'weekday_order' in globals():
    try:
        pivot_df = pivot_df.reindex(weekday_order)
    except Exception:
        pass
        
# Custom colors
custom_colors = [
   # "#ED1C24",  # ADCB
    #"#009ada",  # ADIB
    #"#2ca02c",  # CDB
    "#8C4799",  # EIB
    "#072447",  # ENBD
    #"#003399",  # FAB
    #"#FF671F",  # Mashreq
    #"#bcbd22",  # olive
    #"#17becf"   # cyan
]

# --- Build the figure ---
plt.close('all')
fig, ax = plt.subplots(figsize=(10, 5))

x_vals = pivot_df.index.astype(str).tolist()
bar_width = 0.12   # adjust width so bars don’t overlap

# Each bank → separate bar group
for i, col in enumerate(pivot_df.columns):
    y_vals = pivot_df[col].values
    color = custom_colors[i % len(custom_colors)]
    # shift bars by (i * bar_width)
    ax.bar(
        [x + i*bar_width for x in range(len(x_vals))],
        y_vals,
        width=bar_width,
        label=col,
        color=color
    )

# Fix x-axis ticks to center groups
ax.set_xticks([r + (len(pivot_df.columns)/2 - 0.5)*bar_width for r in range(len(x_vals))])
ax.set_xticklabels(x_vals)

# Labels & formatting
ax.set_title("Weekday-wise Bank Post Counts", fontweight="bold")
ax.set_xlabel("Weekday", fontweight="bold")
ax.set_ylabel("Number of Posts", fontweight="bold")
ax.legend(title="Bank Names", fontsize=8)
ax.grid(True, axis="y", linestyle="--", linewidth=0.5, alpha=0.6)

plt.tight_layout()

# Add vertical separator lines between days
#plt.grid(axis="x", which="major", linestyle="--", linewidth=0.7, color="gray")

# Or, for more control, draw manual vertical lines
for i in range(len(pivot_df.index) - 1):
    plt.axvline(x=i + 0.8, color="gray", linestyle="--", linewidth=0.8)
    
# Add values above each bar
for container in ax.containers:
    ax.bar_label(container, label_type="edge", fontsize=9, padding=2)
# --- Save to PNG in-memory and base64-embed ---
buf = io.BytesIO()
fig.savefig(buf, format="png", dpi=160, bbox_inches="tight")
buf.seek(0)
png_b64 = base64.b64encode(buf.read()).decode("ascii")

chart_img_html = (
    f'<img src="data:image/png;base64,{png_b64}" '
    f'alt="Weekday-wise Bank Post Counts" '
    f'style="max-width:100%;height:auto;display:block;border:0;" />'
)


# In[106]:


# (Optional) also save a physical PNG next to the HTML if you want to attach it separately
with open("weekday_bank_posts.png", "wb") as f:
    f.write(base64.b64decode(png_b64))


# In[107]:


# ---------- Part 2 (negative cards from neg_table) ----------
neg_tmp = neg_df.copy()
# Make sure date is datetime
if "post_date" in neg_tmp.columns:
    neg_tmp["post_date"] = pd.to_datetime(neg_tmp["post_date"], errors="coerce")
# Order by bank, recency
order_cols = ["Bank_Name"] + (["post_date"] if "post_date" in neg_tmp.columns else [])
neg_tmp = neg_tmp.sort_values(order_cols, ascending=[True, False] if len(order_cols)>1 else [True])


# In[108]:


# ---------- Part 3 (side-by-side top-3 from top3 tables) ----------
# Ensure datetime for sorting (won't recompute ranks, just display)
for df_ in (bank3, cust3):
    if "post_date" in df_.columns:
        df_["post_date"] = pd.to_datetime(df_["post_date"], errors="coerce")


# In[109]:


# ---------- CSS ----------
css = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700&display=swap');

  :root{
    --bg:#ffffff; --card:#ffffff; --soft:#f7f9fc; --soft2:#e5e8ef; --text:#072447; --muted:#54698d;
    --pos:#2ec27e; --neg:#ff5c5c; --neu:#f0b429; --chip:#e5e8ef; --chipBorder:#d0d6e5; --shadow:0 10px 30px rgba(0,0,0,.10);
  }

  *{box-sizing:border-box; font-family: 'Plus Jakarta Sans', sans-serif;}

  body{
    margin:0;
    background:#ffffff;
    color:var(--text);
  }

  .wrap{max-width:1150px;margin:34px auto;padding:0 20px;}


  .title{font-size:28px; font-weight:800; letter-spacing:.4px; margin:0 0 6px; color:#072447;}
  .muted{color:var(--muted)}
  .section{margin-top:24px;}

  table{border-collapse:collapse;width:100%;color:#072447;margin-top:10px;}
  th,td{border:1px solid #e5e8ef;padding:8px;font-size:13px;vertical-align:top}
  th{background:#f0f2f8; color:#072447;}
    .summary-table td{ font-weight:600; } 

  .grid{display:grid; grid-template-columns: 1fr 1fr; gap:18px;}
  @media(max-width:900px){ .grid{grid-template-columns:1fr; } }

  .bank-head{margin:24px 0 6px; font-size:22px; font-weight:800; color:#072447}

  .post{
    position:relative; /* for corner logo */
    background: var(--card);
    border:1px solid var(--soft2); border-radius:18px; padding:16px; box-shadow:var(--shadow);
    transition:transform .2s ease, border-color .2s ease; color:var(--text);
  }
  .post-neg{
    background:#fff0f0;
    border:1px solid #ff5c5c;
    color:#072447;
  }
  .post:hover{ transform: translateY(-2px); border-color:#b0bcd4; }

  /* Add corner logo (top-right) */
  .post::after {
    content:"";
    position:absolute;
    top:10px; right:10px;
    width:28px; height:28px;
    background:url('corner_logo.png') no-repeat center center;
    background-size:contain;
    opacity:0.7;
  }

  .head{display:flex; gap:12px; align-items:flex-start; margin-bottom:8px;}
  .meta{display:flex; flex-direction:column; gap:6px;}
  .name, .bank, .date { color:#072447; font-size:13px; }
  .text-summary{ color:#072447; line-height:1.55; }

  mark{ background:#ffeb3b; color:#000; padding:0 3px; border-radius:4px; }

  .badges{display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-top:8px;}
  .badge{font-size:12px; padding:4px 10px; border-radius:999px; border:1px solid #d0d6e5; background:#f2f4f8; color:#072447;}
  .badge-pos{border-color:#1f7a55; background:#e6f8f0; color:#1f7a55;}
  .badge-neg{border-color:#ff5c5c; background:#d7d2cb; color:#ff0000;}
  .badge-neu{border-color:#f0b429; background:#fff7e0; color:#b37400;}

  .stats{display:flex; gap:10px; margin-top:10px; flex-wrap:wrap;}
  .chip{background:#f7f9fc; border:1px solid var(--chipBorder); color:#072447;
        border-radius:10px; padding:6px 10px; font-size:12px;}
  .tag{padding:3px 8px; border-radius:999px; border:1px dashed #d0d6e5; color:#54698d; font-size:12px;}
  .hr{border:0;height:1px;background:#e5e8ef; margin:20px 0;}

  .highlights { margin: 6px 0 10px; padding-left: 18px; }
  .highlights li { margin: 2px 0; }
  .hl-title { margin-top: 6px; font-weight: 700; color: #072447; }

    /* ---------- Tables ---------- */
    table {
      border-collapse: collapse;
      width: 100%;
      color: #072447;
      margin-top: 10px;
    }
    
    th, td {
      border: 1px solid var(--soft2);
      padding: 10px 8px;
      font-size: 13px;
      vertical-align: middle;
    }
    
    /* Header style */
    .summary-table th {
      background: #072447;   /* navy header */
      color: #ffffff;        /* white text */
      font-weight: 700;
      text-align: center;    /* center align all headers */
    }
    
    /* Row style */
    .summary-table td {
      font-weight: 600;
      background: #e4e1dc;   /* table rows background */
      text-align: center;    /* center align all cells by default */
    }

    /* Override for Bank Name column (first col only) */
    .summary-table td:first-child,
    .summary-table th:first-child {
      text-align: left;      /* keep Bank Name left aligned */
      padding-left: 14px;    /* add breathing space */
    }
    /* Negative Post Card */
    .post-neg {
      background: #DAD6D0;        /* new background */
      border: 1px solid #b23b3b;  /* keep a subtle red border (optional) */
      color: #072447;             /* ensure text stays readable */
    }

    /* --- HERO fix: force 2-col grid with 3 stacked rows --- */
        
/* GRID: 2 columns, 3 rows; logo spans all 3 rows */
.hero-grid{
  display:grid;
  grid-template-columns: auto 1fr;  /* left = logo, right = text column */
  grid-template-rows: auto auto auto;
  column-gap: 24px;
  align-items:center;               /* align items vertically in their rows */
  justify-content:center;           /* center whole block on the page */
  margin: 16px 0 24px;
}

/* LOGO: small, fixed height so it never forces wrapping */
.hero-logo{
  grid-row: 1 / span 3;             /* span all three rows */
  display:flex;
  align-items:center;
  justify-content:center;
}
.hero-logo img{
  height: 60px;                     /* ↓ reduce if needed (e.g., 100px) */
  width: auto;
  display:block;
}

/* Each row on the right is its own centered line */
.hero-row{
  display:flex;
  justify-content:center;            /* center text horizontally in the row */
  text-align:center;
  color:#0a2540;
}

.hero-row h1{
  margin:0 0 4px 0;
  font-size: 28px;                   /* adjust to taste */
  font-weight:800;
  line-height:1.2;
}
.hero-row p{
  margin: 2px 0;
  font-size: 16px;
  line-height:1.35;
}

/* Optional: stack on small screens */
@media (max-width: 680px){
  .hero-grid{
    grid-template-columns: 1fr;
    grid-template-rows: auto auto auto auto;
    row-gap: 10px;
    justify-content:stretch;
  }
  .hero-logo{ grid-row:auto; }
  .hero-row{ justify-content:center; }
}

.genai-badge {
  position: absolute;
  top: 60px;
  right: 40px;
  font-size: 14px;
  font-weight: 600;
  color: #072447;        /* navy */
  background: #f0f2f8;   /* light gray background */
  padding: 6px 14px;
  border-radius: 20px;
  box-shadow: 0 2px 6px rgba(0,0,0,0.15);
}
.hero-grid {
  position: relative; /* so badge aligns inside hero block */
}

.post-link {
  margin-top: 8px;
  text-align: right;          /* pushes the link to the right */
  display: block;             /* full width row */
}

.post-link a {
  font-size: 0.9rem;
  font-weight: 600;
  color: #0a66c2;
  text-decoration: none;
}

.post-link a:hover {
  text-decoration: underline;
}




</style>
"""


# In[110]:


def embed_data_url(path):
    ext = os.path.splitext(path)[1].lower()
    mime = "image/png" if ext == ".png" else "image/jpeg"
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"

# point to your actual file (exact filename & case!)
logo_data_url = embed_data_url("enbd_logo.png")


# In[111]:


header = f"""
<div class="hero-grid">
  <div class="hero-logo">
    <img src="{logo_data_url}" alt="ENBD" class="logo">
  </div>

  <div class="genai-badge">⚡Powered by Gen AI</div>

  <div class="hero-row r1">
    <h1>LinkedIn Engagement Analysis for Banks</h1>
  </div>

  <div class="hero-row r2">
    <p>Duration: {fmt_date9(summary_df['min_post_date'].min())} to {fmt_dt_full(summary_df['max_post_date'].max())}</p>
  </div>

  <div class="hero-row r3">
    <p>Number of Banks considered for Analysis: {summary_df['Name_of_bank'].nunique()}</p>
  </div>
</div>
"""


# In[112]:



# ---------- Part 1 ----------
part1_table_html = summary_tbl.to_html(index=False, classes="summary-table", escape=False)
part1_html = f"""
<div class="section">
  <h2>Part 1: Bank Posts Overview</h2>
 
  {part1_table_html}
</div>
"""


# In[113]:


# ---------- Part 1 ----------
part11_table_html = summary_tbl_2.to_html(index=False, classes="summary-table", escape=False)
part11_html = f"""
<div class="section">
  <h2>Part 2: Customer's mentioned Bank Posts Overview</h2>
 
  {part11_table_html}
</div>
"""


# In[114]:


# right after you build neg_tmp (and similarly for bank3, cust3 if needed)
for c in ['total_likes_for_Bank_Posts',
       'total_comments_for_Bank_Posts', 'total_reposts_for_Bank_Posts',
       'total_likes_for_Customer_Posts', 'total_comments_for_Customer_Posts',
       'total_reposts_for_Customer_Posts']:
    if c in neg_tmp.columns:
        neg_tmp[c] = pd.to_numeric(neg_tmp[c], errors="coerce").fillna(0).astype(int)


# In[115]:


def _as_int(x):
    # converts to int, treating NaN/None/"" as 0
    try:
        v = pd.to_numeric(x, errors="coerce")
        if pd.isna(v):
            return 0
        return int(v)
    except Exception:
        return 0


# In[116]:


#top3_bank_authored
#top3_customer_authored
import pandas as pd
import re, html, os

# If you already pasted the genai_highlights() from earlier, keep it.
# Otherwise include that function here. (It uses OpenAI when key is set, else local fallback.)

def _split_highlights(val):
    """Accepts list or string and returns list of up to 3 bullets."""
    if val is None:
        return []
    if isinstance(val, list):
        items = [str(x).strip() for x in val if str(x).strip()]
    else:
        # split on newlines or bullets
        items = [s.strip("•- ").strip() for s in re.split(r"[\n\r]+| • ", str(val)) if s.strip()]
    return items[:3]

def row_highlights(row):
    """
    Prefer a precomputed column 'post_highlights' if present and non-empty,
    otherwise compute with genai_highlights(row['text'], 3).
    """
    # Precomputed?
    if "post_highlights" in row and pd.notna(row["post_highlights"]) and str(row["post_highlights"]).strip():
        return _split_highlights(row["post_highlights"])

    # Compute from text
    txt = row.get("text", "")
    #bullets = genai_highlights(txt, max_bullets=3)  # uses OpenAI if key set, else local fallback
    return _split_highlights(bullets)


# In[117]:


def render_card(row, variant="normal"):
    # white info, summary first; red background variant for negatives
    is_bank = str(row.get("author_type","")).lower().startswith("from")
    cls = "post post-neg" if variant == "Neg" else "post"
    post_text = row.get('post_highlights','') or ""
    # get three highlights (list[str])
    hl = row_highlights(row)

             # --- NEW: post link (permalink) ---
    post_link = (row.get("post_link") or "").strip()
    if post_link:
        link_html = f"""
        <div class="post-link">
          <a href="{html.escape(post_link)}" target="_blank" rel="noopener noreferrer">
            🔗 Open LinkedIn post
          </a>
        </div>
        """
    else:
        link_html = ""

    # build list HTML (one per line)
    if hl:
        hl_html = "<ul class='highlights'>" + "".join(f"<li>{html.escape(h)}</li>" for h in hl) + "</ul>"
    else:
        hl_html = "<ul class='highlights'><li>—</li></ul>"

    return f"""
    <div class="{cls}">
      <div class="head">
        <div class="meta">
          <div class="name"><b>Author Name:</b> {html.escape(str(row.get('author_name','') or ''))}</div>
          <div class="bank"><b>Bank Name:</b> {html.escape(str(row.get('Name_of_bank','') or ''))}</div>
          <div class="date"><b>Post Date:</b> {(row.get('ddmmyyyy'))}</div>
          <div class="hl-title">Key highlights of Post</div>
          {hl_html}

        </div>
      </div>
      <div class="badges">
        {render_badge(row.get('Sentiment',''))}
        <span class="tag">{'Posts by Bank' if is_bank else 'Posts by Customer'}</span>
      </div>
      <div class="stats">
        <div class="chip">👍 Likes: {_as_int(row.get('likes'))}</div>
        <div class="chip">💬 Comments: {_as_int(row.get('comments'))}</div>
        <div class="chip">🔁 Reposts: {_as_int(row.get('reposts'))}</div>
      </div>
        {link_html}
    </div>
    """


# In[118]:


def render_card_neg(row, variant="normal"):
    # white info, summary first; red background variant for negatives
    is_bank = str(row.get("author_type","")).lower().startswith("from")
    cls = "post post-neg" if variant == "Neg" else "post"
    post_text = row.get('post_highlights','') or ""
    # get three highlights (list[str])
    hl = row_highlights(row)
    
    # --- NEW: post link (permalink) ---
    post_link = (row.get("post_link") or "").strip()
    if post_link:
        link_html = f"""
        <div class="post-link">
          <a href="{html.escape(post_link)}" target="_blank" rel="noopener noreferrer">
            🔗 Open LinkedIn post
          </a>
        </div>
        """
    else:
        link_html = ""
        
    # build list HTML (one per line)
    if hl:
        hl_html = "<ul class='highlights'>" + "".join(f"<li>{html.escape(h)}</li>" for h in hl) + "</ul>"
    else:
        hl_html = "<ul class='highlights'><li>—</li></ul>"

    return f"""
    <div class="{cls}">
      <div class="head">
        <div class="meta">
          <div class="name"><b>Author Name:</b> {html.escape(str(row.get('author_name','') or ''))}</div>
          <div class="bank"><b>Bank Name:</b> {html.escape(str(row.get('Name_of_bank','') or ''))}</div>
          <div class="date"><b>Post Date:</b> {(row.get('ddmmyyyy'))}</div>
          <div class="text-summary"><b>Post text:</b> {highlight_text(row.get('post',''))}</div>
          <div class="hl-title">Key highlights of Post</div>
          {hl_html}

        </div>
      </div>
      <div class="badges">
        {render_badge(row.get('Sentiment',''))}
        <span class="tag">{'Posts by Bank' if is_bank else 'Posts by Customer'}</span>
      </div>
      <div class="stats">
        <div class="chip">👍 Likes: {_as_int(row.get('likes'))}</div>
        <div class="chip">💬 Comments: {_as_int(row.get('comments'))}</div>
        <div class="chip">🔁 Reposts: {_as_int(row.get('reposts'))}</div>
      </div>
       {link_html}
    </div>
    """


# In[119]:


neg_tmp = neg_tmp.sort_values(by="Bank_Name", ascending=True)

part2_blocks = ["<h2 class='section'>Part 3: Negative Posts by Customer</h2>"]
if neg_tmp.empty:
    part2_blocks.append("<div class='muted'>No negative posts found.</div>")
else:
    for bank, bdf in neg_tmp.groupby("Name_of_bank", sort=False):
        part2_blocks.append(f"<h3 class='bank-head'>Negative Posts — {html.escape(str(bank))}</h3>")
        # sort within each bank, e.g., by Post_Date
        bdf = bdf.sort_values(by="Bank_Name", ascending=False)
        for _, r in bdf.iterrows():
            part2_blocks.append(render_card_neg(r, variant="Neg"))

part2_html = "\n".join(part2_blocks)


# In[120]:


combined = (
    pd.concat([bank3[["Name_of_bank", "Bank_Name"]], 
               cust3[["Name_of_bank", "Bank_Name"]]])
      .dropna(subset=["Name_of_bank"])
      .drop_duplicates(subset=["Name_of_bank"])
      .sort_values(by="Bank_Name", ascending=True)
)


# In[121]:


banks=combined['Name_of_bank'].to_list()


# In[122]:



# ---------- Part 3: Side-by-side per bank (Top-3) ----------
part3_blocks = ["<h2 class='section'>Part 4: Most Engaged Posts</h2>"]
#banks = sorted(set(bank3["Name_of_bank"].dropna().astype(str)) | set(cust3["Name_of_bank"].dropna().astype(str)))
for bank in banks:
    left_df = bank3[bank3["Name_of_bank"].astype(str) == bank]
    right_df = cust3[cust3["Name_of_bank"].astype(str) == bank]
    left_cards = "\n".join(render_card(r) for _, r in left_df.iterrows()) or "<div class='muted'>No Posts By Bank top posts.</div>"
    right_cards = "\n".join(render_card(r) for _, r in right_df.iterrows()) or "<div class='muted'>No Posts by Customer top posts.</div>"
    part3_blocks.append(f"""
    <h3 class="bank-head">{html.escape(str(bank))}</h3>
    <div class="grid">
      <div>
        <h4> Posts by Bank</h4>
        {left_cards}
      </div>
      <div>
        <h4> Posts by Customer</h4>
        {right_cards}
      </div>
    </div>
    <div class="hr"></div>
    """)
part3_html = "\n".join(part3_blocks)


# In[123]:


# ---------- Header ----------
# header = f"""
# <div class="hero">
#   <!-- ENBD logo top-left -->
#   <img src="enbd_logo.png" alt="ENBD" class="logo">

#   <div class="title"> 📊LinkedIn Engagement Analysis for Banks</div>
#   <div class="muted">Duration: {fmt_date9(summary_df['min_post_date'].min())} to {fmt_dt_full(summary_df['max_date'].max())}</div>
#   <div class="muted">Number of Banks considered for Analysis: {summary_df['Bank_Name'].nunique()}</div>
# </div>
# """

# ---------- Assemble & write ----------
html_doc = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8" /><title>LinkedIn Post Analysis</title>{css}</head>
<body>
  <div class="wrap">
    {header}
    {part1_html}
    {part11_html}
    
    {part2_html}
    {part3_html}
  </div>
</body>
</html>"""

with open(OUT_PATH, "w", encoding="utf-8") as f:
    f.write(html_doc)


# In[124]:


#part1_html


# In[125]:





# In[126]:





# In[ ]:





# In[ ]:




