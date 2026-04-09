#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Customers Mentioning Emirates Islamic (Mentions)
from apify_client import ApifyClient

from openai import OpenAI
import pandas as pd
from datetime import datetime, timedelta
import io, base64
from io import BytesIO
import base64
import matplotlib.pyplot as plt
import html
import re
import streamlit as st
import os
from dotenv import load_dotenv
import pytz
load_dotenv()

# In[2]:

try:
    APIFY_TOKEN = st.secrets["APIFY_TOKEN"]
except Exception:
    APIFY_TOKEN = os.getenv("APIFY_TOKEN")


# In[3]:




# In[4]:


#client = ApifyClient(APIFY_TOKEN)


# In[5]:


# Customers Mentioning Emirates Islamic (Mentions)


client = ApifyClient(APIFY_TOKEN)

since_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

query_mentions = f'''
(
"Emirates Islamic" OR 
"Emirates Islamic Bank" OR
emiratesislamic OR
@emiratesislamic 

)
since:{since_date}
'''

run_input_mentions = {
    "query": query_mentions,
    "maxItems": 1000,
    "mode": "Latest"
}

run = client.actor("igolaizola/x-twitter-scraper-ppe").call(run_input=run_input_mentions)

mentions = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    mentions.append(item)

df_mentions = pd.DataFrame(mentions)
print("Mentions:", len(df_mentions))


# In[6]:


# Tweets Posted by the Bank


# In[7]:


# Tweets Posted by the Bank
query_bank = f'from:emiratesislamic since:{since_date}'

run_input_bank = {
    "query": query_bank,
    "maxItems": 200,
    "mode": "Latest"
}

run = client.actor("igolaizola/x-twitter-scraper-ppe").call(run_input=run_input_bank)

bank_posts = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    bank_posts.append(item)

df_bank_posts = pd.DataFrame(bank_posts)
print("Bank posts:", len(df_bank_posts))


# In[8]:


# Replies / Comments to the Bank (Customer Complaints)


# In[9]:


query_replies = f'to:emiratesislamic since:{since_date}'

run_input_replies = {
    "query": query_replies,
    "maxItems": 1000,
    "mode": "Latest"
}

run = client.actor("igolaizola/x-twitter-scraper-ppe").call(run_input=run_input_replies)

replies = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    replies.append(item)

df_replies = pd.DataFrame(replies)
print("Replies:", len(df_replies))


# In[10]:


df_all = pd.concat([df_mentions, df_bank_posts, df_replies], ignore_index=True)

print("Total posts collected:", len(df_all))


# In[11]:


df_all.to_excel("EI_tweet.xlsx")


# In[12]:


df_all = df_all.drop_duplicates(subset=["id"])


# In[ ]:





# In[13]:


#urls


# ## ENBD

# # Customers Mentioning Emirates NBD

# In[14]:





client = ApifyClient(APIFY_TOKEN)

#since_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")


# In[15]:


query_mentions_enbd = f'''
(
"Emirates NBD" OR
ENBD OR
emiratesnbd OR
@EmiratesNBD_AE OR
@EmiratesNBD_KSA
)
since:{since_date}
'''

run_input_mentions = {
    "query": query_mentions_enbd,
    "maxItems": 1000,
    "mode": "Latest"
}

run = client.actor("igolaizola/x-twitter-scraper-ppe").call(run_input=run_input_mentions)

mentions_enbd = []

for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    mentions_enbd.append(item)

df_mentions_enbd = pd.DataFrame(mentions_enbd)

print("Mentions:", len(df_mentions_enbd))


# # Tweets Posted by Emirates NBD

# In[16]:


query_bank_enbd = f'from:EmiratesNBD_AE since:{since_date}'

run_input_bank = {
    "query": query_bank_enbd,
    "maxItems": 200,
    "mode": "Latest"
}

run = client.actor("igolaizola/x-twitter-scraper-ppe").call(run_input=run_input_bank)

dataset_id = run["defaultDatasetId"]


# In[17]:


rows = []

for item in client.dataset(dataset_id).iterate_items():

    rows.append({
        "tweet_id": item.get("id"),
        "conversation_id": item.get("conversationId"),
        "reply_to": item.get("inReplyToStatusId"),
        "user": item.get("author"),
        "text": item.get("text"),
        "date": item.get("createdAt"),
        "url": item.get("url")
    })

df = pd.DataFrame(rows)


# In[18]:


#df


# In[19]:


query_bank_enbd = f'to:EmiratesNBD_AE since:{since_date}'

run_input_bank = {
    "query": query_bank_enbd,
    "maxItems": 200,
    "mode": "Latest"
}

run = client.actor("igolaizola/x-twitter-scraper-ppe").call(run_input=run_input_bank)

bank_posts_enbd = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    bank_posts_enbd.append(item)

df_bank_posts_enbd = pd.DataFrame(bank_posts_enbd)


# # Customer Replies to Emirates NBD

# In[20]:


query_replies_enbd = f'''
to:EmiratesNBD_AE since:{since_date}
'''

run_input_replies = {
    "query": query_replies_enbd,
    "maxItems": 500,
    "mode": "Latest"
}

run = client.actor("igolaizola/x-twitter-scraper-ppe").call(run_input=run_input_replies)

replies_enbd = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    replies_enbd.append(item)

df_replies_enbd = pd.DataFrame(replies_enbd)


# In[21]:


df_enbd_all = pd.concat(
    [df_mentions_enbd, df_bank_posts_enbd, df_replies_enbd],
    ignore_index=True
)


# In[22]:


df_enbd_all = df_enbd_all.drop_duplicates(subset=["id"]).reset_index(drop=True)

print("Total ENBD posts:", len(df_enbd_all))


# In[23]:





# In[ ]:





# In[24]:


df_enbd_all["bank_ext"]="ENBD"


# In[ ]:





# In[25]:


df_all["bank_ext"]="EI"


# In[26]:


len(df_enbd_all)


# In[27]:


len(df_all)


# In[28]:


df = pd.concat(
    [df_enbd_all, df_all],
    ignore_index=True
)


# In[29]:


len(df)

#df=df.head(25)
# In[30]:



#df = df.head(25)

# In[31]:


#df.to_excel("check.xlsx",index=0)


# In[32]:


urls = (
    df["permalink"]
    .dropna()
    .unique()
    .tolist()
)

print(len(urls))
print(urls[:5])
def chunk_list(lst, size=5):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

url_batches = list(chunk_list(urls, 5))

records = []

for batch in url_batches:

    run_input = {
        "postUrls": batch,
        "resultsLimit": 20,
        "includeOriginalPost": True
    }

    run = client.actor("scraper_one/x-post-replies-scraper").call(run_input=run_input)

    dataset_id = run["defaultDatasetId"]

    for item in client.dataset(dataset_id).iterate_items():
        records.append(item)


EI_ENDB_comments = pd.DataFrame(records)
print("Mentions:", len(EI_ENDB_comments))

EI_ENDB_comments.to_excel("Xpost_df_comments.xlsx")


EI_ENDB_comments["dubai_datetime"] = (
    pd.to_datetime(EI_ENDB_comments["timestamp"], unit="ms", utc=True)
    .dt.tz_convert("Asia/Dubai")
    .dt.tz_localize(None)   # removes timezone
)

EI_ENDB_comments = EI_ENDB_comments.drop_duplicates(subset=["replyId"])


# In[33]:


EI_ENDB_comments


# In[34]:


#EI_ENDB_comments permalink postUrls  replyUrl


# In[35]:


#df.head(5)


# In[36]:


df.to_excel("EI_ENBD_3post.xlsx",index=0)


# In[37]:


#permalink
EI_ENDB_comments.to_excel("EI_ENDB_comments.xlsx",index=0)


# In[38]:


comments=EI_ENDB_comments[
    (EI_ENDB_comments['inReplyTo'].isna()) &
    (EI_ENDB_comments['inReplyTo'] == '')
]


# In[39]:


EI_ENDB_comments_nodup = (
    EI_ENDB_comments
    .sort_values(["postUrl", "dubai_datetime"])
    .drop_duplicates(subset="postUrl", keep="first")
)


# In[40]:


len(EI_ENDB_comments_nodup)


# In[41]:


df['text_1']=df['text']


# In[42]:


EI_ENDB_comments_x=EI_ENDB_comments_nodup[['postUrl','replyText']]


# In[43]:


#permalink
#EI_ENDB_comments_x.to_excel("EI_ENDB_comments_x.xlsx",index=0)


# In[44]:


df_final = df.merge(
    EI_ENDB_comments_x,
    left_on="permalink",
    right_on="postUrl",
    how="left"
)


# In[45]:


len(df)


# In[46]:


len(df_final)


# In[47]:


df_final


# In[ ]:





# In[48]:


import numpy as np

df_final["text_v1"] = np.where(
    df_final["postUrl"].fillna("").str.len() > 5,
    df_final["replyText"],
    df_final["text_1"]
)


# In[49]:


df_final.to_excel("df_final.xlsx")


# In[ ]:





# In[50]:


df_final['text']=df_final['text_v1']


# In[51]:


df=df_final


# In[ ]:





# In[52]:


df.rename(columns={"likes": "Views"}, inplace=True)


# In[53]:


df.rename(columns={"quotes": "likes"}, inplace=True)


# In[54]:


len(df)


# In[55]:


df.to_csv("twitter_date_ei5.csv",index=0)


# In[56]:


df=df[df['username'] != 'centralbankuae']


# In[57]:


df.head(1)


# In[58]:



try:
    key = st.secrets["OPENAI_API_KEY"]
except Exception:
    key = os.getenv("OPENAI_API_KEY")


# In[59]:


#from openai import OpenAI
client = OpenAI(api_key=key)


# In[60]:


def translate_to_english(text: str) -> str:
    """Detect language, translate to English, and clean output."""
    if not isinstance(text, str) or not text.strip():
        return ""
    prompt = (
        "Detect the language and translate the text to fluent English. "
        "Return ONLY the translated English text, no quotes, no extras spaces and clean output.\n\n"
        f"Text:\n{text}"
    )
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return resp.choices[0].message.content.strip()


# In[61]:


df["post"] = df["text"].astype(str).apply(translate_to_english)


# In[62]:


df_tsrf=df


# In[63]:


raw_dt=df


# In[64]:


#df=raw_dt


# In[65]:


#df_tsrf.columns


# In[66]:


df_tsrf["posttype"] = df_tsrf["fullname"].str.lower().isin(
    ["emirates nbd", "emiratesislamic","","emirates nbd deals","emiratesnbd ksa"]
).map({True: "Bank", False: "Cust"})


# In[67]:


#username="author_name"


# 1. Classify sentiment ONLY if the post clearly criticizes or praises the BANK itself.
# 2. If the post talks about stock market (DFM), economy, war, politics, inflation, UAE crisis, or general financial markets WITHOUT blaming the bank, classify as Neutral.
# 3. If multiple banks are mentioned but the criticism is about the whole economy or sector, classify as Neutral.
# 4. Only classify as Negative if the bank is directly blamed (example: app not working, ATM issue, fraud, bad service, system outage).
# 5. Output only ONE word: Positive, Negative, or Neutral.
# 

# In[68]:


def analyze_sentiment(text):
    prompt = f"""
You are analyzing X (Twitter) posts about UAE banks.

Banks:
- Emirates NBD (ENBD)
- Emirates Islamic (EI)

Classify sentiment about the BANK.

IMPORTANT RULES:

1. If a post shows customer complaint, dissatisfaction, service issue, delay, app error,
   poor support, fraud concern, or negative experience → classify as Negative.

2. If the bank replies with apology or support phrases such as:
   "sorry for the inconvenience"
   "we regret"
   "not the experience we expect"
   "please send us details"
   "our team will contact you"
   "we will review this"
   → this indicates the original issue was a customer complaint.
   → classify as Negative.

3. If the post praises the bank, promotion, benefits, offers, achievements → Positive.

4. If the post only mentions the bank with no opinion → Neutral.

Return ONLY ONE word:
Positive
Negative
Neutral

Post:
{text}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content.strip()


# In[69]:


# Apply sentiment analysis
df_tsrf["Sentiment"] = df_tsrf["post"].apply(analyze_sentiment)


# In[70]:


df_tsrf.rename(columns={"username": "author_name"}, inplace=True)


# In[71]:


df_tsrf_dt=df_tsrf


# In[72]:


df_tsrf_dt.to_csv("sent.csv",index=0)


# In[ ]:





# In[ ]:





# In[73]:


df_tsrf["createdAt"] = pd.to_datetime(df_tsrf["createdAt"], utc=True)

df_tsrf["uae_time"] = df_tsrf["createdAt"].dt.tz_convert("Asia/Dubai")


# In[74]:


df_tsrf_dt["ddmmyyyy"] = pd.to_datetime(df_tsrf_dt["uae_time"]).dt.strftime("%d-%m-%Y")


# In[75]:


df_tsrf_dt["uae_time"] = pd.to_datetime(df_tsrf_dt["uae_time"])


# In[76]:


#df_tsrf_dt


# In[77]:
df_tsrf_dt["ddmmyyyy"] = pd.to_datetime(
    df_tsrf_dt["ddmmyyyy"],
    format="%d-%m-%Y"
)
final_df_v2 = df_tsrf_dt[
    df_tsrf_dt["ddmmyyyy"] >= pd.to_datetime("2026-04-04")
]

#final_df_v2=df_tsrf_dt[df_tsrf_dt['ddmmyyyy']>='30-03-2026']



# In[78]:


final_df_v2['ddmmyyyy']


# In[79]:


df_tsrf_dt=final_df_v2


# In[80]:


df_tsrf_dt.to_csv("twiter1.csv",index=0)


# In[81]:


mapping = {

"ADCB_C":'4. Abu Dhabi Commercial Bank (ADCB)',
"ADCB":'4. Abu Dhabi Commercial Bank (ADCB)',

"ADIB_C":"5. Abu Dhabi Islamic Bank (ADIB)",
"ADIB":"5. Abu Dhabi Islamic Bank (ADIB)",

"ENBD_C":"1. Emirates NBD Bank (ENBD)",
"ENBD":"1. Emirates NBD Bank (ENBD)",
    
"EI_C":'2. Emirates Islamic Bank (EIB)',
"EI":'2. Emirates Islamic Bank (EIB)',

"CBD_C":"6. Commercial Bank of Dubai (CBD)",
"CBD":"6. Commercial Bank of Dubai (CBD)",

"FAB_C":"3. First Abu Dhabi Bank (FAB)",
"FAB":"3. First Abu Dhabi Bank (FAB)",

"mashreq_C":'7. Mashreq Bank',
"Mashreq":'7. Mashreq Bank',

"ENBD & EI":'0. Emirates NBD Bank & Emirates Islamic Bank'
    
    #"HSBC": '8. HSBC Bank',
    #"CITI": '9. Citi Bank',
    #"Mashreq": '7. Mashreq Bank',
}



# In[ ]:





# In[82]:


df_tsrf_dt["Bank"]=df_tsrf_dt["bank_ext"]


# In[83]:


def classify_bank(banks):
    if "ENBD" in banks and "EI" in banks:
        return "ENBD"
    elif "ENBD" in banks:
        return "ENBD"
    elif "EI" in banks:
        return "EI"
    else:
        return None


# fallback using fullname
df_tsrf_dt.loc[
    df_tsrf_dt["Bank"].isna() & df_tsrf_dt["fullname"].str.lower().str.contains("emirates nbd"),
    "Bank"
] = "ENBD"

df_tsrf_dt.loc[
    df_tsrf_dt["Bank"].isna() & df_tsrf_dt["fullname"].str.lower().str.contains("emiratesislamic"),
    "Bank"
] = "EI"


# In[84]:


df_tsrf_dt["Bank_Name"] = df_tsrf_dt["Bank"].map(mapping)


# In[85]:


df_tsrf_dt[['Bank', 'Bank_Name']].value_counts().sort_index()


# In[86]:


import os


# In[87]:


_USE_GENAI = bool(os.getenv(key))
if _USE_GENAI:
    try:
        from openai import OpenAI
        _client = OpenAI()
        _MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # fast & cheap; change if you like
    except Exception:
        _USE_GENAI = False
        _client = None


# In[88]:


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
                temperature=0.9,
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


# In[89]:


def _clean_text(t):
    if t is None: return ""
    t = str(t).strip()
    # Collapse whitespace
    t = re.sub(r"\s+", " ", t)
    # Trim super long
    return t[:6000]  # safety


# In[90]:


import re


# In[91]:



df_tsrf_dt["post_highlights"] = df_tsrf_dt["post"].apply(
    lambda txt: " • ".join(genai_highlights(txt, max_bullets=3))
)


# In[92]:


# remove leading 1., 2), 3-, 4: (any digits + optional punctuation) + spaces
df_tsrf_dt["Name_of_bank"] = df_tsrf_dt["Bank_Name"].str.replace(r"^\s*\d+[\.\)\-:]*\s*", "", regex=True)


# In[93]:


df_tsrf_dt.rename(columns={"posttype": "author_type"}, inplace=True)


# In[94]:


df_tsrf_dt[['Bank', 'Bank_Name','author_type']].value_counts().sort_index()


# In[95]:


df_tsrf_dt['Bank_Name'].value_counts()


# In[96]:


df_tsrf_dt['Name_of_bank'].value_counts()


# In[97]:


#df_tsrf_dt.loc[(df_tsrf_dt["Sentiment"] == "Negative") & (df_tsrf_dt["author_type"] == "Bank"), "Sentiment"] = "Positive"


# In[98]:


from datetime import datetime, timedelta


# In[99]:


df1=df_tsrf_dt


# In[100]:


df1.columns


# In[101]:


df1["year"] = pd.to_datetime(df1["ddmmyyyy"], format="%d-%m-%Y").dt.year


# In[102]:


df2=df1[df1["year"]>=2026]


# In[ ]:





# In[103]:


df2["replyingTo_clean"] = df2["replyingTo"].astype(str).str.replace(r"[\[\]']", "", regex=True)


# In[104]:


# CHNAGE COMMENT AND BANK CHANGE TO - cust -- MAY BE WILL NOT GET GET POST WE HAVE COMMENTS
# REMOVE DUPLICATE POST


# In[105]:


neg=df2[df2['Sentiment']=='Negative']
neg_none=df2[df2['Sentiment']!='Negative']


# In[106]:


len(neg_none)


# In[107]:


banks = [
    'EmiratesNBD_AE',
    'EmiratesNBD_EGY',
    'EmiratesNBD_KSA',
    'emiratesislamic'
]

neg_customer = neg[~neg['author_name'].isin(banks)]
neg_bank = neg[neg['author_name'].isin(banks)]


# In[108]:


authors = neg_customer[["author_name"]].drop_duplicates()


# In[109]:





# In[110]:


neg_bank_filtered = neg_bank[
    ~neg_bank["replyingTo_clean"].isin(authors["author_name"])
]


# In[111]:





# In[112]:


df_final = pd.concat([neg_customer, neg_bank_filtered], ignore_index=True)


# In[113]:





# In[114]:


df_final2 = (
    df_final
    .sort_values(["author_name", "uae_time"], ascending=[True, True])
    .drop_duplicates(subset="author_name", keep="first")
)


# In[115]:


df2 = pd.concat([neg_none, df_final2], ignore_index=True)


# In[116]:


len(neg_none)


# In[117]:


len(df_final2)


# In[118]:


len(df2)


# In[119]:


df2.loc[
    (df2["author_type"] == "Bank") & (df2["replyingTo_clean"]!="None"),
    "author_type"
] = "Cust"


# In[120]:


for col in df2.columns:
    if pd.api.types.is_datetime64tz_dtype(df2[col]):
        df2[col] = df2[col].dt.tz_localize(None)

df2.to_excel("check2.xlsx")


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[121]:


df1=df2


# In[122]:


summary = (
    df1.groupby(["Bank_Name","Name_of_bank"])
    .agg(
        totl_nbr_post=("Bank_Name", "size"),
        post_by_bank=("author_type", lambda x: (x == "Bank").sum()),
        post_by_Customers=("author_type", lambda x: (x == "Cust").sum()),

        total_likes_for_Bank_Posts=("likes", lambda x: x[df1.loc[x.index, "author_type"] == "Bank"].sum()),
        total_comments_for_Bank_Posts=("comments", lambda x: x[df1.loc[x.index, "author_type"] == "Bank"].sum()),
        total_reposts_for_Bank_Posts=("retweets", lambda x: x[df1.loc[x.index, "author_type"] == "Bank"].sum()),
        
        total_likes_for_Customer_Posts=("likes", lambda x: x[df1.loc[x.index, "author_type"] == "Cust"].sum()),
        total_comments_for_Customer_Posts=("comments", lambda x: x[df1.loc[x.index, "author_type"] == "Cust"].sum()),
        total_reposts_for_Customer_Posts=("retweets", lambda x: x[df1.loc[x.index, "author_type"] == "Cust"].sum()),
        
        min_post_date=("uae_time", "min"),
        max_post_date=("uae_time", "max"),

        neg_post_count=("Sentiment", lambda x: (x == "Negative").sum()),
    #     neg_post_count= (
    #     "Sentiment",
    #     lambda x: ((x == "Negative") & (df1.loc[x.index, "bank_count"].isin([1, 2]))).sum()
    # ),

        # ✅ NEW: count of Bank posts with likes > 100
         bank_posts_likes_gt_100=(
            "likes",
            lambda x: ((df1.loc[x.index, "author_type"] == "Bank") & (x > 100)).sum()
        )

    )
    .reset_index()
    .sort_values("Bank_Name")  # order by Bank_Name ascending
)


# In[123]:





# In[124]:


post_from_dates = summary["min_post_date"].dropna().unique()
post_to_dates = summary["max_post_date"].dropna().unique()
if len(post_from_dates) > 0 and len(post_to_dates) > 0:
    print(f"\n X post Analysis from {post_from_dates[0]} to {post_to_dates[0]}")
#print(f"\n X post Analysis from {post_from_dates[0]} to {post_to_dates[0]}")


# In[125]:


df1['post_date']=df1["uae_time"]


# In[126]:


df1["weekday_name"] = df1["post_date"].dt.day_name()


# In[127]:


#fatihtahta/reddit-scraper-search-fast


# In[128]:


bank_df=df1[df1['author_type']=='Bank']


# In[129]:


#bank_df


# In[130]:


weekday_bank_counts = bank_df.groupby(
    ["weekday_name", "Bank_Name","Name_of_bank"]
).size().reset_index(name="post_count")


# In[131]:





# In[132]:


######html
import datetime


# In[133]:


today_date = datetime.datetime.today().strftime("%d%b%Y").upper()


# In[134]:


base_name="X-post_analysis"


# In[135]:


OUT_PATH = f"{base_name}.html"


# In[136]:


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


# In[137]:


df = df1.copy()


# In[138]:


df['Sentiment']=df['Sentiment'].str.lower()


# In[139]:


# ensure types
df["post_date"] = pd.to_datetime(df["post_date"], errors="coerce")
for c in ["likes", "comments", "retweets"]:
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)


# In[140]:


# split masks
is_bank_authored = df["author_type"].str.startswith("Bank", na=False)


# In[141]:


# Sort first by Bank_Name then likes descending
df = df.sort_values(['Bank_Name', 'likes'], ascending=[True, False])


# In[142]:


def rank_top3(d):
    # sort by engagement desc, then most recent date
    return d.sort_values(["likes", "post_date"], ascending=[False, False]).head(5)


# In[143]:


# per-bank top 3
top3_bank_authored = (
    df[is_bank_authored]
    .groupby("Bank_Name", group_keys=False)
    .apply(rank_top3)
)


# In[144]:



top3_customer_authored = (
    df[~is_bank_authored]
    .groupby("Bank_Name", group_keys=False)
    .apply(rank_top3)
)


# In[145]:


# (optional) merge both into one dict for rendering
per_bank = {}
for bank in sorted(df["Bank_Name"].dropna().astype(str).unique()):
    per_bank[bank] = {
        "bank": top3_bank_authored[top3_bank_authored["Bank_Name"] == bank],
        "customer": top3_customer_authored[top3_customer_authored["Bank_Name"] == bank],
    }


# In[146]:


neg_table=df1[(df1['Sentiment'] == 'Negative')]


# In[147]:


#neg_table.head()
len(neg_table)


# In[148]:


df1.to_csv("twitt.csv",index=0)


# In[149]:


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


# In[150]:


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


# In[151]:


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


# In[152]:


def render_badge(sent):
    s = (sent or "").strip().lower()
    if s == "positive": return '<span class="badge badge-pos">Positive</span>'
    if s == "negative": return '<span class="badge badge-neg">Negative</span>'
    return '<span class="badge badge-neu">Neutral</span>'


# In[153]:


# ---------- validations & inputs ----------
def must_have(df, name):
    if name not in globals():
        raise ValueError(f"Missing DataFrame: `{name}`")
    obj = globals()[name]
    if not isinstance(obj, pd.DataFrame) or obj.empty:
        raise ValueError(f"`{name}` must be a non-empty pandas DataFrame.")
    return obj


# In[154]:


summary_df = must_have(summary, "summary")
neg_df     = must_have(neg_table, "neg_table")
bank3     = must_have(top3_bank_authored, "top3_bank_authored")
cust3     = must_have(top3_customer_authored, "top3_customer_authored")


# In[155]:





# In[156]:



# ---------- Part 1 (summary-only) ----------
# Ensure dtypes
for c in ["Nbr_followers","totl_nbr_post","post_by_bank","post_by_Customers","comments","likes","retweets"]:
    if c in summary_df.columns:
        summary_df[c] = pd.to_numeric(summary_df[c], errors="coerce")
for c in ["min_post_date","max_post_date"]:
    if c in summary_df.columns:
        summary_df[c] = pd.to_datetime(summary_df[c], errors="coerce")
current_time = datetime.datetime.now(pytz.timezone("Asia/Dubai"))
#duration_text = f"{fmt_date9(summary_df['min_post_date'].min())} to {fmt_dt_full(summary_df['max_post_date'].max())}"
#duration_text = f"{fmt_date9(summary_df['min_post_date'].min())} to {fmt_dt_full(datetime.datetime.now())}"
duration_text = f"{fmt_date9(summary_df['min_post_date'].min())} to {fmt_dt_full(current_time)}"

# Table (exact columns from summary)
part1_cols = {
    "Name_of_bank": "Bank Name",
    #"Nbr_followers": "Number of Followers",
    #"totl_nbr_post": "Number of Posts Considered",
    "post_by_bank": "Posts By Bank",
    #"post_by_Customers": "Posts By Customer",
    "total_likes_for_Bank_Posts": "Total likes for Bank Posts",
    "total_comments_for_Bank_Posts": "Total comments for Bank Posts",
    "total_reposts_for_Bank_Posts": "Total retweets for Bank Posts",

    #"total_likes_for_Customer_Posts": "Total likes for Cust Posts",
    #"total_comments_for_Customer_Posts": "Total comments for Cust Posts",
    #"total_reposts_for_Customer_Posts": "Total reposts for Cust Posts",
    "bank_posts_likes_gt_100": "Bank posts with 100+ likes",
    #"neg_post_count": "Negative Post"

    
}



# In[157]:





# In[158]:


missing = [k for k in part1_cols.keys() if k not in summary_df.columns]
if missing:
    raise ValueError(f"`summary` missing columns: {missing}")
summary_tbl = summary_df[list(part1_cols.keys())].rename(columns=part1_cols)


# In[159]:




# In[160]:


#summary_df


# In[161]:



# Table (exact columns from summary)
part2_cols = {
    "Name_of_bank": "Bank Name",
    #"Nbr_followers": "Number of Followers",
    #"totl_nbr_post": "Number of Posts Considered",
    #"post_by_bank": "Posts By Bank",
    "post_by_Customers": "Customer's post Mentioning Bank",
    "neg_post_count": "Negative Post",
    # "total_likes_for_Bank_Posts": "Total likes for Bank Posts",
    # "total_comments_for_Bank_Posts": "Total comments for Bank Posts",
    # "total_reposts_for_Bank_Posts": "Total reposts for Bank Posts",

    "total_likes_for_Customer_Posts": "Total likes for Cust Posts",
    "total_comments_for_Customer_Posts": "Total comments for Cust Posts",
    "total_reposts_for_Customer_Posts": "Total retwite for Cust Posts",
    #"bank_posts_likes_gt_100": "Bank posts with 100+ likes",
    

    
}


# In[162]:


missing = [k for k in part1_cols.keys() if k not in summary_df.columns]
if missing:
    raise ValueError(f"`summary` missing columns: {missing}")
summary_tbl_2 = summary_df[list(part2_cols.keys())].rename(columns=part2_cols)


# In[163]:





# In[164]:


# Sort weekdays in natural order (Mon–Sun)
weekday_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
weekday_bank_counts["weekday"] = pd.Categorical(weekday_bank_counts["weekday_name"], categories=weekday_order, ordered=True)

pivot_df = weekday_bank_counts.pivot(index="weekday", columns="Name_of_bank", values="post_count").fillna(0)


# In[165]:


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
    #"#ED1C24",  # ADCB
    #"#009ada",  # ADIB
    #"#2ca02c",  # CDB
    "#8C4799",  # EIB
    "#072447",  # ENBD
    "#003399",  # FAB
    "#FF671F",  # Mashreq
    "#bcbd22",  # olive
    "#17becf"   # cyan
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


# In[166]:


# (Optional) also save a physical PNG next to the HTML if you want to attach it separately
with open("weekday_bank_posts.png", "wb") as f:
    f.write(base64.b64decode(png_b64))


# In[167]:


# ---------- Part 2 (negative cards from neg_table) ----------
neg_tmp = neg_df.copy()
# Make sure date is datetime
if "post_date" in neg_tmp.columns:
    neg_tmp["post_date"] = pd.to_datetime(neg_tmp["post_date"], errors="coerce")
# Order by bank, recency
order_cols = ["Bank_Name"] + (["post_date"] if "post_date" in neg_tmp.columns else [])
neg_tmp = neg_tmp.sort_values(order_cols, ascending=[True, False] if len(order_cols)>1 else [True])


# In[168]:


# ---------- Part 3 (side-by-side top-3 from top3 tables) ----------
# Ensure datetime for sorting (won't recompute ranks, just display)
for df_ in (bank3, cust3):
    if "post_date" in df_.columns:
        df_["post_date"] = pd.to_datetime(df_["post_date"], errors="coerce")


# In[169]:


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


# In[170]:


def embed_data_url(path):
    ext = os.path.splitext(path)[1].lower()
    mime = "image/png" if ext == ".png" else "image/jpeg"
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"

# point to your actual file (exact filename & case!)
logo_data_url = embed_data_url("enbd_logo.png")


# In[171]:


header = f"""
<div class="hero-grid">
  <div class="hero-logo">
    <img src="{logo_data_url}" alt="ENBD" class="logo">
  </div>

  <div class="genai-badge">⚡Powered by Gen AI</div>

  <div class="hero-row r1">
    <h1>Twitter Engagement Analysis for Banks</h1>
  </div>

  <div class="hero-row r2">
    <p>Duration: {fmt_date9(summary_df['min_post_date'].min())} to {fmt_dt_full(current_time)}</p>
  </div>

  <div class="hero-row r3">
    <p>Number of Banks considered for Analysis: {summary_df['Name_of_bank'].nunique()}</p>
  </div>
</div>
"""


# In[172]:



# ---------- Part 1 ----------
part1_table_html = summary_tbl.to_html(index=False, classes="summary-table", escape=False)
part1_html = f"""
<div class="section">
  <h2>Part 1: Bank Posts Overview</h2>
 
  {part1_table_html}
</div>
"""


# In[173]:





# In[174]:


# ---------- Part 1 ----------
part11_table_html = summary_tbl_2.to_html(index=False, classes="summary-table", escape=False)
part11_html = f"""
<div class="section">
  <h2>Part 2: Customer's mentioned Bank Posts Overview</h2>
 
  {part11_table_html}
</div>
"""


# In[175]:


# right after you build neg_tmp (and similarly for bank3, cust3 if needed)
for c in ['total_likes_for_Bank_Posts',
       'total_comments_for_Bank_Posts', 'total_reposts_for_Bank_Posts',
       'total_likes_for_Customer_Posts', 'total_comments_for_Customer_Posts',
       'total_reposts_for_Customer_Posts']:
    if c in neg_tmp.columns:
        neg_tmp[c] = pd.to_numeric(neg_tmp[c], errors="coerce").fillna(0).astype(int)


# In[176]:


def _as_int(x):
    # converts to int, treating NaN/None/"" as 0
    try:
        v = pd.to_numeric(x, errors="coerce")
        if pd.isna(v):
            return 0
        return int(v)
    except Exception:
        return 0


# In[177]:


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


# In[ ]:





# In[178]:




# In[179]:


def render_card(row, variant="normal"):
    # white info, summary first; red background variant for negatives
    is_bank = str(row.get("author_type","")).lower().startswith("from")
    cls = "post post-neg" if variant == "Neg" else "post"
    post_text = row.get('post_highlights','') or ""
    # get three highlights (list[str])
    hl = row_highlights(row)

             # --- NEW: post link (permalink) ---
    post_link = (row.get("permalink") or "").strip()
    # if isinstance(links, list) and links:
    #     post_link = links[0]
    # elif isinstance(links, str):
    #     post_link = links.strip()
    # else:
    #     post_link = ""
    if post_link:
        link_html = f"""
        <div class="post-link">
          <a href="{html.escape(post_link)}" target="_blank" rel="noopener noreferrer">
            🔗 X post
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
        <div class="chip">🔁 etweets: {_as_int(row.get('retweets'))}</div>
      </div>
        {link_html}
    </div>
    """


# In[180]:


def render_card_neg(row, variant="normal"):
    # white info, summary first; red background variant for negatives
    is_bank = str(row.get("author_type","")).lower().startswith("from")
    cls = "post post-neg" if variant == "Neg" else "post"
    post_text = row.get('post_highlights','') or ""
    # get three highlights (list[str])
    hl = row_highlights(row)
    
    # --- NEW: post link (permalink) ---
    #post_link = (row.get("links") or "").strip()
    post_link = (row.get("permalink") or "").strip()
    # links = row.get("links")
    # if isinstance(links, list) and links:
    #     post_link = links[0]
    # elif isinstance(links, str):
    #     post_link = links.strip()
    # else:
    #     post_link = ""
    
    if post_link:
        link_html = f"""
        <div class="post-link">
          <a href="{html.escape(post_link)}" target="_blank" rel="noopener noreferrer">
            🔗 X post
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
        <div class="chip">🔁 Retweets: {_as_int(row.get('retweets'))}</div>
      </div>
       {link_html}
    </div>
    """


# In[181]:




# In[182]:


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


# In[183]:


combined = (
    pd.concat([bank3[["Name_of_bank", "Bank_Name"]], 
               cust3[["Name_of_bank", "Bank_Name"]]])
      .dropna(subset=["Name_of_bank"])
      .drop_duplicates(subset=["Name_of_bank"])
      .sort_values(by="Bank_Name", ascending=True)
)


# In[184]:


banks=combined['Name_of_bank'].to_list()


# In[185]:



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


# In[186]:


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
<head><meta charset="utf-8" /><title>Twitter Analysis</title>{css}</head>
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


# In[187]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[188]:


#fatihtahta/reddit-scraper-search-fast

