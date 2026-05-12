import sqlite3, pickle, sys

conn = sqlite3.connect('hn_async.db')
conn.row_factory = sqlite3.Row
rows = conn.execute("SELECT * FROM items WHERE type = 'story' LIMIT 50").fetchall()
conn.close()

stories = [dict(r) for r in rows]
print(f"Loaded {len(stories)} stories")

# Check sizes
data = pickle.dumps(stories)
print(f"Pickled size: {len(data)} bytes ({len(data)/1024:.1f} KB)")

# Check individual story
s = stories[0]
print(f"Sample keys: {list(s.keys())}")
print(f"kids type: {type(s.get('kids'))}, len: {len(str(s.get('kids')))}")
