import psycopg2
conn = psycopg2.connect('postgresql://postgres.jtyykeaeupxbbkaqkfqp:CodeNess6504@aws-0-us-west-2.pooler.supabase.com:6543/postgres')
cur = conn.cursor()
cur.execute('SELECT count(*) FROM disasters')
print('Connection successful! Disasters count:', cur.fetchone()[0])
conn.close()
