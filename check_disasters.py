"""
Merge EM-DAT data into disasters table
"""
import psycopg2

DATABASE_URL = "postgresql://postgres.jtyykeaeupxbbkaqkfqp:CodeNess6504@aws-0-us-west-2.pooler.supabase.com:6543/postgres"

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Check disasters table structure
print("=== DISASTERS TABLE STRUCTURE ===")
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'disasters'
    ORDER BY ordinal_position
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

# Check sample data
print("\n=== SAMPLE DATA ===")
cur.execute("SELECT * FROM disasters LIMIT 5")
cols = [desc[0] for desc in cur.description]
print(f"Columns: {cols}")
for row in cur.fetchall():
    print(row)

# Count existing records
cur.execute("SELECT COUNT(*) FROM disasters")
print(f"\nTotal records: {cur.fetchone()[0]}")

cur.close()
conn.close()
