import os, psycopg, sys
def main():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL no definido", file=sys.stderr); sys.exit(1)
    sql = open(os.path.join(os.path.dirname(__file__),'seed.sql'), 'r', encoding='utf-8').read()
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    print("Seed OK -> public.customers_demo creada")
if __name__ == '__main__': main()
