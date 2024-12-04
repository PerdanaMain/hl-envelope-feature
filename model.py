from config import Config

def get_parts():
    try:
        conn = Config.get_connection()
        cur = conn.cursor()

        query = "SELECT id, web_id, type_id, part_name  FROM pf_parts WHERE type_id = '673b26b9-fb94-40aa-8c33-ccea214c0ef3'"

        cur.execute(query)
        parts = cur.fetchall()
        return parts
    except Exception as e:
        print(f'An exception occurred: {e}')
    finally:
        if conn:
            conn.close()