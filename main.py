import psycopg2

def create_tables(conn):
    with conn.cursor() as cur:
        # создание таблиц
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(40),
            last_name VARCHAR(40),
            email VARCHAR(40) UNIQUE);
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone(
            id SERIAL PRIMARY KEY,
            client_id INTEGER NOT NULL REFERENCES client(id),
            phone_number VARCHAR(20) UNIQUE);
        """)
    conn.commit() 

def drop_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            DROP TABLE IF EXISTS phone;
            DROP TABLE IF EXISTS client;
            """
        )
    conn.commit()

def add_phone(conn, client_id, phone):
    phone_id = add_phone_only(conn, client_id, phone)
    print(f'К клиенту [{client_id}] добавлен телефон {phone} [{phone_id}]') 

def add_phone_only(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO phone(client_id, phone_number)
                VALUES(%s, %s) RETURNING id ;	
            """, (client_id, phone)
        )
        phone_id = cur.fetchone()[0]
    return phone_id

def add_client_only(conn,first_name, last_name, email):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO client(first_name, last_name, email)
                VALUES(%s,%s,%s) RETURNING id; 
            """, (first_name, last_name, email)
        )
        client_id = cur.fetchone()[0]
    return client_id
    
def add_client(conn, first_name, last_name, email, phones=None ):
        client_id = add_client_only(conn,first_name, last_name, email)
        print(f'Клиент {first_name} {last_name} добавлен [{client_id}]')
        if phones != None:
            for phone in phones:
                phone_id = add_phone_only(conn, client_id, phone)
                print(f'- телефон {phone} добавлен [{phone_id}]')


def delete_phones(conn, client_id):
    with conn.cursor() as cur:
        cur.execute(
                    """
                    DELETE FROM phone  
                    WHERE client_id = %s;
                    """, (client_id,)
                )
    conn.commit()
        
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    print(f"Обновление клиента [{client_id}]")
    with conn.cursor() as cur:
        if first_name !=None:
            cur.execute(
                """
                UPDATE client SET first_name=%s WHERE id=%s
                RETURNING id
                """,(first_name, client_id)
                )
            print(f'- first_name обнавлено => {first_name} [{cur.fetchone()[0]}]')

        if last_name !=None:
            cur.execute(
                """
                UPDATE client SET last_name=%s WHERE id=%s
                RETURNING id
                """,(last_name, client_id)
                )
            print(f'- last_name обнавлено => {last_name} [{cur.fetchone()[0]}]')

        if email !=None:
            cur.execute(
                """
                UPDATE client SET email=%s WHERE id=%s
                RETURNING id
                """,(email, client_id)
                )
            print(f'- email обнавлено => {email} [{cur.fetchone()[0]}]')

        if phones != None:
            delete_phones(conn, client_id)
            for phone in phones:
                phone_id = add_phone_only(conn, client_id, phone)
                print(f'- телефон обнавлен => {phone} [{phone_id}]')
        
def delete_client(conn, client_id):
    delete_phones(conn, client_id)
    with conn.cursor() as cur:
        cur.execute(
                """
                DELETE FROM client  
                WHERE id = %s;
                """, (client_id,)
                )
    conn.commit()
    print(f"Клиент [{client_id}] удален")

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        if phone != None:
            cur.execute(
                """
                SELECT c.id, c.first_name, c.last_name, c.email FROM client c
                JOIN phone p ON p.client_id = c.id
                WHERE ph.phone=%s;
                """, (phone,))
        else:
            cur.execute(
                """
                SELECT id, first_name, last_name, email FROM client
                WHERE first_name=%s OR last_name=%s OR email=%s;
                """, (first_name, last_name, email))
        clients = cur.fetchall()
        print("Результаты поиска: ")
        for client in clients:
            print(f'{client[1]} {client[2]} | {client[3]} [{client[0]}]')
            cur.execute(
                """
                SELECT phone_number FROM phone
                WHERE client_id=%s;
                """, (client[0],))
            phones =  cur.fetchall()
            if phones != []:
                for phone in phones:
                    print(f'- {phone[0]}')   
            

with psycopg2.connect(database="clients_db", user="postgres", password="smog1718") as conn:
    drop_tables(conn)
    create_tables(conn)
    add_client(conn,"Иван","Фёдорович","misha_fedr@qwerty.com")
    add_client(conn,"Михаил","Мирошников","mirrosha@qwerty.com")
    add_client(conn,"Иван","Мирошников","falcon@qwerty.com", ['+7-961-075-8945','+7-901-000-8970'])
    add_client(conn,"Хан","Батый","han@orda.com", ['+7-961-669-8317','+7-961-666-8357'])
    add_phone(conn, 2, '+7-901-999-0001')
    change_client(conn, 2, "Иван","Калита","1288@moscow.ru", ['+7-495-000-0001'])
    delete_client(conn, 2)
    find_client(conn, "Иван")