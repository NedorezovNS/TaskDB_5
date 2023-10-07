import psycopg2


def create_base(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE phone_base;
        DROP TABLE client_base;
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS client_base(
        client_id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        surname VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL UNIQUE);
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone_base(
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES client_base(client_id),
        phone VARCHAR(12) UNIQUE);
        """)
    pass


def add_client(conn, name, surname, email, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO client_base(name, surname, email)
        VALUES (%s, %s, %s)
        RETURNING client_id, name, surname, email;
        """, (name, surname, email))
        return print(cur.fetchone())


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phone_base(client_id, phone)
        VALUES(%s, %s)
        RETURNING client_id, phone;
        """, (client_id, phone))
        return print(cur.fetchone())


def change_client(conn, client_id, name=None, surname=None, email=None):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE client_base
        SET name=%s, surname=%s, email=%s
        WHERE client_id=%s;
        """, (name, surname, email, client_id))
        conn.commit()
    pass


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone_base
        WHERE client_id=%s AND phone=%s;
        """, (client_id, phone))
        conn.commit()
    pass


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone_base
        WHERE client_id=%s
        """, (client_id,))

        cur.execute("""
        DELETE FROM client_base
        WHERE client_id=%s
        """, (client_id,))
    pass


def find_client(conn, name=None, surname=None, email=None, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT cb.name, cb.surname, cb.email, pb.phone FROM client_base cb
        JOIN phone_base pb ON cb.client_id=pb.client_id
        WHERE (name = %(name)s) OR (surname = %(surname)s)
        OR (email = %(email)s) OR (phone = %(phone)s);
        """, {'name': name, 'surname': surname, 'email': email, 'phone': phone})
        return print(cur.fetchall())
    pass


def show_all(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT cb.client_id, cb.name, cb.surname, cb.email, pb.phone FROM client_base cb
        JOIN phone_base pb ON pb.client_id=cb.client_id;
        """)
        return print(cur.fetchall())


with psycopg2.connect(database="ClientDataBase", user="postgres", password="130814") as conn:
    create_base(conn)

    add_client(conn, 'Александр', 'Петров', 'SashaP@ya.ru')
    add_client(conn, 'Светлана', 'Пупкова', 'SvetaPup@gmail.com')
    add_client(conn, 'Андрей', 'Духовкин', 'Andreeeeeey@mail.ru')

    add_phone(conn, 1, '87773210055')
    add_phone(conn, 1, '88885558899')
    add_phone(conn, 2, '84447857879')
    add_phone(conn, 3, '88005553535')

    change_client(conn, 1, 'Олег', 'Васильев', 'OlegV@gmail.com')

    delete_phone(conn, 1, '87773210055')

    delete_client(conn, 2)

    find_client(conn, 'Олег')

    show_all(conn)  # Вывод имеющихся данных после всех изменений.
    pass
conn.close()