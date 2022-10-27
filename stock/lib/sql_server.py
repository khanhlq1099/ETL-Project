from pyodbc import connect, Connection, Cursor


def open_session(connection_string: str):
    conn = connect(connection_string)
    cursor = conn.cursor()
    return conn, cursor


def close_session(conn: Connection, cursor: Cursor):
    cursor.close()
    conn.close()
