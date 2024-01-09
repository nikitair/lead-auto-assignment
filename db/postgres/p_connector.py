import os
from logging_config import logger as  logging
import pretty_errors
from dotenv import load_dotenv
import psycopg2
import paramiko

load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# SSH Tunnel configuration
POSTGRES_SSH_HOST = os.getenv("POSTGRES_SSH_HOST")
POSTGRES_SSH_PORT = os.getenv("POSTGRES_SSH_PORT")
POSTGRES_SSH_USERNAME = os.getenv("POSTGRES_SSH_USERNAME")
POSTGRES_SSH_PASSWORD = os.getenv("POSTGRES_SSH_PASSWORD")


# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s - %(levelname)s - %(message)s')

def create_ssh_tunnel():
    """
    Create an SSH tunnel to the PostgreSQL server
    """
    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        POSTGRES_SSH_HOST,
        POSTGRES_SSH_PORT,
        POSTGRES_SSH_USERNAME,
        POSTGRES_SSH_PASSWORD
    )

    # Choose a local port for the tunnel
    local_port = 5432

    # Create an SSH tunnel to the PostgreSQL server
    remote_bind_address = (POSTGRES_HOST, int(POSTGRES_PORT))
    ssh_tunnel = ssh_client.get_transport().open_channel(
        'direct-tcpip', ('127.0.0.1', local_port), remote_bind_address
    )

    # Close the SSH connection (tunnel remains open)
    ssh_client.close()

    return local_port


def postgres_connector(func):
    """
    decorator that connects to Postgres DB and executes inputted query handler; handles exceptions
    """
    def inner(*args, **kwargs):

        # SSH connection
        # local_port = create_ssh_tunnel() 
        # conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER,
        #                         password=POSTGRES_PASSWORD, host='127.0.0.1', port=local_port)
        

        # usual connection
        conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER,
                                password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
        

        logging.info("POSTGRES CONNECTED")
        try:
            return func(conn, *args, **kwargs)
        except Exception as ex:
            logging.error(f"\n!!! POSTGRES ERROR -- {ex}\n")
        finally:
            conn.close()
            logging.info("POSTGRES DISCONNECTED")

    return inner


# demo postgres_connector decorator use case
@postgres_connector
def postgres_demo_query(connector):
    curr = connector.cursor()
    curr.execute("SELECT * FROM demo")
    data = curr.fetchall()
    curr.close()
    logging.debug(data)
    return data


if __name__ == "__main__":
    ...
