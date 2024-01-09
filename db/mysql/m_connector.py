import os
import pretty_errors
from logging_config import logger as logging
from dotenv import load_dotenv
import mysql.connector
import paramiko

load_dotenv()

MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")


# SSH Tunnel configuration
MYSQL_SSH_HOST = os.getenv("MYSQL_SSH_HOST")
MYSQL_SSH_PORT = os.getenv("MYSQL_SSH_PORT")
MYSQL_SSH_USERNAME = os.getenv("MYSQL_SSH_USERNAME")
MYSQL_SSH_PASSWORD = os.getenv("MYSQL_SSH_PASSWORD")


def create_ssh_tunnel():
    """
    Create an SSH tunnel to the MySQL server
    """
    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        MYSQL_SSH_HOST,
        MYSQL_SSH_PORT,
        MYSQL_SSH_USERNAME,
        MYSQL_SSH_PASSWORD
    )

    # Choose a local port for the tunnel
    local_port = 3306

    # Create an SSH tunnel to the MySQL server
    remote_bind_address = (MYSQL_HOST, int(MYSQL_PORT))
    ssh_tunnel = ssh_client.get_transport().open_channel(
        'direct-tcpip', ('127.0.0.1', local_port), remote_bind_address
    )

    # Close the SSH connection (tunnel remains open)
    ssh_client.close()

    return local_port


def mysql_connector(func):
    """
    decorator that connects to MySQL DB and executes inputted query handler; handles exceptions
    """
    def inner(*args, **kwargs):

        # SSH connection
        # local_port = create_ssh_tunnel()

        # connection = mysql.connector.connect(
        #     host='127.0.0.1',  # Localhost (through the tunnel)
        #     port=local_port,
        #     user=MYSQL_USER,
        #     password=MYSQL_PASSWORD,
        #     database=MYSQL_DB
        # )

        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        logging.info("MYSQL CONNECTED")
        try:
            return func(connection, *args, **kwargs)
        except Exception as ex:
            logging.error(f"\n!!! MYSQL ERROR -- {ex}\n")
        finally:
            connection.close()
            logging.info("MYSQL DISCONNECTED")

    return inner


# mysql_connector demo use case
@mysql_connector
def mysql_demo_query(connector):
    curr = connector.cursor()
    curr.execute("SELECT * FROM main.demo")
    data = curr.fetchall()
    curr.close()
    logging.debug(data)
    return data


if __name__ == "__main__":
    ...
