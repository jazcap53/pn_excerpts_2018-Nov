# file: import_and_add_subscribers.py
# andrew jarcho
# 2018-07-03

import os
import sys
import argparse
from mailchimp3 import MailChimp
import psycopg2


class ImportAndAddSubscribers:
    """
    Import subscriber emails from a test db and upsert them to
    a MailChimp List
    """
    def __init__(self):
        """
        Called by: main()
        """
        self.pg_host = ''
        self.pg_name = ''
        self.pg_test_name = ''
        self.pg_user = ''
        self.pg_passwd = ''
        self.pg_conn = None
        self.chimpkey = ''
        self.mc_client = None
        self.list_id = ''

    def get_c_l_args(self, argv=None):
        """
        Get command line arguments
        Called by: main()
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("list_id", help='A MailChimp List ID')
        args = parser.parse_args(argv)
        self.list_id = args.list_id

    def get_env_vars(self):
        """
        Get environment variables
        CHIMPKEY is set in a separate file from the other env vars
        Called by: main()
        """
        try:
            self.pg_host = os.environ['DBHOST']
            self.pg_test_name = os.environ['DBTESTNAME']
            self.pg_user = os.environ['DBUSER']
            self.pg_passwd = os.environ['DBPASSWD']
        except KeyError:
            print('Please set environment variables DBHOST, DBTESTNAME, DBUSER, DBPASSWD')
            sys.exit(1)
        try:
            self.chimpkey = os.environ['CHIMPKEY']
        except KeyError:
            print('Please set environment variable CHIMPKEY')
            sys.exit(1)

    def setup_mc_client(self):
        """
        Set up MailChimp client
        :return: None
        Called by: main()
        """
        self.mc_client = MailChimp(self.chimpkey)

    def connect_pg(self):
        """
        Connect to PostgreSQL database
        :return: None
        Called by: main()
        """
        pg_conn_string = "host = '{}' dbname = '{}' user = '{}' password = '{}'".\
            format(self.pg_host, self.pg_test_name, self.pg_user, self.pg_passwd)

        self.pg_conn = psycopg2.connect(pg_conn_string)

    # up to 500 rows will be accepted by MailChimp at a time
    def read_from_pg(self):
        """
        Read data from Postgresql to be upserted to MailChimp List
        :return: The data read, in a format accepted by MailChimp API
        Called by: main()
        """
        chunk_size = 2
        limit = chunk_size
        offset = 0
        query = ("SELECT email_address, status, trial_exp " +
                 "FROM aj_contact_list " +
                 "ORDER BY email_address LIMIT %s OFFSET %s;")
        while True:
            data = (limit, offset)
            pg_cur = self.pg_conn.cursor()
            pg_cur.execute(query, data)
            members_list = []
            for record in pg_cur:
                list_item = {'email_address': record[0], 'status': record[1],
                             'merge_fields': {'TRIALEXP': record[2].strftime('%Y-%m-%d')}}
                members_list.append(list_item)
            pg_cur.close()
            if not members_list:
                break
            pg_data_dict = {'members': members_list, 'update_existing': True}
            yield pg_data_dict
            offset += chunk_size

    def disconnect_pg(self):
        """
        Close the connection to PostgreSQL
        :return: None
        Called by: main()
        """
        self.pg_conn.close()

    def teardown_mc_client(self):
        """
        Tear down MailChimp client
        :return: None
        Called by: main()
        """
        self.mc_client = None

    def main(self):
        """
        Call functions to upsert MailChimp List with data read from PostgreSQL
        :return: None
        Called by: client code
        """
        self.get_c_l_args()
        self.get_env_vars()
        self.setup_mc_client()
        self.connect_pg()
        pg_data_iter = self.read_from_pg()
        ttl_created = 0
        ttl_updated = 0
        ttl_errors = 0
        while True:
            try:
                item = next(pg_data_iter)
            except StopIteration:
                break
            response = self.mc_client.lists.update_members(self.list_id, item)
            ttl_created += response['total_created']
            ttl_updated += response['total_updated']
            ttl_errors += response['error_count']
        self.disconnect_pg()
        self.teardown_mc_client()
        print('Total created: {}'.format(ttl_created))
        print('Total updated: {}'.format(ttl_updated))
        print('Total errors: {}'.format(ttl_errors))


if __name__ == '__main__':
    iaas = ImportAndAddSubscribers()
    iaas.main()


# works
