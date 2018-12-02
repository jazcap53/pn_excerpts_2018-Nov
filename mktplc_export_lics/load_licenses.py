#!/usr/bin/env python3.6


# file: load_licenses.py
# andrew jarcho
# 2018-04-24


import json
import sys
import argparse
import os
import psycopg2
import requests
import datetime

try:
    from src.time_string_conversion import get_now
except ModuleNotFoundError:
    from time_string_conversion import get_now

try:
    from mktplc_export_lics.src.constants import BASE_URL, DEFAULT_DATE
except ModuleNotFoundError:
    from constants import BASE_URL, DEFAULT_DATE


class LoadLicenses:
    """
    Gets licenses from Marketplace APIs 'Export licenses' endpoint.

    Stores the licenses in PostgreSQL db.

    Endpoint returns only records altered on or after the --modified_date
    c.l.a., if present. If modified_date is None, 'Export licenses' returns
    all records.

    If a record returned from API is already present in the Postgres db,
    and is not identical to the record in Postgres, Postgres will be updated.
    """

    def __init__(self, api_password=None, vendor_id=None, api_user=None,
                 db_password=None, base_url=BASE_URL,
                 modified_datetime=None, outfile=None):
        self.api_password = api_password
        self.vendor_id = vendor_id
        self.api_user = api_user
        self.base_url = base_url
        self.db_host = None
        self.db_name = None
        self.db_user = None
        self.db_password = db_password
        self.mkt_data = []  # from Marketplace API
        self.outfile = outfile
        self.to_stdout = False
        self.cur_time = get_now()
        self.modified_date = modified_datetime
        self.verbose = 0
        self.lcd_data = []
        self.contacts_key_set = set()
        self.addons_key_set = set()
        self.partner_details_key_set = set()
        self.lcd_key_set = set()
        self.license_key_set = set()
        self.ct_insert_bill_contacts = 0
        self.ct_update_bill_contacts = 0
        self.ct_insert_tech_contacts = 0
        self.ct_update_tech_contacts = 0
        self.ct_insert_addons = 0
        self.ct_update_addons = 0
        self.ct_insert_partner_det = 0
        self.ct_update_partner_det = 0
        self.ct_insert_lcd = 0
        self.ct_update_lcd = 0
        self.ct_insert_license = 0
        self.ct_update_license = 0

    def get_args(self, argv=None):
        """Get command line arguments"""
        parser = argparse.ArgumentParser()
        parser.add_argument('-v', '--verbose', help='send debug info to stdout',
                            action='count', default=0)
        parser.add_argument('-s', '--stdout', help='Send output to stdout.',
                            action='store_true')
        parser.add_argument('-o', '--outfile', type=str,
                            help='Send output to file OUTFILE.')
        parser.add_argument('-m', '--modified_date', type=str,
                            default=None,
                            help='Retrieve only items altered on or '
                                 'after MODIFIED_DATE. When MODIFIED_DATE is '
                                 'None, (the default), retrieve all items. '
                                 'Insert any items which have not been seen before. '
                                 'Update items whose key value '
                                 'already exists in the db, and which have been altered.')
        args = parser.parse_args(argv)
        self.outfile = args.outfile
        self.to_stdout = args.stdout
        self.modified_date = args.modified_date
        self.verbose = args.verbose

    def get_env_vars(self):
        """Check that environment variables have been set"""
        try:
            self.api_password = os.environ['APIPASSWD']
            self.vendor_id = os.environ['VENDORID']
            self.api_user = os.environ['APIUSER']
            self.db_host = os.environ['DBHOST']
            self.db_name = os.environ['DBNAME']
            self.db_user = os.environ['DBUSER']
            self.db_password = os.environ['DBPASSWD']
        except KeyError:
            print('Please set environment variables APIPASSWD, VENDORID, '
                  'APIUSER, DBHOST, DBNAME, DBUSER, DBPASSWD', file=sys.stderr)
            sys.exit(1)

    def get_licenses(self):
        """
        Get licenses from Marketplace API using the 'Export licenses' endpoint.
        :return: JSON response
        Called by: main()
        """
        print('Querying Marketplace \'Export licenses\' endpoint...',
              file=sys.stderr)
        url, user, payload = self.get_request_args()
        return requests.get(url, auth=(user, self.api_password),
                            params=payload)

    def get_request_args(self):
        """
        Constructs arguments for 'requests.get()' in 'get_licenses()'.
        :return: Those arguments.
        Called by: get_licenses()
        """
        url = (self.base_url + '/rest/2/vendors/' + self.vendor_id +
               '/reporting/licenses/export')
        user = self.api_user
        payload = {'lastUpdated': self.modified_date[:10] if
                   self.modified_date else None}
        return url, user, payload

    def print_if_verbose(self, arg, file=sys.stdout):
        """
        Print arg param to file param if self.verbose flag is set.
        :param arg: Text to be printed
        :param file: Stream to print on
        :return: None
        Called by: get_addons_key(), get_billing_contact(), get_lcd_key(),
                   get_license_id(), get_partner_details_key(),
                   get_technical_contact(), handle_mkt_response(),
                   is_lcd_item_duplicate(), make_license_id_insert_list(),
                   store_licenses()
        """
        if self.verbose:
            print(arg, file=file)

    def handle_mkt_response(self, mkt_response):
        """
        Extend self.mkt_data list with mkt_response data as a list of dicts.
        :param mkt_response: licenses read from Marketplace API
        :return: None
        Called by: main()
        """
        num_retrieved = 0
        if mkt_response.ok:
            curr_data = mkt_response.json()
            num_retrieved = len(curr_data)
            self.mkt_data.extend(curr_data)
            self.dump_data()
        else:
            self.print_if_verbose(mkt_response.status_code, file=sys.stderr)
        self.print_if_verbose('{} licenses retrieved'.format(num_retrieved),
                              file=sys.stderr)

    def dump_data(self):
        """
        Dump JSON data retrieved from Marketplace 'Export licenses' endpoint.

        Dumps to stdout and / or file, if these options have been selected.
        :return: None
        Called by: handle_mkt_response()
        """
        if self.to_stdout:
            self.dump_to_stdout()
        if self.outfile:
            self.dump_to_file()

    def dump_to_file(self):
        """
        Dump prettified JSON data to output file
        :return: None
        Called by: dump_data()
        """
        with open(self.outfile, 'w') as of:
            json.dump(self.mkt_data, of, sort_keys=True, indent=4,
                      separators=(',', ': '))
            print(file=of)  # make output match that of dump_to_stdout()

    def dump_to_stdout(self):
        """
        Dump prettified JSON data to stdout
        :return: None
        Called by: dump data()
        """
        print(json.dumps(self.mkt_data, sort_keys=True, indent=4,
                         separators=(',', ': ')))

    def store_licenses(self):
        """
        Make connection to Postgres; call fns to load data into tables
        :return: None
        Called by: main()
        """
        self.print_if_verbose('Storing Marketplace license data in db...',
                              file=sys.stderr)

        pn_conn_string = ("host = '{}' dbname = '{}' user = '{}' " +
                          "password = '{}'").format(self.db_host, self.db_name,
                                                    self.db_user,
                                                    self.db_password)
        pn_conn = psycopg2.connect(pn_conn_string)

        # the following will let us tell if an item has already been seen
        pn_cursor = pn_conn.cursor()
        self.get_primary_key_sets(pn_cursor)
        pn_cursor.close()

        self.fill_pn_tables(pn_conn)

    def fill_pn_tables(self, pn_conn):
        """
        Call functions to load data into each Postgre table.
        The pn_contacts, pn_addons, and pn_partner_details tables
        must be loaded first as their UUIDs are needed to load data
        into other tables.
        :param pn_conn:
        :return: None
        Called by: store_licenses()
        """
        pn_cursor = pn_conn.cursor()
        for ix in range(len(self.mkt_data)):
            self.get_billing_contact(pn_cursor, ix)
            self.get_technical_contact(pn_cursor, ix)
            self.get_addons_key(pn_cursor, ix)
            self.get_partner_details_key(pn_cursor, ix)
        pn_conn.commit()
        pn_cursor.close()

        # Next load data for pn_license_contact_details.
        pn_cursor = pn_conn.cursor()
        for ix in range(len(self.mkt_data)):
            self.get_lcd_key(pn_cursor, ix)
        pn_conn.commit()
        pn_cursor.close()

        # Finally load data for pn_licenses.
        pn_cursor = pn_conn.cursor()
        for ix in range(len(self.mkt_data)):
            self.get_license_id(pn_cursor, ix)
        pn_conn.commit()
        pn_cursor.close()

        pn_conn.close()
        self.print_if_verbose('License data stored', file=sys.stderr)

    def get_primary_key_sets(self, pn_cursor):
        """
        Get a set of primary key values from each table.
        These sets will be empty after the first
            run-thru (with empty Postgre db).
        They will be used in subsequent runs to determine whether a
            data item has already been seen.
        :param pn_cursor: on conn to Postgre db
        :return: None
        Called by: store_licenses()
        """
        get_license_key_query = "SELECT license_id FROM pn_licenses;"
        pn_cursor.execute(get_license_key_query)
        self.license_key_set = set(item[0] for item in pn_cursor.fetchall())

        get_lcd_key_query = ("SELECT company, country, " +
                             "region, bill_contact_id, tech_contact_id FROM " +
                             "pn_license_contact_details;")
        pn_cursor.execute(get_lcd_key_query)
        for item in pn_cursor.fetchall():
            self.lcd_key_set.add(item[:3])

        get_partner_details_key_query = "SELECT name FROM pn_partner_details;"
        pn_cursor.execute(get_partner_details_key_query)
        self.partner_details_key_set = set(item[0] for item in pn_cursor.fetchall())

        # No pn_organizations key table yet. pn_organizations is empty now.

        get_addons_key_query = "SELECT key FROM pn_addons;"
        pn_cursor.execute(get_addons_key_query)
        self.addons_key_set = set(item[0] for item in pn_cursor.fetchall())

        get_contacts_key_query = "SELECT email FROM pn_contacts;"
        pn_cursor.execute(get_contacts_key_query)
        self.contacts_key_set = set(item[0] for item in pn_cursor.fetchall())

    def get_billing_contact(self, pn_cursor, ix):
        """
        Get a billing contact from Mktplc to insert or update in Postgre db.
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return: None
        Called by: fill_pn_tables()
        """
        my_billing_contact = self.mkt_data[ix]['contactDetails'].\
            get('billingContact')
        if my_billing_contact:
            self.print_if_verbose('WE HAVE BILLING CONTACT')
            bill_contact_email = \
                self.mkt_data[ix]['contactDetails']['billingContact']['email']
            if bill_contact_email not in self.contacts_key_set:
                # never saw this email: insert it to Postgre
                self.insert_bill_contact(pn_cursor, ix, bill_contact_email)
            else:
                # If bill_contact_email IS present in self.contacts_key_set,
                # detect if new entry is identical to the one already present
                if self.is_contact_item_duplicate(pn_cursor, ix):
                    pass
                else:
                    self.update_bill_contact(pn_cursor, ix)
        else:
            self.print_if_verbose('no billing contact')

    def insert_bill_contact(self, pn_cursor, ix, bill_contact_email):
        """
        Do an INSERT query on pn_contacts table
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :param bill_contact_email: the item to insert
        :return: None
        Called by: get_billing_contact()
        """
        insert_bill_contact_query = ('INSERT INTO pn_contacts (email, ' +
                                     'addr_1, addr_2, city, ' +
                                     'name, phone, postcode, state, ' +
                                     'pgres_last_updated) VALUES ' +
                                     '(%s, %s, %s, %s, %s, %s, %s, %s,' +
                                     '%s);')
        insert_bill_contact_data = tuple(self.make_contact_insert_list(
            self.mkt_data[ix]['contactDetails']['billingContact']))
        pn_cursor.execute(insert_bill_contact_query, insert_bill_contact_data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING INSERT BILL CONTACT QUERY')
        else:
            self.print_if_verbose('INSERT BILL CONTACT QUERY EXECUTED SUCCESSFULLY')
            self.contacts_key_set.add(bill_contact_email)
            self.ct_insert_bill_contacts += 1

    def update_bill_contact(self, pn_cursor, ix):
        """
        Do an UPDATE query on pn_contacts table
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return: None
        Called by: get_billing_contact()
        """
        update_query = ('UPDATE pn_contacts SET (email, ' +
                        'addr_1, addr_2, city, ' +
                        'name, phone, postcode, state, ' +
                        'pgres_last_updated) = ' +
                        '(%s, %s, %s, %s, %s, %s, %s, %s,' +
                        '%s) WHERE email = %s;')

        update_data = self.make_contact_update_list(
            self.mkt_data[ix]['contactDetails']['billingContact'])
        data = tuple(update_data)
        pn_cursor.execute(update_query, data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING BILL CONTACT UPDATE QUERY')
        else:
            self.print_if_verbose('BILL CONTACT UPDATE QUERY EXECUTED SUCCESSFULLY')
            self.ct_update_bill_contacts += 1

    def get_technical_contact(self, pn_cursor, ix):
        """
        Get a technical contact from Mktplc to insert or update in Postgres db.
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return: None
        Called by: fill_pn_tables()
        """
        my_technical_contact = self.mkt_data[ix]['contactDetails'].get('technicalContact')
        if my_technical_contact:
            self.print_if_verbose('we have technical contact')
            tech_contact_email = \
                self.mkt_data[ix]['contactDetails']['technicalContact']['email']
            if tech_contact_email not in self.contacts_key_set:
                # never saw this email: insert it to Postgre
                self.insert_tech_contact(pn_cursor, ix, tech_contact_email)
            else:  # detect if new entry is identical to the one already present
                is_tech_item = True
                if self.is_contact_item_duplicate(pn_cursor, ix, is_tech_item):
                    pass
                else:
                    self.update_tech_contact(pn_cursor, ix)
        else:
            self.print_if_verbose('no technical contact')

    def insert_tech_contact(self, pn_cursor, ix, tech_contact_email):
        """
        Do an INSERT query on pn_contacts table
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :param tech_contact_email: the item to insert
        :return: None
        Called by: get_technical_contact()
        """
        insert_query = ('INSERT INTO pn_contacts (email, ' +
                        'addr_1, addr_2, city, ' +
                        'name, phone, postcode, state, ' +
                        'pgres_last_updated) VALUES ' +
                        '(%s, %s, %s, %s, %s, %s, %s, %s,' +
                        '%s);')
        insert_data = tuple(self.make_contact_insert_list(self.mkt_data[ix]['contactDetails']
                                                    ['technicalContact']))
        pn_cursor.execute(insert_query, insert_data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING INSERT TECH CONTACT QUERY')
        else:
            self.print_if_verbose('INSERT TECH CONTACT QUERY EXECUTED SUCCESSFULLY')
            self.contacts_key_set.add(tech_contact_email)
            self.ct_insert_tech_contacts += 1

    def update_tech_contact(self, pn_cursor, ix):
        """
        Do an UPDATE query on pn_contacts table
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return: None
        """
        update_query = ('UPDATE pn_contacts SET (email, ' +
                        'addr_1, addr_2, city, ' +
                        'name, phone, postcode, state, ' +
                        'pgres_last_updated) = ' +
                        '(%s, %s, %s, %s, %s, %s, %s, %s,' +
                        '%s) WHERE email = %s;')

        update_data = tuple(self.make_contact_update_list(self.mkt_data[ix]['contactDetails']
                                                     ['technicalContact']))
        pn_cursor.execute(update_query, update_data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING TECH CONTACT UPDATE QUERY')
        else:
            self.print_if_verbose('TECH CONTACT UPDATE QUERY EXECUTED SUCCESSFULLY')
            self.ct_update_tech_contacts += 1

    def get_addons_key(self, pn_cursor, ix):
        """
        Get addon from Mktplc for insert or update in Postgre db.
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return: None
        Called by: fill_pn_tables()
        """
        addon_key = self.mkt_data[ix]['addonKey']
        if addon_key not in self.addons_key_set:
            # never saw this addon_key: insert it to Postgre
            self.insert_addon(pn_cursor, ix, addon_key)
        else:  # detect if new entry is identical to the one already present
            if self.is_addon_item_duplicate(pn_cursor, ix):
                pass
            else:
                self.update_addon(pn_cursor, ix)

    def insert_addon(self, pn_cursor, ix, addon_key):
        """
        Do an INSERT query on pn_addons table.
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :param addon_key:
        :return: None
        """
        insert_addon_query = ('INSERT INTO pn_addons (key, ' +
                              'name, pgres_last_updated) VALUES ' +
                              '(%s, %s, %s);')
        insert_addon_list = self.make_addon_insert_list(self.mkt_data[ix])
        insert_addon_data = tuple(insert_addon_list)
        pn_cursor.execute(insert_addon_query, insert_addon_data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING INSERT ADDON QUERY')
        else:
            self.print_if_verbose('INSERT ADDON QUERY EXECUTED SUCCESSFULLY')
            self.addons_key_set.add(addon_key)
            self.ct_insert_addons += 1

    def update_addon(self, pn_cursor, ix):
        """
        Do an UPDATE query on pn_addons table.
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return: None
        """
        update_addon_query = ('UPDATE pn_addons SET (key, name, ' +
                              'pgres_last_updated) = (%s, %s, %s) ' +
                              'WHERE key = %s;')
        update_addon_list = self.make_addon_update_list(self.mkt_data[ix])
        update_addon_data = tuple(update_addon_list)
        pn_cursor.execute(update_addon_query, update_addon_data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING UPDATE ADDON QUERY')
        else:
            self.print_if_verbose('UPDATE ADDON QUERY EXECUTED SUCCESSFULLY')
            self.ct_update_addons += 1

    def get_partner_details_key(self, pn_cursor, ix):
        """
        Get partner_details item from Mktplc for insert or update to Postgre.
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return: None
        Called by: fill_pn_tables()
        """
        if 'partnerDetails' in self.mkt_data[ix]:
            self.print_if_verbose('we have partner details')
            partner_details_name = self.mkt_data[ix]['partnerDetails']['partnerName']
            if partner_details_name not in self.partner_details_key_set:
                # never saw this partner_details_name: insert it
                self.insert_partner_details(pn_cursor, ix, partner_details_name)
            else:  # detect if new entry is identical to the one already present
                if self.is_partner_item_duplicate(pn_cursor, ix):
                    pass
                else:
                    self.update_partner_details(pn_cursor, ix)

    def insert_partner_details(self, pn_cursor, ix, partner_details_name):
        """
        Do an INSERT query on pn_partner_details table.
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :param partner_details_name: partner name for this item
        :return: None
        Called by: get_partner_details_key()
        """
        insert_partner_details_query = \
            ('INSERT INTO pn_partner_details (name, ' +
             'type, bill_contact_name, bill_contact_email, ' +
             'pgres_last_updated) VALUES (%s, %s, %s, %s, %s);')
        insert_partner_details_list = \
            self.make_partner_details_insert_list(self.mkt_data[ix]['partnerDetails'])
        insert_partner_details_data = tuple(insert_partner_details_list)
        pn_cursor.execute(insert_partner_details_query,
                          insert_partner_details_data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING INSERT PARTNER DETAILS QUERY')
        else:
            self.print_if_verbose('INSERT PARTNER DETAILS QUERY EXECUTED SUCCESSFULLY')
            self.partner_details_key_set.add(partner_details_name)
            self.ct_insert_partner_det += 1

    def update_partner_details(self, pn_cursor, ix):
        """
        Do an UPDATE query on pn_partner_details table.
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return: None
        Called by: get_partner_details_key()
        """
        update_partner_query = ('UPDATE pn_partner_details SET ' +
                                '(name, type, bill_contact_name, ' +
                                'bill_contact_email, ' +
                                'pgres_last_updated) = (%s, %s, %s, ' +
                                '%s, %s) WHERE name = %s;')
        update_partner_list = \
            self.make_partner_update_list(self.mkt_data[ix]
                                          ['partnerDetails'])
        update_partner_data = tuple(update_partner_list)
        pn_cursor.execute(update_partner_query, update_partner_data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING UPDATE PARTNER DETAILS QUERY')
        else:
            self.print_if_verbose('UPDATE PARTNER DETAILS QUERY EXECUTED SUCCESSFULLY')
            self.ct_update_partner_det += 1

    def get_lcd_key(self, pn_cursor, ix):
        """
        Insert or update lcd item from Marketplace API to Postgre db.
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return: None
        Called by: fill_pn_tables()
        """
        lcd_key_as_list = self.build_lcd_key_as_list(pn_cursor, ix)
        lcd_key = tuple(lcd_key_as_list)

        if lcd_key[:3] not in self.lcd_key_set:
            # never saw this lcd_key: insert item to Postgre
            self.insert_lcd(pn_cursor, lcd_key)
        else:
            # detect duplicate entry
            if self.is_lcd_item_duplicate(pn_cursor, ix):
                pass
            else:
                self.update_lcd(pn_cursor, lcd_key)

    def insert_lcd(self, pn_cursor, lcd_key):
        """
        Do an INSERT query on pn_license_contact_details table.
        :param pn_cursor: on conn to Postgre db
        :param lcd_key:
        :return: None
        Called by: get_lcd_key()
        """
        insert_lcd_query = (
                'INSERT INTO pn_license_contact_details (company, ' +
                'country, region, bill_contact_id, tech_contact_id, ' +
                'pgres_last_updated) VALUES ' +
                '(%s, %s, %s, %s, %s, %s);')
        insert_lcd_data = lcd_key + tuple([self.cur_time])
        pn_cursor.execute(insert_lcd_query,
                          insert_lcd_data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING INSERT LICENSE CONTACT DETAILS QUERY')
        else:
            self.print_if_verbose('INSERT LICENSE CONTACT DETAILS QUERY EXECUTED SUCCESSFULLY')
            self.lcd_key_set.add(lcd_key[:3])
            self.ct_insert_lcd += 1

    def update_lcd(self, pn_cursor, lcd_key):
        """
        Do an UPDATE query on pn_license_contact_details table.
        :param pn_cursor: on conn to Postgre db
        :param lcd_key: 
        :return: None
        Called by: get_lcd_key()
        """
        update_lcd_query = (
            'UPDATE pn_license_contact_details SET (bill_contact_id, ' +
            'tech_contact_id, pgres_last_updated) = (%s, %s, %s) WHERE' +
            '(company, country, region) = (%s, %s, %s);'
        )

        update_lcd_data = lcd_key[3:5] + tuple([self.cur_time]) + lcd_key[:3]

        pn_cursor.execute(update_lcd_query, update_lcd_data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING UPDATE LICENSE CONTACT DETAILS QUERY')
        else:
            self.print_if_verbose('UPDATE LICENSE CONTACT DETAILS QUERY EXECUTED SUCCESSFULLY')
            self.lcd_key_set.add(lcd_key[:3])
            self.ct_update_lcd += 1

    def build_lcd_key_as_list(self, pn_cursor, ix):
        """

        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return: lcd key as list
        """
        lcd_key_as_list = [
            self.mkt_data[ix]['contactDetails']['company'],
            self.mkt_data[ix]['contactDetails']['country'],
            self.mkt_data[ix]['contactDetails']['region']]

        id_query = 'SELECT id FROM pn_contacts WHERE email = %s;'

        if self.mkt_data[ix]['contactDetails'].get('billingContact'):
            bill_email = self.mkt_data[ix]['contactDetails']['billingContact'].get('email')
            if bill_email:
                bill_id_data = bill_email
                pn_cursor.execute(id_query, (bill_id_data,))
                rslt = pn_cursor.fetchone()[0]
                lcd_key_as_list.append(rslt)
            else:
                lcd_key_as_list.append(None)
        else:
            lcd_key_as_list.append(None)

        tech_email = self.mkt_data[ix]['contactDetails']['technicalContact']['email']
        tech_id_data = tech_email
        pn_cursor.execute(id_query, (tech_id_data,))
        rslt = pn_cursor.fetchone()[0]
        lcd_key_as_list.append(rslt)
        return lcd_key_as_list

    # =================

    def get_license_id(self, pn_cursor, ix):
        """

        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return:
        Called by: fill_pn_tables()
        """
        license_key = self.mkt_data[ix]['licenseId']
        if license_key not in self.license_key_set:
            # never saw this license_key: insert it to Postgre
            self.insert_license(pn_cursor, ix, license_key)
        else:  # detect if new entry is identical to the one already present
            if self.is_license_id_item_duplicate(pn_cursor, ix):
                pass
            else:
                self.update_license(pn_cursor, ix)

    def insert_license(self, pn_cursor, ix, license_key):
        insert_license_id_query = \
            ('INSERT INTO pn_licenses (license_id, ' +
             'addons_id, license_contact_details_id, ' +
             'partner_details_id, organizations_id, ' +
             'addon_key, hosting, host_license_id, ' +
             'last_updated, license_type, ' +
             'maint_start_date, maint_end_date, ' +
             'status, tier, pgres_last_updated) ' +
             'VALUES (%s, %s, %s, %s, %s, ' +
             '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);')
        insert_license_id_list = self.make_license_id_insert_list(
            self.mkt_data, pn_cursor, ix)
        insert_license_id_data = tuple(insert_license_id_list)
        pn_cursor.execute(insert_license_id_query, insert_license_id_data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING INSERT LICENSE ID QUERY')
        else:
            self.print_if_verbose('INSERT LICENSE ID QUERY EXECUTED SUCCESSFULLY')
            # self.license_id_set.add(license_id)
            self.license_key_set.add(license_key)
            self.ct_insert_license += 1

    def update_license(self, pn_cursor, ix):
        update_license_id_query = \
            ('UPDATE pn_licenses SET (license_id, ' +
             'addons_id, license_contact_details_id, ' +
             'partner_details_id, organizations_id, ' +
             'addon_key, hosting, host_license_id, ' +
             'last_updated, license_type, ' +
             'maint_start_date, maint_end_date, ' +
             'status, tier, pgres_last_updated) = (%s, %s, %s, ' +
             '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ' +
             'WHERE license_id = %s;')
        update_license_id_list = self.make_license_id_update_list(
            self.mkt_data, pn_cursor, ix)
        # update_license_id_list.append(self.mkt_data[ix]['licenseId'])
        update_license_id_data = tuple(update_license_id_list)
        pn_cursor.execute(update_license_id_query, update_license_id_data)
        rowcount = pn_cursor.rowcount
        if rowcount != 1:
            self.print_if_verbose('ERROR EXECUTING UPDATE LICENSE ID QUERY')
        else:
            self.print_if_verbose('UPDATE LICENSE ID QUERY EXECUTED SUCCESSFULLY')
            self.ct_update_license += 1

# =================

    def make_contact_insert_list(self, mkt_input_dict):
        """
        Make a list of fields that may be present in mkt_input_dict
        :param mkt_input_dict:
        :return: The list of fields
        Called by: insert_bill_contact(), insert_tech_contact(),
                   is_contact_item_duplicate(), make_contact_update_list()
        """
        # these are the fields that may be present in the response from Mktplc
        contact_insert_list = [mkt_input_dict.get('email', None),
                               mkt_input_dict.get('address1', None),
                               mkt_input_dict.get('address2', None),
                               mkt_input_dict.get('city', None),
                               mkt_input_dict.get('name', None),
                               mkt_input_dict.get('phone', None),
                               mkt_input_dict.get('postcode', None),
                               mkt_input_dict.get('state', None),
                               self.cur_time]
        return contact_insert_list

    def make_contact_update_list(self, mkt_input_dict):
        contact_update_list = self.make_contact_insert_list(mkt_input_dict)
        contact_update_list.append(mkt_input_dict['email'])
        return contact_update_list

    def make_addon_insert_list(self, mkt_input_dict):
        return [mkt_input_dict['addonKey'], mkt_input_dict['addonName'],
                self.cur_time]

    def make_addon_update_list(self, mkt_input_dict):
        addon_update_list = self.make_addon_insert_list(mkt_input_dict)
        addon_update_list.append(mkt_input_dict['addonKey'])
        return addon_update_list

    def make_partner_details_insert_list(self, mkt_input_dict):
        return [mkt_input_dict['partnerName'],
                mkt_input_dict['partnerType'],
                mkt_input_dict['billingContact']['name'],
                mkt_input_dict['billingContact']['email'], self.cur_time]

    def make_partner_update_list(self, mkt_input_dict):
        partner_update_list = self.make_partner_details_insert_list(mkt_input_dict)
        partner_update_list.append(mkt_input_dict['partnerName'])
        return partner_update_list

    def make_lcd_insert_list(self, pn_cursor, ix):
        """

        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return:
        """
        output_list = [self.mkt_data[ix]['contactDetails']['company'],
                       self.mkt_data[ix]['contactDetails']['country'],
                       self.mkt_data[ix]['contactDetails']['region']]

        query = 'SELECT id FROM pn_contacts WHERE email = %s;'

        if self.mkt_data[ix]['contactDetails'].get('billingContact') and \
                self.mkt_data[ix]['contactDetails']['billingContact'].get('email'):
            data = self.mkt_data[ix]['contactDetails']['billingContact']['email']
            data_tuple = tuple([data])
            pn_cursor.execute(query, data_tuple)
            rslt = pn_cursor.fetchone()
            output_list.append(rslt[0])
        else:
            output_list.append(None)

        query = 'SELECT id FROM pn_contacts WHERE email = %s;'
        data = self.mkt_data[ix]['contactDetails']['technicalContact']['email']
        data_tuple = tuple([data])
        pn_cursor.execute(query, data_tuple)
        rslt = pn_cursor.fetchone()
        output_list.append(rslt[0])

        output_list.append(self.cur_time)

        return output_list

    def make_lcd_update_list(self, mkt_input_dict, ix):
        """

        :param mkt_input_dict:
        :param ix: into license data retrieved from Marketplace API
        :return:
        """
        lcd_update_list = self.lcd_data[:]
        lcd_update_list.append(mkt_input_dict[ix]['contactDetails']['company'])
        lcd_update_list.append(mkt_input_dict[ix]['contactDetails']['country'])
        lcd_update_list.append(mkt_input_dict[ix]['contactDetails']['region'])
        return lcd_update_list

    def make_license_id_insert_list(self, mkt_input_dict, pn_cursor, ix):
        """

        :param mkt_input_dict:
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return:
        """
        output = [mkt_input_dict[ix]['licenseId'],
                  self.get_addons_id(pn_cursor, ix, mkt_input_dict),
                  self.get_lcd_id(pn_cursor, ix, mkt_input_dict),
                  self.get_partner_details_id(pn_cursor, ix, mkt_input_dict),
                  # there won't be an orgs id until load_orgs() has been run
                  self.get_organizations_id(pn_cursor, ix, mkt_input_dict),
                  mkt_input_dict[ix]['addonKey'],
                  mkt_input_dict[ix].get('hosting', None),
                  mkt_input_dict[ix].get('hostLicenseId', None),
                  mkt_input_dict[ix]['lastUpdated'],
                  mkt_input_dict[ix]['licenseType'],
                  mkt_input_dict[ix]['maintenanceStartDate'],
                  mkt_input_dict[ix]['maintenanceEndDate'],
                  mkt_input_dict[ix]['status'],
                  mkt_input_dict[ix]['tier'], self.cur_time]
        return output

    def make_license_id_update_list(self, mkt_input_dict, pn_cursor, ix):
        """

        :param mkt_input_dict:
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return:
        """
        license_id_update_list = self.make_license_id_insert_list(
            mkt_input_dict, pn_cursor, ix)
        license_id_update_list.append(mkt_input_dict[ix]['licenseId'])
        return license_id_update_list

    @staticmethod
    def get_id(contact, pn_cursor):
        """

        :param contact:
        :param pn_cursor: on conn to Postgre db
        :return:
        """
        query = 'SELECT id FROM pn_contacts WHERE email = %s'
        if contact:
            data = contact['email']
            pn_cursor.execute(query, (data,))
            contact_id = pn_cursor.fetchone()[0]
            return contact_id
        else:
            return None

    @staticmethod
    def get_addons_id(pn_cursor, ix, mkt_input_dict):
        """

        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :param mkt_input_dict:
        :return:
        """
        query = 'SELECT id FROM pn_addons WHERE key = %s;'
        data = mkt_input_dict[ix]['addonKey']
        pn_cursor.execute(query, (data,))
        addons_id = pn_cursor.fetchone()[0]
        return addons_id

    @staticmethod
    def get_lcd_id(pn_cursor, ix, mkt_input_dict):
        """

        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :param mkt_input_dict:
        :return:
        """
        data = (mkt_input_dict[ix]['contactDetails'].get('company', None),
                mkt_input_dict[ix]['contactDetails'].get('country', None),
                mkt_input_dict[ix]['contactDetails'].get('region', None))
        if all(data):
            query = ('SELECT id FROM pn_license_contact_details WHERE (company, ' +
                     'country, region) = (%s, %s, %s)')
            pn_cursor.execute(query, data)
            lcd_id = pn_cursor.fetchone()[0]
            return lcd_id
        else:
            return None

    def get_partner_details_id(self, pn_cursor, ix, mkt_input_dict):
        """

        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :param mkt_input_dict:
        :return:
        """
        data = (mkt_input_dict[ix].get('partnerName', None))
        if data and all(data):
            self.print_if_verbose('**** have partner name ****')
            query = 'SELECT id FROM pn_partner_details WHERE name = %s;'
            pn_cursor.execute(query, (data,))
            result = pn_cursor.fetchone()
            if result:
                partner_details_id = pn_cursor.fetchone()[0]
                return partner_details_id
            else:
                return None
        else:
            return None

    @staticmethod
    def get_organizations_id(pn_cursor, ix, mkt_input_dict):
        """

        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :param mkt_input_dict:
        :return:
        """
        query = ('SELECT organizations_id FROM pn_licenses WHERE ' +
                 'license_id = %s;')
        data = tuple([mkt_input_dict[ix]['licenseId']])
        pn_cursor.execute(query, data)
        result = pn_cursor.fetchone()
        if result:
            return result[0]
        else:
            return None

    def is_contact_item_duplicate(self, pn_cursor, ix, is_tech=False):
        """
        Tell if tech or bill contact is already in Postgre db
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :param is_tech: if False, this is a billing contact
                        else, this is a technical contact
        :return: True if contact item from Mktplc identical to one already in
                     Postgre db
                 False otherwise
        Called by: get_billing_contact(), get_technical_contact()
        """
        if is_tech:
            mkt_contact_data = self.make_contact_insert_list(self.mkt_data[ix]
                                                             ['contactDetails']
                                                             ['technicalContact'])
        else:
            mkt_contact_data = self.make_contact_insert_list(self.mkt_data[ix]
                                                             ['contactDetails']
                                                             ['billingContact'])
        mkt_contact_data_tuple = tuple(mkt_contact_data)
        query = 'SELECT * FROM pn_contacts WHERE email = %s;'
        data = mkt_contact_data[0]
        pn_cursor.execute(query, (data,))
        pn_cursor_result = pn_cursor.fetchone()
        return pn_cursor_result[1:9] == mkt_contact_data_tuple[:8]

    def is_addon_item_duplicate(self, pn_cursor, ix):
        """
        Is addon item from Mktplc identical to one already in Postgre db?
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return:
        """
        addon_data = self.make_addon_insert_list(self.mkt_data[ix])
        addon_data_tuple = tuple(addon_data)
        query = 'SELECT * FROM pn_addons WHERE key = %s;'
        data = addon_data[0]
        pn_cursor.execute(query, (data,))
        pn_cursor_result = pn_cursor.fetchone()
        return pn_cursor_result[1:3] == addon_data_tuple[0:2]

    def is_partner_item_duplicate(self, pn_cursor, ix):
        """
        Is partner item from Mktplc identical to one already in Postgre db?
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return:
        """
        partner_data = self.make_partner_details_insert_list(self.mkt_data[ix]
                                                             ['partnerDetails'])
        partner_data_tuple = tuple(partner_data)
        query = 'SELECT * FROM pn_partner_details WHERE name = %s;'
        data = partner_data[0]
        pn_cursor.execute(query, (data,))
        pn_cursor_result = pn_cursor.fetchone()
        return pn_cursor_result[1:5] == partner_data_tuple[0:4]

    def is_lcd_item_duplicate(self, pn_cursor, ix):
        """
        Is lcd item from Mktplc identical to one already in Postgre db?
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return:
        """
        self.lcd_data = self.make_lcd_insert_list(pn_cursor, ix)
        lcd_tuple = tuple(self.lcd_data)
        query = ('SELECT * FROM ' +
                 'pn_license_contact_details WHERE ' +
                 '(company, country, region) = (%s, %s, %s);')
        pn_cursor.execute(query, lcd_tuple[:3])
        pn_cursor_result = pn_cursor.fetchone()
        if pn_cursor_result[1:6] != lcd_tuple[:5]:
            self.print_if_verbose('**** NOT SAME ****' * 5)
        else:
            self.print_if_verbose('****** OK ******' * 5)
        return pn_cursor_result[1:6] == lcd_tuple[:5]

    def is_license_id_item_duplicate(self, pn_cursor, ix):
        """
        Is license item from Mktplc identical to one already in Postgre db?
        :param pn_cursor: on conn to Postgre db
        :param ix: into license data retrieved from Marketplace API
        :return:
        """
        license_data = self.make_license_id_insert_list(
            self.mkt_data, pn_cursor, ix
        )
        license_data_dates = license_data[:14]
        license_data_tuple = tuple(license_data_dates)
        query01 = ('SELECT license_id, addons_id, ' +
                   'license_contact_details_id, ' +
                   'partner_details_id, organizations_id, addon_key, ' +
                   'hosting, host_license_id, last_updated::date, ' +
                   'license_type, maint_start_date::date,'
                   'maint_end_date::date, status, tier FROM pn_licenses ' +
                   'WHERE (license_id, ' +
                   'addons_id, license_contact_details_id, ' +
                   'partner_details_id, organizations_id, ' +
                   'addon_key, hosting, host_license_id, ' +
                   'last_updated::date, license_type, ' +
                   'maint_start_date::date, maint_end_date::date, ' +
                   'status, tier) IS NOT DISTINCT FROM ' +
                   '(%s, %s, %s, %s, %s, ' +
                   '%s, %s, %s, %s, %s, %s, %s, %s, %s);')
        pn_cursor.execute(query01, license_data_tuple)
        pn_cursor_result = pn_cursor.fetchone()
        if not pn_cursor_result:
            return False
        pn_cursor_result_w_dates = self.datetimes_to_dates_list(pn_cursor_result)
        return tuple(pn_cursor_result_w_dates) == license_data_tuple

    @staticmethod
    def datetimes_to_dates_list(sequence):
        w_dates = []
        for item in sequence:
            w_dates.append(item if not isinstance(item, datetime.date)
                           else item.strftime('%Y-%m-%d'))
        return w_dates

    def output_stats(self):
        print(self.ct_insert_bill_contacts, 'bill ct inserts')
        print(self.ct_update_bill_contacts, 'bill ct updates')
        print(self.ct_insert_tech_contacts, 'tech_ct inserts')
        print(self.ct_update_tech_contacts, 'tech_ct updates')
        print(self.ct_insert_addons, 'addon inserts')
        print(self.ct_update_addons, 'addon updates')
        print(self.ct_insert_partner_det, 'partner det inserts')
        print(self.ct_update_partner_det, 'partner det updates')
        print(self.ct_insert_lcd, 'lcd inserts')
        print(self.ct_update_lcd, 'lcd updates')
        print(self.ct_insert_license, 'license inserts')
        print(self.ct_update_license, 'license updates')

    def main(self):
        self.get_args()
        self.get_env_vars()
        mkt_response = self.get_licenses()
        self.handle_mkt_response(mkt_response)
        self.store_licenses()
        self.output_stats()


if __name__ == '__main__':
    ll = LoadLicenses()
    ll.main()
