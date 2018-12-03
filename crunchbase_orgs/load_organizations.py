#!/usr/bin/env python3.6


# file: load_organizations.py
# andrew jarcho
# 2018-04-23

import os
import sys
import argparse
import json
import requests
import logging
import time
import psycopg2
import re

try:
    from src.time_string_conversion import get_now
except ModuleNotFoundError:
    from time_string_conversion import get_now

try:
    from crunchbase_orgs.src.constants import BASE_URL, DEFAULT_DATE, \
        API_ENDPOINT, ISP_FILE, TLD_FILE, SLEEP_SECS
except ModuleNotFoundError:
    from constants import BASE_URL, DEFAULT_DATE, API_ENDPOINT, ISP_FILE, \
        TLD_FILE, SLEEP_SECS


class LoadOrganizations:
    """
    Gets organizations from Crunchbase. Lookup by domain name, then by
    company name.
    Uses Crunchbase APIs '/odm-organizations' endpoint.
    Run as, e.g.:
    python3 mktplc_export_lics/src/load_licenses.py -s | \
        python3 crunchbase_orgs/src/load_organizations.py -s
    or
    python3 crunchbase_orgs/src/load_organizations.py -s < <input_file>
    or
    python3 crunchbase_orgs/src/load_organizations.py -s -i \
        <input_file>
    """
    def __init__(self, api_key=None,
                 base_url=BASE_URL, api_endpoint=API_ENDPOINT,
                 db_password=None,
                 domain_search_outfile=None, name_search_outfile=None):
        self.api_key = api_key
        self.url = base_url + api_endpoint
        self.base_url = base_url
        self.api_endpoint = api_endpoint
        self.domain_query_response_dict = None
        self.name_query_response_dict = None
        self.data_source = None
        self.db_host = None
        self.db_name = None
        self.db_user = None
        self.db_password = db_password
        self.domain_search_outfile = domain_search_outfile
        self.name_search_outfile = name_search_outfile
        self.domain_search_to_stdout = False
        self.name_search_to_stdout = False
        self.temp_domain_search_file = 'output_domain_search.temp'
        self.temp_name_search_file = 'output_name_search.temp'
        self.isp_domains = []
        self.tlds = set()
        self.domain_misses = 0
        self.single_domain_hits = 0
        self.multiple_domain_hits = 0
        self.name_misses = 0
        self.single_name_hits = 0
        self.multiple_name_hits = 0
        self.ct_isps = 0
        self.ct_name_queries = 0
        self.time_used_cb = 0
        self.items_examined = 0
        self.items_skipped = 0
        self.items_not_skipped = 0
        self.sql_insert_org = ''
        self.indent_level = 0
        self.verbose = 0
        self.domains_queried = set()
        self.repeat_domains = 0
        self.sess = None  # cb requests session object
        self.ct_stored = 0
        self.cur_time = get_now()
        self.sql_update_org = ''

    def get_c_l_args(self, argv=None):
        """Get command line arguments"""
        parser = argparse.ArgumentParser()
        parser.add_argument('-v', '--verbose', help='send extra debug info to stdout',
                            action='count', default=0)
        parser.add_argument('-s', '--domain_search_to_stdout',
                            help='send domain search output to stdout',
                            action='store_true')
        parser.add_argument('-t', '--name_search_to_stdout',
                            help='send name search output to stdout',
                            action='store_true')
        parser.add_argument('-i', '--infile', type=str,
                            help='read input from file INFILE')
        parser.add_argument('-o', '--domain_search_outfile', type=str,
                            help='send domain search output to file DOMAIN_SEARCH_OUTFILE')
        parser.add_argument('-p', '--name_search_outfile', type=str,
                            help='send name search output to file NAME_SEARCH_OUTFILE')
        args = parser.parse_args(argv)
        self.verbose = args.verbose
        self.data_source = open(args.infile) if args.infile else \
            sys.stdin
        self.domain_search_outfile = args.domain_search_outfile
        self.name_search_outfile = args.name_search_outfile
        self.domain_search_to_stdout = args.domain_search_to_stdout
        self.name_search_to_stdout = args.name_search_to_stdout

    def get_env_vars(self):
        """Check that environment variables have been set"""
        try:
            self.api_key = os.environ['API_KEY']
            self.db_host = os.environ['DBHOST']
            self.db_name = os.environ['DBNAME']
            self.db_user = os.environ['DBUSER']
            self.db_password = os.environ['DBPASSWD']
        except KeyError:
            self.print_indented('Please set environment variables API_KEY, '
                                'DBHOST, DBNAME, DBUSER, DBPASSWD', sys.stderr)
            sys.exit(1)

    @staticmethod
    def setup_logging():
        """Set log file, level, and format"""
        logging.basicConfig(filename='logs/cb_orgs.log',
                            level=logging.INFO, filemode='w',
                            format='%(asctime)s: (%(levelname)s) %(message)s')

    def print_indented(self, text, dest=sys.stdout):
        """
        Indent debug output for greater readability
        """
        if self.verbose:
            print('{}{}'.format('\t' * self.indent_level, text), file=dest)

    def get_each_license(self):
        """Get each license in turn from PostgreSQL"""
        self.connect_to_cb_or_die()
        start_item = 0
        stop_item = float('inf')
        self.print_opening_message()
        payload = self.build_cb_query_payload()
        email_company_iter = self.get_email_and_company()
        while self.items_examined < stop_item:
            if self.items_examined and not self.items_examined % 25:
                self.print_progress()
            # introduce delays to keep from exceeding CB request limit
            if self.items_examined and not self.items_not_skipped % 25 \
                    and self.items_examined > start_item:
                print('SLEEPING {}'.format(SLEEP_SECS))
                time.sleep(SLEEP_SECS)
            try:
                email, company = next(email_company_iter)
                self.items_examined += 1
                if self.items_examined < start_item:
                    self.items_skipped += 1
                    continue
                self.items_not_skipped += 1
                self.handle_company_and_email(company, email, payload)
            except StopIteration:
                break
        self.sess.close()

        self.temp_file_to_json()

    def temp_file_to_json(self):
        """Convert temp file to valid JSON"""
        if self.domain_search_outfile:
            self.convert_domain_search_output()
        if self.name_search_outfile:
            self.convert_name_search_output()
        try:
            os.remove(self.temp_domain_search_file)
        except OSError:
            pass

    def connect_to_cb_or_die(self):
        """
        Try to connect to CB; if not successful, delay and retry.
        :return: None
        Called by: get_each_license()
        """
        connected = False
        backoff = 30
        backoff_multiplier = 1
        while not connected and backoff_multiplier < 5:
            try:
                self.sess = requests.Session()
                connected = True
            except ConnectionError as e:
                print('connection failure {}: sleeping {} seconds'.format(
                    e, backoff * backoff_multiplier), file=sys.stderr)
                time.sleep(backoff * backoff_multiplier)
                backoff_multiplier += 1
        if not connected:
            print('Unable to connect to Crunchbase. Please try again later.',
                  file=sys.stderr)
            sys.exit(0)

    @staticmethod
    def print_opening_message():
        print('Querying Crunchbase odm-organizations endpoint...', file=sys.stderr)

    def get_email_and_company(self):
        """
        Yield a tuple holding a tech contact email and the
            corresponding company name, from Marketplace API
            'Export licenses' endpoint
        :return: The above tuple, or
                 StopIteration
        Called by: get_each_license()`
        """
        self.get_isp_domain_dict()
        licenses = self.get_licenses_as_dict()
        for item in licenses:
            email = item['contactDetails']['technicalContact']['email']
            company = item['contactDetails']['company']
            yield email, company

    def get_isp_domain_dict(self):
        """
        ISP_FILE holds a list of common ISP domains, created by running
            'isp_domains_js_to_py.py' on 'domains/isp_domains.js'.
        Filter input before calling 'eval()' on it.
        :return: None
        Called by: get_email_and_company()
        """
        with open(ISP_FILE) as isp_file:
            isp_string = isp_file.readline().strip()
            m = re.search(r'[^a-zA-Z0-9[\] .,\-\']', isp_string)
            if m:
                print(m.start())
                try:
                    raise ValueError('bad value read in get_isp_domain_dict()')
                except ValueError:
                    raise
            self.isp_domains = eval(isp_string)

    def get_tld_domain_dict(self):
        """
        TLD_FILE holds a list of common TLDs and country codes.
        Filter input before calling 'eval()' on it.
        :return: None
        Called by: shorten()
        """
        with open(TLD_FILE) as tld_file:
            tld_string = tld_file.readline().strip()
            m = re.search(r'[^,a-z\'[\] ]', tld_string)
            if m:
                print(m.start())
                try:
                    raise ValueError('bad value read in get_tld_domain_dict()')
                except ValueError:
                    raise
            self.tlds = set(eval(tld_string))

    def get_licenses_as_dict(self):
        """
        Get licenses as python dict from stdin or from file
        :return: licenses dict
        Called by: get_email_and_company()
        """
        return json.load(self.data_source)

    def print_progress(self):
        """
        Print number of items examined, and elapsed time,
            after every 25th item
        :return:
        Called by: get_each_license()
        """
        self.print_indented('{} items examined in {:.1f} secs  ({} items skipped)'.
                            format(self.items_examined,
                                   self.time_used_cb,
                                   self.items_skipped),
                            sys.stderr)

    def handle_company_and_email(self, company, email, payload):
        """
        Handle values retrieved by 'get_email_and_company()' from
            Marketplace 'Export licences' endpoint
        :param email: the tech contact email for a license
        :param company: associated with that email
        :return: None
        Called by: get_each_license()
        """
        domain = self.get_domain_from(email)

        if not domain:
            self.handle_bad_email(email)
        elif domain in self.isp_domains:
            self.handle_isp_domain(company, email)
        else:
            payload['name'] = None
            payload['domain_name'] = domain
            self.handle_non_isp_domain(company, domain, payload)

    @staticmethod
    def get_domain_from(email):
        """
        Get domain from email as the part of the address after '@'
        :param email: tech contact email retrieved from Marketplace via
                      'Export licenses' endpoint
        :return: a domain if the email address passes very simple checks, else
                 None
        Called by: handle_company_and_email()
        """
        at_sign_ix = email.rfind('@')
        dot_ix = email.rfind('.')
        if at_sign_ix > 0 and dot_ix > at_sign_ix + 1:
            domain = email[at_sign_ix + 1:]
        else:
            domain = None
        if domain and '@' in domain:  # check for 2nd '@' in address
            domain = None
        return domain

    @staticmethod
    def handle_bad_email(email):
        """
        Handle an email address that has no '@' sign
        :param email: retrieved from Marketplace 'Export licenses' endpoint
        :return: None
        Called by: handle_company_and_email()
        """
        logging.warning('Bad email address \'%s\'' % (email,))

    def handle_isp_domain(self, company, email):
        """
        Handle an email address that has a domain belonging to an ISP
        :param email: retrieved from Marketplace 'Export licenses' endpoint
        :param company: associated with that email
        :return: None
        Called by: handle_company_and_email()
        """
        self.ct_isps += 1
        pass  # N.Y.I.

    def query_cb_orgs_by_name(self, company):
        """

        :param company:
        :return:
        Called by: handle_non_isp_domain()
        """
        payload = self.build_cb_query_payload()  # set up http request
        payload['domain_name'] = None
        payload['name'] = company

        response = self.sess.get(self.url, params=payload)
        name_query_response_dict = response.json()
        if self.name_search_outfile or self.name_search_to_stdout:
            self.output_found_name_query_response(name_query_response_dict)  # output response to temp file
        return name_query_response_dict

    def handle_non_isp_domain(self, company, domain, payload):
        """
        Call CB for a domain that is not known to belong to an ISP
        :param company: associated with the tech contact email address
                        from which domain was extracted
        :param domain: extracted from tech contact email address
        :param payload: holds the domain of the company being searched
        :return: bool 'stored', true iff company was stored in
                                'pn_organizations' and not removed
        Called by: handle_company_and_email()
        """
        stored = False
        if self.verbose:
            self.print_indented("Entering 'handle_non_isp_domain()'")
        if domain in self.domains_queried:
            if self.verbose:
                self.print_indented('Domain {} already queried'.format(domain))
            self.repeat_domains += 1
            if self.verbose:
                self.print_indented("Leaving 'handle_non_isp_domain()'")
            return False
        else:
            self.domains_queried.add(domain)
        if self.verbose:
            print("In 'handle_non_isp_domain()' querying by domain")
        start = time.time()
        # query cb by domain name
        domain_response_dict = self.query_cb_orgs_by_domain(payload)
        time_used = time.time() - start
        self.time_used_cb += time_used
        domain_response_len = self.get_response_len(domain_response_dict)
        self.tally_domain_hits(domain_response_len)
        if domain_response_len:  # query yields hit(s)
            pick_ix, pick_company = self.retrieve_pick(company, domain,
                                                       domain_response_dict)
            if pick_company:
                stored = self.store_one_response(domain_response_dict['data']['items']
                                                 [pick_ix], company)
        else:  # query cb by company name
            if self.verbose:
                self.print_indented('Domain query for {} yielded no hits'.
                                    format(domain))
            start = time.time()
            name_response_dict = self.query_cb_orgs_by_name(company)
            if self.verbose:
                print("In 'handle_non_isp_domain()' querying by name")
            time_used = time.time() - start
            self.time_used_cb += time_used
            self.ct_name_queries += 1
            name_response_len = self.get_response_len(name_response_dict)
            self.tally_name_hits(name_response_len)
            if name_response_len:
                pick_ix, pick_company = self.retrieve_pick(company, domain, name_response_dict)
                if pick_company:
                    stored = self.store_one_response(name_response_dict['data']['items']
                                                     [pick_ix], company)
                    self.print_indented('Choice {} made in CB query response'.
                                        format(company))
                    if stored:
                        self.print_indented('Company {} stored in pn_organizations'.
                                            format(company))
                    else:
                        self.print_indented('Unable to store company {} in pn_organizations'.
                                            format(company))
            else:
                self.print_indented('Name query for {} yielded no hits'.
                                    format(company))
        self.print_indented("Leaving 'handle_non_isp_domain()'")
        return stored

    def query_cb_orgs_by_domain(self, payload):
        """
        Get response from Crunchbase for the given domain using CB's
            'domain_name' query, and an 'updated_since' parameter to that
            query (if a 'modified_date' is passed to ctor as a c.l.a.)
        :param domain: the domain being searched in CB
        :return: Crunchbase response
        Called by: handle_non_isp_domain()
        """
        # self.indent_level += 1
        # self.print_indented('Entering query_cb_orgs_by_domain()')
        response = self.sess.get(self.url, params=payload)
        domain_query_response_dict = response.json()
        if self.domain_search_outfile or self.domain_search_to_stdout:
            self.output_found_domain_query_response(domain_query_response_dict)  # output response to temp file
        # self.print_indented('Leaving query_cb_orgs_by_domain()')
        # self.indent_level -= 1
        return domain_query_response_dict

    @staticmethod
    def get_response_len(response_dict):
        return len(response_dict['data']['items'])

    def build_cb_query_payload(self):
        """

        :return:
        Called by:
        """
        return {'user_key': self.api_key}

    def output_found_domain_query_response(self, domain_query_response_dict):
        """
        Append prettified output data to temporary file
        Note: aggregated contents of temp_file will *not* be valid JSON
        :return: None
        Called by: query_cb_orgs_by_domain()
        """
        with open(self.temp_domain_search_file, 'a+') as of:
            json.dump(domain_query_response_dict, of, sort_keys=True, indent=4,
                      separators=(',', ': '))

    def output_found_name_query_response(self, name_query_response_dict):
        """
        Append prettified output data to temporary file
        Note: aggregated contents of temp_file will *not* be valid JSON
        :return: None
        Called by: query_cb_orgs_by_name()
        """

        with open(self.temp_name_search_file, 'a') as of:
            json.dump(name_query_response_dict, of, sort_keys=True, indent=4,
                      separators=(',', ': '))

    def log_response(self, domain, response):
        """
        Call functions to write to log
        :param domain: extracted from tech contact email address
        :param response: to http 'get' request to Crunchbase for domain
        :return: None
        Called by: check_response_length()
        """
        length = len(response['data']['items'])
        if response.status_code != 200:
            self.log_error_response(response)
        elif length:
            self.log_response_found(domain, length)
        else:
            self.log_response_not_found(domain)

    def log_error_response(self, response):
        """
        Log a bad response
        :param response: a response with a non-200 status code
        :return: None
        Called by: log_response()
        """
        logging.warning('Get url %s returns status %s' %
                        (self.url, response.status_code))

    @staticmethod
    def log_response_found(domain, length):
        """
        Log a successful response, with number of items found
        :param domain: searched for by 'get' request to Crunchbase
        :param length: number of hits in response
        :return: None
        Called by: log_response()
        """
        logging.info('FOUND %s (%s) item(s)' % (domain, length))

    @staticmethod
    def log_response_not_found(domain):
        """
        Log a 'not found' response
        :param domain: a domain for which http 'get' request to Crunchbase
            returned no hits
        :return: None
        Called by: log_response()
        """
        logging.info('Nothing found for domain name %s' %
                     (domain,))

    def retrieve_pick(self, company, domain, response_dict):
        """

        :param company: associated with domain
        :param domain: extracted from tech contact email address
        :param response_dict:
        :return:
        Called by: handle_non_isp_domain()
        """
        self.indent_level += 1
        self.print_indented("Entering 'retrieve_pick()'")
        response_name_list = [response_dict['data']['items'][ix]['properties']['name']
                              for ix in range(len(response_dict['data']['items']))]
        self.print_indented('company: {}, domain: {}, response_name_list: {}'.
                            format(company, domain, response_name_list))
        domain = self.shorten(domain)

        pick_ix, best_name = self.pick_match(company, domain, response_dict)

        if best_name:
            self.print_indented('I choose {}'.format(best_name))
            self.print_indented("Leaving 'retrieve_pick()'")
            self.indent_level -= 1
            return pick_ix, best_name  # an index into response_dict, a company
        else:
            self.print_indented('CANNOT CHOOSE')
            self.print_indented("Leaving 'retrieve_pick()'")
            self.indent_level -= 1
            return None, None

    def shorten(self, domain):
        """
        Remove common TLDs and country codes from right end of domain.
        :param domain:
        :return: if there is > 1 segment in the remainder:
                     return its *rightmost* segment
                 else:
                     return the (possibly shortened) domain
        Called by: retrieve_pick()
        """
        if not self.tlds:
            self.get_tld_domain_dict()
        domain = domain.rstrip('.')  # handle cases such as 'company.inc.'
        domain_word_list = domain.split('.')
        while len(domain_word_list) > 1 and domain_word_list[-1].lower() in self.tlds:
            domain_word_list.pop()
        if len(domain_word_list):
            return domain_word_list[-1]
        else:
            return ''

    def pick_match(self, company, domain, query_response_dict):
        """
        Choose best item in a multiple-item response.

        :param company: associated with domain in the Postgre db
        :param domain: from tech contact email address
        :param query_response_dict:
        :return: on single match found:
                     pick_ix_list[0], the index into self.response_list['data']['items'],
                         with location of best response
                     candidate_list[0], holding a response not filtered out by this
                         method
                 on zero or multiple matches found:
                     None, None
        Called by: retrieve_pick()
        """
        if len(company) < 2:
            return None, None
        self.indent_level += 1
        self.print_indented("Entering 'pick_match()'")
        pick_ix_list = []
        candidate_list = []
        company_word_list = company.lower().split(' ')
        for ix, response_item in enumerate(query_response_dict['data']['items']):
            response = response_item['properties']['name'].strip()
            original_response = response
            if response.rfind('.') != -1:
                response = self.shorten(response)

            self.print_indented('looking at domain {}, response {}'.
                                format(domain, response))

            if response.lower() == domain.lower():
                candidate_list = [original_response]
                pick_ix_list = [ix]
                break  # we have a match

            if response.lower().startswith(domain.lower()) and \
                    original_response not in candidate_list:
                candidate_list.append(original_response)
                pick_ix_list.append(ix)

            response_initials = self.get_initials(response)
            if response_initials.lower() == domain.lower():
                candidate_list = [original_response]
                pick_ix_list = [ix]
                break  # we have a match

            if 'Venture' in response and 'Venture' not in domain:
                continue

            response_no_spaces = response.lower().replace(' ', '')

            if response_no_spaces == domain.lower():
                candidate_list = [original_response]
                pick_ix_list = [ix]
                break  # we have a match

            if response_no_spaces.startswith(domain.lower()) \
                    and original_response not in candidate_list:
                candidate_list.append(original_response)
                pick_ix_list.append(ix)

            temp_candidate_list, temp_pick_ix_list = self.pick_by_matches(
                ix, response_item, company_word_list)
            for item in temp_candidate_list:
                if item not in candidate_list:
                    candidate_list.append(item)
            for item in temp_pick_ix_list:
                if item not in pick_ix_list:
                    pick_ix_list.append(item)

        if len(pick_ix_list) == 1:
            self.print_indented("Leaving 'pick_match()'")
            self.indent_level -= 1
            return pick_ix_list[0], candidate_list[0]
        else:
            self.print_indented("Leaving 'pick_match()'")
            self.indent_level -= 1
            return None, None

    def pick_by_matches(self, ix, response_item, company_word_list):
        """

        :param ix:
        :param response_item:
        :param company_word_list:
        :return:
        Called by: pick_match()
        """
        self.indent_level += 1
        self.print_indented("Entering 'pick_by_matches()'")
        most_word_matches = 0
        least_word_mismatches = float('inf')
        response = response_item['properties']['name']
        original_response = response
        if response.rfind('.') != -1:
            response = self.shorten(response)
        response_word_list = response.lower().split()
        candidate_list = []
        pick_ix_list = []

        ct_word_matches = len([item for item in company_word_list
                              if item in response_word_list])
        ct_word_mismatches = len(response_word_list) - ct_word_matches
        ct_word_matches = max(ct_word_matches, most_word_matches)
        ct_word_mismatches = ct_word_mismatches \
            if ct_word_mismatches < least_word_mismatches else \
            least_word_mismatches

        if ct_word_matches and not ct_word_mismatches:
            candidate_list = [original_response]
            pick_ix_list = [ix]
            self.print_indented("Leaving 'pick_by_matches()'")
            self.indent_level -= 1
            return candidate_list, pick_ix_list

        if self.check_mismatches_are_at_end_of_response_list(
                company_word_list,
                response_word_list,
                ct_word_mismatches):
            candidate_list.append(original_response)
            pick_ix_list.append(ix)
            self.print_indented("Leaving 'pick_by_matches()'")
            self.indent_level -= 1
            return candidate_list, pick_ix_list

        self.print_indented("Leaving 'pick_by_matches()'")
        self.indent_level -= 1
        return candidate_list, pick_ix_list

    @staticmethod
    def check_mismatches_are_at_end_of_response_list(company_word_list,
                                                     response_word_list,
                                                     ct_word_mismatches):
        if not company_word_list or not response_word_list:
            return False

        ct_word_matches = 0
        ix = 0
        while company_word_list[ix] == response_word_list[ix]:
            ct_word_matches += 1
            ix += 1
            if ix == len(company_word_list) or ix == len(response_word_list):
                break
        return ct_word_matches and ct_word_matches + ct_word_mismatches == \
            len(response_word_list)

    def tally_domain_hits(self, response_length):
        """

        :return:
        Called by: handle_non_isp_domain()
        """
        if response_length:
            if response_length == 1:
                self.single_domain_hits += 1
            else:
                self.multiple_domain_hits += 1
        else:
            self.domain_misses += 1

    def tally_name_hits(self, response_length):
        """

        :param response_length:
        :return:
        Called by: handle_non_isp_domain()
        """
        if response_length:
            if response_length == 1:
                self.single_name_hits += 1
            else:
                self.multiple_name_hits += 1
        else:
            self.name_misses += 1

    def store_one_response(self, single_response, company):
        """

        :param single_response:
        :param company: from Marketplace data
        :return:
        Called by: handle_non_isp_domain()
        """
        self.indent_level += 1
        self.print_indented("Entering 'store_one_response()'")
        if not single_response['properties']['name'] or \
                not single_response['properties']['domain']:
                    self.indent_level -= 1
                    return False
        stored_part_1 = False
        stored_part_2 = False
        updated_part_1 = False
        updated_part_2 = False

        pg_conn_string = ("host = '{}' dbname = '{}' user = '{}' " +
                          "password = '{}'").format(self.db_host, self.db_name,
                                                    self.db_user,
                                                    self.db_password)
        pg_conn = psycopg2.connect(pg_conn_string)

        data_item_org = self.setup_data_item_org(single_response)

        # part 0: get items already stored
        already_stored = self.get_already_stored(pg_conn)
        if data_item_org[3] in already_stored:
            # self.indent_level -= 1
            # return False
            updated_part_1 = self.do_update(pg_conn, data_item_org)
        else:
            # part 1: store into pn_organizations
            stored_part_1 = self.do_store_part_1(pg_conn, data_item_org)
            # stored_part_2 = False
            if stored_part_1:
                organization_id = self.get_organization_id(pg_conn, single_response)
                license_contact_details_id_list = \
                    self.get_license_contact_details_id_list(pg_conn, company)
                if len(license_contact_details_id_list) == 1:
                    license_contact_details_id = license_contact_details_id_list[0]
                    # part 2: store fk into pn_licenses
                    stored_part_2 = self.do_store_part_2(pg_conn, organization_id,
                                                         license_contact_details_id)
                    if stored_part_2:
                        self.print_indented('Stored fk for {} into pn_licenses'.
                                            format(company))
                        self.ct_stored += 1
                    else:
                        self.print_indented('Failed to store fk for {} into ' +
                                            'pn_licenses'.format(company))
                        self.remove_company_from_orgs(pg_conn,
                                                      single_response['properties']
                                                      ['name'])
                else:
                    self.print_indented('CANNOT LINK ORG {}: Too many License ' +
                                        'Contact Details ids returned'.
                                        format(company))
                    self.remove_company_from_orgs(pg_conn,
                                                  single_response['properties']
                                                  ['name'])
            else:
                pass

        pg_conn.close()
        self.print_indented("Leaving 'store_one_response()'")
        self.indent_level -= 1
        if stored_part_1 and stored_part_2:
            logging.info('{} stored in pn_organizations'.format(company))
            return True
        elif updated_part_1 and updated_part_2:
            logging.info('{} updated in pn_organizations'.format(company))
        else:
            logging.info('{} *not* stored or updated in pn_organizations'.format(company))
            return False

    def get_already_stored(self, pg_conn):
        """

        :param pg_conn:
        :return:
        """
        all_results = []
        query = 'SELECT domain FROM pn_organizations'
        cursor = pg_conn.cursor()
        cursor.execute(query, ())
        cursor_result = cursor.fetchone()
        while cursor_result:
            all_results.append(cursor_result[0])
            cursor_result = cursor.fetchone()
        cursor.close()
        return all_results

    def do_store_part_1(self, conn, data_item_org):  # single_response):
        """

        :param conn:
        :param data_item_org:
        :return:
        Called by: store_one_response()
        """
        if not self.sql_insert_org:
            self.setup_sql_insert_org()

        rowcount = 0
        cursor = conn.cursor()
        insert_1 = self.sql_insert_org
        if data_item_org:
            self.print_indented('Inserting company {} into pn_organizations'.
                                format(data_item_org[0]))
            cursor.execute(insert_1, data_item_org)
            rowcount = cursor.rowcount
        else:
            self.print_indented('Could not insert company {} into ' +
                                'pn_organizations'.format(data_item_org[0]))
        conn.commit()
        cursor.close()

        self.print_indented('rowcount is {} in do_store_part_1()'.format(rowcount))
        if cursor.statusmessage == 'INSERT 0 1':
            self.print_indented('Insert SUCCEEDS in do_store_part_1()')
        else:
            self.print_indented('Insert FAILS in do_store_part_1()')
        self.print_indented('STATUS MESSAGE: {}'.format(cursor.statusmessage))
        return rowcount

    def do_update(self, conn, data_item_org):
        """

        :param conn:
        :param data_item_org:
        :return:
        Called by: store_one_response()
        """
        if not self.sql_update_org:
            self.setup_sql_update_org()

        rowcount = 0
        cursor = conn.cursor()
        update_1 = self.sql_update_org

        if data_item_org:
            data_item_org_augmented = data_item_org[:] + [data_item_org[3]]

            changed = self.is_item_different(cursor, data_item_org_augmented)
            if not changed:
                cursor.close()
                if self.verbose:
                    print('new item same as old; not updating')
                return rowcount

            # data_item_org.append(data_item_org[3])
            self.print_indented('Updating company {} in pn_organizations'.
                                format(data_item_org[0]))
            cursor.execute(update_1, data_item_org_augmented)
            rowcount = cursor.rowcount
            if rowcount == 1:
                self.ct_stored += 1
        else:
            self.print_indented('Could not update company {} in ' +
                                'pn_organizations'.format(data_item_org[0]))
        conn.commit()
        cursor.close()

        self.print_indented('rowcount is {} in do_update()'.format(rowcount))
        if cursor.statusmessage == 'UPDATE 1':
            self.print_indented('Update SUCCEEDS in do_store_part_1()')
        else:
            self.print_indented('Update FAILS in do_store_part_1()')
        self.print_indented('STATUS MESSAGE: {}'.format(cursor.statusmessage))
        return rowcount

    @staticmethod
    def is_item_different(cursor, data_item_org_augmented):
        query = 'SELECT * FROM pn_organizations WHERE domain = %s;'
        data = data_item_org_augmented[3]
        cursor.execute(query, (data,))
        cursor_result = cursor.fetchone()
        result_list = [x for x in cursor_result[1:-1]]
        return result_list != data_item_org_augmented[:-2]

    @staticmethod
    def get_organization_id(conn, single_response):
        """

        :param conn:
        :param single_response:
        :return:
        Called by: store_one_response()
        """
        organization_id = None
        query = 'SELECT id FROM pn_organizations WHERE name = %s'
        data = single_response['properties']['name']
        cursor = conn.cursor()
        cursor.execute(query, (data,))
        cursor_result = cursor.fetchone()
        cursor.close()
        if cursor_result:
            organization_id = cursor_result[0]
        return organization_id

    @staticmethod
    def get_license_contact_details_id_list(conn, company):
        query = '(SELECT id FROM pn_license_contact_details ' \
                'WHERE company = %s);'

        data = company

        cursor = conn.cursor()
        cursor.execute(query, (data,))
        my_id_list = []
        my_result = cursor.fetchone()
        while my_result:
            my_id_list.append(my_result[0])
            my_result = cursor.fetchone()
        cursor.close()
        return my_id_list

    def do_store_part_2(self, conn, org_id, lic_ct_id):
        """

        :param conn:
        :param org_id:
        :param lic_ct_id:
        :return:
        """
        query = 'UPDATE pn_licenses SET organizations_id = %s ' \
                'WHERE license_contact_details_id = %s;'

        data = (org_id, lic_ct_id)
        cursor = conn.cursor()
        cursor.execute(query, data)
        rowcount = cursor.rowcount
        conn.commit()
        cursor.close()
        self.print_indented('rowcount is {} in do_store_part_2()'.
                            format(rowcount))
        return rowcount

    def remove_company_from_orgs(self, conn, company):
        """
        When a company inserted into pn_organizations cannot be linked to
            any counterpart in pn_licenses, delete the company from
            pn_organizations.
        :param conn:
        :param company:
        :return:
        Called by: store_one_response()
        """
        query = 'DELETE FROM pn_organizations o WHERE o.name = %s AND ' \
                '(SELECT DISTINCT l.organizations_id FROM pn_organizations o2 ' \
                'LEFT JOIN pn_licenses l ON l.organizations_id = o2.id ' \
                'WHERE o2.name = %s) IS NULL'

        data = company, company
        cursor = conn.cursor()
        cursor.execute(query, data)
        rowcount = cursor.rowcount
        conn.commit()
        cursor.close()
        if rowcount:
            if self.verbose:
                print('DELETED {} from pn_organizations'.format(company))
            return rowcount
        else:
            if self.verbose:
                print('FAILED TO DELETE {} from pn_organizations'.format(company))
            return -1

    def setup_sql_insert_org(self):
        """
        Set up SQL INSERT statement
        :return:
        Called by: store_one_response()
        """
        base_insert = ('INSERT INTO pn_organizations (name, primary_role, ' +
                       'short_description, domain, homepage_url, ' +
                       'facebook_url, twitter_url, linkedin_url, ' +
                       'api_url, city, region, country, stock_exchange, stock_symbol, ' +
                       'created_at, updated_at, pgres_last_updated) VALUES (%s, %s, %s, %s, ' +
                       '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')

        # update_insert = 'UPDATE SET (name, primary_role, short_description, ' \
        #                 'domain, homepage_url, facebook_url, twitter_url, ' \
        #                 'linkedin_url, city, region, ' \
        #                 'country, stock_exchange, stock_symbol, ' \
        #                 'created_at, updated_at) = (' \
        #                 'EXCLUDED.name, EXCLUDED.primary_role, ' \
        #                 'EXCLUDED.short_description, EXCLUDED.domain, ' \
        #                 'EXCLUDED.homepage_url, EXCLUDED.facebook_url, ' \
        #                 'EXCLUDED.twitter_url, EXCLUDED.linkedin_url, ' \
        #                 'EXCLUDED.city, EXCLUDED.region, ' \
        #                 'EXCLUDED.country, EXCLUDED.stock_exchange, ' \
        #                 'EXCLUDED.stock_symbol, EXCLUDED.created_at, ' \
        #                 'EXCLUDED.updated_at);'

        self.sql_insert_org = base_insert

    def setup_sql_update_org(self):
        """
               Set up SQL UPDATE statement
               :return:
               Called by: store_one_response()
               """
        update = ('UPDATE pn_organizations SET (name, primary_role, ' +
                  'short_description, domain, homepage_url, facebook_url, ' +
                  'twitter_url, linkedin_url, api_url, city, region, ' +
                  'country, stock_exchange, stock_symbol, ' +
                  'created_at, updated_at, pgres_last_updated) = (' +
                  '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' +
                  '%s, %s, %s, %s, %s) WHERE domain = %s;')

        self.sql_update_org = update

    def setup_data_item_org(self, single_response):
        """
        Setup data item for insertions to 'pn_organizations' table
        :param single_response: returned by cb orgs API
        :return: the data item
        Called by: do_store_part_1()
        """
        domain = single_response['properties']['domain']
        name = single_response['properties']['name']

        if domain is None or name is None:
            return None

        data_item_organizations = [single_response['properties']['name'],
                                   single_response['properties']
                                   ['primary_role'],
                                   single_response['properties']
                                   ['short_description'],
                                   single_response['properties']
                                   ['domain'].rstrip('/'),
                                   single_response['properties']
                                   ['homepage_url'],
                                   single_response['properties']
                                   ['facebook_url'],
                                   single_response['properties']
                                   ['twitter_url'],
                                   single_response['properties']
                                   ['linkedin_url'],
                                   single_response['properties']
                                   ['api_url'],
                                   single_response['properties']
                                   ['city_name'],
                                   single_response['properties']
                                   ['region_name'],
                                   single_response['properties']
                                   ['country_code'],
                                   single_response['properties']
                                   ['stock_exchange'],
                                   single_response['properties']
                                   ['stock_symbol'],
                                   single_response['properties']
                                   ['created_at'],
                                   single_response['properties']
                                   ['updated_at'],
                                   self.cur_time]
        return data_item_organizations

    def store_org(self, domain, choice):
        pass  # N.Y.I.

    @staticmethod
    def get_initials(input_string):
        """

        :param input_string:
        :return:
        """
        if not input_string:
            return ''
        out_string = input_string[0]
        for i in range(1, len(input_string)):
            if input_string[i - 1] == ' ':
                out_string += input_string[i]
        return out_string

    def convert_domain_search_output(self):
        """
        Convert contents of temporary file to valid JSON.
        Write to stdout and/or file as specified on command line.
        pre: either self.domain_search_outfile or
        self.domain_search_to_stdout is truthy
        :return: None
        Called by: get_each_license()
        """
        if not self.domain_search_outfile and not self.domain_search_to_stdout:
            return

        outfile = open(self.domain_search_outfile, 'w') \
            if self.domain_search_outfile else None
        outsys = sys.stdout if self.domain_search_to_stdout else None
        if outfile:
            self.write_json_to(self.temp_domain_search_file, outfile)
        if outsys:
            self.write_json_to(self.temp_domain_search_file, outsys)

    def convert_name_search_output(self):
        """
        Convert contents of temporary file to valid JSON.
        Write to stdout and/or file as specified on command line.
        pre: either self.name_search_outfile or
        self.name_search_to_stdout is truthy
        :return: None
        Called by: get_each_license()
        """
        outfile = open(self.name_search_outfile, 'w') \
            if self.name_search_outfile else None
        outsys = sys.stdout if self.name_search_to_stdout else None
        if outfile:
            self.write_json_to(self.temp_name_search_file, outfile)
        if outsys:
            self.write_json_to(self.temp_name_search_file, outsys)
        if outfile:
            outfile.close()

    @staticmethod
    def write_json_to(temp_file, fp):
        """
        Convert contents of temp file to JSON; write to fp
        :param fp: A file open for write, or sys.stdout
        :param temp_file:
        :return: None
        Called by: convert_domain_search_output()
        """
        try:
            with open(temp_file) as infile:
                print('[', file=fp)
                four_spaces = ' ' * 4
                for line in infile:
                    if line.rstrip() == '}{':
                        line_out = four_spaces + '},\n' + four_spaces + '{'
                    else:
                        line_out = four_spaces + line.rstrip()
                    print(line_out, file=fp)
                print(']', file=fp)
        except FileNotFoundError:
            pass

    def report_ok(self):
        return self.items_examined - self.items_skipped == \
               self.ct_isps + self.domain_misses + self.single_domain_hits + \
               self.multiple_domain_hits + self.repeat_domains

    def print_report(self):
        """
        Output stats at end of program run
        :return: None
        Called by: get_each_license()
        """
        print(('{} domains examined:\n' +
               '\t{} domains are ISPs\n' +
               '\t{} domains not found\n' +
               '\t{} single domain hits found\n' +
               '\t{} multiple domain hits found\n' +
               '\t{} repeat domains found\n' +
               '{} names examined:\n' +
               '\t{} names not found\n' +
               '\t{} single name hits found\n' +
               '\t{} multiple name hits found\n' +
               '{:.3f} secs spent in getting {} ' +
               'responses from crunchbase\n' +
               '{} orgs stored or updated in pn_organizations').
              format((self.items_examined -
                      self.items_skipped),
                     self.ct_isps,
                     self.domain_misses,
                     self.single_domain_hits,
                     self.multiple_domain_hits,
                     self.repeat_domains,
                     self.ct_name_queries,
                     self.name_misses,
                     self.single_name_hits,
                     self.multiple_name_hits,
                     self.time_used_cb,
                     (self.domain_misses +
                      self.single_domain_hits +
                      self.multiple_domain_hits +
                      self.name_misses +
                      self.single_name_hits +
                      self.multiple_name_hits),
                     self.ct_stored),
              file=sys.stderr)


def run_load_organizations():
    """Create LoadLicenses instance and call its methods"""
    lo = LoadOrganizations()
    lo.get_c_l_args()
    lo.get_env_vars()
    lo.setup_logging()
    lo.get_each_license()
    lo.print_report()


if __name__ == '__main__':
    run_load_organizations()

