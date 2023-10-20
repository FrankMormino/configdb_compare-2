import os
import sys
import argparse
import logging
import time
import beautifultable
from beautifultable.helpers import BTRowCollection, BTColumnCollection
import peewee
from pathlib import Path


class MySQLCompare:
    SELECT_QUERY = "SELECT * FROM {table}"
    WORK_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    PROJECT_ROOT = WORK_DIR.parent.parent
    OPERATING_SYSTEM = os.name

    ALENZA_CONFIG_SCRIPTS_RELATIVE = "alenza/config/inserts"
    AICON_CONFIG_SCRIPTS_RELATIVE = "aicon/config/inserts"
    CLIENT_SPECIFIC_DIRECTORY = "{}/client".format(AICON_CONFIG_SCRIPTS_RELATIVE)

    TOTAL_WARNINGS = 0

    CONFIG_TABLE_NAME = "config_field"

    # -- Logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    logFormat = "[%(levelname)s] %(asctime)s -- %(message)s"
    logging.basicConfig(format=logFormat, level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p')

    logger.info("Working Directory: {}".format(WORK_DIR))
    logger.info("PROJECT ROOT DIRECTORY: {}".format(PROJECT_ROOT))
    logger.info("OS: {}".format(OPERATING_SYSTEM))

    def __init__(
            self,
            host,
            port,
            database,
            user,
            password,
            max_table_width,
            is_print_table_diff,
            is_print_stored_procs,
            client_specific_keyword,
            comparison_direction
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.max_table_width = max_table_width
        self.is_print_table_diff = is_print_table_diff
        self.is_print_stored_procs = is_print_stored_procs
        self.client_specific_keyword = client_specific_keyword
        self.comparison_direction = comparison_direction
        self.cursor = self.connect()
        self.database_warnings = 0

    def fetch_data_from_db(self, query):
        try:
            cursor = self.cursor.execute_sql(query.format(table=self.CONFIG_TABLE_NAME))
            return cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Failed to fetch data from the database: {e}")
            raise

    def logging_line_break(self):
        """
        Print a log break line
        :return:
        """
        self.logger.info("")

    def generate_missing_stored_proc_statement(self, entries):
        """
        Generate and print stored procedure calls to console to create missing configuration keys
        :param entries:
        :return:
        """
        if len(entries) > 0 and self.is_print_stored_procs:
            print("\n" * 5)
            print("Generating procedure calls ...")
            print("**Printing to make copy-paste-able")
            print("----------" * 5)
            all_missing_key_stored_proc_statements = []
            all_diff_key_stored_proc_statements = []
            for entry in entries:
                difference_type = entry[0]
                config_name = entry[1]
                config_value_database = entry[2]
                config_value_script = entry[3]
                if self.comparison_direction == "scripts":
                    statement = "CALL SP_CREATE_CONFIG_FIELD('{k}', '{v}', null);".format(
                        k=config_name, v=config_value_script
                    )
                elif self.comparison_direction == "server":
                    statement = "CALL SP_CREATE_CONFIG_FIELD('{k}', '{v}', null);".format(
                        k=config_name, v=config_value_database
                    )
                else:
                    self.logger.error("UNKNOWN COMPARISON DIRECTION: {}".format(self.comparison_direction))
                if difference_type == "MISSING":
                    all_missing_key_stored_proc_statements.append(statement)
                elif difference_type == "CHANGE":
                    all_diff_key_stored_proc_statements.append(statement)
                else:
                    self.logger.error("UNKNOWN COMPARISON TYPE: {}".format(difference_type))
            if self.comparison_direction == "scripts":
                print("STORED PROCEDURES FOR MISSING KEYS:")
                for stored_proc in all_missing_key_stored_proc_statements:
                    print(stored_proc)
            elif self.comparison_direction == "server":
                print(
                    "STORED PROCEDURES FOR MISSING KEYS: Since we're trying to CHANGE THE SCRIPTS to MATCH THE SERVER,"
                    "this DOES NOT APPLY! Skipping...")
            print("----------" * 5)
            print("\n" * 5)
            print("STORED PROCEDURES FOR KEYS WITH DIFFERENT VALUES:")
            for stored_proc in all_diff_key_stored_proc_statements:
                print(stored_proc)
            print("----------" * 5)

    @staticmethod
    def parse_stored_proc_statement(statement):
        """
        Parse a stored procedure call statement from .sql script
        :param statement:
        :return:
        """
        line = statement.split('(', 1)[1]
        line = line.replace('"', '')
        line = line.replace("'", '')
        config_name = line.split(",")[0].strip()
        config_value = line.split(",")[1].strip()
        return config_name, config_value

    def connect(self):
        """
        Connect to a MySQL database.

        :return: Database connection object.
        :raises peewee.OperationalError: If connection to the database fails.
        """
        connection = peewee.MySQLDatabase(
            self.database,
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        self.logger.info(f"Attempting to connect to MySQL database at {self.host}:{self.port}...")
        try:
            connection.connect()
            self.logger.info("MySQL Connection Status: Connected")
        except peewee.OperationalError as e:
            self.logger.error(f'MySQL Connection Status: Not Connected. {e}')
            sys.exit(1)
        return connection

    def close_connection(self):
        self.cursor.close()
        self.logger.info("Database connection closed.")

    @staticmethod
    def format_windows_path(operating_system, path):
        """
        If executing on Windows OS, format path to work with Windows
        :param operating_system:
        :param path:
        :return:
        """
        if operating_system == "nt":
            # Windows
            return path.replace("/", "\\")
        else:
            # Linux
            return path

    def filter_data(self, data, substring_condition):
        filtered_data = {}
        for row in data:
            valid_date = row[5]
            if valid_date.strftime('%Y') != '9999':
                continue
            config_name = row[2]
            config_value = row[3]
            if not substring_condition or (substring_condition and substring_condition in config_name):
                filtered_data[config_name] = config_value
        return filtered_data

    def check_for_duplicates(self, data):
        total_warnings = 0
        flagged_keys = dict()
        for config_name, config_value in data.items():
            if config_name in flagged_keys:
                total_warnings += 1
                self.logger.warning(
                    f"'{config_name}' is duplicated with value '{config_value}' in Configuration table")
            flagged_keys[config_name] = config_value
        return flagged_keys, total_warnings

    def analyze_database_config(self, substring_condition):
        self.logger.info("Fetching data from database...")
        data = self.fetch_data_from_db(self.SELECT_QUERY.format(table=self.CONFIG_TABLE_NAME))
        self.logger.info("Filtering data...")
        filtered_data = self.filter_data(data, substring_condition)
        """
        Analyze the database configuration table to retrieve configuration table rows.
        :param substring_condition: Substring to filter keys.
        :return: Dictionary of flagged keys.
        """
        flagged_keys, total_warnings = self.check_for_duplicates(filtered_data)

        try:
            # Process the results
            for config_name, config_value in filtered_data.items():
                if config_name in flagged_keys:
                    total_warnings += 1
                    self.logger.warning(
                        f"'{config_name}' is duplicated with value '{config_value}' in Configuration table")

                # If no substring was provided, take all keys. Else, only take keys that contain substring
                if not substring_condition or (substring_condition and substring_condition in config_name):
                    flagged_keys[config_name] = config_value
        except Exception as e:
            self.logger.error(f"Error processing SQL results: {e}")
            return flagged_keys  # Return whatever keys have been processed so far in case of error

        self.logger.info(f"Returning {len(flagged_keys)} keys from database table...")
        self.TOTAL_WARNINGS += total_warnings
        return flagged_keys

    def analyze_scripts_config(self, scripts_dir, substring_condition):
        """
        Analyze the configuration table insert .sql scripts to retrieve and parse configuration procedure statements
        :param scripts_dir:
        :param substring_condition:
        :return:
        """
        flagged_keys = dict()
        total_warnings = 0
        all_sql_config_insert_scripts = []
        files_directory = self.format_windows_path(
            operating_system=self.OPERATING_SYSTEM,
            path="{root}/{relative}".format(root=self.PROJECT_ROOT, relative=scripts_dir)
        )
        self.logger.info("Globbing files from '{}' ...".format(files_directory))
        for path in Path(files_directory).rglob('*.sql'):
            path_str = str(path)
            if os.path.normpath(self.CLIENT_SPECIFIC_DIRECTORY) in path_str:
                self.logger.info("CLIENT-SPECIFIC FILE FOUND: {}".format(path_str))
                if self.client_specific_keyword and self.client_specific_keyword in path_str:
                    self.logger.info(" -- FILE IS SPECIFIC TO CLIENT! Adding... ")
                    all_sql_config_insert_scripts.append(path)
                elif not self.client_specific_keyword:
                    self.logger.warning("-- CLIENT SPECIFIC KEYWORD NOT PROVIDED! Skipping file...")
                else:
                    self.logger.info(" -- FILE IS NOT FOR THIS CLIENT! Will NOT add... ")
            else:
                self.logger.info("FILE FOUND: {}".format(path))
                all_sql_config_insert_scripts.append(path)
        self.logger.info("Total files found: {}".format(len(all_sql_config_insert_scripts)))
        self.logging_line_break()
        self.logger.info("Starting file analysis...")
        for filename in all_sql_config_insert_scripts:
            self.logger.info(" -- Reading File {}".format(filename))
            with open(filename, 'r') as f:
                flagged = 0
                lines = f.readlines()
                self.logger.info(" -- -- Parsing File {}".format(filename))
                for line in lines:
                    if not line.startswith('CALL') or 'CAST' in line:
                        # Not a stored procedure
                        continue
                    else:
                        config_name, config_value = self.parse_stored_proc_statement(statement=line)
                    if config_name in flagged_keys:
                        # Found duplicated config key
                        total_warnings += 1
                        self.logger.info('WARN: {key} is duplicated with value {value} in INSERT SCRIPTS'.format(
                            key=config_name,
                            value=config_value
                        ))
                    # If no substring was provided, take all keys. Else, only take keys that contain substring
                    if not substring_condition or (substring_condition and substring_condition in config_name):
                        flagged_keys[config_name] = config_value
                        flagged += 1
                self.logger.info("-- -- -- Flagged Keys: {}".format(flagged))
                self.logging_line_break()
        self.logger.info(
            "Returning {} keys from {} files...".format(len(flagged_keys), len(all_sql_config_insert_scripts)))
        return flagged_keys, total_warnings

    def compare_database_to_scripts(self, database_keys, script_keys):
        """
        Compare the state of the database configuration table to that of the .sql scripts to detect differences.
        :param database_keys: Dictionary of keys from the database.
        :param script_keys: Dictionary of keys from the scripts.
        :return: List of differences.
        """
        differences = []

        # Ensure the input arguments are dictionaries
        if not isinstance(database_keys, dict) or not isinstance(script_keys, dict):
            self.logger.error("Input arguments must be dictionaries.")
            return differences  # Return empty list in case of error

        try:
            table = beautifultable.BeautifulTable()
            table.maxwidth = self.max_table_width
            table.header = [
                'DIFFERENCE TYPE',
                'CONFIG NAME',
                f'CONFIG VALUE ({self.database}@{self.host}:{self.port})',
                'CONFIG VALUE (.sql Scripts)'
            ]
        except Exception as e:
            self.logger.error(f"Failed to create table: {e}")
            return differences  # Return empty list in case of error

        try:
            for config_name in script_keys:
                if config_name in database_keys:
                    if script_keys[config_name] != database_keys[config_name]:
                        config_value_database = database_keys[config_name]
                        config_value_script = script_keys[config_name]
                        entry = ['CHANGE', config_name, config_value_database, config_value_script]
                        differences.append(entry)
                        table.rows.append(entry)
                else:
                    config_value_script = script_keys[config_name]
                    entry = ['MISSING', config_name, 'N/A', config_value_script]
                    differences.append(entry)
                    table.rows.append(entry)
        except Exception as e:
            self.logger.error(f"Error comparing keys: {e}")
            # Optionally, you could still return the differences found so far
            # return differences

        self.logger.info(f"Differences Detected: {len(differences)}")
        if self.is_print_table_diff:
            try:
                print(table)
            except Exception as e:
                self.logger.error(f"Error printing table: {e}")

        return differences

    def notify_warnings(self, additional_warnings):
        try:
            # Ensure additional_warnings is a numeric type
            additional_warnings = int(additional_warnings)
        except ValueError as e:
            self.logger.error(f"Invalid value for additional_warnings: {e}")
            return  # Exit the method early in case of error

        try:
            self.TOTAL_WARNINGS += self.database_warnings
            self.TOTAL_WARNINGS += additional_warnings
        except TypeError as e:
            self.logger.error(f"Error updating TOTAL_WARNINGS: {e}")
            return  # Exit the method early in case of error

        if self.TOTAL_WARNINGS:
            try:
                self.logger.warning(
                    f'TOTAL WARNINGS FOUND: {self.TOTAL_WARNINGS}. Please review, as duplicates can cause unexpected results!'
                )
            except Exception as e:
                self.logger.error(f"Error logging warnings: {e}")
        else:
            try:
                self.logger.info("No warnings found.")
            except Exception as e:
                self.logger.error(f"Error logging info: {e}")


def main(args):
    mysql = None  # Declare mysql here, so it's accessible in the finally block
    try:
        logging.info("Arguments: " + str(args))

        logging.warning("IMPORTANT: The 'Comparison Direction' (-cd) is set to '{}'".format(args.comparison_direction))
        if args.comparison_direction == "scripts":
            logging.warning("This means that the main method will compare: THE MYSQL SERVER to THE SQL SCRIPTS! "
                            "This will generate results to MATCH THE SERVER TO THE SCRIPTS")
        elif args.comparison_direction == "server":
            logging.warning("This means that the main method will compare: THE SQL SCRIPTS to THE MYSQL SERVER! "
                            "This will generate results to REPRODUCE THE MYSQL SERVER!")
        logging.warning("Sleeping for 10 seconds so you read this...")
        time.sleep(10)
    except Exception as e:
        logging.error(f"Error during argument logging: {e}")
        return  # Exit the method early in case of error

    try:
        mysql = MySQLCompare(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.passwd,
            max_table_width=args.max_table_width,
            is_print_table_diff=args.print_table,
            is_print_stored_procs=args.print_stored_procs,
            client_specific_keyword=args.client_keyword,
            comparison_direction=args.comparison_direction
        )
    except Exception as e:
        logging.error(f"Error initializing MySQLCompare: {e}")
        return  # Exit the method early in case of error

    try:
        database_flagged_keys = mysql.analyze_database_config(substring_condition=args.contains)
        mysql.logging_line_break()
        alenza_scripts_flagged_keys, alenza_script_warnings = mysql.analyze_scripts_config(
            scripts_dir=mysql.ALENZA_CONFIG_SCRIPTS_RELATIVE,
            substring_condition=args.contains
        )
        mysql.logging_line_break()
        aicon_scripts_flagged_keys, aicon_script_warnings = mysql.analyze_scripts_config(
            scripts_dir=mysql.AICON_CONFIG_SCRIPTS_RELATIVE,
            substring_condition=args.contains
        )
        all_scripts_flagged_keys = {**alenza_scripts_flagged_keys, **aicon_scripts_flagged_keys}
    except Exception as e:
        logging.error(f"Error analyzing configurations: {e}")
        return  # Exit the method early in case of error

    try:
        differences = mysql.compare_database_to_scripts(
            database_keys=database_flagged_keys,
            script_keys=all_scripts_flagged_keys
        )
        if differences:
            mysql.generate_missing_stored_proc_statement(differences)
        else:
            mysql.logger.info("No flagged keys were detected!")
    except Exception as e:
        logging.error(f"Error comparing database to scripts: {e}")

    try:
        mysql.notify_warnings(additional_warnings=alenza_script_warnings + aicon_script_warnings)
    except Exception as e:
        logging.error(f"Error notifying warnings: {e}")
    finally:
        if mysql is not None:
            mysql.close_connection()  # Ensure the connection is closed


if __name__ == '__main__':
    # -- Parser
    parser = argparse.ArgumentParser(description="MySQL Comparison Script")
    # MySQL Connection
    parser.add_argument("--host", type=str, required=True,
                        help="Database hostname or IP")
    parser.add_argument("--port", type=int, required=True,
                        help="Database host port")
    parser.add_argument("-d", "--database", type=str, required=True,
                        help="Database name")
    parser.add_argument("-u", "--user", type=str, required=True,
                        help="Database user")
    parser.add_argument("-p", "--passwd", type=str, required=True,
                        help="Database password")

    # Comparison Direction
    parser.add_argument("-cd", "--comparison_direction", type=str.lower,
                        help="Comparison Direction. Either 'scripts' or 'server'. "
                             "If 'scripts', show procedure for server to match scripts. "
                             "If 'server', show procedure for scripts to match server.",
                        choices=['scripts', 'server'],
                        default="scripts")

    # Filters
    parser.add_argument("-c", "--contains", type=str, default='',
                        help="Limit the results to containing the input substring")

    # Client Specific
    parser.add_argument("-x", "--client_keyword", type=str, default=None,
                        help="If client-specific deploy, restrict .sql script search to ONLY client's files")

    # Stored Procedure Generation
    parser.add_argument("-s", "--print_stored_procs", type=bool, default=False,
                        help="Print Stored Procedures needed to convert to match .sql scripts and destination server")

    # Table
    parser.add_argument("-t", "--print_table", type=bool, default=False,
                        help="Print Table of Differences")
    parser.add_argument("-m", "--max_table_width", type=int, default=300,
                        help="The maximum table width of the results table.")
    try:
        main(parser.parse_args())
    except Exception as e:
        logging.error(f"Error executing main: {e}")
