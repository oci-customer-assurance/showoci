#!/usr/bin/env python3
##########################################################################
# Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
# This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
#
# DISCLAIMER This is not an official Oracle application,  It does not supported by Oracle Support,
# It should NOT be used for utilization calculation purposes, and rather OCI's official
#
# showoci_csv2adw.py
#
# @author: Adi Zohar
#
# Supports Python 3 and above
#
# coding: utf-8
##########################################################################
# showoci_csv2adw - Load showoci csv reports to ADW to be used with usage2adw,
# Currently supported: Compute + Block Volumes
#
##########################################################################
# Tables used:
# - OCI_SHOWOCI_COMPUTE
# - OCI_SHOWOCI_BLOCK_VOLUMES
##########################################################################
import sys
import argparse
import datetime
import csv
import oracledb
import time
import os

version = "23.02.14"


##########################################################################
# Print header centered
##########################################################################
def print_header(name, category):
    options = {0: 90, 1: 60, 2: 30}
    chars = int(options[category])
    print("")
    print('#' * chars)
    print("#" + name.center(chars - 2, " ") + "#")
    print('#' * chars)


##########################################################################
# Get Column from Array
##########################################################################
def get_column_value_from_array(column, array):
    if column in array:
        return array[column][0:3999]
    else:
        return ""


##########################################################################
# Get Currnet Date Time
##########################################################################
def get_current_date_time():
    return str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


##########################################################################
# print count result
##########################################################################
def get_time_elapsed(start_time):
    et = time.time() - start_time
    return ", Process Time " + str('{:02d}:{:02d}:{:02d}'.format(round(et // 3600), (round(et % 3600 // 60)), round(et % 60)))


##########################################################################
# set parser
##########################################################################
def set_parser_arguments():

    parser = argparse.ArgumentParser(formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=80, width=130))
    parser.add_argument('-csv', default="", dest='csvlocation', help='CSV Location from showoci including header')
    parser.add_argument('-du', default="", dest='duser', help='ADB User')
    parser.add_argument('-dp', default="", dest='dpass', help='ADB Password')
    parser.add_argument('-dn', default="", dest='dname', help='ADB DSN')

    parser.add_argument('-usethick', action='store_true', default=False, dest='usethick', help='Use sqlnet thick client library')
    parser.add_argument('-wl', default="", dest='wallet_location', help='Wallet Location')
    parser.add_argument('-wp', default="", dest='wallet_password', help='Wallet Password')

    parser.add_argument('--version', action='version', version='%(prog)s ' + version)

    result = parser.parse_args()

    if not (result.duser and result.dpass and result.dname and result.csvlocation):
        parser.print_help()
        print_header("You must specify database credentials and csv location!!", 0)
        return None

    return result


##########################################################################
# Check Table Structure for Compute
##########################################################################
def handle_compute(connection, csv_location):
    try:

        compute_json = {
            'table_name': "OCI_SHOWOCI_COMPUTE",
            'csv_file': "compute.csv",
            'items': [
                {'col': 'tenant_name           ', 'csv': 'tenant_name           ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":1  "},
                {'col': 'tenant_id             ', 'csv': 'tenant_id             ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":2  "},
                {'col': 'instance_id           ', 'csv': 'instance_id           ', 'type': 'varchar2(1000)', 'pk': 'y', 'fn': ":3  "},
                {'col': 'region_name           ', 'csv': 'region_name           ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":4  "},
                {'col': 'availability_domain   ', 'csv': 'availability_domain   ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":5  "},
                {'col': 'fault_domain          ', 'csv': 'fault_domain          ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":6  "},
                {'col': 'compartment_path      ', 'csv': 'compartment_path      ', 'type': 'varchar2(2000)', 'pk': 'n', 'fn': ":7  "},
                {'col': 'compartment_name      ', 'csv': 'compartment_name      ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":8  "},
                {'col': 'server_name           ', 'csv': 'server_name           ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":9  "},
                {'col': 'status                ', 'csv': 'status                ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":10 "},
                {'col': 'type                  ', 'csv': 'type                  ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":11 "},
                {'col': 'image                 ', 'csv': 'image                 ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":12 "},
                {'col': 'primary_vcn           ', 'csv': 'primary_vcn           ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":13 "},
                {'col': 'primary_subnet        ', 'csv': 'primary_subnet        ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":14 "},
                {'col': 'shape                 ', 'csv': 'shape                 ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":15 "},
                {'col': 'ocpus                 ', 'csv': 'ocpus                 ', 'type': 'number        ', 'pk': 'n', 'fn': "to_number(:16)"},
                {'col': 'memory_gb             ', 'csv': 'memory_gb             ', 'type': 'number        ', 'pk': 'n', 'fn': "to_number(:17)"},
                {'col': 'local_storage_tb      ', 'csv': 'local_storage_tb      ', 'type': 'number        ', 'pk': 'n', 'fn': "to_number(:18)"},
                {'col': 'public_ips            ', 'csv': 'public_ips            ', 'type': 'varchar2(500) ', 'pk': 'n', 'fn': ":19 "},
                {'col': 'private_ips           ', 'csv': 'private_ips           ', 'type': 'varchar2(500) ', 'pk': 'n', 'fn': ":20 "},
                {'col': 'security_groups       ', 'csv': 'security_groups       ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":21 "},
                {'col': 'internal_fqdn         ', 'csv': 'internal_fqdn         ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":22 "},
                {'col': 'time_created          ', 'csv': 'time_created          ', 'type': 'date          ', 'pk': 'n', 'fn': "to_date(:23,'YYYY-MM-DD HH24:MI')"},
                {'col': 'boot_volume           ', 'csv': 'boot_volume           ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":24 "},
                {'col': 'boot_volume_size_gb   ', 'csv': 'boot_volume_size_gb   ', 'type': 'number        ', 'pk': 'n', 'fn': "to_number(:25)"},
                {'col': 'boot_volume_b_policy  ', 'csv': 'boot_volume_b_policy  ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":26 "},
                {'col': 'boot_volume_encryption', 'csv': 'boot_volume_encryption', 'type': 'varchar2(20)  ', 'pk': 'n', 'fn': ":27 "},
                {'col': 'block_volumes         ', 'csv': 'block_volumes         ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":28 "},
                {'col': 'block_volumes_total_gb', 'csv': 'block_volumes_total_gb', 'type': 'number        ', 'pk': 'n', 'fn': "to_number(:29)"},
                {'col': 'block_volumes_b_policy', 'csv': 'block_volumes_b_policy', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":30 "},
                {'col': 'defined_tags          ', 'csv': 'defined_tags          ', 'type': 'varchar2(4000)', 'pk': 'n', 'fn': ":31 "},
                {'col': 'freeform_tags         ', 'csv': 'freeform_tags         ', 'type': 'varchar2(4000)', 'pk': 'n', 'fn': ":32 "},
                {'col': 'extract_date          ', 'csv': 'extract_date          ', 'type': 'date          ', 'pk': 'n', 'fn': "to_date(:33,'YYYY-MM-DD HH24:MI:SS')"}
            ]
        }
        handle_table(connection, compute_json, csv_location)
    except Exception as e:
        raise Exception("\nError at procedure: handle_compute - " + str(e))


##########################################################################
# Check Table Structure for Compute
##########################################################################
def handle_block_volume(connection, csv_location):
    try:

        compute_json = {
            'table_name': "OCI_SHOWOCI_BLOCK_VOLUMES",
            'csv_file': "block_volumes.csv",
            'items': [
                {'col': 'tenant_name        ', 'csv': 'tenant_name        ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":1  "},
                {'col': 'tenant_id          ', 'csv': 'tenant_id          ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":2  "},
                {'col': 'id                 ', 'csv': 'id                 ', 'type': 'varchar2(1000)', 'pk': 'y', 'fn': ":3  "},
                {'col': 'region_name        ', 'csv': 'region_name        ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":4  "},
                {'col': 'availability_domain', 'csv': 'availability_domain', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":5  "},
                {'col': 'fault_domain       ', 'csv': 'fault_domain       ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":6  "},
                {'col': 'compartment_path   ', 'csv': 'compartment_path   ', 'type': 'varchar2(2000)', 'pk': 'n', 'fn': ":7  "},
                {'col': 'compartment_name   ', 'csv': 'compartment_name   ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":8  "},
                {'col': 'display_name       ', 'csv': 'display_name       ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":9  "},
                {'col': 'size_gb            ', 'csv': 'size               ', 'type': 'number        ', 'pk': 'n', 'fn': "to_number(:10)"},
                {'col': 'backup_policy      ', 'csv': 'backup_policy      ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":11 "},
                {'col': 'vpus_per_gb        ', 'csv': 'vpus_per_gb        ', 'type': 'number        ', 'pk': 'n', 'fn': "to_number(:12)"},
                {'col': 'volume_group_name  ', 'csv': 'volume_group_name  ', 'type': 'varchar2(1000)', 'pk': 'n', 'fn': ":13 "},
                {'col': 'instance_name      ', 'csv': 'instance_name      ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":14 "},
                {'col': 'instance_id        ', 'csv': 'instance_id        ', 'type': 'varchar2(100) ', 'pk': 'n', 'fn': ":15 "},
                {'col': 'defined_tags       ', 'csv': 'defined_tags       ', 'type': 'varchar2(4000)', 'pk': 'n', 'fn': ":16 "},
                {'col': 'freeform_tags      ', 'csv': 'freeform_tags      ', 'type': 'varchar2(4000)', 'pk': 'n', 'fn': ":17 "},
                {'col': 'extract_date       ', 'csv': 'extract_date       ', 'type': 'date          ', 'pk': 'n', 'fn': "to_date(:18,'YYYY-MM-DD HH24:MI:SS')"}
            ]
        }
        handle_table(connection, compute_json, csv_location)
    except Exception as e:
        raise Exception("\nError at procedure: handle_compute - " + str(e))


##########################################################################
# Check Table Structure for Compute
##########################################################################
def handle_table(connection, inputdata, csv_location):
    process_location = "Start"
    try:

        start_time = time.time()

        # Parameters
        csv_file = inputdata['csv_file']
        path_filename = csv_location + '_' + csv_file
        table_name = inputdata['table_name']
        tmp_table_name = table_name + "_TMP"
        compute_sql_columns = str(', '.join(x['col'] + " " + x['type'] for x in inputdata['items']))
        merge_sql_columns = str(', '.join("a." + x['col'] + " = b." + x['col'] for x in inputdata['items'] if x['pk'] != "y"))
        insert_def_sql_columns = str(', '.join(x['col'] for x in inputdata['items']))
        insert_val_sql_columns = str(', '.join("b." + x['col'] for x in inputdata['items']))
        insert_bulk_func = str(', '.join(x['fn'] for x in inputdata['items']))
        primary_key = next((col for col in inputdata['items'] if col['pk'] == "y"), None)['col']

        print("\nHandling " + csv_file)

        # Check if file exist
        if not os.path.isfile(path_filename):
            print("   file " + path_filename + " does not exist, skipping...")
            return

        ################################################
        # Check Table Structure and create if not exist
        ################################################
        process_location = "Checking Table Structure"

        # check if tables exist
        with connection.cursor() as cursor:

            sql = "select count(*) from user_tables where table_name = :table_name"
            cursor.execute(sql, table_name=table_name)
            val, = cursor.fetchone()

            # if main table not exist, create it
            if val == 0:
                print("   Table " + table_name + " was not exist, creating")
                sql = "create table " + table_name + " ( " + compute_sql_columns + " ,CONSTRAINT " + table_name + "_PK PRIMARY KEY (" + primary_key + ") USING INDEX) "
                cursor.execute(sql)
                print("   Table " + table_name + " created")
            else:
                print("   Table " + table_name + " exist")

            # check if temp table exist, if not create
            sql = "select count(*) from user_tables where table_name = :table_name"
            cursor.execute(sql, table_name=tmp_table_name)
            val, = cursor.fetchone()

            # if table not exist, create it
            if val == 0:
                print("   Table " + tmp_table_name + " was not exist, creating")
                sql = "create GLOBAL TEMPORARY TABLE " + tmp_table_name + " ( " + compute_sql_columns + " ) ON COMMIT PRESERVE ROWS "
                cursor.execute(sql)
                print("   Table " + tmp_table_name + " created")
            else:
                print("   Table " + tmp_table_name + " exist")

        ################################################
        # Load Data
        ################################################
        num_rows = 0
        process_location = "Before Load Data"

        with open(path_filename, 'rt') as file_in:
            csv_reader = csv.DictReader(file_in)

            # Adjust the batch size to meet memory and performance requirements for oracledb
            batch_size = 5000
            array_size = 1000

            sql = "INSERT INTO " + tmp_table_name + " ("
            sql += insert_def_sql_columns
            sql += ") VALUES ( "
            sql += insert_bulk_func + ")"

            # insert bulk to database
            with connection.cursor() as cursor:

                # Predefine the memory areas to match the table definition
                cursor.setinputsizes(None, array_size)
                process_location = "before CSV load"

                data = []
                for row in csv_reader:
                    rowarray = []
                    for item in inputdata['items']:
                        column = str(item['csv']).strip()
                        value = get_column_value_from_array(column, row)
                        rowarray.append(value)
                    data.append(tuple(rowarray))
                    num_rows += 1

                    # executemany every batch size
                    process_location = "before executemany"
                    if len(data) % batch_size == 0:
                        cursor.executemany(sql, data)
                        data = []

                # if data exist final execute
                if data:
                    cursor.executemany(sql, data)

                print("   Completed file " + csv_file + " - " + str(num_rows) + " Rows Inserted")
                connection.commit()

        ################################################
        # Merge data from tmp to main table
        ################################################
        process_location = "before Merge"

        with connection.cursor() as cursor:

            print("   Merging data to main table...")

            # run merge to oci_update_stats
            sql = "merge into " + table_name + " a using " + tmp_table_name + " b "
            sql += "on (a." + primary_key + " = b." + primary_key + ")"
            sql += "when matched then update set "
            sql += merge_sql_columns
            sql += "when not matched then insert ("
            sql += insert_def_sql_columns
            sql += ") values ("
            sql += insert_val_sql_columns + ")"

            cursor.execute(sql)
            connection.commit()
            print("   Merge Completed, " + str(cursor.rowcount) + " rows merged")

            print("   " + csv_file + " Completed " + get_time_elapsed(start_time))

    except oracledb.DatabaseError as e:
        print("\nDatabaseError at procedure: handle_table() - " + process_location + " - " + str(e) + "\n")
        raise SystemExit

    except Exception as e:
        raise Exception("\nError at Procedure: handle_table() - " + process_location + " - " + str(e))


##########################################################################
# Main
##########################################################################
def main_process():
    cmd = set_parser_arguments()
    if cmd is None:
        exit()

    ############################################
    # Start
    ############################################
    print_header("Running ShowOCI_CSV2ADW", 0)
    print("Starts at " + get_current_date_time())
    print("Command Line : " + ' '.join(x for x in sys.argv[1:]))
    print("Version      : " + version)

    # Init the Oracle Thick Client Library in order to use sqlnet.ora and instant client
    if cmd.usethick:
        oracledb.init_oracle_client()
        print("OracleDB     : Thick Drivers")
    else:
        print("OracleDB     : Thin Drivers")

    ############################################
    # connect to database
    ############################################
    try:
        print("\nConnecting to database " + cmd.dname)
        with oracledb.connect(user=cmd.duser, password=cmd.dpass, dsn=cmd.dname, config_dir=cmd.wallet_location, wallet_location=cmd.wallet_location, wallet_password=cmd.wallet_password) as connection:

            print("   Connected")

            # Handling CSVs
            handle_compute(connection, cmd.csvlocation)
            handle_block_volume(connection, cmd.csvlocation)

    except oracledb.DatabaseError as e:
        print("\nError manipulating database - " + str(e) + "\n")

    except Exception as e:
        print("\nError appeared - " + str(e))

    ############################################
    # print completed
    ############################################
    print("\nCompleted at " + get_current_date_time())


##########################################################################
# Execute Main Process
##########################################################################
main_process()
