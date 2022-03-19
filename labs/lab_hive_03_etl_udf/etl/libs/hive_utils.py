"""
Hive utility functions.
"""

# System imports.
from __future__ import print_function
import os
import re
import tempfile
from datetime import datetime


# Libs.
from .cli_utils import write_info, write_plain, AppError
from .cli_utils import write_txt_file, write_error
from .cli_utils import execute_shell_command
from .cli_utils import EXIT_CODE_FAILURE, EXIT_CODE_SUCCESS

# Version information.

PROGRAM_VERSION = '1.0.5'


class Template(object):
    """A Jinja 2 replacement"""
    def __init__(self, query_tpl):
        self.query_tpl = query_tpl
        self.debug = False

    def render(self, ctx):
        """Renders the result replacing values on the template."""
        result = ''
        try:
            result = self.query_tpl.format(**ctx)
        except KeyError as error:
            write_error('Nothing match {0}\n'.format(error))
            keys = [x for x in ctx.keys()]
            write_error('Received: {0}\n'.format(', '.join(keys)))
        return result

    def activate_debug(self):
        """On debug mode we write more messages to the log."""
        self.debug = True


def submit_hive_query(query, dry_run=False):
    """Submits a Hive query to Hadoop.

    Parameters
    ----------
    dry_run: If true just prints the query instead of executing it.
    query: string containing the query.
    """
    file_name = None

    def valid_result(result_line):
        """Return true if is a data line otherwise returns False

        Parameters
        ----------
        result_line: Check if result is valid or error.
        """
        if 'WARN  - [main:] ~ HiveConf ' in result_line:
            return False
        if not result_line.strip():
            return False
        return True

    try:
        file_desc, file_name = tempfile.mkstemp(
            prefix='query_', suffix='.hql')
        tmp_file = os.fdopen(file_desc, 'w')
        tmp_file.write(query)
        tmp_file.write('\n')
        tmp_file.close()
        write_plain('Hive Query:')
        with open(file_name) as infile:
            write_plain(infile.read())
        if dry_run:
            results, return_code = [], EXIT_CODE_SUCCESS
        else:
            results, return_code = execute_shell_command(
                'hive -f ' + file_name)
        results = [x for x in results if valid_result(x)]
    except IOError:
        results, return_code = [], EXIT_CODE_FAILURE
    finally:
        if file_name:
            os.remove(file_name)
    return results, return_code


def hive_query_template(query_template, ctx, debug_mode=False):
    """Render Hive query template, executes it an returns result.

    Parameters
    ----------
    query_template: the template representing the query
    ctx: dictionary containing the data to fill template.
    debug_mode: if true just prints the commands instead of running it.
    """
    template = Template(query_template)
    query = template.render(ctx)
    if debug_mode:
        write_info(query)
        return [], 0
    return submit_hive_query(query)


def hive_query(query_str, job_name, debug_mode=False):
    """Executes the Hive query and returns results.

    Parameters
    ----------
    query_str: hive query to be executed.
    job_name: Hadoop Job name used to run it.
    debug_mode: if debug mode just print the command.
    """
    ctx = dict()
    ctx['job_name'] = job_name
    ctx['query'] = query_str
    query_tpl = """
        SET mapred.job.name={job_name}
        {query}
        ;
    """
    results, code = hive_query_template(query_tpl, ctx, debug_mode)
    return results, code


def capture_hive_query(query_str, file_prefix, debug_mode=False):
    """Executes the Hive query and saves results."""
    now = datetime.now()
    output_file = '{0}_{1}.txt'.format(
        file_prefix, now.strftime("%Y%m%d_%H%M"))
    job_name = 'Hive: File {0}'.format(output_file)
    results, code = hive_query(query_str, job_name, debug_mode)
    write_txt_file(output_file, results)
    return code


def resolve_template(query_template, context):
    """Render Hive query template, executes it an returns result."""
    template = Template(query_template)
    return template.render(context)


# ---- Hive commands ----


def analyze_table(db_name, table_name, partition_spec=''):
    """Analyze and compute stats of a Hive table"""
    ctx = dict()
    ctx['db_name'] = db_name
    ctx['table_name'] = table_name
    ctx['partition'] = partition_spec
    query_tpl = """
        ANALYZE TABLE {db_name}.{table_name}
           {partition} COMPUTE STATISTICS
        ;
    """
    results, code = hive_query_template(query_tpl, ctx)
    if code != EXIT_CODE_SUCCESS:
        write_error('Failed to analyze table {0}.'.format(
            table_name))
    return results, code


def drop_table(db_name, table_name):
    """Drop hive table"""
    ctx = dict()
    ctx['db_name'] = db_name
    ctx['table_name'] = table_name
    query_tpl = """
        DROP TABLE {{ctx.db_name}}.{{ctx.table_name}}
        ;
    """
    results, code = hive_query_template(query_tpl, ctx)
    if code != EXIT_CODE_SUCCESS:
        write_error('Failed drop table {0}.'.format(
            table_name))
    return results, code


def print_results(results, exit_code):
    """Prints result data and exit code"""
    if exit_code:
        write_error('Failed!')
    else:
        for line in results:
            print(line)
        write_info('Done!')


def find_string(string_list, token):
    """Finds at least one line with the string."""
    for line in string_list:
        if line.find(token) >= 0:
            return True
    return False


def find_partition(table_name, partition):
    """Returns the last full build"""
    ctx = dict()
    ctx['table'] = table_name
    if partition:
        ctx['partition'] = 'PARTITION ({0})'.format(partition)
    else:
        ctx['partition'] = ''
    query_tpl = """
        SHOW PARTITIONS {table} {partition}
        ;
    """
    results, code = hive_query(query_tpl, ctx)
    if code != EXIT_CODE_SUCCESS:
        raise AppError('load failed')
    partition_pattern = re.sub("'", '', partition)
    return (code == 0) and (find_string(results, partition_pattern))
