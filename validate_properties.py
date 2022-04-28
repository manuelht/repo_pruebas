import sys
import os
import re
import logging
import shutil
import pendulum
import copy
from ruamel.yaml import YAML
from datetime import datetime
import dateutil.parser as parser
#from inditex_commons import validations
import validations


logging.basicConfig(level=logging.INFO, format='[%(asctime)s] - [%(levelname)s] - %(message)s')
logger = logging.getLogger()


# Working with RawConfigParse only with sections and proper properties file
"""
config = configparser.RawConfigParser()
config.read('test-pipeline.properties')
config.sections()
"""

VALID_CRON_STRINGS = [
    '@hourly',
    '@daily',
    '@weekly',
    '@monthly',
    '@yearly'
]


def is_valid_cron_schedule(x):
    return x in VALID_CRON_STRINGS


def is_valid_cron_every(x):
    reg_exp = r'(@every (\d+(ns|us|µs|ms|s|m|h))+)'
    return bool(re.search(reg_exp, x))


def is_valid_cron_code(cron):
    # Simplified CRON: accepts * or a valid digit
    # base_reg_exp = r'^((((\d+,)+\d+|(\d+(\/|-)\d+)|\d+|\*) ?))$'
    base_reg_exp = r'^(\d|\*)$'
    digits = ['[1-5]?[0-9]',
              '2[0-3]|1[0-9]|[0-9]',
              '3[01]|[12][0-9]|[1-9]',
              '1[0-2]|[1-9]',
              '[0-6]']
    values = [x.strip() for x in cron.split(' ')]
    if len(values) != 5:
        return False
    for i in range(5):
        if bool(re.search(base_reg_exp.replace('\d', digits[i]), values[i])):
            continue
        return False
    return True


def is_valid_cron(x):
    return is_valid_cron_schedule(x) or is_valid_cron_code(x)


def get_list_from_string_commas(input_str):
    '''
    For a comma separated string of values, returns a list of values
    Removes empty values ( Ex. "a,,b" -> returns ['a','b'])

    :param input_str: str like "a,b,c"
    :return: list of comma separated values from string
             if input is not str, returns []
    '''
    if isinstance(input_str, str):
        return [x.strip() for x in input_str.split(',') if x != '']
    return []


def is_valid_comma_separated_list(x):
    return True if len(get_list_from_string_commas(x)) > 0 else False


def round_to_minute(minute, round_to=5):
    return (round_to * ((minute + round_to - 1) // round_to)) % 60


def localize_date(date, round_minute=True):
    if round_minute:
        date = date.replace(minute=round_to_minute(date.minute, round_to=5), second=0, microsecond=0)
    return date.astimezone(pendulum.timezone('Europe/Madrid'))


def parse_date(date_str):
    return localize_date(parser.parse(date_str))


# Schedule validations
SCHEDULE_CONFIG_KEYS = {
    'interval': (True, str, lambda x: x.strip(), lambda x: is_valid_cron(x), None),
    'start_date': (False, str, lambda x: parse_date(x), None, localize_date(datetime.now()))
}

# Tags validations
TAG_CONFIG_KEYS = {
    'tags': (False, str, lambda x: get_list_from_string_commas(x), lambda x: len(x) > 0, None)
}

# Origin validations
VALID_ORIGINS = [
    'ptr',
    'exadata'
]

ORIGINS_CONFIG_KEYS = {
    'origins': (True, str, lambda x: get_list_from_string_commas(x.upper()), None, None)
}

ORIGIN_CONFIG_KEYS = {
    'schemas': (True, str, lambda x: get_list_from_string_commas(x.upper()), lambda x: len(x) > 0, None)
}

SCHEMA_CONFIG_KEYS = {
    'tables': (True, str, lambda x: get_list_from_string_commas(x.upper()), lambda x: len(x) > 0, None)
}

# Oracle validations
VALID_STRATEGIES = [
    'default',
    'partition',
    'offset_rownum',
    'offset_denserank'
]

VALID_REPLICATION = [
    'FULL_TABLE',
    'INCREMENTAL'
]

# Extractor specific fields
METADATA_FIELDS = {'replication_method', 'replication_key', 'encrypt_columns', 'additional_filters',
                   'strategy', 'partitions', 'partition_columns', 'max_results'}

ORACLE_CONFIG_KEYS = {
    'query_threads': (False, int, None, lambda x: x > 0, 1),
    'fields': (False, str, lambda x: get_list_from_string_commas(x), None, ["*"]),
    'replication_method': (False, str, lambda x: x.strip().upper(), lambda x: x in VALID_REPLICATION, "FULL_TABLE"),
    'replication_key': (lambda x: x.get('replication_method', "") == "INCREMENTAL", str, lambda x: x.strip().upper(), None, None),
    'additional_filters': (False, str, lambda x: x.strip(), None, None),
    'encrypt_columns': (False, str, lambda x: get_list_from_string_commas(x), None, None),
    'strategy': (False, str, lambda x: x.strip().lower(), lambda x: x in VALID_STRATEGIES, "default"),
    'partitions': (lambda x: x.get('strategy', "") == "partition", int, None, lambda x: x > 0, None),
    'partition_column': (lambda x: x.get('strategy', "") == "partition", str, lambda x: x.strip(), None, None),
    'max_results': (lambda x: bool(re.match(r'offset_.*', x.get('strategy', ""))), int, None, lambda x: x > 0, None),
}

# Snowflake validations

VALID_STAGE_TYPES = [
    'gcs',
    'snowflake']

SNOWFLAKE_CONFIG_KEYS = {
    'batch_size_rows': (False, int, None, lambda x: x > 0, None),
    'parallelism': (False, int, None, lambda x: x > 0, None),
    'add_metadata_columns': (False, bool, None, None, None),
    'primary_key_required': (False, bool, None, None, None),
    'no_compression': (False, bool, None, None, None),
    'stage_type': (False, str, lambda x: x.strip().lower(), lambda x: x in VALID_STAGE_TYPES, None),
    'prefix': (False, str, lambda x: x.strip().upper(), None, None),
    'wait_to_load': (False, bool, None, None, None),
    'clean_stage': (False, bool, None, None, None)
}


def properties_file_to_dict(filepath):
    '''
    Reads a properties file and loads found properties into a dict
    A valid property is specified like:
        a = x
    Where a can have multiple levels of nested dicts
        a.a.a = x
        a.a.b = y

    :param filepath: path of input file
    :return: dict with properties read
    '''

    props = {}
    with open(filepath) as f:
        for line in f:
            line = line.rstrip()  # removes trailing whitespace and '\n' chars

            if line.startswith("#") or "=" not in line:
                continue

            k, v = (x.strip() for x in line.split("=", 1))
            k = k.split('.')

            level = props
            for i in k[:-1]:
                if i not in level:
                    level[i] = {}
                level = level[i]

            level[k[-1]] = parse_value(v)

    return props


def merge_dicts(*dict_args):
    """
    Given any number of dictionaries, shallow copy and merge into a new dict,
    precedence goes to key-value pairs in latter dictionaries.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def override_dict(old_config, new_config):
    '''
    Creates a new dict based on old_config which
    upserts (update,create) keys in old_config with new_config

    :param old_config: base dict
    :param new_config: dict to merge with
    :return:
    '''
    merged = copy.deepcopy(old_config)
    merged.update(new_config)
    return merged


def uniquify(seq):
    '''
    Removes duplicates of a list preserving the original order

    :param seq: a given list
    :return: uniquified list
    '''
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def fetch_from_config(key, config):
    '''
    Returns a tuple of selected key, and 'snowflake' key

    :param key: key to extract from dict
    :param config: dict to search
    :return: a tuple consisting of:
                selected key from config, empty dict if does not exist
                snowflake key from config, empty dict if does not exist
                tags key from config, empty dict if does not exist
    '''
    properties = config.get(key.lower(), {})
    snowflake = properties.get('snowflake', {})
    if 'snowflake' in properties:
        del properties['snowflake']

    return properties, snowflake


def parse_value(value):
    '''
    Parses a string value to data type

    :param value: value to parse
    :return: output parsed value
    '''
    if isinstance(value, str):
        # Parse int or bool
        if value.isdigit():
            return int(value)
        else:
            return True if value.lower() == "true" else False if value.lower() == "false" else value

def parse_values_from_dict(keylist, obj):
    '''
    Parses different key data types found in a Dict

    :param keylist: keylist to parse
    :param obj: input Dict
    :return: output Dict, with type changes if needed
    '''
    output = {}
    for key in keylist:
        if key in obj:
            output[key] = parse_value(obj[key])
    return output


def validate_and_check_config(errors, warnings, config, config_keys, config_name="", filter_keys=False):
    new_errors, new_warnings = validations.validate_config(config, config_keys, filter_keys=filter_keys)
    if len(new_warnings) > 0:
        warnings[config_name] = new_warnings
    if len(new_errors) > 0:
        errors[config_name] = new_errors


def log_errors(errors, warnings):
    # Log warnings
    for key in warnings:
        logger.warning(f'Warning on {key} configuration:\n   * %s', '\n   * '.join(warnings[key]))

    # Exit if config has errors
    for key in errors:
        logger.error(f'Invalid {key} configuration:\n   * %s', '\n   * '.join(errors[key]))
    if len(errors) > 0:
        return True
    return False


def validate_file(file_path, write=False):
    yaml = YAML()

    # Get from input parameter
    # properties_parser [file_name]
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} doesn't exists")
        sys.exit(1)
    file_name = os.path.basename(file_path).split('.')[0]
    logger.info(f"Successfully read file {file_path}")

    # Get properties dict from file
    properties = properties_file_to_dict(file_path)

    # Schedule, environment YAMLs base objects
    schedule = dict()
    schedule['schedules'] = []
    environment = dict()
    environment['environments'] = []

    # Base schedule dict for schedule YAML
    base_schedule = {'name': file_name, 'transform': 'skip'}

    # Base environment dict for schedule YAML
    base_environment = {'name': file_name, 'config': {'plugins': {}}}

    # Base extractor plugin
    base_extractor = {'name': "", 'config': {}, 'select': [], 'metadata': {}}

    # Base loader plugin
    base_loader = {'name': "target-snowflake", 'config': {}}

    base_environment['config']['plugins']['extractors'] = [base_extractor]
    base_environment['config']['plugins']['loaders'] = [base_loader]

    # Properties levels:
    #   Global -> Origin -> Schema -> Table

    errors, warnings = {}, {}
    validate_and_check_config(errors, warnings, properties, TAG_CONFIG_KEYS, "tags")
    validate_and_check_config(errors, warnings, properties, ORIGINS_CONFIG_KEYS, "origins")
    tags = properties.get('tags', [])
    origins = properties.get('origins', [])

    # Schedule must be defined in properties file
    if 'schedule' not in properties:
        errors['schedule'] = ['No schedule is defined']

    base_schedule = override_dict(base_schedule, properties.get('schedule'))
    validate_and_check_config(errors, warnings, base_schedule, SCHEDULE_CONFIG_KEYS, "schedule")

    global_snowflake = properties.get('snowflake', {})
    validate_and_check_config(errors, warnings, global_snowflake, SNOWFLAKE_CONFIG_KEYS, "snowflake", filter_keys=True)

    write_environments = {}
    write_schedules = {}
    task_ids = {}

    for origin in origins:

        if origin.lower() not in VALID_ORIGINS:
            errors['origins'] = [f"'{origin}' is not a valid origin, accepted origins are: {VALID_ORIGINS}"]
            continue

        origin_config, origin_snowflake = fetch_from_config(origin, properties)
        validate_and_check_config(errors, warnings, origin_config, ORIGIN_CONFIG_KEYS, origin)
        validate_and_check_config(errors, warnings, origin_snowflake, SNOWFLAKE_CONFIG_KEYS, f"{origin}_snowflake", filter_keys=True)
        tags += [origin]
        origin_snowflake = override_dict(global_snowflake, origin_snowflake)
        schemas = origin_config.get('schemas', [])

        for schema in schemas:
            schema_config, schema_snowflake = fetch_from_config(schema, origin_config)
            validate_and_check_config(errors, warnings, schema_config, SCHEMA_CONFIG_KEYS, f"{origin}_{schema}")
            validate_and_check_config(errors, warnings, schema_snowflake, SNOWFLAKE_CONFIG_KEYS, f"{origin}_{schema}_snowflake", filter_keys=True)
            tags += [schema]
            schema_snowflake = override_dict(origin_snowflake, schema_snowflake)
            tables = schema_config.get('tables', [])

            for table in tables:
                table_config, table_snowflake = fetch_from_config(table, schema_config)
                validate_and_check_config(errors, warnings, table_config, ORACLE_CONFIG_KEYS, f"{origin}_{schema}_{table}", filter_keys=True)
                validate_and_check_config(errors, warnings, table_snowflake, SNOWFLAKE_CONFIG_KEYS, f"{origin}_{schema}_snowflake", filter_keys=True)
                tags += [table]
                table_snowflake = override_dict(schema_snowflake, table_snowflake)

                # Identifiers for this task
                stream_name = f"{schema.upper()}-{table.upper()}"
                id_name = f"{origin.lower()}_{schema.lower()}_{table.lower()}"

                # Create environment for task
                task_environment = override_dict(base_environment, {'name': id_name})

                # Create loader for task
                task_loader = copy.deepcopy(base_loader)
                task_loader['config'] = table_snowflake

                # Create extractor for task
                task_extractor = {}
                task_extractor['name'] = f"tap-{origin.lower()}"
                task_extractor['config'] = {}
                task_extractor['config']['filter_schemas'] = schema.upper()
                task_extractor['select'] = []
                task_extractor['metadata'] = {}
                task_extractor['metadata'][stream_name] = {}

                for k in table_config:
                    # select fields
                    if k == 'fields':
                        fields = [f"{stream_name}.{field}" for field in table_config.get(k)]
                        for field in fields:
                            task_extractor['select'].append(field)
                    # metadata
                    # special encrypt_fields
                    elif k in METADATA_FIELDS:
                        new_key = k
                        if "replication_" in k:
                            new_key = k.replace('_', '-')
                        task_extractor['metadata'][stream_name][new_key] = table_config.get(k)
                    # config
                    else:
                        task_extractor['config'][k] = table_config.get(k)

                # Set extractor/loader in environment YAML
                task_environment['config']['plugins']['extractors'] = [override_dict(base_extractor, task_extractor)]
                task_environment['config']['plugins']['loaders'] = [task_loader]
                environment['environments'] = [task_environment]


                # Set schedule YAML for task
                task_schedule = {}
                task_schedule['name'] = id_name
                task_schedule['extractor'] = task_extractor['name']
                task_schedule['loader'] = task_loader['name']
                schedule['schedules'] = [override_dict(base_schedule, task_schedule)]

                path = f"pipelines/{file_name}/{id_name}"
                write_environments[path] = copy.deepcopy(environment)
                write_schedules[path] = copy.deepcopy(schedule)
                task_ids[path] = id_name

    if log_errors(errors, warnings):
        logger.info(f"Properties file {file_path} is KO")
        return

    logger.info(f"Properties file {file_path} is OK")

    if write:
        logger.info(f"Writing files for pipeline {file_name}...")
        # If pipeline already exists, delete first, then write
        pipeline_path = f"pipelines/{file_name}"
        if os.path.exists(pipeline_path):
            shutil.rmtree(pipeline_path)
        os.makedirs(pipeline_path)

        try:
            # Write schedule
            with open(f"{pipeline_path}/schedule_interval", 'w') as f:
                f.write(f"{base_schedule['interval']}")

            with open(f"{pipeline_path}/schedule_start", 'w') as f:
                f.write(f"{base_schedule['start_date'].isoformat()}")
            logger.info("Schedule files created OK")

            # Write tags
            tags = ",".join(uniquify(tags))
            with open(f"{pipeline_path}/tags", 'w') as f:
                f.write(f"{tags}")
            logger.info("Tag file created OK")

            # Write all environments
            # Create output path
            for path in task_ids:
                os.makedirs(path, exist_ok=True)

                # Write environment.yml
                with open(f"{path}/environment.yml", 'w') as f:
                    yaml.dump(write_environments[path], f)

                # Write schedule.yml
                with open(f"{path}/schedule.yml", 'w') as f:
                    yaml.dump(write_schedules[path], f)

                # Write meltano environment id as environment variable
                with open(f"{path}/env", 'w') as f:
                    f.write(f"export MELTANO_ENVIRONMENT={task_ids[path]}")

                logger.info(f"Task {task_ids[path]} created OK")
        except Exception as ex:
            logger.error(f"Error found when writing file: {ex}")
            logger.info(f"Files created will be deleted to avoid inconsistent state")
            if os.path.exists(pipeline_path):
                shutil.rmtree(pipeline_path)


def main():
    for file in sys.argv[1:]:
        validate_file(file, write=False)


if __name__ == "__main__":
    main()
