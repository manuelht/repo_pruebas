import re
import inspect
import jasypt
import exceptions


def raise_(ex):
    raise ex


CAST_EXPRESSION = {
    int: lambda x: int(x),
    str: lambda x: str(x),
    bool: lambda x: True if x.lower() == "true" else False if x.lower() == "false" else raise_(Exception(exceptions.CastException))
}


def decrypt_password(config, password_key="password", decrypt_key="decrypt_key"):
    # Decrypt password if encrypted
    errors, warnings = [], []
    if password_key in config:
        if config[password_key].startswith('ENC('):
            if decrypt_key not in config:
                errors.append(f"Provided a encrypted password but not a key to decipher it")
            else:
                try:
                    text = re.search(r'ENC\((.*?)\)', config[password_key]).group(1)
                    config[password_key] = jasypt.decrypt(text, config[decrypt_key].encode('utf-8'))
                except Exception as ex:
                    errors.append(f"Could not decrypt password, check password and decryption key to be valid")
    else:
        warnings.append(f"Key '{password_key}' was marked to be checked for decrypt, but it's not set")

    return errors


def _is_callable(x):
    return callable(x)


def _validate_keys_config(config, config_keys):
    """ Validate types, apply transformation/preprocess and check conditions if needed for config """
    errors, warnings = [], []
    for key in config_keys:
        required, data_type, transformation, validation, default = config_keys[key]

        # Required field
        if _is_callable(required):
            try:
                required = required(config)
            except Exception as ex:
                errors.append(f"Exception when determining if key '{key}' is a required field: {ex}")
                continue
        if required is True:
            if config.get(key, None) is None:
                errors.append(f"Required key '{key}' is missing from config")
            elif config.get(key, "") == "":
                errors.append(f"Required key '{key}' can't be empty")

        # If not required, it may not be in the config
        if key in config:
            # Check data type
            if not isinstance(config[key], data_type):
                error_string = f"Provided key '{key}' with type {type(config[key])}, it should be {data_type}"
                try:
                    config[key] = CAST_EXPRESSION[data_type](config[key])
                    warnings.append(error_string + ", value was casted successfully")
                except KeyError as ex:
                    errors.append(error_string + ", this value type cast is not defined")
                    continue
                except Exception as ex:
                    errors.append(error_string + ", value could not be casted")
                    continue

            # Apply transformation
            if _is_callable(transformation):
                try:
                    config[key] = transformation(config[key])
                except Exception as ex:
                    errors.append(f"Exception when preprocessing key '{key}': {ex}")
                    continue

            # Apply validation
            if _is_callable(validation):
                try:
                    if not validation(config[key]):
                        errors.append(f"Provided key '{key}' with value '{config[key]}' does not met validation condition")
                except Exception as ex:
                    errors.append(f"Exception when validating key '{key}': {ex}")
                    continue
        # Default it if necessary
        else:
            if _is_callable(default):
                try:
                    default = default(config)
                except Exception as ex:
                    errors.append(f"Exception when setting default value on key '{key}': {ex}")
                    continue
            if default is not None:
                config[key] = default

    return errors, warnings


def validate_config(config, config_keys={}, password_decrypt=False, password_fields=[], filter_keys=True):
    # Required keys
    config_errors, config_warnings = _validate_keys_config(config, config_keys)

    # Check if password needs decrypt
    if password_decrypt:
        for password in password_fields:
            decrypt_errors, decrypt_warnings = decrypt_password(config, password)
            config_errors = config_errors + decrypt_errors
            config_warnings = config_warnings + decrypt_warnings

    # Remove extra keys
    if filter_keys:
        delete_keys = []
        for key in config:
            if key not in config_keys:
                delete_keys.append(key)
        for key in delete_keys:
            del config[key]

    return config_errors, config_warnings
