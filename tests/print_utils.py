import os
import platform
from enum import Enum
from collections.abc import Iterable

if platform.system() == "Windows":
    os.system('color')  # enable colors in consoles


class ColorEnum(Enum):
    """
    Class to store different values to get colors in a console print
    """
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def color_string(s: str, color: ColorEnum):
    return color.value + s + ColorEnum.ENDC.value


def s_to_warning(message: str):
    return color_string(f"WARNING: {message}", ColorEnum.WARNING)


def s_to_fail(message: str):
    return color_string(message, ColorEnum.FAIL)


def s_to_pass(message: str):
    return color_string(message, ColorEnum.OKGREEN)


def get_blue_sep(n: int = 70):
    return color_string(n * '-', ColorEnum.OKBLUE)


def get_blue_title(tester_name: str, method_name: str, n: int = 10):
    message = f"{n * '#'} {tester_name}.{method_name} {n * '#'}"
    return color_string(message, ColorEnum.OKBLUE)


def string_status(instance):
    """
    transform all the attributes and the values of the instance of a class in a string
    :param instance:
    :return: None
    """
    if isinstance(instance, Iterable) and not isinstance(instance, str):
        s = "[\n" + "\n".join([string_status(e) for e in instance]) + "\n]"
    else:
        if '__class__' not in dir(instance):
            s = str(instance)
        else:
            attribute_names = [m for m in dir(instance) if not m.startswith('__') and not callable(getattr(instance, m))]
            if len(attribute_names):
                s = ""
                for attribute_name in attribute_names:
                    attribute = getattr(instance, attribute_name)
                    if isinstance(attribute, Enum):
                        attribute = attribute.name
                    s = s + f"{attribute_name} -> {attribute}\n"
            else:
                s = str(instance)
    return s


def print_status(instance):
    """
    print all the attributes and the values of the instance of a class
    :param instance:
    :return: None
    """
    print(string_status(instance))
