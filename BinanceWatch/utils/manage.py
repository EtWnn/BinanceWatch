import os
import shutil

from BinanceWatch.utils.paths import get_data_path


def clear_data():
    """
    Erase every files and folders created / saved in the data folder
    Keep the root folder

    :return: None
    :rtype: None
    """
    data_path = get_data_path()
    try:
        shutil.rmtree(data_path)
    except FileNotFoundError:
        pass

    try:  # recreate the data folder path
        os.makedirs(get_data_path())
    except FileExistsError:
        pass
