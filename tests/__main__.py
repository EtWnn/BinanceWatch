import argparse
import traceback
from types import ModuleType
from typing import List, Optional, Dict

import tests.print_utils as pu
import tests


def get_test_modules(module_names: Optional[List[str]] = None) -> List[ModuleType]:
    """
    from a list of module names, will return the list of module objects belonging to the tests package
    if the list is empty, will return all the modules with 'test_' in the name
    :param module_names: list of modules from the tests package to fetch
    :return:
    """
    if module_names is None:
        module_names = [m for m in dir(tests) if 'test_' in m]
    test_modules = []
    for module_name in module_names:
        try:
            test_modules.append(getattr(tests, module_name))
        except AttributeError:
            message = f"The test module {module_name} was skipped because the file was not found in the tests package"
            print(pu.s_to_warning(message))
    return test_modules


def run_test_module(module: ModuleType, common_kwargs=None, verbose=1) -> Dict:
    """
    will run all the functions from a module whose name has 'test_' in it and return an execution report
    :param module: module that host the functions
    :param common_kwargs: kwargs to provide to all functions
    :param verbose: the higher the more information are printed
    :return: dictionary with execution report ex: {'success': 10, 'failure': 1, 'total': 11}
    """
    if common_kwargs is None:
        common_kwargs = {}
    n_success = 0
    n_failure = 0
    tests_names = [f_name for f_name in dir(module) if 'test_' in f_name and callable(getattr(module, f_name))]
    for test_name in tests_names:
        if verbose:
            print(pu.get_blue_title(module.__name__, test_name))
        try:
            getattr(module, test_name)(**common_kwargs)
            n_success += 1
        except Exception:  # general catch to display the error reports without stopping the tests
            n_failure += 1
            if verbose:
                print(pu.s_to_fail(traceback.format_exc()))
            pass
        if verbose:
            print(pu.get_blue_sep())
    return {'success': n_success, 'failure': n_failure, 'total': len(tests_names)}


def print_report(report: Dict):
    """
    print nicely a report of test execution
    :param report:
    """
    print("\n\nSummary:\n")
    total_success = 0
    total_tests = 0
    for module_name, dico in report.items():
        if dico['total']:
            total_success += dico['success']
            total_tests += dico['total']
            rate = dico['success'] / dico['total']
            if rate == 1:
                s_result = pu.s_to_pass(f"{dico['success'] / dico['total']:.0%}")
            else:
                s_result = pu.s_to_fail(f"{dico['success'] / dico['total']:.0%}")
            print(f"\t- {module_name}: {dico['success']}/{dico['total']} ({s_result}) tests were passed")
        else:
            print(f"\t- {module_name}: no tests were executed")


def run(tests_names=None, common_kwargs=None):
    """
    main function, launch the tests function from a list of test modules names
    :param tests_names: testers to execute
    :param common_kwargs: kwargs for all modules' functions
    :return:
    """
    if common_kwargs is None:
        common_kwargs = {}
    test_modules = get_test_modules(tests_names)

    report = {}
    for test_module in test_modules:
        report[test_module.__name__] = run_test_module(test_module, common_kwargs)

    if len(report):
        print_report(report)
    else:
        print("No report to print")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--test_modules', metavar="<test_modules>", required=False,
                        help='names of the test modules to be executed', type=str, nargs='+')
    parser.add_argument('-v', '--verbose', metavar="<verbose>", required=False,
                        help='verbose: the higher the more information are printed', type=int)

    args = parser.parse_args()

    testers_args = {'verbose': 0}
    if args.verbose:
        testers_args['verbose'] = args.verbose

    run(args.test_modules, testers_args)
