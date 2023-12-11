
import pkg_resources


def pip_get_package_path(package_name):
    try:
        package = pkg_resources.get_distribution(package_name)
        return package.location
    except pkg_resources.DistributionNotFound:
        print(f"Package '{package_name}' is not installed.")
        return None
