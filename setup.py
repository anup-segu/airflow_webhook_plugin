import re
import os
from setuptools import setup, find_packages


def parse_requirements():
    """Rudimentary parser for the `requirements.txt` file
    We just want to separate regular packages from links to pass them to the
    `install_requires` and `dependency_links` params of the `setup()`
    functional properly.
    """
    try:
        requirements = [
            line.strip() for line in
            local_file('requirements.txt').splitlines()
        ]
    except IOError:
        raise RuntimeError("Couldn't find the `requirements.txt' file :(")

    links = []
    pkgs = []
    for req in requirements:
        if not req:
            continue
        if 'http:' in req or 'https:' in req:
            links.append(req)
            name, version = re.findall(r"\#egg=([^\-]+)-(.+$)", req)[0]
            pkgs.append('{0}=={1}'.format(name, version))
        else:
            pkgs.append(req)

    return pkgs, links


def local_file(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read()


install_requires, dependency_links = parse_requirements()

if __name__ == '__main__':
    description = "Plugin for apache-airflow to run tasks via webhooks"
    setup(
        name="airflow_webhook_plugin",
        version='0.0.1',
        description=description,
        long_description=local_file('README.md'),
        author='Yipit Coders',
        author_email='coders@yipitdata.com',
        packages=find_packages(exclude=['*tests*']),
        install_requires=install_requires,
        include_package_data=True,
        dependency_links=dependency_links,
        classifiers=[
            'Programming Language :: Python',
        ],
        zip_safe=False,
    )
