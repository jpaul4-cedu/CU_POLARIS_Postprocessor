# setup.py

from setuptools import setup, find_packages

setup(
    name='CU_POLARIS_Postprocessor',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pandas',
        

        # Add other dependencies here
    ],
    description='A package for conducting high speed parallel processing on POLARIS output data',
    author='Joe Paul',
    author_email='jpaul4@clemson.edu',
    url='https://github.com/yourusername/my_package',
)
