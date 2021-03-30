import os
from setuptools import setup

this_directory = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(this_directory, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

about = {}
with open(os.path.join(this_directory, 'BinanceWatch/__init__.py'), encoding='utf-8') as f:
    exec(f.read(), about)

setup(
    name='BinanceWatch',
    version=about['__version__'],
    packages=['BinanceWatch',
              'tests',
              'BinanceWatch.storage',
              'BinanceWatch.utils'],
    url='https://github.com/EtWnn/BinanceWatch',
    author='EtWnn',
    author_email='',
    license='MIT',
    description='Local tracker of a binance account',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    install_requires=['numpy', 'tqdm', 'dateparser', 'requests', 'python-binance>=0.7.9', 'appdirs'],
    keywords='binance exchange wallet save tracking history bitcoin ethereum btc eth',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
