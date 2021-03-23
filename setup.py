from setuptools import setup
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='BinanceWatch',
    version='0.1.0',
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
    long_description_content_type='text/markdown',
    install_requires=['numpy', 'tqdm', 'dateparser', 'requests', 'python-binance', 'appdirs'],
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
