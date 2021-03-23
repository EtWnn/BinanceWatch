from setuptools import setup

setup(
    name='BinanceWatch',
    version='0.1',
    packages=['BinanceWatch', 'tests'],
    url='https://github.com/EtWnn/BinanceWatch',
    author='EtWnn',
    author_email='',
    license='MIT',
    description='Local tracker of a binance account',
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
