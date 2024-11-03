from setuptools import setup, find_packages

setup(
    name="stock_inventory",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "numpy",
        "python-dateutil",
        "pytz",
        "six",
        "typing_extensions",
        "tzdata",
        "psycopg2",
        "pandas",
        "sqlalchemy",
    ],
    entry_points={
        "console_scripts": [
            "my_project-cli=my_module.main:main",  # Change this to your entry function
        ]
    },
    author="Chinkun Yeh",
    description="Advance Bank of Taiwan Stock Inventory",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    #url="https://github.com/yourusername/my_project",  # Replace with your repo URL if applicable
    #classifiers=[
    #    "Programming Language :: Python :: 3",
    #],
)

