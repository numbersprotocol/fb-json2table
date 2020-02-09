import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="fbjson2table", # Replace with your own username
    version="1.2.0",
    author="Numbers",
    author_email="hi@numbersprotocol.io",
    description="Parse Facebook archive JSON files to tables",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/numbersprotocol/fb-json2table",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
    install_requires=[
        "pandas>=0.24.1"
    ]
)
