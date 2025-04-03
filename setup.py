from setuptools import setup, find_packages

setup(
    name="audit_project",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "pandas>=1.5.0",
        "openpyxl>=3.0.0",
        "PyQt6>=6.5.0",
        # Remove PyQt6-tools as it's not widely available
        "Pillow>=10.0.0",
        "python-dotenv>=1.0.0",
        "pywin32>=305",
        "chardet>=5.0.0",
        # Add other specific library requirements
        "polars>=0.19.0",
        "pyarrow>=14.0.0",
        "dask>=2024.1.0",
        "numpy>=1.22.0",
        "scipy>=1.10.0",
        "scikit-learn>=1.2.0",
        "loguru>=0.7.0",
        "rich>=10.0.0",
        "tqdm>=4.65.0",
    ],
    python_requires=">=3.8",
)