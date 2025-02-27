from setuptools import setup, find_packages

setup(
    name="audit_project",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "pandas>=1.5.0",
        "openpyxl>=3.0.0",
        "asyncio>=3.4.3",
        "customtkinter>=5.2.0",
        "Pillow>=10.0.0",
        "python-dotenv>=1.0.0",
        "pywin32>=305",
        "chardet>=5.0.0",
    ],
    python_requires=">=3.8",
)