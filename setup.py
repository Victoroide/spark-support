from setuptools import find_packages, setup

setup(
    name="mobile-tracking-etl",
    version="1.0.0",
    description="Modular PySpark ETL pipeline for mobile tracking data",
    author="FICCT Data Engineering Team",
    packages=find_packages(),
    install_requires=[
        "pyspark==3.5.0",
        "py4j==0.10.9.7",
    ],
    extras_require={
        "dev": [
            "pytest==7.4.3",
            "pytest-cov==4.1.0",
            "black==23.12.1",
            "isort==5.13.2",
            "pylint==3.0.3",
            "mypy==1.7.1",
        ],
    },
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "mobile-etl=src.main:main",
        ],
    },
)
