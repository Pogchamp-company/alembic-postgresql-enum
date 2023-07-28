from setuptools import setup, find_packages

setup(
    name='alembic-postgresql-enum',
    version='0.1.4',
    license='MIT',
    author="RustyGuard",
    packages=find_packages('src'),
    package_dir={'': 'alembic_postgresql_enum'},
    url='https://github.com/RustyGuard/alembic-postgresql-enum.git',
    keywords='alembic,postgresql,postgres,enum,autogenerate,alter,create',
    install_requires=[
        'alembic',
        'SQLAlchemy'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ]
)
