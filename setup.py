from setuptools import setup, find_packages

setup(
    name='alembic-postgresql-enum',
    version='0.1',
    license='MIT',
    author="RustyGuard",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/RustyGuard/alembic-postgresql-enum.git',
    keywords='alembic,postgresql,postgres,enum,autogenerate,alter,create',
    install_requires=[
        'alembic',
        'SQLAlchemy'
    ],
)
