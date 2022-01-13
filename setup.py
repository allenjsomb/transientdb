from setuptools import setup
from Cython.Build import cythonize

setup(
    name='transientdb',
    ext_modules=cythonize(
        [
            'src/transientdb.py', 
            'src/routes/*.py',
            'src/util/*.py'
        ],
        language_level="3"
    )
)
