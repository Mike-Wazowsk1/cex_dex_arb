from distutils.core import setup
from Cython.Build import cythonize

setup(ext_modules = cythonize('binance_socket_c.pyx',language_level=3,extra_compile_args=["/Ox"] ))