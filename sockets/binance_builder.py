from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension

setup(name = 'binance_socket_c',ext_modules=[Extension("binance_socket_c",
                                                        sources=['binance_socket_c.pyx'],
                                                        extra_compile_args=['/Ox'])])