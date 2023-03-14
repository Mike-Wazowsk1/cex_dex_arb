from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
from Cython.Distutils import build_ext

setup(name = 'binance_socket_c',ext_modules=[Extension("binance_socket_c",
                                                        sources=['binance_socket_c.pyx'],
                                                        cython_directives = {'language_level' : "3"}
                                                        )],cmdclass = {'build_ext': build_ext},)