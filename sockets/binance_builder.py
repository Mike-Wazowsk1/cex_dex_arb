from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
from Cython.Distutils import build_ext
ext = [Extension("binance_socket_ci",
                 sources=['binance_socket_ci.pyx'],
                 )]
for e in ext:
    e.cython_directives = {'language_level': "3"} #all are Python-3
setup(name='binance_socket_ci', ext_modules=ext, cmdclass={
      'build_ext': build_ext}, language_level=3)
