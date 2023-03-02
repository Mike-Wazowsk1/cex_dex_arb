from collections import namedtuple

dbname = "cex_dex"
db_user= "cex_dex_admin"
db_pass = "ZXC123"
host = "localhost"

tmp = namedtuple("DB", ["dbname","user","password","host"])

DB = tmp(dbname,db_user,db_pass,host)